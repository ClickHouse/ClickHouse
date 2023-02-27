#include <exception>
#include <optional>
#include <string_view>

#include <type_traits>
#include <unordered_map>
#include <base/defines.h>

#include <Poco/Logger.h>
#include <Poco/RegularExpression.h>

#include "Common/Exception.h"
#include <Common/ArenaUtils.h>
#include <Common/logger_useful.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/Regexps.h>
#include <Functions/checkHyperscanRegexp.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/RegExpTreeDictionary.h>
#include <Dictionaries/YAMLRegExpTreeDictionarySource.h>

#include <re2_st/stringpiece.h>

#include "config.h"

#if USE_VECTORSCAN
#    include <hs.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
    extern const int UNSUPPORTED_METHOD;
    extern const int INCORRECT_DICTIONARY_DEFINITION;
}

const std::string kRegExp = "regexp";
const std::string kId = "id";
const std::string kParentId = "parent_id";
const std::string kKeys = "keys";
const std::string kValues = "values";

namespace
{
    /// StringPiece represents a back-reference or a string lateral
    struct StringPiece
    {
        int ref_num = -1;
        String literal;

        explicit StringPiece(const String & literal_) : literal(literal_) {}
        explicit StringPiece(int ref_) : ref_num(ref_) {}
    };

    Field parseStringToField(const String & raw, DataTypePtr data_type)
    try
    {
        ReadBufferFromString buffer(raw);
        auto col = data_type->createColumn();
        auto serialization = data_type->getSerialization(ISerialization::Kind::DEFAULT);
        serialization->deserializeWholeText(*col, buffer, FormatSettings{});
        return (*col)[0];
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                        "Cannot parse {} for data type {}, Reason is: {}",
                        raw, data_type->getName(), getCurrentExceptionMessage(false));
    }
}

struct RegExpTreeDictionary::RegexTreeNode
{
    std::vector<UInt64> children;
    UInt64      id;
    UInt64      parent_id;
    std::string regex;
    re2_st::RE2 searcher;

    RegexTreeNode(UInt64 id_, UInt64 parent_id_, const String & regex_, const re2_st::RE2::Options & regexp_options):
        id(id_), parent_id(parent_id_), regex(regex_), searcher(regex_, regexp_options) {}

    bool match(const char * haystack, size_t size) const
    {
        return searcher.Match(haystack, 0, size, re2_st::RE2::Anchor::UNANCHORED, nullptr, 0);
    }

    struct AttributeValue
    {
        Field field;
        std::vector<StringPiece> pieces;

        constexpr bool containsBackRefs() const { return !pieces.empty(); }
    };

    std::unordered_map<String, AttributeValue> attributes;
};

std::vector<StringPiece> createStringPieces(const String & value, int num_captures, const String & regex, Poco::Logger * logger)
{
    std::vector<StringPiece> result;
    String literal;
    for (size_t i = 0; i < value.size(); ++i)
    {
        if ((value[i] == '\\' || value[i] == '$') && i + 1 < value.size())
        {
            if (isNumericASCII(value[i+1]))
            {
                if (!literal.empty())
                {
                    result.push_back(StringPiece(literal));
                    literal = "";
                }
                int ref_num = value[i+1]-'0';
                if (ref_num >= num_captures)
                    LOG_TRACE(logger,
                        "Reference Id {} in set string is invalid, the regexp {} only has {} capturing groups",
                        ref_num, regex, num_captures-1);
                result.push_back(StringPiece(ref_num));
                ++i;
                continue;
            }
        }
        literal += value[i];
    }
    if (result.empty())
        return result;
    if (!literal.empty())
        result.push_back(StringPiece(literal));
    return result;
}

void RegExpTreeDictionary::calculateBytesAllocated()
{
    for (const String & regex : simple_regexps)
        bytes_allocated += regex.size();
    bytes_allocated += sizeof(UInt64) * regexp_ids.size();
    bytes_allocated += (sizeof(RegexTreeNode) + sizeof(UInt64)) * regex_nodes.size();
    bytes_allocated += 2 * sizeof(UInt64) * topology_order.size();
}

void RegExpTreeDictionary::initRegexNodes(Block & block)
{
    auto id_column = block.getByName(kId).column;
    auto pid_column = block.getByName(kParentId).column;
    auto regex_column = block.getByName(kRegExp).column;
    auto keys_column = block.getByName(kKeys).column;
    auto values_column = block.getByName(kValues).column;

#ifdef USE_VECTORSCAN
    SlowWithHyperscanChecker checker;
#endif

    size_t size = block.rows();
    for (size_t i = 0; i < size; i++)
    {
        UInt64 id = id_column->getUInt(i);
        UInt64 parent_id = pid_column->getUInt(i);
        String regex = (*regex_column)[i].safeGet<String>();

        if (regex_nodes.contains(id))
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "There are duplicate id {}", id);

        if (id == 0)
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "There are invalid id {}", id);


        re2_st::RE2::Options regexp_options;
        regexp_options.set_log_errors(false);
        RegexTreeNodePtr node = std::make_shared<RegexTreeNode>(id, parent_id, regex, regexp_options);

        int num_captures = std::min(node->searcher.NumberOfCapturingGroups() + 1, 10);

        Array keys = (*keys_column)[i].safeGet<Array>();
        Array values = (*values_column)[i].safeGet<Array>();
        size_t keys_size = keys.size();
        for (size_t i = 0; i < keys_size; i++)
        {
            const String & name = keys[i].safeGet<String>();
            const String & value = values[i].safeGet<String>();
            if (structure.hasAttribute(name))
            {
                const auto & attr = structure.getAttribute(name);
                auto string_pieces = createStringPieces(value, num_captures, regex, logger);
                if (!string_pieces.empty())
                {
                    node->attributes[name] = RegexTreeNode::AttributeValue{.field = values[i], .pieces = std::move(string_pieces)};
                }
                else
                {
                    Field field = parseStringToField(values[i].safeGet<String>(), attr.type);
                    node->attributes[name] = RegexTreeNode::AttributeValue{.field = std::move(field)};
                }
            }
        }
        regex_nodes.emplace(id, node);
#if USE_VECTORSCAN
        if (use_vectorscan && !checker.isSlow(regex))
        {
            simple_regexps.push_back(regex);
            regexp_ids.push_back(id);
        }
        else
#endif
            complex_regexp_nodes.push_back(node);
    }
}

void RegExpTreeDictionary::initGraph()
{
    for (const auto & [id, value]: regex_nodes)
    {
        UInt64 pid = value->parent_id;
        if (pid == 0) // this is root
            continue;
        if (regex_nodes.contains(pid))
            regex_nodes[pid]->children.push_back(id);
        else
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Unknown parent id {}", pid);
    }
    std::set<UInt64> visited;
    UInt64 topology_id = 0;
    for (const auto & [id, value]: regex_nodes)
        if (value->parent_id == 0) // this is root node.
            initTopologyOrder(id, visited, topology_id);
    if (topology_order.size() != regex_nodes.size())
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Invalid Regex tree");
}

void RegExpTreeDictionary::initTopologyOrder(UInt64 node_idx, std::set<UInt64> & visited, UInt64 & topology_id)
{
    visited.insert(node_idx);
    for (UInt64 child_idx : regex_nodes[node_idx]->children)
        if (visited.contains(child_idx))
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Invalid Regex tree. The input tree is cyclical");
        else
            initTopologyOrder(child_idx, visited, topology_id);
    topology_order[node_idx] = topology_id++;
}

void RegExpTreeDictionary::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        QueryPipeline pipeline(source_ptr->loadAll());
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            initRegexNodes(block);
        }
        initGraph();
        if (simple_regexps.empty() && complex_regexp_nodes.empty())
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "There are no available regular expression. Please check your config");
        LOG_INFO(logger, "There are {} simple regexps and {} complex regexps", simple_regexps.size(), complex_regexp_nodes.size());
        /// If all the regexps cannot work with hyperscan, we should set this flag off to avoid exceptions.
        if (simple_regexps.empty())
            use_vectorscan = false;
        if (!use_vectorscan)
            return;
        #if USE_VECTORSCAN
        try
        {
            std::vector<std::string_view> regexps_views(simple_regexps.begin(), simple_regexps.end());
            hyperscan_regex = MultiRegexps::getOrSet<true, false>(regexps_views, std::nullopt);
            hyperscan_regex->get();
        }
        catch (Exception & e)
        {
            /// Some compile errors will be thrown as LOGICAL ERROR and cause crash, e.g. empty expression or expressions are too large.
            /// We catch the error here and rethrow again.
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Error occurs when compiling regular expressions, reason: {}", e.message());
        }
        #endif
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Dictionary {} does not support updating manual fields", name);
    }
}

RegExpTreeDictionary::RegExpTreeDictionary(
    const StorageID & id_,
    const DictionaryStructure & structure_,
    DictionarySourcePtr source_ptr_,
    Configuration configuration_,
    bool use_vectorscan_)
    : IDictionary(id_),
      structure(structure_),
      source_ptr(source_ptr_),
      configuration(configuration_),
      use_vectorscan(use_vectorscan_),
      logger(&Poco::Logger::get("RegExpTreeDictionary"))
{
    if (auto * ch_source = typeid_cast<ClickHouseDictionarySource *>(source_ptr.get()))
    {
        Block sample_block;
        /// id, parent_id, regex, keys, values
        sample_block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), kId));
        sample_block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), kParentId));
        sample_block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), kRegExp));
        sample_block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), kKeys));
        sample_block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), kValues));
        ch_source->sample_block = std::move(sample_block);
    }

    loadData();
    calculateBytesAllocated();
}

std::pair<String, bool> processBackRefs(const String & data, const re2_st::RE2 & searcher, const std::vector<StringPiece> & pieces)
{
    re2_st::StringPiece haystack(data.data(), data.size());
    re2_st::StringPiece matches[10];
    String result;
    searcher.Match(haystack, 0, data.size(), re2_st::RE2::Anchor::UNANCHORED, matches, 10);
    /// if the pattern is a single '$1' but fails to match, we would use the default value.
    if (pieces.size() == 1 && pieces[0].ref_num >= 0 && pieces[0].ref_num < 10 && matches[pieces[0].ref_num].empty())
        return std::make_pair(result, true);
    for (const auto & item : pieces)
    {
        if (item.ref_num >= 0 && item.ref_num < 10)
            result += matches[item.ref_num].ToString();
        else
            result += item.literal;
    }
    return {result, false};
}

// walk towards root and collect attributes.
// The return value means whether we finish collecting.
bool RegExpTreeDictionary::setAttributes(
    UInt64 id,
    std::unordered_map<String, Field> & attributes_to_set,
    const String & data,
    std::unordered_set<UInt64> & visited_nodes,
    const std::unordered_map<String, const DictionaryAttribute &> & attributes,
    const std::unordered_map<String, ColumnPtr> & defaults,
    size_t key_index) const
{

    if (visited_nodes.contains(id))
        return attributes_to_set.size() == attributes.size();
    visited_nodes.emplace(id);
    const auto & node_attributes = regex_nodes.at(id)->attributes;
    for (const auto & [name, value] : node_attributes)
    {
        if (!attributes.contains(name) || attributes_to_set.contains(name))
            continue;
        if (value.containsBackRefs())
        {
            auto [updated_str, use_default] = processBackRefs(data, regex_nodes.at(id)->searcher, value.pieces);
            if (use_default)
            {
                DefaultValueProvider default_value(attributes.at(name).null_value, defaults.at(name));
                attributes_to_set[name] = default_value.getDefaultValue(key_index);
            }
            else
                attributes_to_set[name] = parseStringToField(updated_str, attributes.at(name).type);
        }
        else
            attributes_to_set[name] = value.field;
    }

    auto parent_id = regex_nodes.at(id)->parent_id;
    if (parent_id > 0)
        setAttributes(parent_id, attributes_to_set, data, visited_nodes, attributes, defaults, key_index);

    /// if all the attributes have set, the walking through can be stopped.
    return attributes_to_set.size() == attributes.size();
}

namespace
{
    struct MatchContext
    {
        std::set<UInt64> matched_idx_set;
        std::vector<std::pair<UInt64, UInt64>> matched_idx_sorted_list;

        const std::vector<UInt64> & regexp_ids ;
        const std::unordered_map<UInt64, UInt64> & topology_order;

        MatchContext(const std::vector<UInt64> & regexp_ids_, const std::unordered_map<UInt64, UInt64> & topology_order_)
            : regexp_ids(regexp_ids_), topology_order(topology_order_) {}

        [[maybe_unused]]
        void insertIdx(unsigned int idx)
        {
            UInt64 node_id = regexp_ids[idx-1];
            UInt64 topological_order = topology_order.at(node_id);
            matched_idx_set.emplace(node_id);
            matched_idx_sorted_list.push_back(std::make_pair(topological_order, node_id));
        }

        void insertNodeID(UInt64 id)
        {
            UInt64 topological_order = topology_order.at(id);
            matched_idx_set.emplace(id);
            matched_idx_sorted_list.push_back(std::make_pair(topological_order, id));
        }

        /// Sort by topological order, which indicates the matching priorities.
        void sort()
        {
            std::sort(matched_idx_sorted_list.begin(), matched_idx_sorted_list.end());
        }

        bool contains(UInt64 idx) const
        {
            return matched_idx_set.contains(idx);
        }
    };
}

std::unordered_map<String, ColumnPtr> RegExpTreeDictionary::match(
    const ColumnString::Chars & keys_data,
    const ColumnString::Offsets & keys_offsets,
    const std::unordered_map<String, const DictionaryAttribute &> & attributes,
    const std::unordered_map<String, ColumnPtr> & defaults) const
{

#if USE_VECTORSCAN
    hs_scratch_t * scratch = nullptr;
    if (use_vectorscan)
    {
        hs_error_t err = hs_clone_scratch(hyperscan_regex->get()->getScratch(), &scratch);

        if (err != HS_SUCCESS)
        {
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for hyperscan");
        }
    }

    MultiRegexps::ScratchPtr smart_scratch(scratch);
#endif

    std::unordered_map<String, MutableColumnPtr> columns;

    /// initialize columns
    for (const auto & [name, attr] : attributes)
    {
        auto col_ptr = attr.type->createColumn();
        col_ptr->reserve(keys_offsets.size());
        columns[name] = std::move(col_ptr);
    }

    UInt64 offset = 0;
    for (size_t key_idx = 0; key_idx < keys_offsets.size(); ++key_idx)
    {
        auto key_offset = keys_offsets[key_idx];
        UInt64 length = key_offset - offset - 1;

        MatchContext match_result(regexp_ids, topology_order);

#if USE_VECTORSCAN
        if (use_vectorscan)
        {
            auto on_match = [](unsigned int id,
                            unsigned long long /* from */, // NOLINT
                            unsigned long long /* to */, // NOLINT
                            unsigned int /* flags */,
                            void * context) -> int
            {
                static_cast<MatchContext *>(context)->insertIdx(id);
                return 0;
            };
            hs_error_t err = hs_scan(
                hyperscan_regex->get()->getDB(),
                reinterpret_cast<const char *>(keys_data.data()) + offset,
                static_cast<unsigned>(length),
                0,
                smart_scratch.get(),
                on_match,
                &match_result);

            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan data with vectorscan");
        }
#endif

        for (const auto & node_ptr : complex_regexp_nodes)
        {
            if (node_ptr->match(reinterpret_cast<const char *>(keys_data.data()) + offset, length))
            {
                match_result.insertNodeID(node_ptr->id);
            }
        }

        match_result.sort();
        /// Walk through the regex tree util all attributes are set;
        std::unordered_map<String, Field> attributes_to_set;
        std::unordered_set<UInt64> visited_nodes;

        /// Some node matches but its parents cannot match. In this case we must regard this node unmatched.
        auto is_invalid = [&](UInt64 id)
        {
            while (id)
            {
                if (!match_result.contains(id))
                    return false;
                id = regex_nodes.at(id)->parent_id;
            }
            return true;
        };

        String str = String(reinterpret_cast<const char *>(keys_data.data()) + offset, length);

        for (auto item : match_result.matched_idx_sorted_list)
        {
            UInt64 id = item.second;
            if (!is_invalid(id))
                continue;
            if (visited_nodes.contains(id))
                continue;
            if (setAttributes(id, attributes_to_set, str, visited_nodes, attributes, defaults, key_idx))
                break;
        }

        for (const auto & [name, attr] : attributes)
        {
            if (attributes_to_set.contains(name))
                continue;

            DefaultValueProvider default_value(attr.null_value, defaults.at(name));
            columns[name]->insert(default_value.getDefaultValue(key_idx));
        }

        /// insert to columns
        for (const auto & [name, value] : attributes_to_set)
            columns[name]->insert(value);

        offset = key_offset;
    }

    std::unordered_map<String, ColumnPtr> result;
    for (auto & [name, mutable_ptr] : columns)
        result.emplace(name, std::move(mutable_ptr));

    return result;
}

Columns RegExpTreeDictionary::getColumns(
    const Strings & attribute_names,
    const DataTypes & result_types,
    const Columns & key_columns,
    const DataTypes & key_types,
    const Columns & default_values_columns) const
{
    /// valid check
    if (key_columns.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expect 1 key for DictGet, but got {} arguments", key_columns.size());
    }
    structure.validateKeyTypes(key_types);

    std::unordered_map<String, const DictionaryAttribute &> attributes;
    std::unordered_map<String, ColumnPtr> defaults;

    for (size_t i = 0; i < attribute_names.size(); i++)
    {
        const auto & attribute = structure.getAttribute(attribute_names[i], result_types[i]);
        attributes.emplace(attribute.name, attribute);
        defaults[attribute.name] = default_values_columns[i];
    }

    /// calculate matches
    const ColumnString * key_column = typeid_cast<const ColumnString *>(key_columns[0].get());
    const auto & columns_map = match(
        key_column->getChars(),
        key_column->getOffsets(),
        attributes,
        defaults);

    Columns result;
    for (const String & name : attribute_names)
        result.push_back(columns_map.at(name));

    return result;
}

void registerDictionaryRegExpTree(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string &,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             bool) -> DictionaryPtr
    {

        if (!dict_struct.key.has_value() || dict_struct.key.value().size() != 1 || (*dict_struct.key)[0].type->getName() != "String")
        {
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                            "dictionary regexp_tree should have one primary key with string value "
                            "to represent regular expressions");
        }

        String dictionary_layout_prefix = config_prefix + ".layout" + ".regexp_tree";
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        RegExpTreeDictionary::Configuration configuration{
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false), .lifetime = dict_lifetime};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
        if (!context->getSettings().regexp_dict_allow_other_sources && typeid_cast<YAMLRegExpTreeDictionarySource *>(source_ptr.get()) == nullptr)
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                            "regexp_tree dictionary doesn't accept sources other than yaml source. "
                            "To active it, please set regexp_dict_allow_other_sources=true");

        return std::make_unique<RegExpTreeDictionary>(dict_id, dict_struct, std::move(source_ptr), configuration, context->getSettings().regexp_dict_allow_hyperscan);
    };

    factory.registerLayout("regexp_tree", create_layout, true);
}

}
