#include <exception>
#include <optional>
#include <string_view>

#include <type_traits>
#include <unordered_map>
#include <base/defines.h>

#include <Poco/Logger.h>
#include <Poco/RegularExpression.h>

#include <Common/ArenaUtils.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/OptimizedRegularExpression.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/Regexps.h>
#include <Functions/checkHyperscanRegexp.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Sources/BlocksListSource.h>

#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/RegExpTreeDictionary.h>
#include <Dictionaries/YAMLRegExpTreeDictionarySource.h>

#include "config.h"

#if USE_VECTORSCAN
#    include <hs.h>
#    include <hs_compile.h>
#endif

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
    extern const SettingsBool regexp_dict_allow_hyperscan;
    extern const SettingsBool regexp_dict_flag_case_insensitive;
    extern const SettingsBool regexp_dict_flag_dotall;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
    extern const int UNSUPPORTED_METHOD;
    extern const int INCORRECT_DICTIONARY_DEFINITION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
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

    Field parseStringToField(const String & raw, const DataTypePtr data_type)
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

struct ExternalRegexpQueryBuilder final : public ExternalQueryBuilder
{
    explicit ExternalRegexpQueryBuilder(const ExternalQueryBuilder & builder) : ExternalQueryBuilder(builder) {}

    void composeLoadAllQuery(WriteBuffer & out) const override
    {
        writeString("SELECT id, parent_id, regexp, keys, values FROM ", out);
        if (!db.empty())
        {
            writeQuoted(db, out);
            writeChar('.', out);
        }
        if (!schema.empty())
        {
            writeQuoted(schema, out);
            writeChar('.', out);
        }
        writeQuoted(table, out);
        if (!where.empty())
        {
            writeString(" WHERE ", out);
            writeString(where, out);
        }
    }
};

struct RegExpTreeDictionary::RegexTreeNode
{
    std::vector<UInt64> children;
    UInt64      id;
    UInt64      parent_id;
    std::string regex;
    re2::RE2 searcher;

    RegexTreeNode(UInt64 id_, UInt64 parent_id_, const String & regex_, const re2::RE2::Options & regexp_options):
        id(id_), parent_id(parent_id_), regex(regex_), searcher(regex_, regexp_options) {}

    bool match(const char * haystack, size_t size) const
    {
        return searcher.Match(haystack, 0, size, re2::RE2::Anchor::UNANCHORED, nullptr, 0);
    }

    struct AttributeValue
    {
        Field field;
        std::vector<StringPiece> pieces;
        String original_value;

        constexpr bool containsBackRefs() const { return !pieces.empty(); }
    };

    std::unordered_map<String, AttributeValue> attributes;
};

std::vector<StringPiece> createStringPieces(const String & value, int num_captures, const String & regex, LoggerPtr logger)
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


        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false);
        regexp_options.set_case_sensitive(!flag_case_insensitive);
        regexp_options.set_dot_nl(flag_dotall);
        RegexTreeNodePtr node = std::make_shared<RegexTreeNode>(id, parent_id, regex, regexp_options);

        int num_captures = std::min(node->searcher.NumberOfCapturingGroups() + 1, 10);

        Array keys = (*keys_column)[i].safeGet<Array>();
        Array values = (*values_column)[i].safeGet<Array>();
        size_t keys_size = keys.size();
        for (size_t j = 0; j < keys_size; j++)
        {
            const String & name_ = keys[j].safeGet<String>();
            const String & value = values[j].safeGet<String>();
            if (structure.hasAttribute(name_))
            {
                const auto & attr = structure.getAttribute(name_);
                auto string_pieces = createStringPieces(value, num_captures, regex, logger);
                if (!string_pieces.empty())
                {
                    node->attributes[name_] = RegexTreeNode::AttributeValue{.field = values[j], .pieces = std::move(string_pieces), .original_value = value};
                }
                else
                {
                    Field field = parseStringToField(value, attr.type);
                    node->attributes[name_] = RegexTreeNode::AttributeValue{.field = std::move(field), .pieces = {}, .original_value = value};
                }
            }
        }
        regex_nodes.emplace(id, node);

#if USE_VECTORSCAN
        String required_substring;
        bool is_trivial, required_substring_is_prefix;
        std::vector<std::string> alternatives;

        if (use_vectorscan)
            OptimizedRegularExpression::analyze(regex, required_substring, is_trivial, required_substring_is_prefix, alternatives);

        for (auto & alter : alternatives)
        {
            if (alter.size() < 3)
            {
                alternatives.clear();
                break;
            }
        }
        if (!required_substring.empty())
        {
            simple_regexps.push_back(required_substring);
            regexp_ids.push_back(id);
        }
        else if (!alternatives.empty())
        {
            for (auto & alternative : alternatives)
            {
                simple_regexps.push_back(alternative);
                regexp_ids.push_back(id);
            }
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
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Unknown parent id {} in regexp tree dictionary", pid);
    }
    std::set<UInt64> visited;
    UInt64 topology_id = 0;
    for (const auto & [id, value]: regex_nodes)
        if (value->parent_id == 0) // this is root node.
            initTopologyOrder(id, visited, topology_id);
    /// If there is a cycle and all nodes have a parent, this condition will be met.
    if (topology_order.size() != regex_nodes.size())
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "The regexp tree is cyclical. Please check your config.");
}

void RegExpTreeDictionary::initTopologyOrder(UInt64 node_idx, std::set<UInt64> & visited, UInt64 & topology_id)
{
    visited.insert(node_idx);
    for (UInt64 child_idx : regex_nodes[node_idx]->children)
        /// there is a cycle when dfs the graph.
        if (visited.contains(child_idx))
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "The regexp tree is cyclical. Please check your config.");
        else
            initTopologyOrder(child_idx, visited, topology_id);
    topology_order[node_idx] = topology_id++;
}

void RegExpTreeDictionary::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        QueryPipeline pipeline(source_ptr->loadAll());
        DictionaryPipelineExecutor executor(pipeline, configuration.use_async_executor);

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
        std::vector<const char *> patterns;
        std::vector<unsigned int> flags;
        std::vector<size_t> lengths;

        // Notes:
        // - Always set HS_FLAG_SINGLEMATCH because we only care about whether a pattern matches at least once
        // - HS_FLAG_CASELESS is supported by hs_compile_lit_multi, so we should set it if flag_case_insensitive is set.
        // - HS_FLAG_DOTALL is not supported by hs_compile_lit_multi, but the '.' wildcard can't appear in any of the simple regexps
        //   anyway, so even if flag_dotall is set, we only need to configure the RE2 searcher, and don't need to set any Hyperscan flags.
        unsigned int flag_bits = HS_FLAG_SINGLEMATCH;
        if (flag_case_insensitive)
            flag_bits |= HS_FLAG_CASELESS;

        for (const std::string & simple_regexp : simple_regexps)
        {
            patterns.push_back(simple_regexp.data());
            lengths.push_back(simple_regexp.size());
            flags.push_back(flag_bits);
        }

        hs_database_t * db = nullptr;
        hs_compile_error_t * compile_error;

        std::unique_ptr<unsigned int[]> ids;
        ids.reset(new unsigned int[patterns.size()]);
        for (size_t i = 0; i < patterns.size(); i++)
            ids[i] = static_cast<unsigned>(i+1);

        hs_error_t err = hs_compile_lit_multi(patterns.data(), flags.data(), ids.get(), lengths.data(), static_cast<unsigned>(patterns.size()), HS_MODE_BLOCK, nullptr, &db, &compile_error);
        origin_db.reset(db);
        if (err != HS_SUCCESS)
        {
            /// CompilerError is a unique_ptr, so correct memory free after the exception is thrown.
            MultiRegexps::CompilerErrorPtr error(compile_error);

            if (error->expression < 0)
                throw Exception::createRuntime(ErrorCodes::LOGICAL_ERROR, String(error->message));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pattern '{}' failed with error '{}'", patterns[error->expression], String(error->message));
        }

        /// We allocate the scratch space only once, then copy it across multiple threads with hs_clone_scratch
        /// function which is faster than allocating scratch space each time in each thread.
        hs_scratch_t * scratch = nullptr;
        err = hs_alloc_scratch(db, &scratch);
        origin_scratch.reset(scratch);
        /// If not HS_SUCCESS, it is guaranteed that the memory would not be allocated for scratch.
        if (err != HS_SUCCESS)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not allocate scratch space for vectorscan");
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
    bool use_vectorscan_,
    bool flag_case_insensitive_,
    bool flag_dotall_)
    : IDictionary(id_),
      structure(structure_),
      source_ptr(source_ptr_),
      configuration(configuration_),
      use_vectorscan(use_vectorscan_),
      flag_case_insensitive(flag_case_insensitive_),
      flag_dotall(flag_dotall_),
      logger(getLogger("RegExpTreeDictionary"))
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
        ch_source->query_builder = std::make_shared<ExternalRegexpQueryBuilder>(*ch_source->query_builder);
        ch_source->load_all_query = ch_source->query_builder->composeLoadAllQuery();
    }

    loadData();
    calculateBytesAllocated();
}

// Thin wrapper around unordered_map<String, Field> that manages the collection of attribute values subject to the
// behavior specified by collect_values_limit
class RegExpTreeDictionary::AttributeCollector : public std::unordered_map<String, Field>
{
private:
    std::optional<size_t> collect_values_limit; // std::nullopt means single-value mode, i.e. don't collect
    size_t n_full_attributes;

public:
    explicit AttributeCollector(std::optional<size_t> collect_values_limit_)
        : collect_values_limit(collect_values_limit_), n_full_attributes(0)
    {
    }

    constexpr bool collecting() const { return collect_values_limit != std::nullopt; }

    // Add a name-value pair to the collection if there's space
    void add(const String & attr_name, Field field, std::unordered_set<String> * const defaults = nullptr)
    {
        if (collect_values_limit)
        {
            if (!this->contains(attr_name))
                (*this)[attr_name] = Array();

            Array & values = (*this)[attr_name].safeGet<Array &>();
            if (values.size() < *collect_values_limit)
            {
                values.push_back(std::move(field));
                if (values.size() == *collect_values_limit)
                    n_full_attributes++;
            }
        }
        else if (!this->contains(attr_name) && (!defaults || !defaults->contains(attr_name)))
        {
            (*this)[attr_name] = std::move(field);
            n_full_attributes++;
        }
    }

    // Just occupy a space
    void addDefault(const String & attr_name, std::unordered_set<String> * const defaults)
    {
        assert (!collect_values_limit);
        if (!this->contains(attr_name) && !defaults->contains(attr_name))
        {
            defaults->insert(attr_name);
            n_full_attributes++;
        }
    }

    // Checks if no more values can be added for a given attribute
    bool full(const String & attr_name, std::unordered_set<String> * const defaults = nullptr) const
    {
        if (collect_values_limit)
        {
            auto it = this->find(attr_name);
            if (it == this->end())
                return false;
            return it->second.safeGet<const Array &>().size() >= *collect_values_limit;
        }
        else
        {
            return this->contains(attr_name) || (defaults && defaults->contains(attr_name));
        }
    }

    // Returns the number of full attributes
    size_t attributesFull() const { return n_full_attributes; }
};

std::pair<String, bool> processBackRefs(const String & data, const re2::RE2 & searcher, const std::vector<StringPiece> & pieces)
{
    std::string_view matches[10];
    String result;
    searcher.Match({data.data(), data.size()}, 0, data.size(), re2::RE2::Anchor::UNANCHORED, matches, 10);
    /// if the pattern is a single '$1' but fails to match, we would use the default value.
    if (pieces.size() == 1 && pieces[0].ref_num >= 0 && pieces[0].ref_num < 10 && matches[pieces[0].ref_num].empty())
        return std::make_pair(result, true);
    for (const auto & item : pieces)
    {
        if (item.ref_num >= 0 && item.ref_num < 10)
            result += String{matches[item.ref_num]};
        else
            result += item.literal;
    }
    return {result, false};
}

// walk towards root and collect attributes.
// The return value means whether we finish collecting.
bool RegExpTreeDictionary::setAttributes(
    UInt64 id,
    AttributeCollector & attributes_to_set,
    const String & data,
    std::unordered_set<UInt64> & visited_nodes,
    const std::unordered_map<String, const DictionaryAttribute &> & attributes,
    const std::unordered_map<String, ColumnPtr> & defaults,
    size_t key_index) const
{

    if (visited_nodes.contains(id))
        return attributes_to_set.attributesFull() == attributes.size();
    visited_nodes.emplace(id);
    const auto & node_attributes = regex_nodes.at(id)->attributes;
    for (const auto & [name_, value] : node_attributes)
    {
        if (!attributes.contains(name_) || attributes_to_set.full(name_))
            continue;

        if (value.containsBackRefs())
        {
            auto [updated_str, use_default] = processBackRefs(data, regex_nodes.at(id)->searcher, value.pieces);
            if (use_default)
            {
                // Back-ref processing failed.
                // - If not collecting values, set the default value immediately while we're still on this node.
                //   Otherwise, a value from a different node could take its place before we set it to the default value post-walk.
                // - If collecting values, don't add anything. If we find no other matches for this attribute,
                //   then we'll set its value to the default Array value later.
                if (!attributes_to_set.collecting())
                {
                    DefaultValueProvider default_value(attributes.at(name_).null_value, defaults.at(name_));
                    attributes_to_set.add(name_, default_value.getDefaultValue(key_index));
                }
            }
            else
                attributes_to_set.add(name_, parseStringToField(updated_str, attributes.at(name_).type));
        }
        else
            attributes_to_set.add(name_, value.field);
    }

    auto parent_id = regex_nodes.at(id)->parent_id;
    if (parent_id > 0)
        setAttributes(parent_id, attributes_to_set, data, visited_nodes, attributes, defaults, key_index);

    /// if all attributes are full, we can stop walking the tree
    return attributes_to_set.attributesFull() == attributes.size();
}

bool RegExpTreeDictionary::setAttributesShortCircuit(
    UInt64 id,
    AttributeCollector & attributes_to_set,
    const String & data,
    std::unordered_set<UInt64> & visited_nodes,
    const std::unordered_map<String, const DictionaryAttribute &> & attributes,
    std::unordered_set<String> * defaults) const
{
    if (visited_nodes.contains(id))
        return attributes_to_set.attributesFull() == attributes.size();
    visited_nodes.emplace(id);
    const auto & node_attributes = regex_nodes.at(id)->attributes;
    for (const auto & [name_, value] : node_attributes)
    {
        if (!attributes.contains(name_) || attributes_to_set.full(name_, defaults))
            continue;

        if (value.containsBackRefs())
        {
            auto [updated_str, use_default] = processBackRefs(data, regex_nodes.at(id)->searcher, value.pieces);
            if (use_default)
            {
                // Back-ref processing failed.
                // - If not collecting values, set the default value immediately while we're still on this node.
                //   Otherwise, a value from a different node could take its place before we set it to the default value post-walk.
                // - If collecting values, don't add anything. If we find no other matches for this attribute,
                //   then we'll set its value to the default Array value later.
                if (!attributes_to_set.collecting())
                    attributes_to_set.addDefault(name_, defaults);
            }
            else
                attributes_to_set.add(name_, parseStringToField(updated_str, attributes.at(name_).type), defaults);
        }
        else
            attributes_to_set.add(name_, value.field, defaults);
    }

    auto parent_id = regex_nodes.at(id)->parent_id;
    if (parent_id > 0)
        setAttributesShortCircuit(parent_id, attributes_to_set, data, visited_nodes, attributes, defaults);

    /// if all attributes are full, we can stop walking the tree
    return attributes_to_set.attributesFull() == attributes.size();
}

/// a temp struct to store all the matched result.
struct MatchContext
{
    std::set<UInt64> matched_idx_set;
    std::vector<std::pair<UInt64, UInt64>> matched_idx_sorted_list;

    const std::vector<UInt64> & regexp_ids ;
    const std::unordered_map<UInt64, UInt64> & topology_order;
    const char * data;
    size_t length;
    const std::map<UInt64, RegExpTreeDictionary::RegexTreeNodePtr> & regex_nodes;

    size_t pre_match_counter = 0;
    size_t match_counter = 0;

    MatchContext(
        const std::vector<UInt64> & regexp_ids_,
        const std::unordered_map<UInt64, UInt64> & topology_order_,
        const char * data_, size_t length_,
        const std::map<UInt64, RegExpTreeDictionary::RegexTreeNodePtr> & regex_nodes_
    )
    : regexp_ids(regexp_ids_),
        topology_order(topology_order_),
        data(data_),
        length(length_),
        regex_nodes(regex_nodes_)
    {}

    [[maybe_unused]]
    void insertIdx(unsigned int idx)
    {
        UInt64 node_id = regexp_ids[idx-1];
        pre_match_counter++;
        if (!regex_nodes.at(node_id)->match(data, length))
        {
            return;
        }
        match_counter++;
        matched_idx_set.emplace(node_id);

        UInt64 topological_order = topology_order.at(node_id);
        matched_idx_sorted_list.push_back(std::make_pair(topological_order, node_id));
    }

    [[maybe_unused]]
    void insertNodeID(UInt64 id)
    {
        matched_idx_set.emplace(id);

        UInt64 topological_order = topology_order.at(id);
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

std::unordered_map<String, ColumnPtr> RegExpTreeDictionary::match(
    const ColumnString::Chars & keys_data,
    const ColumnString::Offsets & keys_offsets,
    const std::unordered_map<String, const DictionaryAttribute &> & attributes,
    DefaultMapOrFilter default_or_filter,
    std::optional<size_t> collect_values_limit) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(default_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefaultMap>(default_or_filter));


#if USE_VECTORSCAN
    hs_scratch_t * scratch = nullptr;
    if (use_vectorscan)
    {
        hs_error_t err = hs_clone_scratch(origin_scratch.get(), &scratch);

        if (err != HS_SUCCESS)
        {
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for hyperscan");
        }
    }

    MultiRegexps::ScratchPtr smart_scratch(scratch);
#endif

    std::unordered_map<String, MutableColumnPtr> columns;

    /// initialize columns
    for (const auto & [name_, attr] : attributes)
    {
        auto col_ptr = (collect_values_limit ? std::make_shared<DataTypeArray>(attr.type) : attr.type)->createColumn();
        col_ptr->reserve(keys_offsets.size());
        columns[name_] = std::move(col_ptr);
    }

    std::optional<RefDefaultMap> default_map;
    std::optional<RefFilter> default_mask;
    if (is_short_circuit)
    {
        default_mask = std::get<RefFilter>(default_or_filter).get();
        default_mask.value().get().resize(keys_offsets.size());
    }
    else
    {
        default_map = std::get<RefDefaultMap>(default_or_filter).get();
    }

    UInt64 offset = 0;
    for (size_t key_idx = 0; key_idx < keys_offsets.size(); ++key_idx)
    {
        auto key_offset = keys_offsets[key_idx];
        UInt64 length = key_offset - offset - 1;

        const char * begin = reinterpret_cast<const char *>(keys_data.data()) + offset;

        MatchContext match_result(regexp_ids, topology_order, begin, length, regex_nodes);

#if USE_VECTORSCAN
        if (use_vectorscan)
        {
            /// pre-select all the possible matches
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
                origin_db.get(),
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
        AttributeCollector attributes_to_set{collect_values_limit};
        std::unordered_set<UInt64> visited_nodes;

        /// Some node matches but its parents cannot match. In this case we must regard this node unmatched.
        auto is_valid = [&](UInt64 id)
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

        if (is_short_circuit)
        {
            std::unordered_set<String> defaults;

            for (auto item : match_result.matched_idx_sorted_list)
            {
                UInt64 id = item.second;
                if (!is_valid(id))
                    continue;
                if (visited_nodes.contains(id))
                    continue;
                if (setAttributesShortCircuit(id, attributes_to_set, str, visited_nodes, attributes, &defaults))
                    break;
            }

            for (const auto & [name_, attr] : attributes)
            {
                if (attributes_to_set.contains(name_))
                    continue;

                columns[name_]->insertDefault();
                default_mask.value().get()[key_idx] = 1;
            }

            /// insert to columns
            for (const auto & [name_, value] : attributes_to_set)
            {
                columns[name_]->insert(value);
                default_mask.value().get()[key_idx] = 0;
            }
        }
        else
        {
            for (auto item : match_result.matched_idx_sorted_list)
            {
                UInt64 id = item.second;
                if (!is_valid(id))
                    continue;
                if (visited_nodes.contains(id))
                    continue;
                if (setAttributes(id, attributes_to_set, str, visited_nodes, attributes,
                        default_map.value().get(), key_idx))
                    break;
            }

            for (const auto & [name_, attr] : attributes)
            {
                if (attributes_to_set.contains(name_))
                    continue;

                DefaultValueProvider default_value(
                    collect_values_limit ? DataTypeArray(attr.type).getDefault() : attr.null_value,
                        default_map.value().get().at(name_));
                columns[name_]->insert(default_value.getDefaultValue(key_idx));
            }

            /// insert to columns
            for (const auto & [name_, value] : attributes_to_set)
                columns[name_]->insert(value);
        }

        offset = key_offset;
    }

    std::unordered_map<String, ColumnPtr> result;
    for (auto & [name_, mutable_ptr] : columns)
        result.emplace(name_, std::move(mutable_ptr));

    return result;
}

Pipe RegExpTreeDictionary::read(const Names & , size_t max_block_size, size_t) const
{

    auto it = regex_nodes.begin();
    size_t block_size = 0;
    BlocksList result;

    for (;;)
    {
        Block block;
        auto col_id = std::make_shared<DataTypeUInt64>()->createColumn();
        auto col_pid = std::make_shared<DataTypeUInt64>()->createColumn();
        auto col_regex = std::make_shared<DataTypeString>()->createColumn();
        auto col_keys = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())->createColumn();
        auto col_values = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())->createColumn();

        for (;it != regex_nodes.end() && block_size < max_block_size; it++, block_size++)
        {
            col_id->insert(it->first);
            const auto & node = it->second;
            col_pid->insert(node->parent_id);
            col_regex->insert(node->regex);
            std::vector<Field> keys, values;
            for (const auto & [key, attr] : node->attributes)
            {
                keys.push_back(key);
                values.push_back(attr.original_value);
            }
            col_keys->insert(Array(keys.begin(), keys.end()));
            col_values->insert(Array(values.begin(), values.end()));
        }

        block.insert(ColumnWithTypeAndName(std::move(col_id),std::make_shared<DataTypeUInt64>(),kId));
        block.insert(ColumnWithTypeAndName(std::move(col_pid),std::make_shared<DataTypeUInt64>(),kParentId));
        block.insert(ColumnWithTypeAndName(std::move(col_regex),std::make_shared<DataTypeString>(),kRegExp));
        block.insert(ColumnWithTypeAndName(std::move(col_keys),std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),kKeys));
        block.insert(ColumnWithTypeAndName(std::move(col_values),std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),kValues));
        result.push_back(std::move(block));
        if (it == regex_nodes.end())
            break;
        block_size = 0;
    }

    return Pipe(std::make_shared<BlocksListSource>(std::move(result)));
}

Columns RegExpTreeDictionary::getColumnsImpl(
    const Strings & attribute_names,
    const DataTypes & result_types,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultsOrFilter defaults_or_filter,
    std::optional<size_t> collect_values_limit) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(defaults_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefaults>(defaults_or_filter));

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
        DataTypePtr attribute_type = result_types[i];
        if (collect_values_limit)
        {
            if (!WhichDataType(attribute_type).isArray())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Expected Array result type for attribute `{}`, got `{}`",
                    attribute_names[i],
                    attribute_type->getName());
            attribute_type = assert_cast<const DataTypeArray &>(*attribute_type).getNestedType();
        }
        const auto & attribute = structure.getAttribute(attribute_names[i], attribute_type);
        attributes.emplace(attribute.name, attribute);
        if (!is_short_circuit)
        {
            const Columns & default_values_columns = std::get<RefDefaults>(defaults_or_filter).get();
            defaults[attribute.name] = default_values_columns[i];
        }
    }

    /// calculate matches
    const ColumnString * key_column = typeid_cast<const ColumnString *>(key_columns[0].get());
    if (key_column == nullptr)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a ColumnString column");

    const auto & columns_map = match(
        key_column->getChars(),
        key_column->getOffsets(),
        attributes,
        is_short_circuit ? std::get<RefFilter>(defaults_or_filter).get()/*default_mask*/ : DefaultMapOrFilter{defaults},
        collect_values_limit);

    Columns result;
    for (const String & name_ : attribute_names)
        result.push_back(columns_map.at(name_));

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

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
        const auto * clickhouse_source = typeid_cast<const ClickHouseDictionarySource *>(source_ptr.get());
        bool use_async_executor
            = clickhouse_source && clickhouse_source->isLocal() && context->getSettingsRef()[Setting::dictionary_use_async_executor];

        RegExpTreeDictionary::Configuration configuration{
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false),
            .lifetime = dict_lifetime,
            .use_async_executor = use_async_executor,
        };

        return std::make_unique<RegExpTreeDictionary>(
            dict_id,
            dict_struct,
            std::move(source_ptr),
            configuration,
            context->getSettingsRef()[Setting::regexp_dict_allow_hyperscan],
            context->getSettingsRef()[Setting::regexp_dict_flag_case_insensitive],
            context->getSettingsRef()[Setting::regexp_dict_flag_dotall]);
    };

    factory.registerLayout("regexp_tree", create_layout, true);
}

}
