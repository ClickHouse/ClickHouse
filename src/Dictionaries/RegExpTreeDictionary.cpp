#include "RegExpTreeDictionary.h"

#include <optional>
#include <string_view>

#include <fmt/format.h>

#include <type_traits>
#include <base/defines.h>

#include <Poco/RegularExpression.h>

#include <Common/ArenaUtils.h>

#include <Functions/Regexps.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

#include "config_dictionaries.h"

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
}

inline const std::string kId = "id";
inline const std::string kParentId = "parent_id";
inline const std::string kRegExp = "regexp";

void RegExpTreeDictionary::createAttributes()
{
    for (const auto & pair : structure.attribute_name_to_index)
    {
        const auto & attribute_name = pair.first;
        const auto & attribute_index = pair.second;

        const auto & dictionary_attribute = structure.attributes[attribute_index];

        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            Attribute attribute{dictionary_attribute.underlying_type, {}, ContainerType<ValueType>()};
            names_to_attributes[attribute_name] = std::move(attribute);
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, std::move(type_call));
    }
}

void RegExpTreeDictionary::calculateBytesAllocated()
{
    for (auto & pair : names_to_attributes)
    {
        const auto & name = pair.first;
        auto & attribute = pair.second;

        bytes_allocated += name.size() + sizeof(attribute);

        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            const auto & container = std::get<ContainerType<ValueType>>(attribute.container);
            bytes_allocated += sizeof(ContainerType<ValueType>);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                /// It is not accurate calculations
                bytes_allocated += sizeof(Array) * container.size();
            }
            else
            {
                bytes_allocated += container.allocated_bytes();
            }

            bucket_count = container.capacity();
        };

        callOnDictionaryAttributeType(attribute.type, type_call);

        if (!attribute.nullable_set.empty())
        {
            bytes_allocated += sizeof(*attribute.nullable_set.begin()) * attribute.nullable_set.size();
        }
    }

    for (const auto & regexp : regexps)
    {
        bytes_allocated += regexp.size();
    }

    bytes_allocated += 2 * keys_to_ids.size() * sizeof(UInt64);
    bytes_allocated += 2 * ids_to_parent_ids.size() * sizeof(UInt64);
    bytes_allocated += 2 * ids_to_child_ids.size() * sizeof(UInt64);

    bytes_allocated += string_arena.size();
}

void RegExpTreeDictionary::resizeAttributes(const size_t size)
{
    for (auto & pair : names_to_attributes)
    {
        auto & attribute = pair.second;

        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            auto & container = std::get<ContainerType<ValueType>>(attribute.container);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                container.resize(size, ValueType());
            }
            else
            {
                container.resize_fill(size, ValueType());
            }
        };

        callOnDictionaryAttributeType(attribute.type, std::move(type_call));
    }
}

void RegExpTreeDictionary::setRegexps(const Block & block)
{
    const auto & column = *block.getByName(kRegExp).column;

    for (size_t i = 0; i < column.size(); ++i)
    {
        regexps.push_back(column[i].get<std::string>());
    }
}

void RegExpTreeDictionary::setIdToParentId(const Block & block)
{
    const auto & column = *block.getByName(kParentId).column;

    for (UInt64 i = 0; i < column.size(); ++i)
    {
        if (column[i].isNull())
        {
            continue;
        }

        const auto parent_id = keys_to_ids[column[i].get<UInt64>()];

        ids_to_parent_ids[i] = parent_id;
        ids_to_child_ids[parent_id] = i;
    }
}

void RegExpTreeDictionary::setAttributeValue(Attribute & attribute, const UInt64 key, const Field & value)
{
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        if (value.isNull())
        {
            attribute.nullable_set.insert(key);
            return;
        }

        auto & attribute_value = value.safeGet<AttributeType>();
        auto & container = std::get<ContainerType<ValueType>>(attribute.container);

        if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto arena_value = copyStringInArena(string_arena, attribute_value);
            container[key] = arena_value;
        }
        else
        {
            container[key] = attribute_value;
        }
    };

    callOnDictionaryAttributeType(attribute.type, std::move(type_call));
}

void RegExpTreeDictionary::blockToAttributes(const Block & block)
{
    const auto ids_column = block.getByName(kId).column;

    const auto ids_size = ids_column->size();

    for (UInt64 i = 0; i < ids_size; ++i)
    {
        keys_to_ids[(*ids_column)[i].get<UInt64>()] = i;
    }

    setRegexps(block);
    setIdToParentId(block);

    resizeAttributes(ids_size);

    for (auto & [name, attribute] : names_to_attributes)
    {
        const auto & attribute_column = *block.getByName(name).column;

        for (size_t j = 0; j < ids_size; ++j)
        {
            setAttributeValue(attribute, j, attribute_column[j]);
        }
    }
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
            blockToAttributes(block);
        }
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Dictionary {} does not support updating manual fields", name);
    }
}

RegExpTreeDictionary::RegExpTreeDictionary(
    const StorageID & id_, const DictionaryStructure & structure_, DictionarySourcePtr source_ptr_, Configuration configuration_)
    : IDictionary(id_), structure(structure_), source_ptr(source_ptr_), configuration(configuration_)
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

std::unordered_set<UInt64> RegExpTreeDictionary::matchSearchAllIndices(const std::string & key) const
{
#if USE_VECTORSCAN
    std::vector<std::string_view> regexps_views(regexps.begin(), regexps.end());

    const auto & hyperscan_regex = MultiRegexps::get<true, false>(regexps_views, std::nullopt);

    hs_scratch_t * scratch = nullptr;
    hs_error_t err = hs_clone_scratch(hyperscan_regex->getScratch(), &scratch);

    if (err != HS_SUCCESS)
    {
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for hyperscan");
    }

    MultiRegexps::ScratchPtr smart_scratch(scratch);

    std::unordered_set<UInt64> matches;
    auto on_match = [](unsigned int id,
                       unsigned long long /* from */, // NOLINT
                       unsigned long long /* to */, // NOLINT
                       unsigned int /* flags */,
                       void * context) -> int
    {
        static_cast<std::unordered_set<UInt64> *>(context)->insert(id);
        return 0;
    };

    err = hs_scan(hyperscan_regex->getDB(), key.c_str(), key.size(), 0, smart_scratch.get(), on_match, &matches);

    if (err != HS_SUCCESS)
    {
        throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan {} with vectorscan", key);
    }

    return matches;
#else
#    void(key)
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Multi search all indices is not implemented when USE_VECTORSCAN is off");
#endif // USE_VECTORSCAN
}

template <typename AttributeType, typename SetValueFunc, typename DefaultValueExtractor>
void RegExpTreeDictionary::getColumnImpl(
    const Attribute & attribute,
    const std::optional<UInt64> match_index_opt,
    const bool is_nullable,
    SetValueFunc && set_value_func,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & container = std::get<ContainerType<AttributeType>>(attribute.container);

    const auto index = 0;
    if (match_index_opt.has_value())
    {
        const auto & match_index = match_index_opt.value();

        if (is_nullable)
        {
            set_value_func(index, container[match_index], attribute.nullable_set.contains(match_index));
        }
        else
        {
            set_value_func(index, container[match_index], false);
        }

        found_count.fetch_add(1, std::memory_order_relaxed);
    }
    else
    {
        if (is_nullable)
        {
            set_value_func(index, default_value_extractor[index], default_value_extractor.isNullAt(index));
        }
        else
        {
            set_value_func(index, default_value_extractor[index], false);
        }
    }

    query_count.fetch_add(1, std::memory_order_relaxed);
}

UInt64 RegExpTreeDictionary::getRoot(const std::unordered_set<UInt64> & indices) const
{
    for (const auto & index : indices)
    {
        if (!ids_to_parent_ids.contains(index - 1))
        {
            return index - 1;
        }
    }

    return *indices.begin() - 1;
}

std::optional<UInt64> RegExpTreeDictionary::getLastMatchIndex(const std::unordered_set<UInt64> matches, const Attribute & attribute) const
{
    if (matches.empty())
    {
        return std::nullopt;
    }

    auto match_index = getRoot(matches);
    auto last_match_index = match_index;

    while (ids_to_child_ids.contains(match_index))
    {
        match_index = ids_to_child_ids.at(match_index);

        if (!matches.contains(match_index))
        {
            break;
        }

        if (!attribute.nullable_set.contains(match_index))
        {
            last_match_index = match_index;
        }
    }

    return last_match_index;
}

std::string format(const std::string & pattern, const std::vector<std::string> & arguments)
{
    using context = fmt::format_context;

    std::vector<fmt::basic_format_arg<context>> fmt_arguments;
    fmt_arguments.reserve(arguments.size());

    for (auto const & argument : arguments)
    {
        fmt_arguments.push_back(fmt::detail::make_arg<context>(argument));
    }

    return fmt::vformat(pattern, fmt::basic_format_args<context>(fmt_arguments.data(), fmt_arguments.size()));
}

std::string formatMatches(const std::string & key, const std::string & regexp_pattern, const std::string & pattern)
{
    Poco::RegularExpression regexp(regexp_pattern);
    Poco::RegularExpression::MatchVec matches;

    regexp.match(key, 0, matches);

    std::vector<std::string> arguments;

    for (size_t i = 1; i < matches.size(); ++i)
    {
        const auto & match = matches[i];
        arguments.emplace_back(key.substr(match.offset, match.length));
    }

    return format(pattern, arguments);
}

ColumnPtr RegExpTreeDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes &,
    const ColumnPtr & default_values_column) const
{
    if (key_columns.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected 1 key for getColumn, got {}", std::to_string(key_columns.size()));
    }
    const auto & key_column = key_columns.front();

    if (key_column->size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected 1 key for getColumn, got {}", std::to_string(key_column->size()));
    }
    const auto key = (*key_column)[0].get<std::string>();

    if (!names_to_attributes.contains(attribute_name))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown attribute {}", attribute_name);
    }

    ColumnPtr result;

    const auto keys_size = 1;

    const auto & attribute = names_to_attributes.at(attribute_name);
    const auto & dictionary_attribute = structure.getAttribute(attribute_name, result_type);

    const auto is_nullable = !attribute.nullable_set.empty();
    ColumnUInt8::MutablePtr null_mask;
    if (is_nullable)
    {
        null_mask = ColumnUInt8::create(keys_size, false);
    }

    const auto matches = matchSearchAllIndices(key);
    const auto match_index_opt = getLastMatchIndex(matches, attribute);

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(dictionary_attribute.null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, keys_size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            getColumnImpl<ValueType>(
                attribute,
                match_index_opt,
                false,
                [&](size_t, const Array & value, bool) { column.get()->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            if (is_nullable)
            {
                getColumnImpl<ValueType>(
                    attribute,
                    match_index_opt,
                    true,
                    [&](size_t index, StringRef value, bool is_null)
                    {
                        if (is_null)
                        {
                            null_mask->getData()[index] = true;
                        }

                        if (match_index_opt.has_value())
                        {
                            const auto formatted_value = formatMatches(key, regexps[match_index_opt.value()], value.toString());
                            column.get()->insertData(formatted_value.data(), formatted_value.size());
                        }
                        else
                        {
                            column.get()->insertData(value.data, value.size);
                        }
                    },
                    default_value_extractor);
            }
            else
            {
                getColumnImpl<ValueType>(
                    attribute,
                    match_index_opt,
                    false,
                    [&](size_t, const StringRef value, bool) { column.get()->insertData(value.data, value.size); },
                    default_value_extractor);
            }
        }
        else
        {
            if (is_nullable)
            {
                getColumnImpl<ValueType>(
                    attribute,
                    match_index_opt,
                    true,
                    [&](size_t index, const auto value, bool is_null)
                    {
                        if (is_null)
                        {
                            null_mask->getData()[index] = true;
                        }
                        column->getData()[index] = value;
                    },
                    default_value_extractor);
            }
            else
            {
                getColumnImpl<ValueType>(
                    attribute,
                    match_index_opt,
                    false,
                    [&](size_t index, const auto value, bool) { column->getData()[index] = value; },
                    default_value_extractor);
            }
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, std::move(type_call));

    if (is_nullable)
    {
        result = ColumnNullable::create(result, std::move(null_mask));
    }

    return result;
}

void registerDictionaryRegExpTree(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr,
                             bool) -> DictionaryPtr
    {
        if (dict_struct.id)
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout `{}`", full_name);
        }

        String dictionary_layout_prefix = config_prefix + ".layout" + ".reg-exp-tree";
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        RegExpTreeDictionary::Configuration configuration{
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false), .lifetime = dict_lifetime};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        return std::make_unique<RegExpTreeDictionary>(dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    factory.registerLayout("reg-exp-tree", create_layout, false);
}

}
