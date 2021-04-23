#include "DirectDictionary.h"

#include <Core/Defines.h>
#include <Common/HashTable/HashMap.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

template <DictionaryKeyType dictionary_key_type>
DirectDictionary<dictionary_key_type>::DirectDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
{
    if (!source_ptr->supportsSelectiveLoad())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "{}: source cannot be used with DirectDictionary", full_name);
}

template <DictionaryKeyType dictionary_key_type>
Columns DirectDictionary<dictionary_key_type>::getColumns(
    const Strings & attribute_names,
    const DataTypes & result_types,
    const Columns & key_columns,
    const DataTypes & key_types [[maybe_unused]],
    const Columns & default_values_columns) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());
    const auto requested_keys = extractor.extractAllKeys();

    DictionaryStorageFetchRequest request(dict_struct, attribute_names, result_types, default_values_columns);

    HashMap<KeyType, size_t> key_to_fetched_index;
    key_to_fetched_index.reserve(requested_keys.size());

    auto fetched_columns_from_storage = request.makeAttributesResultColumns();
    for (size_t attribute_index = 0; attribute_index < request.attributesSize(); ++attribute_index)
    {
        if (!request.shouldFillResultColumnWithIndex(attribute_index))
            continue;

        auto & fetched_column_from_storage = fetched_columns_from_storage[attribute_index];
        fetched_column_from_storage->reserve(requested_keys.size());
    }

    size_t fetched_key_index = 0;

    Columns block_key_columns;
    size_t dictionary_keys_size = dict_struct.getKeysNames().size();
    block_key_columns.reserve(dictionary_keys_size);

    BlockInputStreamPtr stream = getSourceBlockInputStream(key_columns, requested_keys);

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        /// Split into keys columns and attribute columns
        for (size_t i = 0; i < dictionary_keys_size; ++i)
            block_key_columns.emplace_back(block.safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> block_keys_extractor(block_key_columns, arena_holder.getComplexKeyArena());
        auto block_keys = block_keys_extractor.extractAllKeys();

        for (size_t attribute_index = 0; attribute_index < request.attributesSize(); ++attribute_index)
        {
            if (!request.shouldFillResultColumnWithIndex(attribute_index))
                continue;

            const auto & block_column = block.safeGetByPosition(dictionary_keys_size + attribute_index).column;
            fetched_columns_from_storage[attribute_index]->insertRangeFrom(*block_column, 0, block_keys.size());
        }

        for (size_t block_key_index = 0; block_key_index < block_keys.size(); ++block_key_index)
        {
            auto block_key = block_keys[block_key_index];
            key_to_fetched_index[block_key] = fetched_key_index;
            ++fetched_key_index;
        }

        block_key_columns.clear();
    }

    stream->readSuffix();

    Field value_to_insert;

    size_t requested_keys_size = requested_keys.size();

    auto result_columns = request.makeAttributesResultColumns();

    for (size_t attribute_index = 0; attribute_index < result_columns.size(); ++attribute_index)
    {
        if (!request.shouldFillResultColumnWithIndex(attribute_index))
            continue;

        auto & result_column = result_columns[attribute_index];

        const auto & fetched_column_from_storage = fetched_columns_from_storage[attribute_index];
        const auto & default_value_provider = request.defaultValueProviderAtIndex(attribute_index);

        result_column->reserve(requested_keys_size);

        for (size_t requested_key_index = 0; requested_key_index < requested_keys_size; ++requested_key_index)
        {
            const auto requested_key = requested_keys[requested_key_index];
            const auto * it = key_to_fetched_index.find(requested_key);

            if (it)
                fetched_column_from_storage->get(it->getMapped(), value_to_insert);
            else
                value_to_insert = default_value_provider.getDefaultValue(requested_key_index);

            result_column->insert(value_to_insert);
        }
    }

    query_count.fetch_add(requested_keys_size, std::memory_order_relaxed);

    return request.filterRequestedColumns(result_columns);
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr DirectDictionary<dictionary_key_type>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    return getColumns({ attribute_name }, { result_type }, key_columns, key_types, { default_values_column }).front();
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr DirectDictionary<dictionary_key_type>::hasKeys(
    const Columns & key_columns,
    const DataTypes & key_types [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> requested_keys_extractor(key_columns, arena_holder.getComplexKeyArena());
    auto requested_keys = requested_keys_extractor.extractAllKeys();
    size_t requested_keys_size = requested_keys.size();

    HashMap<KeyType, size_t> requested_key_to_index;
    requested_key_to_index.reserve(requested_keys_size);

    for (size_t i = 0; i < requested_keys.size(); ++i)
    {
        auto requested_key = requested_keys[i];
        requested_key_to_index[requested_key] = i;
    }

    auto result = ColumnUInt8::create(requested_keys_size, false);
    auto & result_data = result->getData();

    Columns block_key_columns;
    size_t dictionary_keys_size = dict_struct.getKeysNames().size();
    block_key_columns.reserve(dictionary_keys_size);

    BlockInputStreamPtr stream = getSourceBlockInputStream(key_columns, requested_keys);

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        /// Split into keys columns and attribute columns
        for (size_t i = 0; i < dictionary_keys_size; ++i)
            block_key_columns.emplace_back(block.safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> block_keys_extractor(block_key_columns, arena_holder.getComplexKeyArena());
        size_t block_keys_size = block_keys_extractor.getKeysSize();

        for (size_t i = 0; i < block_keys_size; ++i)
        {
            auto block_key = block_keys_extractor.extractCurrentKey();

            const auto * it = requested_key_to_index.find(block_key);
            assert(it);

            size_t result_data_found_index = it->getMapped();
            result_data[result_data_found_index] = true;

            block_keys_extractor.rollbackCurrentKey();
        }

        block_key_columns.clear();
    }

    stream->readSuffix();

    query_count.fetch_add(requested_keys_size, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr DirectDictionary<dictionary_key_type>::getHierarchy(
    ColumnPtr key_column,
    const DataTypePtr & key_type) const
{
    if (dictionary_key_type == DictionaryKeyType::simple)
    {
        auto result = getKeysHierarchyDefaultImplementation(this, key_column, key_type);
        query_count.fetch_add(key_column->size(), std::memory_order_relaxed);
        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type>
ColumnUInt8::Ptr DirectDictionary<dictionary_key_type>::isInHierarchy(
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr & key_type) const
{
    if (dictionary_key_type == DictionaryKeyType::simple)
    {
        auto result = getKeysIsInHierarchyDefaultImplementation(this, key_column, in_key_column, key_type);
        query_count.fetch_add(key_column->size(), std::memory_order_relaxed);
        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type>
BlockInputStreamPtr DirectDictionary<dictionary_key_type>::getSourceBlockInputStream(
    const Columns & key_columns [[maybe_unused]],
    const PaddedPODArray<KeyType> & requested_keys [[maybe_unused]]) const
{
    size_t requested_keys_size = requested_keys.size();

    BlockInputStreamPtr stream;

    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        std::vector<UInt64> ids;
        ids.reserve(requested_keys_size);

        for (auto key : requested_keys)
            ids.emplace_back(key);

        stream = source_ptr->loadIds(ids);
    }
    else
    {
        std::vector<size_t> requested_rows;
        requested_rows.reserve(requested_keys_size);
        for (size_t i = 0; i < requested_keys_size; ++i)
            requested_rows.emplace_back(i);

        stream = source_ptr->loadKeys(key_columns, requested_rows);
    }

    return stream;
}

template <DictionaryKeyType dictionary_key_type>
BlockInputStreamPtr DirectDictionary<dictionary_key_type>::getBlockInputStream(const Names & /* column_names */, size_t /* max_block_size */) const
{
    return source_ptr->loadAll();
}

namespace
{
    template <DictionaryKeyType dictionary_key_type>
    DictionaryPtr createDirectDictionary(
        const std::string & full_name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr)
    {
        const auto * layout_name = dictionary_key_type == DictionaryKeyType::simple ? "direct" : "complex_key_direct";

        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        {
            if (dict_struct.key)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "'key' is not supported for dictionary of layout '{}'",
                    layout_name);
        }
        else
        {
            if (dict_struct.id)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "'id' is not supported for dictionary of layout '{}'",
                    layout_name);
        }

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        if (config.has(config_prefix + ".lifetime.min") || config.has(config_prefix + ".lifetime.max"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'lifetime' parameter is redundant for the dictionary' of layout '{}'",
                layout_name);

        return std::make_unique<DirectDictionary<dictionary_key_type>>(dict_id, dict_struct, std::move(source_ptr));
    }
}

template class DirectDictionary<DictionaryKeyType::simple>;
template class DirectDictionary<DictionaryKeyType::complex>;

void registerDictionaryDirect(DictionaryFactory & factory)
{
    factory.registerLayout("direct", createDirectDictionary<DictionaryKeyType::simple>, false);
    factory.registerLayout("complex_key_direct", createDirectDictionary<DictionaryKeyType::complex>, true);
}


}
