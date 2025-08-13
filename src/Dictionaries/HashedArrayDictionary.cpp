#include <Dictionaries/HashedArrayDictionary.h>

#include <Common/ArenaUtils.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
HashedArrayDictionary<dictionary_key_type, sharded>::HashedArrayDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const HashedArrayDictionaryStorageConfiguration & configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , log(getLogger("HashedArrayDictionary"))
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(configuration_)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
    buildHierarchyParentToChildIndexIfNeeded();
    calculateBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
ColumnPtr HashedArrayDictionary<dictionary_key_type, sharded>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultOrFilter default_or_filter) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t keys_size = extractor.getKeysSize();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, attribute_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    auto & attribute = attributes[attribute_index];

    return getAttributeColumn(attribute, dictionary_attribute, keys_size, default_or_filter, extractor);
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
Columns HashedArrayDictionary<dictionary_key_type, sharded>::getColumns(
    const Strings & attribute_names,
    const DataTypes & attribute_types,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultsOrFilter defaults_or_filter) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(defaults_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefaults>(defaults_or_filter));

    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t keys_size = extractor.getKeysSize();

    IColumn::Filter * default_mask = nullptr;
    if (is_short_circuit)
    {
        default_mask = &std::get<RefFilter>(defaults_or_filter).get();
        default_mask->resize(keys_size);
    }

    KeyIndexToElementIndex key_index_to_element_index;

    /** Optimization for multiple attributes.
      * For each key save element index in key_index_to_element_index array.
      * Later in type_call for attribute use getItemsImpl specialization with key_index_to_element_index array
      * instead of DictionaryKeyExtractor.
      */
    if (attribute_names.size() > 1)
    {
        size_t keys_found = 0;

        key_index_to_element_index.resize(keys_size);

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = extractor.extractCurrentKey();
            auto shard = getShard(key);
            const auto & key_attribute_container = key_attribute.containers[shard];

            auto it = key_attribute_container.find(key);
            if (it == key_attribute_container.end())
            {
                if constexpr (sharded)
                    key_index_to_element_index[key_index] = std::make_pair(-1, shard);
                else
                    key_index_to_element_index[key_index] = -1;

                if (default_mask)
                    (*default_mask)[key_index] = 1;
            }
            else
            {
                if constexpr (sharded)
                    key_index_to_element_index[key_index] = std::make_pair(it->getMapped(), shard);
                else
                    key_index_to_element_index[key_index] = it->getMapped();

                if (default_mask)
                    (*default_mask)[key_index] = 0;

                ++keys_found;
            }

            extractor.rollbackCurrentKey();
        }

        query_count.fetch_add(keys_size, std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
    }

    size_t attribute_names_size = attribute_names.size();

    Columns result_columns;
    result_columns.reserve(attribute_names_size);

    for (size_t i = 0; i < attribute_names_size; ++i)
    {
        ColumnPtr result_column;

        const auto & attribute_name = attribute_names[i];
        const auto & attribute_type = attribute_types[i];

        const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, attribute_type);
        const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
        auto & attribute = attributes[attribute_index];

        if (is_short_circuit)
        {
            if (attribute_names_size > 1)
                result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size,
                    *default_mask, key_index_to_element_index);
            else
                result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size,
                    *default_mask, extractor);
        }
        else
        {
            const Columns & default_values_columns = std::get<RefDefaults>(defaults_or_filter).get();
            const auto & default_values_column = default_values_columns[i];
            if (attribute_names_size > 1)
                result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size,
                    default_values_column, key_index_to_element_index);
            else
                result_column = getAttributeColumn(attribute, dictionary_attribute, keys_size,
                    default_values_column, extractor);
        }

        result_columns.emplace_back(std::move(result_column));
    }

    return result_columns;
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
ColumnUInt8::Ptr HashedArrayDictionary<dictionary_key_type, sharded>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    size_t keys_size = extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size, false);
    auto & out = result->getData();

    size_t keys_found = 0;

    for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
    {
        auto requested_key = extractor.extractCurrentKey();
        auto shard = getShard(requested_key);
        const auto & key_attribute_container = key_attribute.containers[shard];

        out[requested_key_index] = key_attribute_container.find(requested_key) != key_attribute_container.end();

        keys_found += out[requested_key_index];
        extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
ColumnPtr HashedArrayDictionary<dictionary_key_type, sharded>::getHierarchy(ColumnPtr key_column [[maybe_unused]], const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        std::optional<UInt64> null_value;

        if (!dictionary_attribute.null_value.isNull())
            null_value = dictionary_attribute.null_value.safeGet<UInt64>();


        auto is_key_valid_func = [&, this](auto & key)
        {
            const auto & key_attribute_container = key_attribute.containers[getShard(key)];
            return key_attribute_container.find(key) != key_attribute_container.end();
        };

        size_t keys_found = 0;

        auto get_parent_func = [&, this](auto & hierarchy_key)
        {
            std::optional<UInt64> result;
            auto shard = getShard(hierarchy_key);
            const auto & key_attribute_container = key_attribute.containers[shard];

            auto it = key_attribute_container.find(hierarchy_key);

            if (it == key_attribute_container.end())
                return result;

            size_t key_index = it->getMapped();

            if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[shard][key_index])
                return result;
            const auto & parent_keys_container = std::get<AttributeContainerShardsType<UInt64>>(hierarchical_attribute.containers)[shard];

            UInt64 parent_key = parent_keys_container[key_index];
            if (null_value && *null_value == parent_key)
                return result;

            result = parent_key;
            keys_found += 1;

            return result;
        };

        auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return dictionary_hierarchy_array;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
ColumnUInt8::Ptr HashedArrayDictionary<dictionary_key_type, sharded>::isInHierarchy(
    ColumnPtr key_column [[maybe_unused]],
    ColumnPtr in_key_column [[maybe_unused]],
    const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        PaddedPODArray<UInt64> keys_in_backup_storage;
        const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        std::optional<UInt64> null_value;

        if (!dictionary_attribute.null_value.isNull())
            null_value = dictionary_attribute.null_value.safeGet<UInt64>();


        auto is_key_valid_func = [&](auto & key)
        {
            const auto & key_attribute_container = key_attribute.containers[getShard(key)];
            return key_attribute_container.find(key) != key_attribute_container.end();
        };

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;
            auto shard = getShard(hierarchy_key);
            const auto & key_attribute_container = key_attribute.containers[shard];

            auto it = key_attribute_container.find(hierarchy_key);

            if (it == key_attribute_container.end())
                return result;

            size_t key_index = it->getMapped();

            if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[shard][key_index])
                return result;

            const auto & parent_keys_container = std::get<AttributeContainerShardsType<UInt64>>(hierarchical_attribute.containers)[shard];
            UInt64 parent_key = parent_keys_container[key_index];
            if (null_value && *null_value == parent_key)
                return result;

            result = parent_key;
            keys_found += 1;

            return result;
        };

        auto result = getKeysIsInHierarchyColumn(keys, keys_in, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return result;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
DictionaryHierarchicalParentToChildIndexPtr HashedArrayDictionary<dictionary_key_type, sharded>::getHierarchicalIndex() const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (hierarchical_index)
            return hierarchical_index;

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        DictionaryHierarchicalParentToChildIndex::ParentToChildIndex parent_to_child;
        for (size_t shard = 0; shard < configuration.shards; ++shard)
        {
            HashMap<size_t, UInt64> index_to_key;
            index_to_key.reserve(element_counts[shard]);

            for (auto & [key, value] : key_attribute.containers[shard])
                index_to_key[value] = key;

            parent_to_child.reserve(parent_to_child.size() + index_to_key.size());

            const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
            const auto & parent_keys_container = std::get<AttributeContainerShardsType<UInt64>>(hierarchical_attribute.containers)[shard];

            size_t parent_keys_container_size = parent_keys_container.size();
            for (size_t i = 0; i < parent_keys_container_size; ++i)
            {
                if (unlikely(hierarchical_attribute.is_index_null) && (*hierarchical_attribute.is_index_null)[shard][i])
                    continue;

                const auto * it = index_to_key.find(i);
                if (it == index_to_key.end())
                    continue;

                auto child_key = it->getMapped();
                auto parent_key = parent_keys_container[i];
                parent_to_child[parent_key].emplace_back(child_key);
            }
        }

        return std::make_shared<DictionaryHierarchicalParentToChildIndex>(parent_to_child);
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
ColumnPtr HashedArrayDictionary<dictionary_key_type, sharded>::getDescendants(
    ColumnPtr key_column [[maybe_unused]],
    const DataTypePtr &,
    size_t level [[maybe_unused]],
    DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t keys_found = 0;
        auto result = getKeysDescendantsArray(keys, *parent_to_child_index, level, keys_found);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return result;
    }
    else
    {
        return nullptr;
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & dictionary_attribute : dict_struct.attributes)
    {
        auto type_call = [&, this](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            auto is_index_null = dictionary_attribute.is_nullable ? std::make_optional<std::vector<typename Attribute::RowsMask>>(configuration.shards) : std::nullopt;
            Attribute attribute{dictionary_attribute.underlying_type, AttributeContainerShardsType<ValueType>(configuration.shards), std::move(is_index_null)};
            attributes.emplace_back(std::move(attribute));
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }

    key_attribute.containers.resize(configuration.shards);
    element_counts.resize(configuration.shards);

    string_arenas.resize(configuration.shards);
    for (auto & arena : string_arenas)
        arena = std::make_unique<Arena>();
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::updateData()
{
    if (!update_field_loaded_block || update_field_loaded_block->rows() == 0)
    {
        QueryPipeline pipeline(source_ptr->loadUpdatedAll());
        DictionaryPipelineExecutor executor(pipeline, configuration.use_async_executor);
        pipeline.setConcurrencyControl(false);
        update_field_loaded_block.reset();
        Block block;

        while (executor.pull(block))
        {
            if (!block.rows())
                continue;

            convertToFullIfSparse(block);

            /// We are using this to keep saved data if input stream consists of multiple blocks
            if (!update_field_loaded_block)
                update_field_loaded_block = std::make_shared<DB::Block>(block.cloneEmpty());

            for (size_t attribute_index = 0; attribute_index < block.columns(); ++attribute_index)
            {
                const IColumn & update_column = *block.getByPosition(attribute_index).column.get();
                MutableColumnPtr saved_column = update_field_loaded_block->getByPosition(attribute_index).column->assumeMutable();
                saved_column->insertRangeFrom(update_column, 0, update_column.size());
            }
        }
    }
    else
    {
        auto pipe = source_ptr->loadUpdatedAll();
        mergeBlockWithPipe<dictionary_key_type>(
            dict_struct.getKeysSize(),
            *update_field_loaded_block,
            std::move(pipe));
    }

    if (update_field_loaded_block)
    {
        resize(update_field_loaded_block->rows());
        DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
        blockToAttributes(*update_field_loaded_block.get(), arena_holder, /* shard = */ 0);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::blockToAttributes(const Block & block, DictionaryKeysArenaHolder<dictionary_key_type> & arena_holder, size_t shard)
{
    if (unlikely(shard >= configuration.shards))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shard number {} is out of range: 0..{}", shard, configuration.shards - 1);

    size_t skip_keys_size_offset = dict_struct.getKeysSize();

    Columns key_columns;
    key_columns.reserve(skip_keys_size_offset);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < skip_keys_size_offset; ++i)
        key_columns.emplace_back(block.safeGetByPosition(i).column);

    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    Field column_value_to_insert;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();

        auto it = key_attribute.containers[shard].find(key);

        if (it != key_attribute.containers[shard].end())
        {
            keys_extractor.rollbackCurrentKey();
            continue;
        }

        if constexpr (std::is_same_v<KeyType, StringRef>)
            key = copyStringInArena(*string_arenas[shard], key);

        key_attribute.containers[shard].insert({key, element_counts[shard]});

        for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
        {
            const IColumn & attribute_column = *block.safeGetByPosition(skip_keys_size_offset + attribute_index).column;
            auto & attribute = attributes[attribute_index];
            bool attribute_is_nullable = attribute.is_index_null.has_value();

            attribute_column.get(key_index, column_value_to_insert);

            auto type_call = [&](const auto & dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using AttributeValueType = DictionaryValueType<AttributeType>;

                auto & attribute_container = std::get<AttributeContainerShardsType<AttributeValueType>>(attribute.containers)[shard];
                attribute_container.emplace_back();

                if (attribute_is_nullable)
                {
                    (*attribute.is_index_null)[shard].emplace_back();

                    if (column_value_to_insert.isNull())
                    {
                        (*attribute.is_index_null)[shard].back() = true;
                        return;
                    }
                }

                if constexpr (std::is_same_v<AttributeValueType, StringRef>)
                {
                    String & value_to_insert = column_value_to_insert.safeGet<String>();
                    StringRef string_in_arena_reference = copyStringInArena(*string_arenas[shard], value_to_insert);
                    attribute_container.back() = string_in_arena_reference;
                }
                else
                {
                    auto value_to_insert = static_cast<AttributeValueType>(column_value_to_insert.safeGet<AttributeValueType>());
                    attribute_container.back() = value_to_insert;
                }
            };

            callOnDictionaryAttributeType(attribute.type, type_call);
        }

        ++element_counts[shard];
        ++total_element_count;
        keys_extractor.rollbackCurrentKey();
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::resize(size_t total_rows)
{
    if (unlikely(!total_rows))
        return;

    /// In multi shards configuration it is pointless.
    if constexpr (sharded)
        return;

    for (auto & container : key_attribute.containers)
        container.reserve(total_rows);
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
template <typename KeysProvider>
ColumnPtr HashedArrayDictionary<dictionary_key_type, sharded>::getAttributeColumn(
    const Attribute & attribute,
    const DictionaryAttribute & dictionary_attribute,
    size_t keys_size,
    DefaultOrFilter default_or_filter,
    KeysProvider && keys_object) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(default_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefault>(default_or_filter));

    ColumnPtr result;

    bool is_attribute_nullable = attribute.is_index_null.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (attribute.is_index_null)
    {
        col_null_map_to = ColumnUInt8::create(keys_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        auto column = ColumnProvider::getColumn(dictionary_attribute, keys_size);

        if (is_short_circuit)
        {
            IColumn::Filter & default_mask = std::get<RefFilter>(default_or_filter).get();

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                auto * out = column.get();

                getItemsShortCircuitImpl<ValueType, false>(
                    attribute, keys_object, [&](const size_t, const Array & value, bool) { out->insert(value); }, default_mask);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                auto * out = column.get();

                if (is_attribute_nullable)
                    getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        keys_object,
                        [&](size_t row, StringRef value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out->insertData(value.data, value.size);
                        },
                        default_mask);
                else
                    getItemsShortCircuitImpl<ValueType, false>(
                        attribute,
                        keys_object,
                        [&](size_t, StringRef value, bool) { out->insertData(value.data, value.size); },
                        default_mask);
            }
            else
            {
                auto & out = column->getData();

                if (is_attribute_nullable)
                    getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        keys_object,
                        [&](size_t row, const auto value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out[row] = value;
                        },
                        default_mask);
                else
                    getItemsShortCircuitImpl<ValueType, false>(
                        attribute, keys_object, [&](size_t row, const auto value, bool) { out[row] = value; }, default_mask);
            }
        }
        else
        {
            const ColumnPtr & default_values_column = std::get<RefDefault>(default_or_filter).get();

            DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(
                dictionary_attribute.null_value, default_values_column);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                auto * out = column.get();

                getItemsImpl<ValueType, false>(
                    attribute,
                    keys_object,
                    [&](const size_t, const Array & value, bool) { out->insert(value); },
                    default_value_extractor);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                auto * out = column.get();

                if (is_attribute_nullable)
                    getItemsImpl<ValueType, true>(
                        attribute,
                        keys_object,
                        [&](size_t row, StringRef value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out->insertData(value.data, value.size);
                        },
                        default_value_extractor);
                else
                    getItemsImpl<ValueType, false>(
                        attribute,
                        keys_object,
                        [&](size_t, StringRef value, bool) { out->insertData(value.data, value.size); },
                        default_value_extractor);
            }
            else
            {
                auto & out = column->getData();

                if (is_attribute_nullable)
                    getItemsImpl<ValueType, true>(
                        attribute,
                        keys_object,
                        [&](size_t row, const auto value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out[row] = value;
                        },
                        default_value_extractor);
                else
                    getItemsImpl<ValueType, false>(
                        attribute,
                        keys_object,
                        [&](size_t row, const auto value, bool) { out[row] = value; },
                        default_value_extractor);
            }
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedArrayDictionary<dictionary_key_type, sharded>::getItemsImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_containers = std::get<AttributeContainerShardsType<AttributeType>>(attribute.containers);
    const size_t keys_size = keys_extractor.getKeysSize();

    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();
        auto shard = getShard(key);
        const auto & key_attribute_container = key_attribute.containers[shard];
        const auto & attribute_container = attribute_containers[shard];

        const auto it = key_attribute_container.find(key);

        if (it != key_attribute_container.end())
        {
            size_t element_index = it->getMapped();

            const auto & element = attribute_container[element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[shard][element_index]);
            else
                set_value(key_index, element, false);

            ++keys_found;
        }
        else
        {
            if constexpr (is_nullable)
                set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
            else
                set_value(key_index, default_value_extractor[key_index], false);
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
template <typename AttributeType, bool is_nullable, typename ValueSetter>
void HashedArrayDictionary<dictionary_key_type, sharded>::getItemsShortCircuitImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value,
    IColumn::Filter & default_mask) const
{
    const auto & attribute_containers = std::get<AttributeContainerShardsType<AttributeType>>(attribute.containers);
    const size_t keys_size = keys_extractor.getKeysSize();
    default_mask.resize(keys_size);
    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();
        auto shard = getShard(key);
        const auto & key_attribute_container = key_attribute.containers[shard];
        const auto & attribute_container = attribute_containers[shard];

        const auto it = key_attribute_container.find(key);

        if (it != key_attribute_container.end())
        {
            size_t element_index = it->getMapped();

            const auto & element = attribute_container[element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[shard][element_index]);
            else
                set_value(key_index, element, false);

            default_mask[key_index] = 0;

            ++keys_found;
        }
        else
        {
            default_mask[key_index] = 1;
            set_value(key_index, AttributeType{}, true);
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedArrayDictionary<dictionary_key_type, sharded>::getItemsImpl(
    const Attribute & attribute,
    const KeyIndexToElementIndex & key_index_to_element_index,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_containers = std::get<AttributeContainerShardsType<AttributeType>>(attribute.containers);
    const size_t keys_size = key_index_to_element_index.size();
    size_t shard = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        ssize_t element_index;
        if constexpr (sharded)
        {
            element_index = key_index_to_element_index[key_index].first;
            shard = key_index_to_element_index[key_index].second;
        }
        else
        {
            element_index = key_index_to_element_index[key_index];
        }

        if (element_index != -1)
        {
            const auto & attribute_container = attribute_containers[shard];

            size_t found_element_index = static_cast<size_t>(element_index);
            const auto & element = attribute_container[found_element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[shard][found_element_index]);
            else
                set_value(key_index, element, false);
        }
        else
        {
            if constexpr (is_nullable)
                set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
            else
                set_value(key_index, default_value_extractor[key_index], false);
        }
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
template <typename AttributeType, bool is_nullable, typename ValueSetter>
void HashedArrayDictionary<dictionary_key_type, sharded>::getItemsShortCircuitImpl(
    const Attribute & attribute,
    const KeyIndexToElementIndex & key_index_to_element_index,
    ValueSetter && set_value,
    IColumn::Filter & default_mask [[maybe_unused]]) const
{
    const auto & attribute_containers = std::get<AttributeContainerShardsType<AttributeType>>(attribute.containers);
    const size_t keys_size = key_index_to_element_index.size();
    size_t shard = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        ssize_t element_index;
        if constexpr (sharded)
        {
            element_index = key_index_to_element_index[key_index].first;
            shard = key_index_to_element_index[key_index].second;
        }
        else
        {
            element_index = key_index_to_element_index[key_index];
        }

        if (element_index != -1)
        {
            const auto & attribute_container = attribute_containers[shard];

            size_t found_element_index = static_cast<size_t>(element_index);
            const auto & element = attribute_container[found_element_index];

            if constexpr (is_nullable)
                set_value(key_index, element, (*attribute.is_index_null)[shard][found_element_index]);
            else
                set_value(key_index, element, false);
        }
        else
        {
            set_value(key_index, AttributeType{}, true);
        }
    }
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        std::optional<DictionaryParallelLoaderType> parallel_loader;
        if constexpr (sharded)
            parallel_loader.emplace(*this);

        QueryPipeline pipeline(source_ptr->loadAll());
        DictionaryPipelineExecutor executor(pipeline, configuration.use_async_executor);
        pipeline.setConcurrencyControl(false);

        UInt64 pull_time_microseconds = 0;
        UInt64 process_time_microseconds = 0;

        size_t total_rows = 0;
        size_t total_blocks = 0;
        String dictionary_name = getFullName();

        Block block;
        while (true)
        {
            Stopwatch watch_pull;
            bool has_data = executor.pull(block);
            pull_time_microseconds += watch_pull.elapsedMicroseconds();

            if (!has_data)
                break;

            ++total_blocks;
            total_rows += block.rows();

            Stopwatch watch_process;
            resize(total_rows);

            if (parallel_loader)
            {
                parallel_loader->addBlock(std::move(block));
            }
            else
            {
                DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
                blockToAttributes(block, arena_holder, /* shard = */ 0);
            }
            process_time_microseconds += watch_process.elapsedMicroseconds();
        }

        if (parallel_loader)
            parallel_loader->finish();

        LOG_DEBUG(log,
            "Finished {}reading {} blocks with {} rows to dictionary {} from pipeline in {:.2f} sec and inserted into hashtable in {:.2f} sec",
            configuration.use_async_executor ? "asynchronous " : "",
            total_blocks, total_rows,
            dictionary_name,
            pull_time_microseconds / 1000000.0, process_time_microseconds / 1000000.0);
    }
    else
    {
        updateData();
    }

    if (configuration.require_nonempty && 0 == total_element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.",
            getFullName());
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::buildHierarchyParentToChildIndexIfNeeded()
{
    if (!dict_struct.hierarchical_attribute_index)
        return;

    if (dict_struct.attributes[*dict_struct.hierarchical_attribute_index].bidirectional)
        hierarchical_index = getHierarchicalIndex();
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
void HashedArrayDictionary<dictionary_key_type, sharded>::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & container : key_attribute.containers)
        bytes_allocated += container.size();

    for (auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            for (const auto & container : std::get<AttributeContainerShardsType<ValueType>>(attribute.containers))
            {
                bytes_allocated += sizeof(AttributeContainerType<ValueType>);

                if constexpr (std::is_same_v<ValueType, Array>)
                {
                    /// It is not accurate calculations
                    bytes_allocated += sizeof(Array) * container.size();
                }
                else
                {
                    bytes_allocated += container.allocated_bytes();
                }

                bucket_count += container.capacity();
            }
        };

        callOnDictionaryAttributeType(attribute.type, type_call);

        if (attribute.is_index_null.has_value())
            for (const auto & container : attribute.is_index_null.value())
                bytes_allocated += container.size();
    }

    /// `bucket_count` should be a sum over all shards,
    /// but it should not be a sum over all attributes, since it is used to
    /// calculate load_factor like this: `element_count / bucket_count`
    /// While element_count is a sum over all shards, not over all attributes.
    if (attributes.size())
        bucket_count /= attributes.size();

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();

    if (hierarchical_index)
    {
        hierarchical_index_bytes_allocated = hierarchical_index->getSizeInBytes();
        bytes_allocated += hierarchical_index_bytes_allocated;
    }
    for (const auto & string_arena : string_arenas)
        bytes_allocated += string_arena->allocatedBytes();
}

template <DictionaryKeyType dictionary_key_type, bool sharded>
Pipe HashedArrayDictionary<dictionary_key_type, sharded>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    PaddedPODArray<HashedArrayDictionary::KeyType> keys;
    keys.reserve(total_element_count);

    for (const auto & container : key_attribute.containers)
        for (auto & [key, _] : container)
            keys.emplace_back(key);

    ColumnsWithTypeAndName key_columns;

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        auto keys_column = getColumnFromPODArray(std::move(keys));
        key_columns = {ColumnWithTypeAndName(std::move(keys_column), std::make_shared<DataTypeUInt64>(), dict_struct.id->name)};
    }
    else
    {
        key_columns = deserializeColumnsWithTypeAndNameFromKeys(dict_struct, keys, 0, keys.size());
    }

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator = std::make_shared<DictionarySourceCoordinator>(dictionary, column_names, std::move(key_columns), max_block_size);
    auto result = coordinator->read(num_streams);

    return result;
}

template class HashedArrayDictionary<DictionaryKeyType::Simple, /* sharded */ false>;
template class HashedArrayDictionary<DictionaryKeyType::Simple, /* sharded */ true>;
template class HashedArrayDictionary<DictionaryKeyType::Complex, /* sharded */ false>;
template class HashedArrayDictionary<DictionaryKeyType::Complex, /* sharded */ true>;

void registerDictionaryArrayHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             ContextPtr global_context,
                             DictionarySourcePtr source_ptr,
                             DictionaryKeyType dictionary_key_type) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed array dictionary");
        if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed array dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        std::string dictionary_layout_name = dictionary_key_type == DictionaryKeyType::Simple ? "hashed_array" : "complex_key_hashed_array";
        std::string dictionary_layout_prefix = ".layout." + dictionary_layout_name;

        Int64 shards = config.getInt(config_prefix + dictionary_layout_prefix + ".shards", 1);
        if (shards <= 0 || 128 < shards)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARDS parameter should be within [1, 128]", full_name);

        Int64 shard_load_queue_backlog = config.getInt(config_prefix + dictionary_layout_prefix + ".shard_load_queue_backlog", 10000);
        if (shard_load_queue_backlog <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: SHARD_LOAD_QUEUE_BACKLOG parameter should be greater then zero", full_name);

        if (source_ptr->hasUpdateField() && shards > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: SHARDS parameter does not supports for updatable source (UPDATE_FIELD)", full_name);

        HashedArrayDictionaryStorageConfiguration configuration{require_nonempty, dict_lifetime, static_cast<size_t>(shards), static_cast<UInt64>(shard_load_queue_backlog)};

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
        const auto & settings = context->getSettingsRef();

        const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
        configuration.use_async_executor = clickhouse_source && clickhouse_source->isLocal() && settings[Setting::dictionary_use_async_executor];

        if (settings[Setting::max_execution_time].totalSeconds() > 0)
            configuration.load_timeout = std::chrono::seconds(settings[Setting::max_execution_time].totalSeconds());

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            if (shards > 1)
                return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Simple, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Simple, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
        }

        if (shards > 1)
            return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Complex, true>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        return std::make_unique<HashedArrayDictionary<DictionaryKeyType::Complex, false>>(
            dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    factory.registerLayout("hashed_array",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/)
        {
            return create_layout(a, b, c, d, global_context, std::move(e), DictionaryKeyType::Simple);
        }, false);
    factory.registerLayout("complex_key_hashed_array",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/)
        {
            return create_layout(a, b, c, d, global_context, std::move(e), DictionaryKeyType::Complex);
        }, true);
}

}
