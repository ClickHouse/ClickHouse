#include <Dictionaries/HashedDictionary.h>

#include <numeric>
#include <boost/noncopyable.hpp>

#include <Common/ArenaUtils.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/scope_guard_safe.h>

#include <Core/Defines.h>

#include <DataTypes/DataTypesDecimal.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>
#include <Dictionaries/HashedDictionaryCollectionTraits.h>

namespace CurrentMetrics
{
    extern const Metric HashedDictionaryThreads;
    extern const Metric HashedDictionaryThreadsActive;
}

namespace DB
{

using namespace HashedDictionaryImpl;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded> class HashedDictionary;

/// Implementation parallel dictionary load for SHARDS
template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
class ParallelDictionaryLoader : public boost::noncopyable
{
    using HashedDictionary = HashedDictionary<dictionary_key_type, sparse, sharded>;

public:
    explicit ParallelDictionaryLoader(HashedDictionary & dictionary_)
        : dictionary(dictionary_)
        , shards(dictionary.configuration.shards)
        , pool(CurrentMetrics::HashedDictionaryThreads, CurrentMetrics::HashedDictionaryThreadsActive, shards)
        , shards_queues(shards)
    {
        UInt64 backlog = dictionary.configuration.shard_load_queue_backlog;
        LOG_TRACE(dictionary.log, "Will load the dictionary using {} threads (with {} backlog)", shards, backlog);

        shards_slots.resize(shards);
        std::iota(shards_slots.begin(), shards_slots.end(), 0);

        for (size_t shard = 0; shard < shards; ++shard)
        {
            shards_queues[shard].emplace(backlog);
            pool.scheduleOrThrowOnError([this, shard, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );

                /// Do not account memory that was occupied by the dictionaries for the query/user context.
                MemoryTrackerBlockerInThread memory_blocker;

                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);
                setThreadName("HashedDictLoad");

                threadWorker(shard);
            });
        }
    }

    void addBlock(Block block)
    {
        IColumn::Selector selector = createShardSelector(block, shards_slots);
        Blocks shards_blocks = splitBlock(selector, block);

        for (size_t shard = 0; shard < shards; ++shard)
        {
            if (!shards_queues[shard]->push(std::move(shards_blocks[shard])))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to shards queue #{}", shard);
        }
    }

    void finish()
    {
        for (auto & queue : shards_queues)
            queue->finish();

        Stopwatch watch;
        pool.wait();
        UInt64 elapsed_ms = watch.elapsedMilliseconds();
        LOG_TRACE(dictionary.log, "Processing the tail took {}ms", elapsed_ms);
    }

    ~ParallelDictionaryLoader()
    {
        try
        {
            for (auto & queue : shards_queues)
                queue->clearAndFinish();

            /// NOTE: It is OK to not pass the exception next, since on success finish() should be called which will call wait()
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(dictionary.log, "Exception had been thrown during parallel load of the dictionary");
        }
    }

private:
    HashedDictionary & dictionary;
    const size_t shards;
    ThreadPool pool;
    std::vector<std::optional<ConcurrentBoundedQueue<Block>>> shards_queues;
    std::vector<UInt64> shards_slots;
    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;

    void threadWorker(size_t shard)
    {
        Block block;
        DictionaryKeysArenaHolder<dictionary_key_type> arena_holder_;
        auto & shard_queue = *shards_queues[shard];

        while (shard_queue.pop(block))
        {
            Stopwatch watch;
            dictionary.blockToAttributes(block, arena_holder_, shard);
            UInt64 elapsed_ms = watch.elapsedMilliseconds();
            if (elapsed_ms > 1'000)
                LOG_TRACE(dictionary.log, "Block processing for shard #{} is slow {}ms (rows {}).", shard, elapsed_ms, block.rows());
        }

        if (!shard_queue.isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pull non finished shards queue #{}", shard);
    }

    /// Split block to shards smaller block, using 'selector'.
    Blocks splitBlock(const IColumn::Selector & selector, const Block & block)
    {
        Blocks out_blocks(shards);
        for (size_t shard = 0; shard < shards; ++shard)
            out_blocks[shard] = block.cloneEmpty();

        size_t columns = block.columns();
        for (size_t col = 0; col < columns; ++col)
        {
            MutableColumns splitted_columns = block.getByPosition(col).column->scatter(shards, selector);
            for (size_t shard = 0; shard < shards; ++shard)
                out_blocks[shard].getByPosition(col).column = std::move(splitted_columns[shard]);
        }

        return out_blocks;
    }

    IColumn::Selector createShardSelector(const Block & block, const std::vector<UInt64> & slots)
    {
        size_t num_rows = block.rows();
        IColumn::Selector selector(num_rows);

        size_t skip_keys_size_offset = dictionary.dict_struct.getKeysSize();
        Columns key_columns;
        key_columns.reserve(skip_keys_size_offset);
        for (size_t i = 0; i < skip_keys_size_offset; ++i)
            key_columns.emplace_back(block.safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto key = keys_extractor.extractCurrentKey();
            size_t shard = dictionary.getShard(key);
            selector[i] = slots[shard];
            keys_extractor.rollbackCurrentKey();
        }

        return selector;
    }
};

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
HashedDictionary<dictionary_key_type, sparse, sharded>::HashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const HashedDictionaryConfiguration & configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , log(&Poco::Logger::get("HashedDictionary"))
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

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
HashedDictionary<dictionary_key_type, sparse, sharded>::~HashedDictionary()
{
    /// Do a regular sequential destroy in case of non sharded dictionary
    ///
    /// Note, that even in non-sharded dictionaries you can have multiple hash
    /// tables, since each attribute is stored in a separate hash table.
    if constexpr (!sharded)
        return;

    size_t shards = std::max<size_t>(configuration.shards, 1);
    ThreadPool pool(CurrentMetrics::HashedDictionaryThreads, CurrentMetrics::HashedDictionaryThreadsActive, shards);

    size_t hash_tables_count = 0;
    auto schedule_destroy = [&hash_tables_count, &pool](auto & container)
    {
        if (container.empty())
            return;

        pool.trySchedule([&container, thread_group = CurrentThread::getGroup()]
        {
            SCOPE_EXIT_SAFE(
                if (thread_group)
                    CurrentThread::detachFromGroupIfNotDetached();
            );

            /// Do not account memory that was occupied by the dictionaries for the query/user context.
            MemoryTrackerBlockerInThread memory_blocker;

            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);
            setThreadName("HashedDictDtor");

            clearContainer(container);
        });

        ++hash_tables_count;
    };

    if (attributes.empty())
    {
        for (size_t shard = 0; shard < shards; ++shard)
        {
            schedule_destroy(no_attributes_containers[shard]);
        }
    }
    else
    {
        for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
        {
            getAttributeContainers(attribute_index, [&](auto & containers)
            {
                for (size_t shard = 0; shard < shards; ++shard)
                {
                    schedule_destroy(containers[shard]);
                }
            });
        }
    }

    LOG_TRACE(log, "Destroying {} non empty hash tables (using {} threads)", hash_tables_count, pool.getMaxThreads());
    pool.wait();
    LOG_TRACE(log, "Hash tables destroyed");
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
ColumnPtr HashedDictionary<dictionary_key_type, sparse, sharded>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types [[maybe_unused]],
    const ColumnPtr & default_values_column) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    const size_t size = extractor.getKeysSize();

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    auto & attribute = attributes[attribute_index];

    bool is_attribute_nullable = attribute.is_nullable_sets.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (is_attribute_nullable)
    {
        col_null_map_to = ColumnUInt8::create(size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(dictionary_attribute.null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType, false>(
                attribute,
                extractor,
                [&](const size_t, const Array & value, bool) { out->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    extractor,
                    [&](size_t row, StringRef value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out->insertData(value.data, value.size);
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    extractor,
                    [&](size_t, StringRef value, bool) { out->insertData(value.data, value.size); },
                    default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            if (is_attribute_nullable)
                getItemsImpl<ValueType, true>(
                    attribute,
                    extractor,
                    [&](size_t row, const auto value, bool is_null)
                    {
                        (*vec_null_map_to)[row] = is_null;
                        out[row] = value;
                    },
                    default_value_extractor);
            else
                getItemsImpl<ValueType, false>(
                    attribute,
                    extractor,
                    [&](size_t row, const auto value, bool) { out[row] = value; },
                    default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse, sharded>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::Complex)
        dict_struct.validateKeyTypes(key_types);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, arena_holder.getComplexKeyArena());

    size_t keys_size = extractor.getKeysSize();

    auto result = ColumnUInt8::create(keys_size, false);
    auto & out = result->getData();

    size_t keys_found = 0;

    if (unlikely(attributes.empty()))
    {
        for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
        {
            auto key = extractor.extractCurrentKey();
            const auto & container = no_attributes_containers[getShard(key)];
            out[requested_key_index] = container.find(key) != container.end();
            keys_found += out[requested_key_index];
            extractor.rollbackCurrentKey();
        }

        query_count.fetch_add(keys_size, std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
        return result;
    }

    const auto & attribute = attributes.front();
    bool is_attribute_nullable = attribute.is_nullable_sets.has_value();

    getAttributeContainers(0 /*attribute_index*/, [&](const auto & containers)
    {
        for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
        {
            auto key = extractor.extractCurrentKey();
            auto shard = getShard(key);
            const auto & container = containers[shard];

            out[requested_key_index] = container.find(key) != container.end();
            if (is_attribute_nullable && !out[requested_key_index])
                out[requested_key_index] = (*attribute.is_nullable_sets)[shard].find(key) != nullptr;

            keys_found += out[requested_key_index];

            extractor.rollbackCurrentKey();
        }
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
ColumnPtr HashedDictionary<dictionary_key_type, sparse, sharded>::getHierarchy(ColumnPtr key_column [[maybe_unused]], const DataTypePtr &) const
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
            null_value = dictionary_attribute.null_value.get<UInt64>();

        const CollectionsHolder<UInt64> & child_key_to_parent_key_maps = std::get<CollectionsHolder<UInt64>>(hierarchical_attribute.containers);

        auto is_key_valid_func = [&](auto & hierarchy_key)
        {
            auto shard = getShard(hierarchy_key);

            if (unlikely(hierarchical_attribute.is_nullable_sets) && (*hierarchical_attribute.is_nullable_sets)[shard].find(hierarchy_key))
                return true;

            const auto & map = child_key_to_parent_key_maps[shard];
            return map.find(hierarchy_key) != map.end();
        };

        size_t keys_found = 0;

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            const auto & map = child_key_to_parent_key_maps[getShard(hierarchy_key)];
            auto it = map.find(hierarchy_key);
            if (it == map.end())
                return result;

            UInt64 parent_key = getValueFromCell(it);
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

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse, sharded>::isInHierarchy(
    ColumnPtr key_column [[maybe_unused]],
    ColumnPtr in_key_column [[maybe_unused]],
    const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (key_column->isNullable())
            key_column = assert_cast<const ColumnNullable *>(key_column.get())->getNestedColumnPtr();

        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        PaddedPODArray<UInt64> keys_in_backup_storage;
        const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        std::optional<UInt64> null_value;

        if (!dictionary_attribute.null_value.isNull())
            null_value = dictionary_attribute.null_value.get<UInt64>();

        const CollectionsHolder<UInt64> & child_key_to_parent_key_maps = std::get<CollectionsHolder<UInt64>>(hierarchical_attribute.containers);

        auto is_key_valid_func = [&](auto & hierarchy_key)
        {
            auto shard = getShard(hierarchy_key);

            if (unlikely(hierarchical_attribute.is_nullable_sets) && (*hierarchical_attribute.is_nullable_sets)[shard].find(hierarchy_key))
                return true;

            const auto & map = child_key_to_parent_key_maps[shard];
            return map.find(hierarchy_key) != map.end();
        };

        size_t keys_found = 0;

        auto get_parent_key_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            const auto & map = child_key_to_parent_key_maps[getShard(hierarchy_key)];
            auto it = map.find(hierarchy_key);
            if (it == map.end())
                return result;

            UInt64 parent_key = getValueFromCell(it);
            if (null_value && *null_value == parent_key)
                return result;

            result = parent_key;
            keys_found += 1;

            return result;
        };

        auto result = getKeysIsInHierarchyColumn(keys, keys_in, is_key_valid_func, get_parent_key_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);

        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
DictionaryHierarchyParentToChildIndexPtr HashedDictionary<dictionary_key_type, sparse, sharded>::getHierarchicalIndex() const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (hierarchical_index)
            return hierarchical_index;

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;
        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
        const CollectionsHolder<UInt64> & child_key_to_parent_key_maps = std::get<CollectionsHolder<UInt64>>(hierarchical_attribute.containers);

        size_t size = 0;
        for (const auto & map : child_key_to_parent_key_maps)
            size += map.size();

        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;
        parent_to_child.reserve(size);

        for (const auto & map : child_key_to_parent_key_maps)
        {
            for (const auto & [child_key, parent_key] : map)
            {
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

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
ColumnPtr HashedDictionary<dictionary_key_type, sparse, sharded>::getDescendants(
    ColumnPtr key_column [[maybe_unused]],
    const DataTypePtr &,
    size_t level [[maybe_unused]],
    DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index [[maybe_unused]]) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t keys_found;
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

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    HashTableGrowerWithPrecalculationAndMaxLoadFactor grower(configuration.max_load_factor);

    for (const auto & dictionary_attribute : dict_struct.attributes)
    {
        auto type_call = [&, this](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            auto is_nullable_sets = dictionary_attribute.is_nullable ? std::make_optional<NullableSets>(configuration.shards) : std::optional<NullableSets>{};
            if constexpr (IsBuiltinHashTable<typename CollectionsHolder<ValueType>::value_type>)
            {
                CollectionsHolder<ValueType> collections;
                collections.reserve(configuration.shards);
                for (size_t i = 0; i < configuration.shards; ++i)
                    collections.emplace_back(grower);

                Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_sets), std::move(collections)};
                attributes.emplace_back(std::move(attribute));
            }
            else
            {
                Attribute attribute{dictionary_attribute.underlying_type, std::move(is_nullable_sets), CollectionsHolder<ValueType>(configuration.shards)};
                for (auto & container : std::get<CollectionsHolder<ValueType>>(attribute.containers))
                    container.max_load_factor(configuration.max_load_factor);
                attributes.emplace_back(std::move(attribute));
            }

            if constexpr (IsBuiltinHashTable<typename CollectionsHolder<ValueType>::value_type>)
                LOG_TRACE(log, "Using builtin hash table for {} attribute", dictionary_attribute.name);
            else
                LOG_TRACE(log, "Using sparsehash for {} attribute", dictionary_attribute.name);
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }

    if (unlikely(attributes.size()) == 0)
    {
        no_attributes_containers.reserve(configuration.shards);
        for (size_t i = 0; i < configuration.shards; ++i)
            no_attributes_containers.emplace_back(grower);
    }

    string_arenas.resize(configuration.shards);
    for (auto & arena : string_arenas)
        arena = std::make_unique<Arena>();
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::updateData()
{
    /// NOTE: updateData() does not preallocation since it may increase memory usage.

    if (!update_field_loaded_block || update_field_loaded_block->rows() == 0)
    {
        QueryPipeline pipeline(source_ptr->loadUpdatedAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
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
        blockToAttributes(*update_field_loaded_block.get(), arena_holder, /* shard= */ 0);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::blockToAttributes(const Block & block, DictionaryKeysArenaHolder<dictionary_key_type> & arena_holder, UInt64 shard)
{
    size_t skip_keys_size_offset = dict_struct.getKeysSize();
    size_t new_element_count = 0;

    Columns key_columns;
    key_columns.reserve(skip_keys_size_offset);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < skip_keys_size_offset; ++i)
        key_columns.emplace_back(block.safeGetByPosition(i).column);

    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    Field column_value_to_insert;

    size_t attributes_size = attributes.size();

    if (unlikely(attributes_size == 0))
    {
        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            auto key = keys_extractor.extractCurrentKey();

            if constexpr (std::is_same_v<KeyType, StringRef>)
                key = copyStringInArena(*string_arenas[shard], key);

            no_attributes_containers[shard].insert(key);
            keys_extractor.rollbackCurrentKey();
            ++new_element_count;
        }

        element_count += new_element_count;
        return;
    }

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
    {
        const IColumn & attribute_column = *block.safeGetByPosition(skip_keys_size_offset + attribute_index).column;
        auto & attribute = attributes[attribute_index];
        bool attribute_is_nullable = attribute.is_nullable_sets.has_value();

        /// Number of elements should not take into account multiple attributes.
        new_element_count = 0;

        getAttributeContainers(attribute_index, [&](auto & containers)
        {
            using ContainerType = std::decay_t<decltype(containers.front())>;
            using AttributeValueType = typename ContainerType::mapped_type;

            for (size_t key_index = 0; key_index < keys_size; ++key_index)
            {
                auto key = keys_extractor.extractCurrentKey();
                auto & container = containers[shard];

                auto it = container.find(key);
                bool key_is_nullable_and_already_exists = attribute_is_nullable && (*attribute.is_nullable_sets)[shard].find(key) != nullptr;

                if (key_is_nullable_and_already_exists || it != container.end())
                {
                    keys_extractor.rollbackCurrentKey();
                    continue;
                }

                if constexpr (std::is_same_v<KeyType, StringRef>)
                    key = copyStringInArena(*string_arenas[shard], key);

                attribute_column.get(key_index, column_value_to_insert);

                if (attribute_is_nullable && column_value_to_insert.isNull())
                {
                    (*attribute.is_nullable_sets)[shard].insert(key);
                    ++new_element_count;
                    keys_extractor.rollbackCurrentKey();
                    continue;
                }

                if constexpr (std::is_same_v<AttributeValueType, StringRef>)
                {
                    String & value_to_insert = column_value_to_insert.get<String>();
                    StringRef arena_value = copyStringInArena(*string_arenas[shard], value_to_insert);
                    container.insert({key, arena_value});
                }
                else
                {
                    auto value_to_insert = static_cast<AttributeValueType>(column_value_to_insert.get<AttributeValueType>());
                    container.insert({key, value_to_insert});
                }

                ++new_element_count;
                keys_extractor.rollbackCurrentKey();
            }

            keys_extractor.reset();
        });
    }

    element_count += new_element_count;
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::resize(size_t added_rows)
{
    if (unlikely(!added_rows))
        return;

    /// In multi shards configuration it is pointless.
    if constexpr (sharded)
        return;

    size_t attributes_size = attributes.size();

    if (unlikely(attributes_size == 0))
    {
        size_t reserve_size = added_rows + no_attributes_containers.front().size();
        resizeContainer(no_attributes_containers.front(), reserve_size);
        return;
    }

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
    {
        getAttributeContainers(attribute_index, [added_rows](auto & containers)
        {
            auto & container = containers.front();
            size_t reserve_size = added_rows + container.size();
            resizeContainer(container, reserve_size);
        });
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void HashedDictionary<dictionary_key_type, sparse, sharded>::getItemsImpl(
    const Attribute & attribute,
    DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
    ValueSetter && set_value [[maybe_unused]],
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_containers = std::get<CollectionsHolder<AttributeType>>(attribute.containers);
    const size_t keys_size = keys_extractor.getKeysSize();

    size_t keys_found = 0;

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        auto key = keys_extractor.extractCurrentKey();
        auto shard = getShard(key);

        const auto & container = attribute_containers[getShard(key)];
        const auto it = container.find(key);

        if (it != container.end())
        {
            set_value(key_index, getValueFromCell(it), false);
            ++keys_found;
        }
        else
        {
            if constexpr (is_nullable)
            {
                bool is_value_nullable = ((*attribute.is_nullable_sets)[shard].find(key) != nullptr) || default_value_extractor.isNullAt(key_index);
                set_value(key_index, default_value_extractor[key_index], is_value_nullable);
            }
            else
            {
                set_value(key_index, default_value_extractor[key_index], false);
            }
        }

        keys_extractor.rollbackCurrentKey();
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        std::optional<ParallelDictionaryLoader<dictionary_key_type, sparse, sharded>> parallel_loader;
        if constexpr (sharded)
            parallel_loader.emplace(*this);

        QueryPipeline pipeline = QueryPipeline(source_ptr->loadAll());

        PullingPipelineExecutor executor(pipeline);
        Block block;
        DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;

        while (executor.pull(block))
        {
            resize(block.rows());
            if (parallel_loader)
                parallel_loader->addBlock(block);
            else
                blockToAttributes(block, arena_holder, /* shard= */ 0);
        }

        if (parallel_loader)
            parallel_loader->finish();
    }
    else
    {
        updateData();
    }

    if (configuration.require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "{}: dictionary source is empty and 'require_nonempty' property is set.",
            getFullName());
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::buildHierarchyParentToChildIndexIfNeeded()
{
    if (!dict_struct.hierarchical_attribute_index)
        return;

    if (dict_struct.attributes[*dict_struct.hierarchical_attribute_index].bidirectional)
        hierarchical_index = getHierarchicalIndex();
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
void HashedDictionary<dictionary_key_type, sparse, sharded>::calculateBytesAllocated()
{
    size_t attributes_size = attributes.size();
    bytes_allocated += attributes_size * sizeof(attributes.front());

    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
    {
        /// bucket_count should be a sum over all shards (CollectionsHolder),
        /// but it should not be a sum over all attributes, since it is used to
        /// calculate load_factor like this:
        ///
        ///    element_count / bucket_count
        ///
        /// While element_count is a sum over all shards, not over all attributes.
        bucket_count = 0;

        getAttributeContainers(attribute_index, [&](const auto & containers)
        {
            for (const auto & container : containers)
            {
                bytes_allocated += sizeof(container);
                bytes_allocated += getBufferSizeInBytes(container);
                bucket_count += getBufferSizeInCells(container);
            }
        });

        const auto & attribute = attributes[attribute_index];
        bytes_allocated += sizeof(attribute.is_nullable_sets);

        if (attribute.is_nullable_sets.has_value())
        {
            for (auto & is_nullable_set : *attribute.is_nullable_sets)
                bytes_allocated += is_nullable_set.getBufferSizeInBytes();
        }
    }

    if (unlikely(attributes_size == 0))
    {
        for (const auto & container : no_attributes_containers)
        {
            bytes_allocated += sizeof(container);
            bytes_allocated += getBufferSizeInBytes(container);
            bucket_count += getBufferSizeInCells(container);
        }
    }

    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();

    if (hierarchical_index)
    {
        hierarchical_index_bytes_allocated = hierarchical_index->getSizeInBytes();
        bytes_allocated += hierarchical_index_bytes_allocated;
    }

    for (const auto & arena : string_arenas)
        bytes_allocated += arena->allocatedBytes();
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
Pipe HashedDictionary<dictionary_key_type, sparse, sharded>::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    PaddedPODArray<HashedDictionary::KeyType> keys;

    /// NOTE: could read multiple shards in parallel
    if (!attributes.empty())
    {
        const auto & attribute = attributes.front();

        getAttributeContainers(0 /*attribute_index*/, [&](auto & containers)
        {
            for (const auto & container : containers)
            {
                keys.reserve(container.size());

                for (const auto & [key, _] : container)
                {
                    keys.emplace_back(key);
                }
            }
        });

        if (attribute.is_nullable_sets)
        {
            for (auto & is_nullable_set : *attribute.is_nullable_sets)
            {
                keys.reserve(is_nullable_set.size());

                for (auto & node : is_nullable_set)
                    keys.emplace_back(node.getKey());
            }
        }
    }
    else
    {
        for (const auto & container : no_attributes_containers)
        {
            keys.reserve(keys.size() + container.size());

            for (const auto & key : container)
                keys.emplace_back(getSetKeyFromCell(key));
        }
    }

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

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
template <typename GetContainersFunc>
void HashedDictionary<dictionary_key_type, sparse, sharded>::getAttributeContainers(size_t attribute_index, GetContainersFunc && get_containers_func)
{
    assert(attribute_index < attributes.size());

    auto & attribute = attributes[attribute_index];

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        auto & attribute_containers = std::get<CollectionsHolder<ValueType>>(attribute.containers);
        std::forward<GetContainersFunc>(get_containers_func)(attribute_containers);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

template <DictionaryKeyType dictionary_key_type, bool sparse, bool sharded>
template <typename GetContainersFunc>
void HashedDictionary<dictionary_key_type, sparse, sharded>::getAttributeContainers(size_t attribute_index, GetContainersFunc && get_containers_func) const
{
    const_cast<std::decay_t<decltype(*this)> *>(this)->getAttributeContainers(attribute_index, [&](auto & attribute_containers)
    {
        std::forward<GetContainersFunc>(get_containers_func)(attribute_containers);
    });
}

template class HashedDictionary<DictionaryKeyType::Simple, false, /*sparse*/ false /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Simple, false /*sparse*/, true /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Simple, true /*sparse*/, false /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Simple, true /*sparse*/, true /*sharded*/>;

template class HashedDictionary<DictionaryKeyType::Complex, false /*sparse*/, false /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Complex, false /*sparse*/, true /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Complex, true /*sparse*/, false /*sharded*/>;
template class HashedDictionary<DictionaryKeyType::Complex, true /*sparse*/, true /*sharded*/>;

void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             DictionaryKeyType dictionary_key_type,
                             bool sparse) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        else if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        std::string dictionary_layout_name;

        if (dictionary_key_type == DictionaryKeyType::Simple)
            dictionary_layout_name = sparse ? "sparse_hashed" : "hashed";
        else
            dictionary_layout_name = sparse ? "complex_key_sparse_hashed" : "complex_key_hashed";

        const std::string dictionary_layout_prefix = ".layout." + dictionary_layout_name;
        const bool preallocate = config.getBool(config_prefix + dictionary_layout_prefix + ".preallocate", false);
        if (preallocate)
            LOG_WARNING(&Poco::Logger::get("HashedDictionary"), "'prellocate' attribute is obsolete, consider looking at 'shards'");

        Int64 shards = config.getInt(config_prefix + dictionary_layout_prefix + ".shards", 1);
        if (shards <= 0 || shards > 128)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARDS parameter should be within [1, 128]", full_name);

        Int64 shard_load_queue_backlog = config.getInt(config_prefix + dictionary_layout_prefix + ".shard_load_queue_backlog", 10000);
        if (shard_load_queue_backlog <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARD_LOAD_QUEUE_BACKLOG parameter should be greater then zero", full_name);

        float max_load_factor = static_cast<float>(config.getDouble(config_prefix + dictionary_layout_prefix + ".max_load_factor", 0.5));
        if (max_load_factor < 0.5f || max_load_factor > 0.99f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: max_load_factor parameter should be within [0.5, 0.99], got {}", full_name, max_load_factor);

        HashedDictionaryConfiguration configuration{
            static_cast<UInt64>(shards),
            static_cast<UInt64>(shard_load_queue_backlog),
            max_load_factor,
            require_nonempty,
            dict_lifetime,
        };

        if (source_ptr->hasUpdateField() && shards > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARDS parameter does not supports for updatable source (UPDATE_FIELD)", full_name);

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            if (sparse)
            {
                if (shards > 1)
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
                else
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            }
            else
            {
                if (shards > 1)
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
                else
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            }
        }
        else
        {
            if (sparse)
            {
                if (shards > 1)
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
                else
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            }
            else
            {
                if (shards > 1)
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
                else
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false, false>>(dict_id, dict_struct, std::move(source_ptr), configuration);
            }
        }
    };

    using namespace std::placeholders;

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Simple, /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Simple, /* sparse = */ true); }, false);
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Complex, /* sparse = */ false); }, true);
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr /* global_context */, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::Complex, /* sparse = */ true); }, true);

}

}
