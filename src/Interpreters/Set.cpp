#include <atomic>
#include <optional>
#include <shared_mutex>

#include <Core/Field.h>

#include <Columns/ColumnsNumber.h>

#include <Common/Logger.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/DiskSet.h>
#include <Interpreters/DiskSetBuilder.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <base/arithmeticOverflow.h>
#include <base/range.h>
#include <base/sort.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace ProfileEvents
{
    extern const Event SetsSpilledToDisk;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

Set::Set(const SizeLimits & limits_, size_t max_elements_to_fill_, bool transform_null_in_)
    :  limits(limits_), transform_null_in(transform_null_in_), max_elements_to_fill(max_elements_to_fill_)
    , log(getLogger("Set")), cast_cache(std::make_unique<InternalCastFunctionCache>())
{}


Set::~Set() = default;

namespace
{

/// On disk, a set keeps the same keys as its method of `SetVariants` keeps in memory, except for strings:
/// - Numbers (`key8` to `key64`) and packed keys (`keys32` to `keys256`, `nullable_keys128`,
///   `nullable_keys256`) stay as they are.
/// - `hashed` keys are already a 128-bit hash of the key columns and stay as they are.
/// - `key_string` and `key_fixed_string` keep the whole string in memory, but on disk only its 128-bit hash,
///   computed as the `hashed` method computes it.
/// `DiskSetMethod<Method>` is the method whose keys go to disk: `Method` itself, or `hashed` for strings.
template <typename Method>
using DiskSetMethod = std::conditional_t<
    std::is_same_v<typename Method::Key, std::string_view>,
    decltype(SetVariants::hashed)::element_type,
    Method>;

/// A set on disk stores the keys of its `DiskSetMethod` at the same width as in memory.
template <typename Method>
using DiskSetKey = typename DiskSetMethod<Method>::Key;

/// Converts key columns to fixed-width disk keys, skipping the rows marked in `null_map`.
/// Without a null map, the keys retain their input row positions for batched lookups.
template <typename Method>
ColumnPtr computeDiskSetKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map)
{
    typename Method::State state(key_columns, key_sizes, nullptr);
    Arena pool;
    auto keys = ColumnVector<typename Method::Key>::create();
    keys->reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (null_map && (*null_map)[row])
            continue;
        auto key_holder = state.getKeyHolder(row, pool);
        keys->getData().push_back(keyHolderGetKey(key_holder));
    }
    return keys;
}

}

void Set::setSpillSettings(SetSpillSettings spill_settings_)
{
    chassert(!isCreated());
    spill_settings = std::move(spill_settings_);
}

void Set::finishInsert()
{
    /// Only a set that `CreatingSetsTransform` fills can spill, and such a set is read only after
    /// `is_created` publishes it, so publishing the disk set here takes no lock.
    if (disk_set_builder)
    {
        disk_set = disk_set_builder->finish();
        is_truncated = disk_set_builder->isTruncated();
        disk_set_builder.reset();
    }
    is_created = true;
}

bool Set::isLargeEnoughToSpill() const
{
    return data->getTotalByteCount() >= DiskSetBuilder::getMinBytesInRun(spill_settings.max_bytes_before_external_set);
}

bool Set::isSpillNeededBeforeInsert(const ColumnRawPtrs & key_columns, size_t rows) const
{
    const size_t threshold = spill_settings.max_bytes_before_external_set;

    if (!threshold || !isLargeEnoughToSpill())
        return false;

    /// Query accounting can briefly become negative while a concurrent free saturates its counter.
    const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    const UInt64 available_memory = threshold - std::min<UInt64>(threshold, query_memory_usage);

    /// Spilling needs memory while the table still exists.
    const size_t spill_memory = DiskSetBuilder::estimateMemoryToWriteRun(
        threshold, spill_settings.max_block_size, getDiskSetKeyBytes(), spill_settings.tmp_data->getSettings().buffer_size);
    if (spill_memory > available_memory)
        return true;

    /// The insertion grows the table and its arena, and the state of a method with fixed keys packs the keys of
    /// the whole chunk before inserting them.
    size_t insert_memory = data->estimateGrowthMemory(key_columns, /*start_row=*/0, rows);
    if (common::addOverflow(insert_memory, data->estimatePreparedKeysMemory(rows, key_sizes), insert_memory))
        return true;

    if (fill_set_elements)
    {
        /// Explicit elements copy the new keys.
        for (const auto * column : key_columns)
        {
            if (common::addOverflow(insert_memory, column->byteSize(), insert_memory))
                return true;
        }
    }

    return insert_memory > available_memory - spill_memory;
}

void Set::spill(std::string_view reason)
{
    logSpill("while it is built", reason);

    /// `fill_set_elements` mainly serves index analysis. Since the set itself has to be spilled, it is safe
    /// to assume that its elements would not fit in memory for index analysis later either. Index analysis
    /// could possibly work on disk, but this is not implemented yet.
    fill_set_elements = false;
    set_elements.clear();

    data->callOnMethod([&]<typename Method>(const Method & method)
    {
        compute_disk_set_keys = computeDiskSetKeys<DiskSetMethod<Method>>;
        disk_set_builder = createDiskSetBuilderWithKeys(method, limits);
    });

    data.reset();
    ProfileEvents::increment(ProfileEvents::SetsSpilledToDisk);
}

bool Set::isSpillNeeded() const
{
    return spill_settings.max_bytes_before_external_set
        && getCurrentQueryMemoryUsage() > static_cast<Int64>(spill_settings.max_bytes_before_external_set)
        && isLargeEnoughToSpill();
}

void Set::spillAfterBuild() const
{
    if (spill_after_build_started.exchange(true))
        return;

    std::unique_ptr<DiskSet> new_disk_set;
    ComputeDiskSetKeys new_compute_disk_set_keys = nullptr;
    {
        /// The table does not change after the build, so other lookups keep reading it while its keys are
        /// written.
        std::shared_lock lock(rwlock);

        /// Only the lookup that started the spill replaces the table, so the table is still in memory here.
        chassert(data && !disk_set);
        logSpill("while it is used", "query memory exceeds the spill threshold");

        data->callOnMethod([&]<typename Method>(const Method & method)
        {
            /// The keys are final, so the builder applies no size limits and keeps all of them.
            new_disk_set = createDiskSetBuilderWithKeys(method, SizeLimits{})->finish();
            new_compute_disk_set_keys = computeDiskSetKeys<DiskSetMethod<Method>>;
        });
    }

    /// It is freed when the function returns, after the lock is released, so the lookups waiting for the lock
    /// do not also wait for the table to be freed.
    std::unique_ptr<SetVariants> table;
    {
        /// The exclusive lock waits for the lookups in progress, which hold the shared lock for one chunk
        /// each.
        std::lock_guard lock(rwlock);
        compute_disk_set_keys = new_compute_disk_set_keys;
        disk_set = std::move(new_disk_set);
        table = std::move(data);
    }
    ProfileEvents::increment(ProfileEvents::SetsSpilledToDisk);
}

void Set::logSpill(std::string_view stage, std::string_view reason) const
{
    LOG_TRACE(log, "Switching the set of IN to external mode {}: {} "
        "(keys in memory: {}, set memory: {}, query memory: {}, spill threshold: {})",
        stage,
        reason,
        data->getTotalRowCount(),
        formatReadableSizeWithBinarySuffix(data->getTotalByteCount()),
        formatReadableSizeWithBinarySuffix(std::max<Int64>(0, getCurrentQueryMemoryUsage())),
        formatReadableSizeWithBinarySuffix(spill_settings.max_bytes_before_external_set));
}

size_t Set::getDiskSetKeyBytes() const
{
    return data->callOnMethod([]<typename Method>(const Method &) { return sizeof(DiskSetKey<Method>); });
}

template <typename Method>
std::unique_ptr<DiskSetBuilder> Set::createDiskSetBuilderWithKeys(const Method & method, const SizeLimits & builder_limits) const
{
    using Key = DiskSetKey<Method>;

    auto builder = createDiskSetBuilder<Key>(
        spill_settings.tmp_data,
        builder_limits,
        spill_settings.max_bytes_before_external_set,
        spill_settings.max_block_size,
        spill_settings.min_free_disk_space,
        spill_settings.process_list_element);

    const size_t max_rows_in_column = spill_settings.max_block_size;
    auto keys = ColumnVector<Key>::create();
    for (auto it = method.data.begin(); it != method.data.end(); ++it)
    {
        /// Every method except `key_string` and `key_fixed_string` keeps the disk keys themselves in its
        /// table: numbers, packed keys and the hashes of `hashed`. The string methods keep strings, and their
        /// disk keys are the keys that the `hashed` method computes for them.
        if constexpr (std::is_same_v<DiskSetMethod<Method>, Method>)
            keys->getData().push_back(it->getValue());
        else
            keys->getData().push_back(Method::getHashedKey(it->getValue()));

        if (keys->size() == max_rows_in_column)
            builder->add(std::exchange(keys, ColumnVector<Key>::create()));
    }

    if (!keys->empty())
        builder->add(std::move(keys));
    return builder;
}

void Set::executeDiskSet(const ColumnRawPtrs & key_columns, ColumnUInt8::Container & vec_res, bool negative, ConstNullMapPtr null_map) const
{
    const size_t rows = vec_res.size();
    auto keys = compute_disk_set_keys(key_columns, key_sizes, rows, /*null_map=*/nullptr);

    disk_set->containsBatch(*keys, {vec_res.data(), rows});

    /// Keys with a `NULL` component are not in the set, as in `executeImplCase`.
    for (size_t row = 0; row < rows; ++row)
        vec_res[row] = negative ^ (vec_res[row] && !(null_map && (*null_map)[row]));
}


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map,
    ColumnUInt8::Container * out_filter)
{
    if (null_map)
    {
        if (out_filter)
            insertFromBlockImplCase<Method, true, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, true, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
    else
    {
        if (out_filter)
            insertFromBlockImplCase<Method, false, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, false, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
}


template <typename Method, bool has_null_map, bool build_filter>
void NO_INLINE Set::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    [[maybe_unused]] ConstNullMapPtr null_map,
    [[maybe_unused]] ColumnUInt8::Container * out_filter)
{
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (build_filter)
                {
                    (*out_filter)[i] = false;
                }
                continue;
            }
        }

        [[maybe_unused]] auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if constexpr (build_filter)
            (*out_filter)[i] = emplace_result.isInserted();
    }
}


DataTypes Set::getElementTypes(DataTypes types, bool transform_null_in)
{
    for (auto & type : types)
    {
        /// Strip LowCardinality recursively to match what insertFromColumns does:
        /// it calls recursiveRemoveLowCardinality on the column side.
        type = recursiveRemoveLowCardinality(type);

        if (!transform_null_in)
            type = removeNullable(type);
    }

    return types;
}


void Set::setHeader(const ColumnsWithTypeAndName & header)
{
    std::lock_guard lock(rwlock);

    if (!data->empty())
        return;

    keys_size = header.size();
    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);
    data_types.reserve(keys_size);
    set_elements_types.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(header.at(i).column->convertToFullColumnIfConst());
        key_columns.emplace_back(materialized_columns.back().get());
        data_types.emplace_back(header.at(i).type);
        set_elements_types.emplace_back(header.at(i).type);

        /// Convert low cardinality column to full.
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_types.back().get()))
        {
            data_types.back() = low_cardinality_type->getDictionaryType();
            materialized_columns.emplace_back(key_columns.back()->convertToFullColumnIfLowCardinality());
            key_columns.back() = materialized_columns.back().get();
        }

        /// Strip LowCardinality recursively from set_elements_types so they match what
        /// recursiveRemoveLowCardinality does to columns in insertFromColumns.
        set_elements_types.back() = recursiveRemoveLowCardinality(set_elements_types.back());
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
    {
        /// We convert nullable columns to non nullable we also need to update nullable types
        for (size_t i = 0; i < set_elements_types.size(); ++i)
        {
            data_types[i] = removeNullable(data_types[i]);
            set_elements_types[i] = removeNullable(set_elements_types[i]);
        }

        extractNestedColumnsAndNullMap(key_columns, null_map);
    }

    /// Choose data structure to use for the set.
    data->init(SetVariants::chooseMethod(key_columns, key_sizes));
}

void Set::fillSetElements()
{
    fill_set_elements = true;
    set_elements.reserve(keys_size);
    for (const auto & type : set_elements_types)
        set_elements.emplace_back(type->createColumn());
}

bool Set::insertFromBlock(const ColumnsWithTypeAndName & columns)
{
    Columns cols;
    cols.reserve(columns.size());
    for (const auto & column : columns)
        cols.emplace_back(column.column);
    return insertFromColumns(cols);
}

bool Set::insertFromColumns(const Columns & columns)
{
    size_t rows = columns.at(0)->size();

    SetKeyColumns holder;
    /// Filter to extract distinct values from the block.
    if (fill_set_elements)
        holder.filter = ColumnUInt8::create(rows);

    bool inserted = insertFromColumns(columns, holder);
    if (inserted && fill_set_elements)
    {
        if (max_elements_to_fill && max_elements_to_fill < data->getTotalRowCount())
        {
            /// Drop filled elementes
            fill_set_elements = false;
            set_elements.clear();
        }
        else
            appendSetElements(holder);
    }

    return inserted;
}

bool Set::insertFromColumns(const Columns & columns, SetKeyColumns & holder)
{
    std::lock_guard lock(rwlock);

    if (data && data->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method Set::setHeader must be called before Set::insertFromBlock");

    holder.key_columns.reserve(keys_size);
    holder.materialized_columns.reserve(keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        holder.materialized_columns.emplace_back(recursiveRemoveLowCardinality(columns.at(i)->convertToFullIfWrapped()));
        holder.key_columns.emplace_back(holder.materialized_columns.back().get());
    }

    size_t rows = columns.at(0)->size();

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
        null_map_holder = extractNestedColumnsAndNullMap(holder.key_columns, null_map);

    if (!disk_set_builder && isSpillNeededBeforeInsert(holder.key_columns, rows))
        spill("the projected growth of the set exceeds the memory left under the spill threshold");

    if (disk_set_builder)
    {
        auto keys = compute_disk_set_keys(holder.key_columns, key_sizes, rows, null_map);
        if (!keys->empty())
            disk_set_builder->add(std::move(keys));
        return true;
    }

    data->callOnMethod([&](auto & method)
    {
        insertFromBlockImpl(method, holder.key_columns, rows, *data, null_map, holder.filter ? &holder.filter->getData() : nullptr);
    });

    bool within_limits = limits.check(data->getTotalRowCount(), data->getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    if (!within_limits)
        is_truncated = true;
    /// Actual allocations and concurrent operators can consume more memory than projected before insertion.
    else if (isSpillNeeded())
        spill("query memory exceeds the spill threshold after insertion");
    return within_limits;
}

void Set::appendSetElements(SetKeyColumns & holder)
{
    if (holder.key_columns.size() != keys_size || set_elements.size() != keys_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of key columns for set. Expected {} got {} and {}",
                        keys_size, holder.key_columns.size(), set_elements.size());

    size_t rows = holder.key_columns.at(0)->size();
    for (size_t i = 0; i < keys_size; ++i)
    {
        auto filtered_column = holder.key_columns[i]->filter(holder.filter->getData(), rows);
        if (set_elements[i]->empty())
            set_elements[i] = IColumn::mutate(std::move(filtered_column));
        else
            set_elements[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());
        if (transform_null_in && holder.null_map_holder)
            set_elements[i]->insert(Null{});
    }
}

void Set::checkIsCreated() const
{
    if (!is_created.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to use set before it has been built.");
}

std::shared_ptr<const PlainRanges> Set::getPlainRanges() const
{
    /// The result is cached on first use, so reading `set_elements` before the set is filled would not
    /// merely give one wrong answer - it would pin an empty range set for every later caller.
    checkIsCreated();

    callOnce(
        plain_ranges_once,
        [this]
        {
            /// A tuple set has no single-column range representation; leave the cache null.
            if (set_elements.size() != 1)
                return;

            const auto & column = *set_elements.front();
            Ranges ranges;
            ranges.reserve(column.size());
            for (size_t i = 0; i < column.size(); ++i)
                ranges.emplace_back(column[i]);

            plain_ranges = std::make_shared<const PlainRanges>(ranges, /*may_have_intersection*/ true, /*ordered*/ false);
        });

    return plain_ranges;
}

Columns Set::getSetElements() const
{
    checkIsCreated();
    Columns result;
    result.reserve(set_elements.size());
    for (const auto & col : set_elements)
        result.push_back(col->getPtr());
    return result;
}

ColumnPtr Set::castToKeyType(const ColumnWithTypeAndName & column, size_t key, ColumnPtr & non_member_rows) const
{
    /// The constant columns to the left of `IN` are not supported directly, so they are materialized. The key
    /// types have no top-level `LowCardinality`, see `setHeader`, and without it the NULLs of a
    /// `LowCardinality(Nullable)` probe are those of a `Nullable` one.
    const ColumnWithTypeAndName probe = {
        column.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality(),
        removeLowCardinality(column.type),
        column.name};
    const DataTypePtr & key_type = data_types[key];

    ColumnPtr result;
    if (canContainNull(*key_type) && !key_type->isNullable() && !probe.type->isNullable())
    {
        /// A `Variant` or `Dynamic` key type stores NULLs as its own values, the same way a join compares
        /// such keys, so the NULLs of a `Variant` or `Dynamic` probe convert as they are.
        result = castColumnAccurate(probe, key_type, cast_cache.get());
    }
    else
    {
        const DataTypePtr key_type_without_nullable = removeNullable(key_type);
        ColumnPtr values;

        /// A value the key type cannot represent is simply not a member - which is what the literal `IN` list
        /// concludes, because it compares in a common supertype. Converting it strictly would raise
        /// `CANNOT_CONVERT_TYPE` on the rows the read path happens to deliver, so `v IN set_table` would fail
        /// where `v IN (1)` returns 0. A tuple does not fit when one of its fields does not, including a NULL
        /// in a field that cannot hold it. The lenient conversion produces a `Nullable`, so the key type must
        /// be one that `accurateCastOrNull` can produce. When the probe already has the key type (up to
        /// `Nullable`), no value can fail to fit, so the cheap path is kept: an exact-type cast is a no-op,
        /// and that is the common `x IN set_table` case.
        if (canBeAccurateCastOrNullTarget(key_type) && !removeNullable(probe.type)->equals(*key_type_without_nullable))
        {
            const ColumnPtr converted = castColumnAccurateOrNullSkipNulls(probe, key_type_without_nullable, cast_cache.get());
            const auto & converted_nullable = assert_cast<const ColumnNullable &>(*converted);
            values = converted_nullable.getNestedColumnPtr();
            /// The NULLs of the conversion are the values that did not fit. Even when the set can hold a
            /// NULL, a value that merely does not fit is not that NULL.
            non_member_rows = mergeNullMaps(std::move(non_member_rows), converted_nullable.getNullMapColumnPtr());
        }
        else
        {
            values = castColumnAccurateSkipNulls(probe, key_type_without_nullable, cast_cache.get());
        }

        /// The NULLs of the probe, kept in the null map of a `Nullable` or the discriminators of a `Variant`,
        /// stay out of the conversion. With `transform_null_in = 1`, a nullable key type keeps them to match
        /// a NULL of the set. Otherwise they are not members: with `transform_null_in = 0` the set holds no
        /// NULL and stores the nested columns of nullable keys, see `insertFromColumns`, and a non-nullable
        /// key type has no NULL to match. Nullable fields inside a non-null tuple remain part of the key
        /// value in either mode.
        const ColumnPtr null_map = getSourceNullMap(*probe.column);
        if (transform_null_in && key_type->isNullable())
        {
            if (null_map)
                result = ColumnNullable::create(values, null_map);
            else
                result = makeNullable(values);
        }
        else
        {
            result = std::move(values);
            non_member_rows = mergeNullMaps(std::move(non_member_rows), null_map);
        }
    }

    non_member_rows = mergeNullMaps(std::move(non_member_rows), getDateTime64CastLossMap(probe, key_type));
    return result;
}

ColumnPtr Set::execute(const ColumnsWithTypeAndName & columns, bool negative) const
{
    size_t num_key_columns = columns.size();

    if (0 == num_key_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns passed to Set::execute method.");

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(columns.at(0).column->size());

    if (vec_res.empty())
        return res;

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(vec_res.data(), 1, vec_res.size());
        else
            memset(vec_res.data(), 0, vec_res.size());
        return res;
    }

    checkColumnsNumber(num_key_columns);

    /// The key columns converted to the key types of the set, and the columns that own them.
    ColumnRawPtrs key_columns;
    key_columns.reserve(num_key_columns);
    Columns materialized_columns;
    materialized_columns.reserve(num_key_columns);

    ColumnPtr non_member_rows;
    for (size_t i = 0; i < num_key_columns; ++i)
    {
        materialized_columns.emplace_back(castToKeyType(columns.at(i), i, non_member_rows));
        key_columns.emplace_back(materialized_columns.back().get());
    }

    const auto * null_map = non_member_rows ? &assert_cast<const ColumnUInt8 &>(*non_member_rows).getData() : nullptr;

    if (disk_set)
    {
        executeDiskSet(key_columns, vec_res, negative, null_map);
        return res;
    }

    data->callOnMethod([&](auto & method) { executeImpl(method, key_columns, vec_res, negative, vec_res.size(), null_map); });

    /// The set is usually built before the rest of the query takes its memory, so it also spills while it is
    /// used. Spilling takes the lock exclusively, so this lookup releases its shared lock first.
    if (isSpillNeeded())
    {
        lock.unlock();
        spillAfterBuild();
    }
    return res;
}

bool Set::hasNull() const
{
    checkIsCreated();

    if (!transform_null_in)
        return false;

    if (data_types.size() != 1)
        return false;

    if (!data_types[0]->isNullable())
        return false;

    auto col = data_types[0]->createColumn();
    col->insert(Field());
    auto res = execute({ColumnWithTypeAndName(std::move(col), data_types[0], std::string())}, false);
    return res->getBool(0);
}

bool Set::empty() const
{
    std::shared_lock lock(rwlock);
    return data && data->empty();
}

size_t Set::getTotalRowCount() const
{
    std::shared_lock lock(rwlock);

    if (disk_set)
        return disk_set->getTotalRowCount();

    chassert(data);
    return data->getTotalRowCount();
}

size_t Set::getTotalByteCount() const
{
    std::shared_lock lock(rwlock);

    if (disk_set)
        return disk_set->getTotalByteCount();

    chassert(data);
    return data->getTotalByteCount();
}


template <typename Method>
void NO_INLINE Set::executeImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    if (null_map)
        executeImplCase<Method, true>(method, key_columns, vec_res, negative, rows, null_map);
    else
        executeImplCase<Method, false>(method, key_columns, vec_res, negative, rows, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::executeImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    Arena pool;
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// Clustered key columns (e.g. a primary key prefix) arrive in runs of equal consecutive
    /// rows. The consecutive-keys optimization in ColumnsHashing handles them inside `findKey`:
    /// the last-element cache compares the key with the previous row's before probing the hash
    /// table, and `HashMethodHashed` additionally compares the raw key bytes before even
    /// calculating the hash.
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            vec_res[i] = negative;
        }
        else
        {
            auto find_result = state.findKey(method.data, i, pool);
            vec_res[i] = negative ^ find_result.isFound();
        }
    }
}


void Set::checkColumnsNumber(size_t num_key_columns) const
{
    if (data_types.size() != num_key_columns)
    {
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                        "Number of columns in section IN doesn't match. {} at left, {} at right.",
                        num_key_columns, data_types.size());
    }
}

bool Set::areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    /// Out-of-bound access can happen when same set expression built with different columns.
    /// Caller may call this method to make sure that the set is indeed the one they want
    /// without awaring data_types.size().
    if (set_type_idx >= data_types.size())
        return false;
    return removeNullable(recursiveRemoveLowCardinality(data_types[set_type_idx]))
        ->equals(*removeNullable(recursiveRemoveLowCardinality(other_type)));
}

void Set::checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    if (!this->areTypesEqual(set_type_idx, other_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Types of column {} in section IN don't match: "
                        "{} on the left, {} on the right", toString(set_type_idx + 1),
                        other_type->getName(), data_types[set_type_idx]->getName());
}

static UInt64 getNextMergeTreeSetIndexId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

MergeTreeSetIndex::MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_)
    : has_all_keys(set_elements.size() == indexes_mapping_.size())
    , indexes_mapping(std::move(indexes_mapping_))
    , instance_id(getNextMergeTreeSetIndexId())
{
    ::sort(indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return std::tie(l.key_index, l.tuple_index) < std::tie(r.key_index, r.tuple_index);
        });

    /// Deduplicate key columns. This can make the condition weaker, e.g.:
    ///     (x + 1, x + 2) IN (10, 10)
    /// effectively turns into:
    ///     (x + 1) IN (10)
    indexes_mapping.erase(std::unique(
        indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return l.key_index == r.key_index;
        }), indexes_mapping.end());

    size_t tuple_size = indexes_mapping.size();
    ordered_set.resize(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = set_elements[indexes_mapping[i].tuple_index];

    Block block_to_sort;
    SortDescription sort_description;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        String column_name = "_" + toString(i);
        block_to_sort.insert({ordered_set[i], nullptr, column_name});
        sort_description.emplace_back(column_name, 1, 1);
    }

    sortBlock(block_to_sort, sort_description);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = block_to_sort.getByPosition(i).column;

    /// Only fixed-width key columns keep the cached ranges buffer small and bounded: their
    /// columns hold at most one value, so reuse cannot grow them. Variable-width columns
    /// (e.g. `ColumnString`) do not release reserved capacity on popBack, so caching them in
    /// thread-local storage could pin arbitrarily large buffers after the query ends.
    cache_ranges = std::all_of(ordered_set.begin(), ordered_set.end(),
        [](const ColumnPtr & column) { return column->valuesHaveFixedSize(); });
}

MergeTreeSetIndex::FieldValueRanges & MergeTreeSetIndex::getFieldValueRangesBuffer(FieldValueRanges & scratch) const
{
    size_t tuple_size = indexes_mapping.size();

    if (!cache_ranges)
    {
        /// Query-scoped scratch buffer: normally tracked, freed when the caller returns.
        scratch.reserve(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            scratch.emplace_back(*ordered_set[i]);
        return scratch;
    }

    struct CacheEntry
    {
        UInt64 set_index_id;
        FieldValueRanges ranges;
    };
    /// Must exceed the number of IN-set key conditions checked per mark within one query,
    /// otherwise every call misses and rebuilds the buffer as before this cache existed.
    static constexpr size_t max_entries = 8;
    thread_local std::vector<CacheEntry> cache;

    for (auto & entry : cache)
        if (entry.set_index_id == instance_id)
            return entry.ranges;

    /// The cache outlives the query, so its memory (also freed here on eviction) must not be
    /// charged to whichever query happens to be running on this thread. See FieldValue::update
    /// for the same blocker on reallocations of the cached columns.
    MemoryTrackerBlockerInThread blocker;

    FieldValueRanges ranges;
    ranges.reserve(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        ranges.emplace_back(*ordered_set[i], /*block_memory_tracker=*/ true);

    if (cache.size() >= max_entries)
        cache.erase(cache.begin());
    cache.push_back({instance_id, std::move(ranges)});
    return cache.back().ranges;
}

/** Return the BoolMask where:
  * 1: the intersection of the set and the range is non-empty
  * 2: the range contains elements not in the set
  */
BoolMask MergeTreeSetIndex::checkInRange(const std::vector<int> & key_col_to_sparse_pos, const Ranges & sparse_key_ranges, const DataTypes & sparse_data_types, bool single_point) const
{
    auto get_sparse_info = [&](size_t key_column) -> std::pair<bool, size_t>
    {
        bool is_key_col_present = (key_column < key_col_to_sparse_pos.size() && key_col_to_sparse_pos[key_column] != -1);
        const size_t sparse_pos = is_key_col_present ? static_cast<size_t>(key_col_to_sparse_pos[key_column]) : 0;
        return {is_key_col_present, sparse_pos};
    };

    size_t tuple_size = indexes_mapping.size();

    FieldValueRanges scratch;
    FieldValueRanges & ranges = getFieldValueRangesBuffer(scratch);
    for (size_t i = 0; i < tuple_size; ++i)
    {
        size_t key_column = indexes_mapping[i].key_index;
        auto [is_key_col_present, sparse_pos] = get_sparse_info(key_column);

        FieldValueRange & range = ranges[i];

        if (!is_key_col_present)
        {
            /// We have no range information for this key column.
            /// Most likely earlier columns were high cardinality, so this column and later column marks were not loaded into memory
            /// Treat it as completely unconstrained; since we do not have the type information,
            /// we do not know whether to createWholeUniverse() or createWholeUniverseWithoutNull().
            /// So, we choose the more relaxed option:
            /// [-inf, +inf] instead of ( -inf, +inf ).
            range.left.update(NEGATIVE_INFINITY);
            range.right.update(POSITIVE_INFINITY);
            range.left_included = true;
            range.right_included = true;
            continue;
        }

        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            sparse_key_ranges[sparse_pos],
            indexes_mapping[i].functions,
            sparse_data_types[sparse_pos],
            single_point);

        if (!new_range)
            return {true, true};

        range.left.update(new_range->left);
        range.right.update(new_range->right);
        range.left_included = new_range->left_included;
        range.right_included = new_range->right_included;
    }

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return lhs.isNullAt(row) ? 0 : -1; // +Inf == +Inf
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    /// Because ordered_set is sorted lexicographically, the elements we're looking for are
    /// consecutive. Use binary search to find the range of indices.

    /// The part about left_included/right_included is a little tricky. It was initially implemented
    /// incorrectly:
    ///   begin = lower_bound(..., left_point, tuple_less_unaware_of_includedness);
    ///   if (!all(ranges[..].left_included) && equals(left_point, begin))
    ///       begin += 1;
    /// This breaks on the following example:
    ///   key_ranges = [(0, +inf), (-inf, +inf)]  (the 0 is not included),
    ///   ordered_set = [[0], [0]].
    /// The incorrect implementation would output begin == 0 and conclude that the set element is
    /// inside the range (it isn't). The `begin += 1` won't happen because (0, 0) != (0, -inf).

    auto indices = collections::range(0, size());
    size_t begin = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return true if set[row] is below the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].left, row);

                if (cmp > 0)
                    return false;
                /// Note: if some range has left_included == false then the left ends of all
                /// subsequent ranges' don't matter. (Symmetrically for right.)
                /// It's the only way to make sense of the notion of a range of tuples where the
                /// included/excluded flags are given per element.
                if (cmp < 0 || (cmp == 0 && !ranges[i].left_included))
                    return true;
            }
            return false;
        }) - indices.begin();
    size_t end = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return false if set[row] is above the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].right, row);

                if (cmp > 0 || (cmp == 0 && !ranges[i].right_included))
                    return false;
                if (cmp < 0)
                    return true;
            }
            return true;
        }) - indices.begin();

    if (begin > end)
    {
        /// TODO: Remove the #ifndef and always throw after
        ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
        ///       (What happens here is: the applyMonotonicFunctionsChainToRange call above applies
        ///        nonmonotonic functions, and we end up with left > right.)
#ifndef NDEBUG
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary search result in MergeTreeSetIndex");
#else
        return {true, true};
#endif
    }

    bool can_be_true = begin < end;

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool at_most_one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & r = ranges[i];
        if (r.left.isNormal() && r.right.isNormal())
        {
            if (0 != r.left.column->compareAt(0, 0, *r.right.column, 1))
            {
                at_most_one_element_range = false;
                break;
            }
        }
        else if ((r.left.isPositiveInfinity() && r.right.isPositiveInfinity()) || (r.left.isNegativeInfinity() && r.right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            at_most_one_element_range = false;
            break;
        }
    }
    if (at_most_one_element_range && has_all_keys)
    {
        /// Here we know that there is at most one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).
        return {can_be_true, !can_be_true};
    }

    return {can_be_true, true};
}

BoolMask MergeTreeSetIndex::checkInRange(const Ranges & key_ranges, const DataTypes & data_types, bool single_point) const
{
    size_t tuple_size = indexes_mapping.size();

    FieldValueRanges scratch;
    FieldValueRanges & ranges = getFieldValueRangesBuffer(scratch);
    for (size_t i = 0; i < tuple_size; ++i)
    {
        if (indexes_mapping[i].key_index >= key_ranges.size())
            return {true, true};

        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            key_ranges[indexes_mapping[i].key_index],
            indexes_mapping[i].functions,
            data_types[indexes_mapping[i].key_index],
            single_point);

        if (!new_range)
            return {true, true};

        FieldValueRange & range = ranges[i];
        range.left.update(new_range->left);
        range.right.update(new_range->right);
        range.left_included = new_range->left_included;
        range.right_included = new_range->right_included;
    }

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return lhs.isNullAt(row) ? 0 : -1; // +Inf == +Inf
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    /// Because ordered_set is sorted lexicographically, the elements we're looking for are
    /// consecutive. Use binary search to find the range of indices.

    /// The part about left_included/right_included is a little tricky. It was initially implemented
    /// incorrectly:
    ///   begin = lower_bound(..., left_point, tuple_less_unaware_of_includedness);
    ///   if (!all(ranges[..].left_included) && equals(left_point, begin))
    ///       begin += 1;
    /// This breaks on the following example:
    ///   key_ranges = [(0, +inf), (-inf, +inf)]  (the 0 is not included),
    ///   ordered_set = [[0], [0]].
    /// The incorrect implementation would output begin == 0 and conclude that the set element is
    /// inside the range (it isn't). The `begin += 1` won't happen because (0, 0) != (0, -inf).

    auto indices = collections::range(0, size());
    size_t begin = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return true if set[row] is below the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].left, row);

                if (cmp > 0)
                    return false;
                /// Note: if some range has left_included == false then the left ends of all
                /// subsequent ranges' don't matter. (Symmetrically for right.)
                /// It's the only way to make sense of the notion of a range of tuples where the
                /// included/excluded flags are given per element.
                if (cmp < 0 || (cmp == 0 && !ranges[i].left_included))
                    return true;
            }
            return false;
        }) - indices.begin();
    size_t end = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return false if set[row] is above the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].right, row);

                if (cmp > 0 || (cmp == 0 && !ranges[i].right_included))
                    return false;
                if (cmp < 0)
                    return true;
            }
            return true;
        }) - indices.begin();

    if (begin > end)
    {
        /// TODO: Remove the #ifndef and always throw after
        ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
        ///       (What happens here is: the applyMonotonicFunctionsChainToRange call above applies
        ///        nonmonotonic functions, and we end up with left > right.)
#ifndef NDEBUG
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary search result in MergeTreeSetIndex");
#else
        return {true, true};
#endif
    }

    bool can_be_true = begin < end;

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool at_most_one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & r = ranges[i];
        if (r.left.isNormal() && r.right.isNormal())
        {
            if (0 != r.left.column->compareAt(0, 0, *r.right.column, 1))
            {
                at_most_one_element_range = false;
                break;
            }
        }
        else if ((r.left.isPositiveInfinity() && r.right.isPositiveInfinity()) || (r.left.isNegativeInfinity() && r.right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            at_most_one_element_range = false;
            break;
        }
    }
    if (at_most_one_element_range && has_all_keys)
    {
        /// Here we know that there is at most one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).
        return {can_be_true, !can_be_true};
    }

    return {can_be_true, true};
}

bool MergeTreeSetIndex::hasMonotonicFunctionsChain() const
{
    for (const auto & mapping : indexes_mapping)
        if (!mapping.functions.empty())
            return true;
    return false;
}

void MergeTreeSetIndex::FieldValue::update(const Field & x)
{
    if (x.isNegativeInfinity() || x.isPositiveInfinity())
        value = x;
    else
    {
        /// When the column belongs to the thread-local buffer of getFieldValueRangesBuffer, which
        /// outlives the query, its reallocations must not be charged to the current query.
        std::optional<MemoryTrackerBlockerInThread> blocker;
        if (block_memory_tracker)
            blocker.emplace();
        /// Keep at most one element in column.
        if (!column->empty())
            column->popBack(1);
        column->insert(x);
        value = Field(); // Set back to normal value.
    }
}

}
