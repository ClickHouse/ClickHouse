#include <Processors/Transforms/LimitByGroupFilter.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Processors/IProcessor.h>
#include <base/PackedStringRef.h>
#include <base/arithmeticOverflow.h>
#include <Common/Arena.h>
#include <Common/BitHelpers.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char limit_by_transform_pause[];
extern const char limit_by_transform_mid_loop_pause[];
}

namespace
{

AggregateDataPtr mappedFromGroupIndex(size_t group_index)
{
    return reinterpret_cast<AggregateDataPtr>(static_cast<uintptr_t>(group_index) + 1);
}

size_t groupIndexFromMapped(AggregateDataPtr mapped)
{
    return static_cast<size_t>(reinterpret_cast<uintptr_t>(mapped) - 1);
}

/// Whether the hash method copies its keys into the arena of the variants.
template <typename Method>
constexpr bool stores_keys_in_arena
    = std::is_same_v<typename Method::Key, std::string_view> || std::is_same_v<typename Method::Key, PackedStringRef>;

/// Peak additional table memory of `keys` new keys. The variants dispatcher also instantiates this helper
/// for two-level tables, which `LIMIT BY` never selects.
template <typename Data>
size_t estimateTableGrowth(const Data & data, size_t keys)
{
    if constexpr (requires { data.estimateGrowthMemory(keys); })
        return data.estimateGrowthMemory(keys);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY table does not support growth estimation");
}

/// The buffer that `items` allocates to hold `new_items` more items, or zero when its capacity suffices. The vector
/// grows by doubling, and the last reallocation holds the old and the new buffer at once.
template <typename T>
size_t estimateVectorGrowth(const std::vector<T> & items, size_t new_items)
{
    const size_t required = items.size() + new_items;
    size_t capacity = items.capacity();
    if (required <= capacity)
        return 0;

    capacity = std::max<size_t>(capacity, 1);
    while (capacity < required)
        capacity *= 2;
    return capacity * sizeof(T);
}

/// Returns the fingerprints of the groups with their counts, in group insertion order.
class FingerprintExtractorImpl final : public SeenGroupExtractor
{
public:
    FingerprintExtractorImpl(std::vector<UInt128> fingerprints_, std::vector<UInt64> counts_)
        : fingerprints(std::move(fingerprints_))
        , counts(std::move(counts_))
    {
        chassert(fingerprints.size() == counts.size());
    }

    SeenGroups next(size_t max_rows, size_t max_bytes) override
    {
        chassert(max_rows > 0);

        size_t rows = std::min(max_rows, fingerprints.size() - position);
        /// Every group takes the same number of bytes, so the byte target directly bounds the rows: the batch ends
        /// with the group that reaches it.
        if (max_bytes)
        {
            constexpr size_t group_bytes = sizeof(UInt128) + sizeof(UInt64);
            rows = std::min(rows, (max_bytes - 1) / group_bytes + 1);
        }
        if (rows == 0)
            return {};

        auto keys = ColumnUInt128::create();
        keys->getData().insert(fingerprints.data() + position, fingerprints.data() + position + rows);
        auto seen_rows = ColumnUInt64::create();
        seen_rows->getData().insert(counts.data() + position, counts.data() + position + rows);
        position += rows;

        MutableColumns columns;
        columns.push_back(std::move(keys));
        return {.keys = std::move(columns), .seen_rows = std::move(seen_rows)};
    }

    size_t getTotalByteCount() const override
    {
        return fingerprints.capacity() * sizeof(UInt128) + counts.capacity() * sizeof(UInt64);
    }

private:
    const std::vector<UInt128> fingerprints;
    const std::vector<UInt64> counts;
    size_t position = 0;
};

/// The stride of the rows that `estimateSerializedBytes` serializes. Serializing every row would cost as
/// much as hashing them, while the rows of a chunk mostly share the paths and types that make their
/// serialized form larger than their memory, so a sample of the rows estimates the others.
constexpr size_t serialized_bytes_sample_step = 256;

/// Estimates the bytes that the serialized values of the rows `[start_row, start_row + num_rows)` of `column`
/// take, from every `serialized_bytes_sample_step`-th row, starting with the first. Every sampled value is
/// serialized into a scratch arena and rolled back, so the scratch holds one value at a time. Requires at
/// least one row. Only keys of dynamic structure need it, so the call stays out of the estimates of the
/// other keys.
NO_INLINE size_t estimateSerializedBytes(const IColumn & column, size_t start_row, size_t num_rows)
{
    chassert(num_rows > 0);
    Arena scratch;
    const IColumn::SerializationSettings settings;
    size_t sampled_rows = 0;
    size_t sampled_bytes = 0;
    for (size_t offset = 0; offset < num_rows; offset += serialized_bytes_sample_step)
    {
        const char * begin = nullptr;
        const size_t value_bytes = column.serializeValueIntoArena(start_row + offset, scratch, begin, &settings).size();
        scratch.rollback(value_bytes);
        sampled_bytes += value_bytes;
        ++sampled_rows;
    }

    size_t scaled_bytes = 0;
    if (common::mulOverflow(sampled_bytes, num_rows, scaled_bytes))
        return std::numeric_limits<size_t>::max();
    return scaled_bytes / sampled_rows;
}

/// Estimates the bytes that the rows `[start_row, start_row + num_rows)` can copy into the key arena. A
/// serialized key writes the value of each column with a length prefix, so a column takes about its memory
/// plus a prefix per row. A column of dynamic structure (`JSON`, `Dynamic`) also writes into every value what
/// its values share in the column, such as the path of a `JSON` value or the type of a `Dynamic` one, so its
/// serialized size is estimated from sampled rows as well, and the larger estimate counts.
size_t estimateKeyBytes(const ColumnRawPtrs & key_columns, size_t start_row, size_t num_rows)
{
    size_t bytes = 0;
    for (const auto * column : key_columns)
    {
        size_t column_bytes = num_rows * sizeof(UInt64);
        if (start_row == 0 && num_rows == column->size())
            column_bytes += column->byteSize();
        else
        {
            for (size_t row = start_row; row < start_row + num_rows; ++row)
                column_bytes += column->byteSizeAt(row);
        }
        if (column->hasDynamicStructure())
            column_bytes = std::max(column_bytes, estimateSerializedBytes(*column, start_row, num_rows));
        bytes += column_bytes;
    }
    return bytes;
}

/// A resumable traversal of a `GROUP BY` hash table that the table must outlive.
template <typename Data>
class TableTraversal
{
public:
    explicit TableTraversal(const Data & table) : it(table.begin()), end(table.end()) {}

    /// Calls `func(key, mapped)` for the following values until it returns false. Returns whether the traversal
    /// reached the end.
    template <typename Func>
    bool visit(Func && func)
    {
        while (it != end)
        {
            const bool can_continue = func(it->getKey(), it->getMapped());
            ++it;
            if (!can_continue)
                break;
        }
        return it == end;
    }

private:
    typename Data::const_iterator it;
    typename Data::const_iterator end;
};

/// `StringHashMap` has no iterator across its submaps, so its traversal resumes from a `ValuePosition`.
template <typename Data>
requires requires { typename Data::ValuePosition; }
class TableTraversal<Data>
{
public:
    explicit TableTraversal(const Data & table_)
        : table(table_)
    {
    }

    template <typename Func>
    bool visit(Func && func)
    {
        return table.forEachValueFrom(position, std::forward<Func>(func));
    }

private:
    const Data & table;
    typename Data::ValuePosition position;
};

/// Keeps the hash table alive while its groups are materialized one batch at a time.
template <typename Method>
class GroupExtractorImpl final : public SeenGroupExtractor
{
public:
    GroupExtractorImpl(
        Method & method_, std::unique_ptr<AggregatedDataVariants> data_, std::vector<UInt64> group_counts_,
        DataTypes key_types_, Sizes key_sizes_)
        : method(method_)
        , data(std::move(data_))
        , group_counts(std::move(group_counts_))
        , key_types(std::move(key_types_))
        , key_sizes(std::move(key_sizes_))
        , traversal(method.data)
    {
    }

    SeenGroups next(size_t max_rows, size_t max_bytes) override
    {
        chassert(max_rows > 0);

        if (!data)
            return {};

        MutableColumns columns;
        std::vector<IColumn *> raw_columns;
        columns.reserve(key_types.size());
        raw_columns.reserve(key_types.size());
        for (const auto & key_type : key_types)
        {
            columns.push_back(key_type->createColumn());
            raw_columns.push_back(columns.back().get());
        }

        /// Packed keys may store their columns in another order; the shuffled pointers follow it.
        const auto shuffled_key_sizes = method.shuffleKeyColumns(raw_columns, key_sizes);
        const Sizes & sizes = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

        auto seen_rows = ColumnUInt64::create();
        auto & counts = seen_rows->getData();

        /// Returns whether the batch can take another group.
        auto has_room = [&]
        {
            if (counts.size() >= max_rows)
                return false;
            if (max_bytes)
            {
                size_t bytes = seen_rows->allocatedBytes();
                for (const auto & column : columns)
                    bytes += column->allocatedBytes();
                if (bytes >= max_bytes)
                    return false;
            }
            return true;
        };

        auto add_group = [&](const auto & key, AggregateDataPtr mapped)
        {
            method.insertKeyIntoColumns(key, raw_columns, sizes, &serialization_settings);
            counts.push_back(group_counts[groupIndexFromMapped(mapped)]);
            return has_room();
        };

        bool exhausted = false;
        /// The `NULL` group lives outside the table, so it comes first.
        if constexpr (requires { method.data.hasNullKeyData(); })
        {
            if (!null_key_extracted)
            {
                null_key_extracted = true;
                if (method.data.hasNullKeyData())
                {
                    raw_columns[0]->insertDefault();
                    counts.push_back(group_counts[groupIndexFromMapped(method.data.getNullKeyData())]);
                }
            }
        }

        if (has_room())
            exhausted = traversal.visit(add_group);

        /// Returned columns own their values, including keys extracted from the arena.
        if (exhausted)
            data.reset();

        if (counts.empty())
            return {};

        return {.keys = std::move(columns), .seen_rows = std::move(seen_rows)};
    }

    size_t getTotalByteCount() const override
    {
        return (data ? data->allocatedBytes() : 0) + group_counts.capacity() * sizeof(UInt64);
    }

private:
    Method & method;
    std::unique_ptr<AggregatedDataVariants> data;
    const std::vector<UInt64> group_counts;
    const DataTypes key_types;
    const Sizes key_sizes;
    /// `LIMIT BY` serializes keys with default settings, so the extracted keys are deserialized the same way.
    const IColumn::SerializationSettings serialization_settings{};
    bool null_key_extracted = false;
    TableTraversal<typename Method::Data> traversal;
};

}

ColumnNumbers calculateLimitByKeyColumnsPositions(const Block & header, const Names & column_names)
{
    ColumnNumbers positions;
    positions.reserve(column_names.size());
    for (const auto & name : column_names)
    {
        const auto position = header.getPositionByName(name);
        const auto & column = header.getByPosition(position).column;
        if (!(column && isColumnConst(*column)))
            positions.push_back(position);
    }
    return positions;
}

UInt64 computeGroupLimitEnd(UInt64 length, UInt64 offset)
{
    if (length > std::numeric_limits<UInt64>::max() - offset)
        return std::numeric_limits<UInt64>::max();
    return length + offset;
}

ChunkRowRange shrinkRunToLimitWindow(
    UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run, UInt64 group_offset, UInt64 group_limit_end)
{
    /// Rows from this run consumed by the group's OFFSET prefix.
    const UInt64 offset_rows_in_run
        = group_rows_seen_before_run < group_offset ? std::min(group_offset - group_rows_seen_before_run, run_row_count) : 0;

    /// The group's row count after skipping the OFFSET-covered prefix of this run.
    const UInt64 group_rows_seen_after_offset = group_rows_seen_before_run + offset_rows_in_run;

    /// Remaining rows this group can still contribute before reaching the limit end.
    const UInt64 remaining_rows_until_limit_end
        = group_rows_seen_after_offset < group_limit_end ? group_limit_end - group_rows_seen_after_offset : 0;

    const UInt64 rows_kept_from_run = std::min(run_row_count - offset_rows_in_run, remaining_rows_until_limit_end);

    chassert(offset_rows_in_run + rows_kept_from_run <= run_row_count);

    return {run_start_row + offset_rows_in_run, rows_kept_from_run};
}

LimitByGroupFilter::LimitByGroupFilter(
    const Block & header,
    const Names & column_names,
    UInt64 group_offset_,
    UInt64 group_limit_end_,
    const IProcessor & owner_,
    bool keep_fingerprints_)
    : grouping_key_positions(calculateLimitByKeyColumnsPositions(header, column_names))
    , group_offset(group_offset_)
    , group_limit_end(group_limit_end_)
    , data(std::make_unique<AggregatedDataVariants>())
    , owner(owner_)
{
    /// Constant columns do not help distinguish groups because they have the same value on every row.
    Names key_names;
    key_names.reserve(grouping_key_positions.size());
    key_types.reserve(grouping_key_positions.size());
    for (const auto position : grouping_key_positions)
    {
        const auto & column = header.getByPosition(position);
        key_names.push_back(column.name);
        key_types.push_back(column.type);
    }

    data->keys_size = key_names.size();
    const auto type = AggregatedDataVariants::chooseMethod(header, key_names, data->key_sizes);
    data->init(type);
    retains_fingerprints = keep_fingerprints_
        && !std::ranges::all_of(key_types, [](const DataTypePtr & key_type) { return key_type->isComparable(); });

    ColumnsHashing::HashMethodContextSettings ctx_settings;
    ctx_settings.max_threads = 1;
    hash_method_context = AggregatedDataVariants::createCache(type, ctx_settings);
}

void LimitByGroupFilter::processRunInsideWindow(
    UInt64 run_start_row, UInt64 run_row_count, size_t group_idx, UInt64 group_rows_seen_before_run)
{
    const auto slice = shrinkRunToLimitWindow(run_start_row, run_row_count, group_rows_seen_before_run, group_offset, group_limit_end);

    if (slice.length > 0)
        output_slices.push_back(slice);

    group_counts[group_idx] = group_rows_seen_before_run + run_row_count;
}

void LimitByGroupFilter::addGroupFingerprint(size_t row, const ColumnRawPtrs & key_columns)
{
    /// The fingerprint is computed from the original row, like the fingerprints of later input rows;
    /// rebuilding the row from the serialized key could change the bytes of aggregate states.
    group_fingerprints.push_back(ColumnsHashing::hash128(row, key_columns.size(), key_columns));
}

size_t LimitByGroupFilter::getTotalByteCount() const
{
    return data->allocatedBytes() + group_counts.capacity() * sizeof(UInt64)
        + group_fingerprints.capacity() * sizeof(UInt128);
}

bool LimitByGroupFilter::isExhausted() const
{
    return data->type == AggregatedDataVariants::Type::without_key && !group_counts.empty() && group_counts[0] >= group_limit_end;
}

void LimitByGroupFilter::prepareForInsert(Chunk & chunk) const
{
    materializeChunk(chunk, grouping_key_positions);
}

ColumnRawPtrs LimitByGroupFilter::getKeyColumns(const Columns & columns) const
{
    ColumnRawPtrs key_columns;
    key_columns.reserve(grouping_key_positions.size());
    for (const auto position : grouping_key_positions)
        key_columns.push_back(columns[position].get());
    return key_columns;
}

size_t LimitByGroupFilter::estimateGroupStateGrowth(size_t new_groups) const
{
    size_t growth = estimateVectorGrowth(group_counts, new_groups);
    if (retains_fingerprints)
        growth += estimateVectorGrowth(group_fingerprints, new_groups);
    return growth;
}

template <typename Method>
size_t LimitByGroupFilter::estimateGrowthMemoryImpl(
    const Method & hash_method, const ColumnRawPtrs & key_columns, size_t start_row, size_t num_rows) const
{
    size_t growth = estimateTableGrowth(hash_method.data, num_rows);
    if constexpr (stores_keys_in_arena<Method>)
    {
        const size_t arena_growth = data->aggregates_pool->estimateGrowthMemory(num_rows, estimateKeyBytes(key_columns, start_row, num_rows));
        if (common::addOverflow(growth, arena_growth, growth))
            return std::numeric_limits<size_t>::max();
    }
    if (common::addOverflow(growth, estimateGroupStateGrowth(num_rows), growth))
        return std::numeric_limits<size_t>::max();
    return growth;
}

size_t LimitByGroupFilter::estimateGrowthMemory(const Chunk & chunk) const
{
    const size_t num_rows = chunk.getNumRows();
    const auto key_columns = getKeyColumns(chunk.getColumns());
    switch (data->type)
    {
        case AggregatedDataVariants::Type::EMPTY:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY state is not initialized");
        case AggregatedDataVariants::Type::without_key:
            /// All rows form one group.
            return group_counts.empty() ? sizeof(UInt64) : 0;
#define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
            return estimateGrowthMemoryImpl(*data->NAME, key_columns, 0, num_rows);
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

size_t LimitByGroupFilter::estimateFilteringMemory(const Chunk & chunk) const
{
    const size_t num_rows = chunk.getNumRows();

    /// A run takes one slice, and every row can start one. A sparse selection also builds a row mask, and the
    /// output can copy the whole chunk.
    const size_t slices_bytes = roundUpToPowerOfTwoOrZero(num_rows * sizeof(ChunkRowRange));
    const size_t mask_bytes = roundUpToPowerOfTwoOrZero(num_rows + IColumn::Filter::pad_left + IColumn::Filter::pad_right);

    /// Packed-key methods prepare a key per row before probing the table.
    auto estimate_prepared_keys = [num_rows, &key_sizes = data->key_sizes]<typename Method>(const Method &) -> size_t
    {
        if constexpr (requires { Method::State::estimatePreparedKeysMemory(num_rows, key_sizes); })
            return Method::State::estimatePreparedKeysMemory(num_rows, key_sizes);
        else
            return 0;
    };

    size_t prepared_keys_bytes = 0;
    switch (data->type)
    {
        case AggregatedDataVariants::Type::EMPTY:
        case AggregatedDataVariants::Type::without_key:
            break;
#define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
            prepared_keys_bytes = estimate_prepared_keys(*data->NAME); \
            break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    }

    return chunk.allocatedBytes() + slices_bytes + mask_bytes + prepared_keys_bytes;
}

template <bool checked_insertion, typename Method>
bool LimitByGroupFilter::consumeImpl(
    Method & hash_method, const ColumnRawPtrs & grouping_key_columns, UInt64 row_count,
    const InsertionCheck * can_insert, UInt64 & processed_rows)
{
    if constexpr (SetAggregationMethod<Method>)
    {
        /// LimitBy keeps a group index in the cell's mapped slot, so it cannot use a set method. `chooseMethod`
        /// never returns one here; this branch exists only because the dispatch macro is generated over every
        /// `AggregatedDataVariants::Type`, including the set ones that `GROUP BY` without aggregates uses.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY does not support void-mapped aggregation methods");
    }
    else
    {
        typename Method::State state(grouping_key_columns, data->key_sizes, hash_method_context);
        /// The loop reads these through locals: its stores into the table may alias the members, so the
        /// members would be reloaded for every row.
        Arena & pool = *data->aggregates_pool;
        const IProcessor & processor = owner;

        UInt64 current_run_start_row = 0;
        size_t current_run_group_idx = 0;

        auto insert_group = [&](auto & emplace_result, size_t row) ALWAYS_INLINE
        {
            /// Assign a stable index into `group_counts` to the grouping key just inserted.
            const size_t group_idx = group_counts.size();
            group_counts.push_back(0);
            if (retains_fingerprints)
                addGroupFingerprint(row, grouping_key_columns);
            emplace_result.setMapped(mappedFromGroupIndex(group_idx));
            return group_idx;
        };

        FailPointInjection::pauseFailPoint(FailPoints::limit_by_transform_pause);

        for (UInt64 row_idx = 0; row_idx < row_count; ++row_idx)
        {
            if (processor.isCancelled())
                return false;

            if (row_idx == 5)
                FailPointInjection::pauseFailPoint(FailPoints::limit_by_transform_mid_loop_pause);

            size_t row_group_idx = 0;
            if constexpr (checked_insertion)
            {
                auto find_result = state.findKey(hash_method.data, row_idx, pool);
                if (find_result.isFound())
                    row_group_idx = groupIndexFromMapped(find_result.getMapped());
                else if ((*can_insert)(estimateGrowthMemoryImpl(hash_method, grouping_key_columns, row_idx, 1)))
                {
                    auto key_emplace_result = state.emplaceKey(hash_method.data, row_idx, pool);
                    chassert(key_emplace_result.isInserted());
                    row_group_idx = insert_group(key_emplace_result, row_idx);
                }
                else
                {
                    /// The rejected row is not counted; the runs before it are complete.
                    if (row_idx > 0)
                        processRun(current_run_start_row, row_idx - current_run_start_row, current_run_group_idx);
                    processed_rows = row_idx;
                    return true;
                }
            }
            else
            {
                auto key_emplace_result = state.emplaceKey(hash_method.data, row_idx, pool);
                if (key_emplace_result.isInserted()) /// New grouping key
                    row_group_idx = insert_group(key_emplace_result, row_idx);
                else /// Existing grouping key
                {
                    row_group_idx = groupIndexFromMapped(key_emplace_result.getMapped());
                    chassert(row_group_idx < group_counts.size());
                }
            }

            if (row_idx == 0)
                current_run_group_idx = row_group_idx;
            else if (row_group_idx != current_run_group_idx) /// Local run ended
            {
                processRun(current_run_start_row, row_idx - current_run_start_row, current_run_group_idx);
                current_run_group_idx = row_group_idx;
                current_run_start_row = row_idx;
            }
        }

        /// Flush the final run, which extends to the end of the chunk.
        processRun(current_run_start_row, row_count - current_run_start_row, current_run_group_idx);
        processed_rows = row_count;
        return true;
    }
}

Chunk LimitByGroupFilter::filterImpl(Chunk chunk, const InsertionCheck * can_insert, UInt64 & processed_rows)
{
    /// `output_slices` is a member scratch buffer reused across chunks. A previous call may have thrown after
    /// populating it (for example MEMORY_LIMIT_EXCEEDED while the grouping hash table grows), and the caller
    /// can call again with the next chunk, so always start from an empty buffer.
    output_slices.clear();

    const UInt64 row_count = chunk.getNumRows();
    processed_rows = row_count;
    if (row_count == 0)
        return chunk;

    auto chunk_columns = chunk.detachColumns();

    /// Without non-constant grouping keys every row belongs to one logical group and is processed as one run.
    if (data->type == AggregatedDataVariants::Type::without_key)
    {
        if (group_counts.empty())
            group_counts.push_back(0);
        processRun(0, row_count, 0);
    }
    else
    {
        Columns normalized_grouping_key_columns;
        normalized_grouping_key_columns.reserve(grouping_key_positions.size());
        ColumnRawPtrs grouping_key_columns;
        grouping_key_columns.reserve(grouping_key_positions.size());
        for (size_t position : grouping_key_positions)
        {
            normalized_grouping_key_columns.push_back(removeSpecialRepresentations(chunk_columns[position]->convertToFullColumnIfConst()));
            grouping_key_columns.push_back(normalized_grouping_key_columns.back().get());
        }

        /// `consumeImpl` maps rows to groups and splits the chunk into runs.
        bool completed = true;
        switch (data->type)
        {
#define M(NAME, IS_TWO_LEVEL) \
            case AggregatedDataVariants::Type::NAME: \
                completed = can_insert \
                    ? consumeImpl<true>(*data->NAME, grouping_key_columns, row_count, can_insert, processed_rows) \
                    : consumeImpl<false>(*data->NAME, grouping_key_columns, row_count, can_insert, processed_rows); \
                break;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

            case AggregatedDataVariants::Type::EMPTY:
            case AggregatedDataVariants::Type::without_key:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in LimitByGroupFilter");
        }

        if (!completed)
            return chunk;
    }

    /// No row from this chunk survived `LIMIT BY`.
    if (output_slices.empty())
        return chunk;

    materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);
    return chunk;
}

Chunk LimitByGroupFilter::filter(Chunk chunk)
{
    UInt64 processed_rows = 0;
    return filterImpl(std::move(chunk), nullptr, processed_rows);
}

LimitByGroupFilter::FilterResult LimitByGroupFilter::filterWithInsertionCheck(Chunk chunk, const InsertionCheck & can_insert)
{
    chassert(hasKeyColumns());
    UInt64 processed_rows = 0;
    auto result = filterImpl(std::move(chunk), &can_insert, processed_rows);
    return {.chunk = std::move(result), .processed_rows = processed_rows};
}

template <bool is_two_level, typename Method>
std::unique_ptr<SeenGroupExtractor> LimitByGroupFilter::createGroupExtractor(Method & method)
{
    if constexpr (is_two_level || SetAggregationMethod<Method>)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT BY uses only single-level hash methods that map keys to groups");
    else
    {
        auto key_sizes = data->key_sizes;
        return std::make_unique<GroupExtractorImpl<Method>>(
            method, std::move(data), std::move(group_counts), std::move(key_types), std::move(key_sizes));
    }
}

std::unique_ptr<SeenGroupExtractor> LimitByGroupFilter::extractSeenGroups() &&
{
    chassert(hasKeyColumns());
    chassert(getTotalRowCount() > 0);

    if (retains_fingerprints)
    {
        /// The fingerprints and the counts describe the groups completely, so the table and its arena go at once.
        data.reset();
        return std::make_unique<FingerprintExtractorImpl>(std::move(group_fingerprints), std::move(group_counts));
    }

    switch (data->type)
    {
        case AggregatedDataVariants::Type::EMPTY:
        case AggregatedDataVariants::Type::without_key:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Groups cannot be extracted from LIMIT BY without key columns");
#define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
            return createGroupExtractor<IS_TWO_LEVEL>(*data->NAME);
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

}
