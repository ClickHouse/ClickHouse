#include <Processors/Transforms/LimitByGroupMapping.h>

#include <Columns/ColumnConst.h>

#include <limits>

namespace DB
{

UInt64 computeGroupLimitEnd(UInt64 length, UInt64 offset)
{
    if (length > std::numeric_limits<UInt64>::max() - offset)
        return std::numeric_limits<UInt64>::max();
    return length + offset;
}

GroupingKeys filterNonConstKeys(const Block & header, const Names & column_names)
{
    GroupingKeys non_const_keys;
    non_const_keys.names.reserve(column_names.size());
    non_const_keys.positions.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        auto position = header.getPositionByName(column_name);
        const auto & column = header.getByPosition(position).column;
        if (!(column && isColumnConst(*column)))
        {
            non_const_keys.names.emplace_back(column_name);
            non_const_keys.positions.emplace_back(position);
        }
    }
    return non_const_keys;
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

LimitByGroupMapping::LimitByGroupMapping(const Block & header, const Names & column_names)
    : keys(filterNonConstKeys(header, column_names))
{
    data.keys_size = keys.names.size();
    auto type = AggregatedDataVariants::chooseMethod(header, keys.names, data.key_sizes);
    data.init(type);

    ColumnsHashing::HashMethodContextSettings ctx_settings;
    ctx_settings.max_threads = 1;
    hash_method_context = AggregatedDataVariants::createCache(type, ctx_settings);
}

size_t LimitByGroupMapping::allocatedBytes() const
{
    return data.allocatedBytes() + group_counts.capacity() * sizeof(UInt64);
}

template <bool is_two_level, typename Method>
requires MapAggregationMethod<Method>
void LimitByGroupMapping::extractGroupsImpl(Method & hash_method, MutableColumns & key_columns, PaddedPODArray<UInt64> & rows_seen)
{
    std::vector<IColumn *> raw_key_columns;
    raw_key_columns.reserve(key_columns.size());
    for (auto & key_column : key_columns)
        raw_key_columns.push_back(key_column.get());

    /// A packed fixed-key method reads the key parts in its own column order, so the raw pointers are
    /// permuted to match before any key is inserted. This does not move the columns themselves.
    auto shuffled_key_sizes = hash_method.shuffleKeyColumns(raw_key_columns, data.key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : data.key_sizes;

    /// The keys were inserted through a hash method context built with the default settings, so they
    /// have to be read back with the same ones.
    const IColumn::SerializationSettings serialization_settings{};

    auto extract_from_table = [&](auto & table)
    {
        /// The NULL group lives outside the cells, so `forEachValue` below does not see it. A two-level
        /// table keeps it in its first bucket, which this same branch covers.
        if constexpr (Method::low_cardinality_optimization || Method::one_key_nullable_optimization)
        {
            if (table.hasNullKeyData())
            {
                raw_key_columns[0]->insertDefault();
                rows_seen.push_back(group_counts[groupIndexFromMapped(table.getNullKeyData())]);
            }
        }

        table.forEachValue(
            [&](const auto & key, auto & mapped)
            {
                hash_method.insertKeyIntoColumns(key, raw_key_columns, key_sizes_ref, &serialization_settings);
                rows_seen.push_back(group_counts[groupIndexFromMapped(mapped)]);
            });
    };

    if constexpr (is_two_level)
    {
        for (auto & bucket : hash_method.data.impls)
            extract_from_table(bucket);
    }
    else
    {
        extract_from_table(hash_method.data);
    }
}

void LimitByGroupMapping::extractGroups(MutableColumns & key_columns, PaddedPODArray<UInt64> & rows_seen)
{
    rows_seen.reserve(rows_seen.size() + group_counts.size());
    for (auto & key_column : key_columns)
        key_column->reserve(key_column->size() + group_counts.size());

    switch (data.type)
    {
#define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
            extractGroupsImpl<IS_TWO_LEVEL>(*data.NAME, key_columns, rows_seen); \
            break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        case AggregatedDataVariants::Type::EMPTY:
        case AggregatedDataVariants::Type::without_key:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in LimitByGroupMapping::extractGroups");
    }
}

}
