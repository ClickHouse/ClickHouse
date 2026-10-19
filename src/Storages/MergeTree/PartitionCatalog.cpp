#include <Storages/MergeTree/PartitionCatalog.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <optional>

namespace ProfileEvents
{
extern const Event PartitionValueIndexCatalogBuilds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Compares row `lhs_row` of `lhs` with row `rhs_row` of `rhs` lexicographically. NULL is greater than any value,
/// which matches `KeyCondition`, where NULL in a key is treated as positive infinity.
int compareRows(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row)
{
    for (size_t i = 0; i < lhs.size(); ++i)
    {
        if (int res = lhs[i]->compareAt(lhs_row, rhs_row, *rhs[i], /*nan_direction_hint=*/1))
            return res;
    }
    return 0;
}

bool typesEqual(const DataTypes & lhs, const DataTypes & rhs)
{
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](const auto & l, const auto & r) { return l->equals(*r); });
}

MutableColumns createValueColumns(const DataTypes & types, size_t reserve)
{
    MutableColumns columns;
    columns.reserve(types.size());
    for (const auto & type : types)
    {
        columns.push_back(removeLowCardinality(type)->createColumn());
        columns.back()->reserve(reserve);
    }
    return columns;
}

Columns toColumns(MutableColumns && columns)
{
    Columns res;
    res.reserve(columns.size());
    for (auto & column : columns)
        res.push_back(std::move(column));
    return res;
}

/// Builds the partition set for `ids` (in ascending order). The values of the partitions that are also in `previous`
/// are taken from it in its value order, the values of the new partitions are taken from the parts and sorted, and
/// the two sorted sequences are merged.
PartitionSetPtr buildPartitionSet(
    std::vector<String> ids,
    const std::vector<size_t> & first_part_of_partition,
    const RangesInDataParts & parts,
    const DataTypes & types,
    const PartitionSetPtr & previous)
{
    const size_t num_partitions = ids.size();
    const size_t num_columns = types.size();

    /// For each partition of `previous`, its position in `ids`, if it is still there.
    std::vector<std::optional<size_t>> previous_to_new;
    /// Positions in `ids` of the partitions that are not in `previous`.
    std::vector<size_t> added;

    if (previous)
    {
        const auto & previous_ids = previous->ids;
        previous_to_new.resize(previous_ids.size());
        size_t i = 0;
        for (size_t j = 0; j < num_partitions; ++j)
        {
            while (i < previous_ids.size() && previous_ids[i] < ids[j])
                ++i;

            if (i < previous_ids.size() && previous_ids[i] == ids[j])
                previous_to_new[i++] = j;
            else
                added.push_back(j);
        }
    }
    else
    {
        added.resize(num_partitions);
        for (size_t j = 0; j < num_partitions; ++j)
            added[j] = j;
    }

    MutableColumns added_columns = createValueColumns(types, added.size());
    for (size_t position : added)
    {
        const auto & value = parts[first_part_of_partition[position]].data_part->partition.value;
        if (value.size() != num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} has a value of {} columns, but the partition key has {} columns",
                ids[position], value.size(), num_columns);

        for (size_t c = 0; c < num_columns; ++c)
            added_columns[c]->insert(value[c]);
    }
    Columns added_values = toColumns(std::move(added_columns));

    /// Rows of `added_values` in value order.
    std::vector<size_t> added_order(added.size());
    for (size_t i = 0; i < added_order.size(); ++i)
        added_order[i] = i;
    std::sort(added_order.begin(), added_order.end(), [&](size_t lhs, size_t rhs) { return compareRows(added_values, lhs, added_values, rhs) < 0; });

    /// Rows of `previous->sorted_values` of the partitions that are still there, in value order.
    std::vector<size_t> kept_rows;
    if (previous)
    {
        kept_rows.reserve(num_partitions - added.size());
        for (size_t row = 0; row < previous->value_order.size(); ++row)
        {
            if (previous_to_new[previous->value_order[row]])
                kept_rows.push_back(row);
        }
    }

    auto set = std::make_shared<PartitionSet>();
    set->types = types;
    set->value_order.reserve(num_partitions);
    MutableColumns sorted_columns = createValueColumns(types, num_partitions);

    auto take_kept = [&](size_t row)
    {
        for (size_t c = 0; c < num_columns; ++c)
            sorted_columns[c]->insertFrom(*previous->sorted_values[c], row);
        set->value_order.push_back(*previous_to_new[previous->value_order[row]]);
    };

    auto take_added = [&](size_t row)
    {
        for (size_t c = 0; c < num_columns; ++c)
            sorted_columns[c]->insertFrom(*added_values[c], row);
        set->value_order.push_back(added[row]);
    };

    size_t kept_pos = 0;
    size_t added_pos = 0;
    while (kept_pos < kept_rows.size() || added_pos < added_order.size())
    {
        if (added_pos == added_order.size()
            || (kept_pos < kept_rows.size() && compareRows(previous->sorted_values, kept_rows[kept_pos], added_values, added_order[added_pos]) <= 0))
            take_kept(kept_rows[kept_pos++]);
        else
            take_added(added_order[added_pos++]);
    }

    chassert(set->value_order.size() == num_partitions);
    set->sorted_values = toColumns(std::move(sorted_columns));
    set->ids = std::move(ids);
    return set;
}

}

bool PartitionCatalog::canBuildForTypes(const DataTypes & types)
{
    if (types.empty())
        return false;

    return std::all_of(types.begin(), types.end(), [](const DataTypePtr & type)
    {
        WhichDataType which(removeNullable(removeLowCardinality(type)));
        return which.isInteger() || which.isDecimal() || which.isDateOrDate32OrDateTimeOrDateTime64() || which.isTime() || which.isTime64()
            || which.isEnum() || which.isStringOrFixedString() || which.isUUID() || which.isIPv4() || which.isIPv6();
    });
}

PartitionCatalogPtr PartitionCatalog::build(const RangesInDataPartsPtr & parts, const DataTypes & types, const PartitionSetPtr & previous)
{
    ProfileEvents::increment(ProfileEvents::PartitionValueIndexCatalogBuilds);

    auto catalog = std::make_shared<PartitionCatalog>();
    catalog->source = parts;
    catalog->source_raw = parts.get();
    catalog->total_parts = parts->size();

    std::vector<String> ids;
    std::vector<size_t> first_part_of_partition;

    for (size_t i = 0; i < parts->size(); ++i)
    {
        const auto & part = (*parts)[i].data_part;
        catalog->total_granules += part->index_granularity->getMarksCountWithoutFinal();

        const auto & partition_id = part->info.getPartitionId();
        if (!ids.empty() && ids.back() == partition_id)
            continue;

        if (!ids.empty())
        {
            if (!(ids.back() < partition_id))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Parts are not sorted by partition ID: partition {} goes after partition {}",
                    partition_id, ids.back());

            catalog->part_ranges.back().second = i;
        }

        ids.push_back(partition_id);
        first_part_of_partition.push_back(i);
        catalog->part_ranges.emplace_back(i, i);
    }

    if (!ids.empty())
        catalog->part_ranges.back().second = parts->size();

    const bool can_reuse_previous = previous && typesEqual(previous->types, types);
    if (can_reuse_previous && previous->ids == ids)
        catalog->partition_set = previous;
    else
        catalog->partition_set = buildPartitionSet(std::move(ids), first_part_of_partition, *parts, types, can_reuse_previous ? previous : nullptr);

    return catalog;
}

}
