#include <Processors/Transforms/DistinctTopKTransform.h>

#include <Columns/ColumnConst.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/SipHash.h>
#include <Common/iota.h>
#include <Processors/Transforms/DistinctSetFilter.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_COLLATION;
    extern const int LOGICAL_ERROR;
}

DistinctTopKTransform::DistinctTopKTransform(
    SharedHeader header_,
    const Names & columns_,
    size_t k_,
    const SortDescription & sort_description_,
    TopKThresholdTrackerPtr threshold_tracker_)
    : IAccumulatingTransform(header_, header_)
    , key_positions(calculateDistinctKeyColumnsPositions(*header_, columns_))
    , sort_description(sort_description_)
    , k(k_)
    , max_kept_rows(k_ + std::max(k_ / 2, min_trim_slack))
    , threshold_tracker(std::move(threshold_tracker_))
{
    if (k == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DistinctTopKTransform requires a positive limit");

    const auto & header = *header_;
    kept.reserve(header.columns());
    for (const auto & column : header)
        kept.push_back(column.type->createColumn());

    sort_positions.reserve(sort_description.size());
    for (const auto & column_description : sort_description)
    {
        const size_t position = header.getPositionByName(column_description.column_name);
        if (column_description.collator && !kept[position]->isCollationSupported())
            throw Exception(ErrorCodes::BAD_COLLATION, "Collations could be specified only for String, LowCardinality(String), "
                            "Nullable(String) or for Array or Tuple, containing them.");
        sort_positions.push_back(position);
    }

    for (size_t position : sort_positions)
        boundary.push_back(kept[position]->cloneEmpty());

    size_t keys_bytes = 0;
    packed_keys = true;
    for (size_t position : key_positions)
    {
        const auto & column = *kept[position];
        if (!column.isFixedAndContiguous())
        {
            packed_keys = false;
            break;
        }
        key_sizes.push_back(column.sizeOfValueIfFixed());
        keys_bytes += key_sizes.back();
    }
    if (keys_bytes > sizeof(UInt128))
        packed_keys = false;
}

UInt128 DistinctTopKTransform::getKey(size_t row) const
{
    if (packed_keys)
        return packFixed<UInt128>(row, key_columns.size(), key_columns, key_sizes);

    SipHash hash;
    for (const auto * column : key_columns)
        column->updateHashWithValue(row, hash);
    return hash.get128();
}

template <typename LhsColumn, typename RhsColumn>
int DistinctTopKTransform::compareRows(LhsColumn && lhs_column, size_t lhs_row, RhsColumn && rhs_column, size_t rhs_row) const
{
    for (size_t i = 0; i < sort_positions.size(); ++i)
    {
        const auto & column_description = sort_description[i];
        const IColumn & lhs = lhs_column(i);
        const IColumn & rhs = rhs_column(i);

        const int res = column_description.collator
            ? lhs.compareAtWithCollation(lhs_row, rhs_row, rhs, column_description.nulls_direction, *column_description.collator)
            : lhs.compareAt(lhs_row, rhs_row, rhs, column_description.nulls_direction);

        if (res != 0)
            return res * column_description.direction;
    }
    return 0;
}

bool DistinctTopKTransform::isBelowBoundary(const Columns & columns, size_t row) const
{
    if (!has_boundary)
        return true;

    return compareRows(
        [&](size_t i) -> const IColumn & { return *columns[sort_positions[i]]; }, row,
        [&](size_t i) -> const IColumn & { return *boundary[i]; }, 0) < 0;
}

void DistinctTopKTransform::compareWithBoundary(const Columns & columns, size_t num_rows)
{
    compare_results.assign(num_rows, static_cast<Int8>(0));

    /// A single sort column needs no tracking of the rows equal so far.
    if (sort_positions.size() == 1 && !sort_description.front().collator)
    {
        const auto & column_description = sort_description.front();
        columns[sort_positions.front()]->compareColumn(
            *boundary.front(), 0, nullptr, compare_results, column_description.direction, column_description.nulls_direction);
        return;
    }

    rows_to_compare.resize(num_rows);
    iota(rows_to_compare.data(), num_rows, UInt64(0));

    for (size_t i = 0; i < sort_positions.size(); ++i)
    {
        const auto & column_description = sort_description[i];

        /// `compareColumn` does not support collations; the rows equal so far are compared row by row.
        if (column_description.collator)
            break;

        columns[sort_positions[i]]->compareColumn(
            *boundary[i], 0, &rows_to_compare, compare_results, column_description.direction, column_description.nulls_direction);

        if (rows_to_compare.empty())
            break;
    }
}

void DistinctTopKTransform::appendRow(const Columns & columns, size_t row, UInt128 key)
{
    for (size_t i = 0; i < kept.size(); ++i)
        kept[i]->insertFrom(*columns[i], row);
    kept_keys.push_back(key);
    stale.push_back(0);
    ++appended_since_boundary;
}

void DistinctTopKTransform::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    /// Trims set the boundary too, but with few distinct keys the set never grows enough to be trimmed.
    if (appended_since_boundary && kept_keys.size() - num_stale >= k)
        updateBoundary();

    const size_t num_rows = chunk.getNumRows();
    const Columns & columns = chunk.getColumns();

    key_columns.clear();
    for (size_t position : key_positions)
        key_columns.push_back(columns[position].get());

    auto chunk_column = [&](size_t i) -> const IColumn & { return *columns[sort_positions[i]]; };
    auto kept_column = [&](size_t i) -> const IColumn & { return *kept[sort_positions[i]]; };

    /// Most rows are usually not better than the boundary, so they are compared with it in bulk, the way
    /// `PartialSortingTransform` compares the rows with its threshold. A trim in the middle of the chunk only tightens
    /// the boundary, so a row worse than the old boundary is worse than the new one too.
    const bool compared_with_boundary = has_boundary;
    const size_t compared_boundary_version = boundary_version;
    size_t num_candidates = num_rows;
    if (compared_with_boundary)
    {
        compareWithBoundary(columns, num_rows);

        candidate_rows.clear();
        for (size_t row = 0; row < num_rows; ++row)
            if (compare_results[row] <= 0)
                candidate_rows.push_back(static_cast<UInt32>(row));
        num_candidates = candidate_rows.size();
    }

    auto process_row = [&](size_t row)
    {
        /// Not better than the worst of `k` kept keys: neither a new key nor an improvement can reach the result.
        if (compared_with_boundary && compare_results[row] > 0)
            return;

        const bool known_below_boundary
            = compared_with_boundary && compare_results[row] < 0 && compared_boundary_version == boundary_version;
        if (!known_below_boundary && !isBelowBoundary(columns, row))
            return;

        const UInt128 key = getKey(row);

        auto * it = index.find(key);
        if (it)
        {
            size_t & kept_row = it->getMapped();

            if (compareRows(chunk_column, row, kept_column, kept_row) >= 0)
                return;

            /// An improved key gets a new row, the old one stays until the next trim.
            stale[kept_row] = 1;
            ++num_stale;
            kept_row = kept_keys.size();
            appendRow(columns, row, key);
        }
        else
        {
            bool inserted = false;
            index.emplace(key, it, inserted);
            it->getMapped() = kept_keys.size();
            appendRow(columns, row, key);
        }

        if (kept_keys.size() > max_kept_rows)
            trim();
    };

    if (num_candidates <= max_kept_rows || !use_sorted_visit)
    {
        if (compared_with_boundary)
        {
            for (size_t row : candidate_rows)
                process_row(row);
        }
        else
        {
            for (size_t row = 0; row < num_rows; ++row)
                process_row(row);
        }
        return;
    }

    /// More rows may pass the boundary than the set keeps. When the rows arrive in the best order first, e.g. with
    /// `ORDER BY ts DESC` over rows inserted by ascending `ts`, every row would improve the set and the set would be
    /// trimmed every `min_trim_slack` rows. So the best rows by the first sort column are visited first, up to the
    /// first one worse than the boundary in that column: all the following ones are worse too.
    const auto & first_description = sort_description.front();
    const IColumn & first_column = chunk_column(0);
    const auto direction = first_description.direction == -1
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;
    const size_t num_sorted_rows = std::min(num_rows, 2 * max_kept_rows);

    if (first_description.collator)
        first_column.getPermutationWithCollation(
            *first_description.collator, direction, IColumn::PermutationSortStability::Unstable, num_sorted_rows,
            first_description.nulls_direction, permutation);
    else
        first_column.getPermutation(
            direction, IColumn::PermutationSortStability::Unstable, num_sorted_rows, first_description.nulls_direction, permutation);

    auto worse_than_boundary_in_first_column = [&](size_t row)
    {
        if (!has_boundary)
            return false;

        const int res = first_description.collator
            ? first_column.compareAtWithCollation(row, 0, *boundary[0], first_description.nulls_direction, *first_description.collator)
            : first_column.compareAt(row, 0, *boundary[0], first_description.nulls_direction);
        return res * first_description.direction > 0;
    };

    /// Only the first `num_sorted_rows` rows of the permutation are defined: some columns (`ColumnLowCardinality`) return
    /// no more than the limit.
    for (size_t i = 0; i < num_sorted_rows; ++i)
    {
        if (worse_than_boundary_in_first_column(permutation[i]))
            return;
        process_row(permutation[i]);
    }

    /// The set has not filled with the best rows, e.g. because they are copies of a few keys, so the remaining rows are
    /// visited in their order. The sorting did not pay off and is not repeated for the following chunks.
    use_sorted_visit = false;
    visited_rows.assign(num_rows, static_cast<UInt8>(0));
    for (size_t i = 0; i < num_sorted_rows; ++i)
        visited_rows[permutation[i]] = 1;
    for (size_t row = 0; row < num_rows; ++row)
        if (!visited_rows[row])
            process_row(row);
}

void DistinctTopKTransform::updateBoundary()
{
    trim_rows.clear();
    for (size_t row = 0; row < kept_keys.size(); ++row)
        if (!stale[row])
            trim_rows.push_back(row);

    if (trim_rows.size() < k)
        return;

    auto kept_column = [&](size_t i) -> const IColumn & { return *kept[sort_positions[i]]; };

    std::nth_element(
        trim_rows.begin(), trim_rows.begin() + (k - 1), trim_rows.end(),
        [&](size_t lhs, size_t rhs) { return compareRows(kept_column, lhs, kept_column, rhs) < 0; });

    /// The `k`-th best key is the new boundary: `k` keys are at least as good as it. It never moves back: the kept keys
    /// that defined the previous boundary are still kept, and are still at least as good as it.
    const size_t boundary_row = trim_rows[k - 1];
    for (size_t i = 0; i < sort_positions.size(); ++i)
    {
        auto new_boundary = boundary[i]->cloneEmpty();
        new_boundary->insertFrom(*kept[sort_positions[i]], boundary_row);
        boundary[i] = std::move(new_boundary);
    }

    has_boundary = true;
    ++boundary_version;
    appended_since_boundary = 0;

    if (threshold_tracker)
    {
        Field value;
        boundary.front()->get(0, value);
        threshold_tracker->testAndSet(value);
    }
}

void DistinctTopKTransform::trim()
{
    updateBoundary();
    if (trim_rows.size() > k)
        trim_rows.resize(k);

    trim_filter.assign(kept_keys.size(), static_cast<UInt8>(0));
    for (size_t row : trim_rows)
        trim_filter[row] = 1;

    for (auto & column : kept)
        column = IColumn::mutate(column->filter(trim_filter, trim_rows.size()));

    size_t new_row = 0;
    for (size_t row = 0; row < kept_keys.size(); ++row)
        if (trim_filter[row])
            kept_keys[new_row++] = kept_keys[row];
    kept_keys.resize(new_row);

    stale.assign(new_row, 0);
    num_stale = 0;

    index.clear();
    for (size_t row = 0; row < kept_keys.size(); ++row)
    {
        decltype(index)::LookupResult it = nullptr;
        bool inserted = false;
        index.emplace(kept_keys[row], it, inserted);
        it->getMapped() = row;
    }
}

Chunk DistinctTopKTransform::generate()
{
    if (kept_keys.empty())
        return {};

    if (num_stale || kept_keys.size() > k)
        trim();

    const size_t num_rows = kept_keys.size();
    const auto & header = getOutputPort().getHeader();

    Columns result;
    result.reserve(kept.size());
    for (size_t i = 0; i < kept.size(); ++i)
    {
        const auto & header_column = header.getByPosition(i).column;
        if (header_column && isColumnConst(*header_column))
            result.push_back(header_column->cloneResized(num_rows));
        else
            result.push_back(std::move(kept[i]));
    }

    kept_keys.clear();
    index.clear();
    return Chunk(std::move(result), num_rows);
}

}
