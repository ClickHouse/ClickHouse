#include <Interpreters/AdaptiveAggregation/MultiPassTopK/TopKCandidates.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cstring>
#include <limits>

namespace DB
{

namespace
{

UInt64 fold(UInt64 lhs, UInt64 rhs)
{
    const auto product = static_cast<unsigned __int128>(lhs) * rhs;
    return static_cast<UInt64>(product) ^ static_cast<UInt64>(product >> 64);
}

/// The hash of a `String` key for its cell. It reads at most four 8-byte words of the key, the first, the
/// last and two in between, so that it costs the same for a key of any length, and reads a key of up to 16
/// bytes whole. Keys that agree in those words and in the length share their cell, which loosens its bound
/// but never breaks it.
UInt32 hashString(std::string_view key)
{
    const char * data = key.data();
    const size_t size = key.size();
    UInt64 first = 0;
    UInt64 second = 0;
    UInt64 third = 0;
    UInt64 last = 0;
    if (size > 16)
    {
        first = unalignedLoad<UInt64>(data);
        second = unalignedLoad<UInt64>(data + size / 3);
        third = unalignedLoad<UInt64>(data + size * 2 / 3 - 8);
        last = unalignedLoad<UInt64>(data + size - 8);
    }
    else if (size >= 8)
    {
        first = unalignedLoad<UInt64>(data);
        last = unalignedLoad<UInt64>(data + size - 8);
    }
    else if (size >= 4)
    {
        first = unalignedLoad<UInt32>(data);
        last = unalignedLoad<UInt32>(data + size - 4);
    }
    else if (size > 0)
    {
        first = static_cast<UInt8>(data[0]) | (UInt64{static_cast<UInt8>(data[size / 2])} << 8) | (UInt64{static_cast<UInt8>(data[size - 1])} << 16);
    }

    const UInt64 hash = fold(first ^ 0x9E3779B97F4A7C15ULL, second ^ 0xC2B2AE3D27D4EB4FULL)
        + fold(third ^ 0x165667B19E3779F9ULL, last ^ (0x27D4EB2F165667C5ULL + size));
    return static_cast<UInt32>(fold(hash, 0x9FB21C651E98DF25ULL));
}

}

void TopKCandidates::hashKeys(const ColumnRawPtrs & keys, size_t rows, PaddedPODArray<UInt32> & hashes)
{
    hashes.assign(rows, WEAK_HASH32_INITIAL_VALUE);
    for (const auto * key : keys)
    {
        /// A `String` key takes the hash of `hashString`, which needs the column in full. Another column
        /// takes its weak hash, which is the same for every representation of it.
        const ColumnPtr full = key->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
        if (const auto * strings = typeid_cast<const ColumnString *>(full.get()))
        {
            for (size_t i = 0; i < rows; ++i)
                hashes[i] = combineWeakHash32(hashString(strings->getDataAt(i)), hashes[i]);
        }
        else
        {
            full->computeHashInto(0, rows, hashes.data(), /*initial=*/false);
        }
    }
}

namespace
{

/// The cells of the keys in the first `keys_size` columns of `block`.
TopKCellSet cellsOfKeys(const Block & block, size_t keys_size)
{
    ColumnRawPtrs keys;
    for (size_t i = 0; i < keys_size; ++i)
        keys.push_back(block.getByPosition(i).column.get());

    PaddedPODArray<UInt32> hashes;
    TopKCandidates::hashKeys(keys, block.rows(), hashes);
    TopKCellSet cells(TopKCandidates::num_cells);
    for (const UInt32 hash : hashes)
        cells.set(TopKCandidates::cell(hash));
    return cells;
}

}

TopKCandidates::TopKCandidates(ConstBlockPtr seed_, size_t keys_size, std::optional<TopKDistinctRank> distinct_rank_)
    : seed(std::move(seed_))
    , distinct_rank(std::move(distinct_rank_))
    , candidate_cells(cellsOfKeys(*seed, keys_size))
    , cell_counts(num_cells)
{
}

void TopKCandidates::addCellCounts(const std::vector<UInt32> & counts)
{
    std::lock_guard lock(mutex);
    for (size_t i = 0; i < num_cells; ++i)
        cell_counts[i] += counts[i];
}

TopKCandidateProof TopKCandidates::prove(const ColumnRawPtrs & keys, const PaddedPODArray<UInt64> & rows, UInt64 nth_rank)
{
    const size_t num_candidates = rows.size();

    /// The counts of the other keys of every cell. Only the cells with candidates count every row (see
    /// `TopKCandidateStream::countRuns`), so only there the rows of the candidates come off.
    std::vector<UInt64> others;
    {
        std::lock_guard lock(mutex);
        others = std::move(cell_counts);
    }
    PaddedPODArray<UInt32> hashes;
    hashKeys(keys, num_candidates, hashes);
    for (size_t i = 0; i < num_candidates; ++i)
    {
        auto & cell_count = others[cell(hashes[i])];
        chassert(cell_count >= rows[i]);
        cell_count -= rows[i];
    }

    TopKCandidateProof proof;
    proof.bound = *std::ranges::max_element(others);
    proof.hot_cells = std::make_shared<TopKCellSet>(num_cells);
    for (size_t i = 0; i < num_cells; ++i)
        if (others[i] > nth_rank)
            proof.hot_cells->set(i);
    proof.proven.resize(num_candidates);
    for (size_t i = 0; i < num_candidates; ++i)
    {
        proof.proven[i] = rows[i] && !proof.hot_cells->test(cell(hashes[i]));
        proof.num_proven += proof.proven[i];
    }
    return proof;
}

TopKCandidateStream::TopKCandidateStream(TopKCandidatesPtr candidates_, const Block & header, const Names & keys)
    : candidates(std::move(candidates_))
{
    for (const auto & key : keys)
        key_positions.push_back(header.getPositionByName(key));
    if (const auto & distinct_rank = candidates->distinct_rank)
    {
        for (const auto & value : distinct_rank->values)
            value_positions.push_back(header.getPositionByName(value));
        if (!distinct_rank->condition.empty())
            condition_position = header.getPositionByName(distinct_rank->condition);
    }
}

bool TopKCandidateStream::keepCandidateRows(Columns & columns, UInt64 & num_rows)
{
    /// Allocated by the thread of the stream: the pipeline of the pass is built in one thread, which would otherwise
    /// zero the counts of all the streams.
    if (cell_counts.empty())
        cell_counts.assign(TopKCandidates::num_cells, 0);

    /// A cell counts at most the rows since its last flush, which therefore stay within its 32 bits.
    if (unflushed_rows + num_rows > std::numeric_limits<UInt32>::max())
        flush();
    unflushed_rows += num_rows;

    key_columns.clear();
    for (const size_t position : key_positions)
        key_columns.push_back(columns[position].get());
    TopKCandidates::hashKeys(key_columns, num_rows, hashes);

    filter.resize(num_rows);
    size_t kept = 0;
    if (candidates->distinct_rank)
    {
        kept = countRuns(columns, num_rows);
    }
    else
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            const size_t cell = TopKCandidates::cell(hashes[i]);
            ++cell_counts[cell];
            const UInt8 keep = candidates->candidate_cells.test(cell);
            filter[i] = keep;
            kept += keep;
        }
    }

    if (kept == 0)
        return false;

    if (kept < num_rows)
    {
        for (auto & column : columns)
            column = column->filter(filter, kept);
        num_rows = kept;
    }
    return true;
}

void TopKCandidateStream::flush()
{
    /// A stream that took no rows since its last flush adds nothing, and one that took none at all has no counts.
    if (unflushed_rows == 0)
        return;
    candidates->addCellCounts(cell_counts);
    std::ranges::fill(cell_counts, 0);
    unflushed_rows = 0;
}

namespace
{

template <typename T>
void andRepeatsOfValues(const char * data, PaddedPODArray<UInt8> & repeats)
{
    for (size_t i = 1; i < repeats.size(); ++i)
        repeats[i] &= unalignedLoad<T>(data + i * sizeof(T)) == unalignedLoad<T>(data + (i - 1) * sizeof(T));
}

/// Keeps marked in `repeats` the rows of a column of fixed-size values that hold the same bytes as the row before.
/// Another column, except a `ColumnString`, marks no row.
void andRepeatsOfFixed(const IColumn & column, PaddedPODArray<UInt8> & repeats)
{
    if (!column.isFixedAndContiguous())
    {
        std::ranges::fill(repeats, 0);
        return;
    }
    const char * data = column.getRawData().data();
    switch (const size_t size = column.sizeOfValueIfFixed())
    {
        case 1: andRepeatsOfValues<UInt8>(data, repeats); return;
        case 2: andRepeatsOfValues<UInt16>(data, repeats); return;
        case 4: andRepeatsOfValues<UInt32>(data, repeats); return;
        case 8: andRepeatsOfValues<UInt64>(data, repeats); return;
        case 16: andRepeatsOfValues<UInt128>(data, repeats); return;
        default:
            for (size_t i = 1; i < repeats.size(); ++i)
                repeats[i] &= memcmp(data + i * size, data + (i - 1) * size, size) == 0;
    }
}

/// Keeps marked in `repeats` the rows of a `ColumnString` that hold the same bytes as the row before. Compares only the
/// rows still marked, which the other columns and the hashes of the keys leave few.
void andRepeatsOfStrings(const ColumnString & strings, PaddedPODArray<UInt8> & repeats)
{
    for (size_t i = 1; i < repeats.size(); ++i)
        if (repeats[i])
            repeats[i] = strings.getDataAt(i) == strings.getDataAt(i - 1);
}

}

void TopKCandidateStream::markRepeats(const Columns & columns, size_t num_rows)
{
    repeats.resize(num_rows);
    if (num_rows == 0)
        return;
    repeats[0] = 0;
    for (size_t i = 1; i < num_rows; ++i)
        repeats[i] = hashes[i] == hashes[i - 1];

    /// The strings come last, as their comparisons need the rows that the other columns leave marked.
    Columns strings;
    const auto mark = [&](size_t position)
    {
        auto column = columns[position]->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
        if (typeid_cast<const ColumnString *>(column.get()))
            strings.push_back(std::move(column));
        else
            andRepeatsOfFixed(*column, repeats);
    };
    for (const size_t position : key_positions)
        mark(position);
    for (const size_t position : value_positions)
        mark(position);
    for (const auto & column : strings)
        andRepeatsOfStrings(assert_cast<const ColumnString &>(*column), repeats);
}

size_t TopKCandidateStream::countRuns(const Columns & columns, size_t num_rows)
{
    markRepeats(columns, num_rows);

    ColumnPtr condition_column;
    const UInt8 * condition = nullptr;
    if (condition_position)
    {
        condition_column = columns[*condition_position]->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
        condition = assert_cast<const ColumnUInt8 &>(*condition_column).getData().data();
    }

    size_t kept = 0;
    bool previous_taken = false;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const size_t cell = TopKCandidates::cell(hashes[i]);
        const UInt8 keep = candidates->candidate_cells.test(cell);
        filter[i] = keep;
        kept += keep;
        const bool taken = !condition || condition[i];
        /// The cells with candidates count every row, so that the rows of the candidates come off exactly.
        if (keep || (taken && !(previous_taken && repeats[i])))
            ++cell_counts[cell];
        previous_taken = taken;
    }
    return kept;
}

}
