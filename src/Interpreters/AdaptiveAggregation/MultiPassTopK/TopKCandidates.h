#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Block_fwd.h>
#include <Core/Names.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK_fwd.h>
#include <Common/PODArray.h>
#include <base/types.h>

#include <boost/dynamic_bitset.hpp>

#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// A set of the cells of a hash, a bit for every cell.
using TopKCellSet = boost::dynamic_bitset<>;

/// The columns of a rank that counts the distinct values of a group (`uniqExact`), and the condition of the
/// rows that it counts (`uniqExactIf`) when the condition is a `UInt8` column.
struct TopKDistinctRank
{
    Names values;
    /// Empty when the rank counts the values of every row.
    String condition;
};

/// What the candidate pass proves (see `TopKCandidates::prove`): the hot cells, whose other keys may outrank the n-th
/// best candidate, and the proven candidates, those of the other cells, the cold ones, that hold rows. The proven
/// candidates are groups of the result.
struct TopKCandidateProof
{
    std::shared_ptr<TopKCellSet> hot_cells;
    /// Marks the proven candidates, `num_proven` of them. A candidate that an earlier run found may hold no rows of
    /// this one, when the data or a literal of the query changed (see `TopKMemo`), and is then no group.
    PaddedPODArray<UInt8> proven;
    size_t num_proven = 0;
    /// The most rows that another key holds in a cell, or for a rank that counts distinct values the most runs.
    UInt64 bound = 0;
};

/// The candidate keys of the candidate pass of a multi-pass top-K aggregation (see
/// `addMultiPassTopK`), and the counters that bound the rank of every other key.
///
/// The pass aggregates the rows of the candidate keys exactly, and counts the rows of every key in a cell of
/// the weak hash of the key (see `TopKCandidateStream`). The rank of a group (`count`, `uniqExact` or
/// `uniqExactIf`) never exceeds its rows, so the rows of a cell minus the rows of the candidates in it bound the
/// rank of every other key in the cell. A row of a cell without candidates cannot belong to a candidate, so the
/// pass skips the aggregation for it and only counts it.
///
/// A rank that counts distinct values gets a tighter bound in the cells without candidates. There a row
/// counts only when the rank takes its values and it starts a run: the previous row of the block differs from
/// it in the key or in the values, or the rank does not take that row. A run of equal rows adds one distinct
/// value at most, so the count of a cell still bounds the distinct values of every key in it. Rows repeat in
/// a table sorted by the column of the values, such as one of the events of users.
struct TopKCandidates
{
    /// The cells are the top bits of the hash. 2^16 cells of 32-bit counters keep the cells of a thread in
    /// its L2 cache and the rows of a cell low enough to bound the ranks of a top far below the rows of a
    /// large table.
    static constexpr size_t cell_bits = 16;
    static constexpr size_t num_cells = size_t{1} << cell_bits;

    static size_t cell(UInt32 hash) { return hash >> (32 - cell_bits); }

    /// Computes the weak hashes of the first `rows` rows of the keys, chained over the key columns as in the
    /// scatter by a key. Every representation of a key (a full column, `ColumnConst`, `ColumnSparse`) hashes
    /// the same.
    static void hashKeys(const ColumnRawPtrs & keys, size_t rows, PaddedPODArray<UInt32> & hashes);

    /// `seed_` holds the candidate keys in its first `keys_size` columns, followed by empty aggregate states,
    /// in the mergeable layout of the aggregation: merging it into an empty table creates exactly the
    /// candidate keys.
    TopKCandidates(ConstBlockPtr seed_, size_t keys_size, std::optional<TopKDistinctRank> distinct_rank_);

    const ConstBlockPtr seed;
    /// Set when the rank counts distinct values: then the cells without candidates count runs of equal rows.
    const std::optional<TopKDistinctRank> distinct_rank;
    /// The cells of the candidate keys.
    const TopKCellSet candidate_cells;

    /// Adds the counts of the cells of one stream of the pass.
    void addCellCounts(const std::vector<UInt32> & counts);

    /// Proves the top after the pass finished, from its groups: the candidates, whose keys are `keys` and whose rows
    /// are `rows`. A key of a cell holds at most the rows of the cell less those of the candidates in it, so a cell is
    /// hot when that bound exceeds `nth_rank`, the n-th best rank of the candidates. Called once.
    TopKCandidateProof prove(const ColumnRawPtrs & keys, const PaddedPODArray<UInt64> & rows, UInt64 nth_rank);

private:
    std::mutex mutex;
    std::vector<UInt64> cell_counts;
};

/// One stream of the candidate pass (see `TopKCandidates`): counts the rows of its chunks in the cells of their keys,
/// and keeps the rows of the cells that hold a candidate, which the stream aggregates.
class TopKCandidateStream
{
public:
    /// The keys `keys`, and the columns of a rank that counts distinct values, are columns of `header`.
    TopKCandidateStream(TopKCandidatesPtr candidates_, const Block & header, const Names & keys);

    /// Counts the rows of a chunk in their cells, and keeps in `columns` only the rows of the cells that hold a
    /// candidate. Returns false when no row is kept.
    bool keepCandidateRows(Columns & columns, UInt64 & num_rows);

    /// Adds the counts of the stream to the candidates.
    void flush();

private:
    /// Counts the rows of the cells for a rank that counts distinct values, and marks in `filter` the rows of the cells
    /// that hold a candidate: a row of a cell without candidates counts only when it starts a run. Returns the number
    /// of marked rows.
    size_t countRuns(const Columns & columns, size_t num_rows);

    /// Marks in `repeats` the rows whose keys and rank's values hold the same bytes as those of the row before. Byte
    /// equality implies that `uniqExact` and the keys take the rows as equal, which value equality does not: two
    /// `NaN`, or `0` and `-0`, compare equal but differ. A column of other values marks no row, so that every row of
    /// it counts.
    void markRepeats(const Columns & columns, size_t num_rows);

    const TopKCandidatesPtr candidates;
    std::vector<size_t> key_positions;
    std::vector<size_t> value_positions;
    std::optional<size_t> condition_position;

    /// The 32-bit counts of the cells, allocated by the thread of the stream, with the rows they took since their
    /// last flush.
    std::vector<UInt32> cell_counts;
    UInt64 unflushed_rows = 0;

    PaddedPODArray<UInt32> hashes;
    PaddedPODArray<UInt8> filter;
    ColumnRawPtrs key_columns;
    PaddedPODArray<UInt8> repeats;
};

}
