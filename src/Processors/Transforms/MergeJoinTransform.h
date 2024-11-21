#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <array>

#include <boost/core/noncopyable.hpp>

#include <Common/PODArray.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <IO/ReadBuffer.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <Interpreters/TableJoin.h>

namespace Poco { class Logger; }

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class FullMergeJoinCursor;

using FullMergeJoinCursorPtr = std::unique_ptr<FullMergeJoinCursor>;

/// Used instead of storing previous block
struct JoinKeyRow
{
    JoinKeyRow() = default;

    JoinKeyRow(const FullMergeJoinCursor & cursor, size_t pos);

    bool equals(const FullMergeJoinCursor & cursor) const;
    bool asofMatch(const FullMergeJoinCursor & cursor, ASOFJoinInequality asof_inequality) const;

    void reset();

    std::vector<ColumnPtr> row;
};

/// Remembers previous key if it was joined in previous block
class AnyJoinState : boost::noncopyable
{
public:
    void set(size_t source_num, const FullMergeJoinCursor & cursor);
    void setValue(Chunk value_);

    void reset(size_t source_num);

    bool empty() const;

    /// current keys
    JoinKeyRow keys[2];

    /// for LEFT/RIGHT join use previously joined row from other table.
    Chunk value;
};

/// Accumulate blocks with same key and cross-join them
class AllJoinState : boost::noncopyable
{
public:
    struct Range
    {
        Range() = default;

        explicit Range(Chunk chunk_, size_t begin_, size_t length_)
            : begin(begin_)
            , length(length_)
            , current(begin_)
            , chunk(std::move(chunk_))
        {
            assert(length > 0 && begin + length <= chunk.getNumRows());
        }

        size_t begin;
        size_t length;

        size_t current;
        Chunk chunk;
    };

    AllJoinState(const FullMergeJoinCursor & lcursor, size_t lpos,
                 const FullMergeJoinCursor & rcursor, size_t rpos)
        : keys{JoinKeyRow(lcursor, lpos), JoinKeyRow(rcursor, rpos)}
    {
    }

    void addRange(size_t source_num, Chunk chunk, size_t begin, size_t length)
    {
        if (source_num == 0)
            left.emplace_back(std::move(chunk), begin, length);
        else
            right.emplace_back(std::move(chunk), begin, length);
    }

    bool next()
    {
        /// advance right to one row, when right finished, advance left to next block
        assert(!left.empty() && !right.empty());

        if (finished())
            return false;

        bool has_next_right = nextRight();
        if (has_next_right)
            return true;

        return nextLeft();
    }

    bool finished() const { return lidx >= left.size(); }

    size_t blocksStored() const { return left.size() + right.size(); }
    const Range & getLeft() const { return left[lidx]; }
    const Range & getRight() const { return right[ridx]; }

    /// Left and right types can be different because of nullable
    JoinKeyRow keys[2];

private:
    bool nextLeft()
    {
        lidx += 1;
        return lidx < left.size();
    }

    bool nextRight()
    {
        /// cycle through right rows
        right[ridx].current += 1;
        if (right[ridx].current >= right[ridx].begin + right[ridx].length)
        {
            /// reset current row index to the beginning, because range will be accessed again
            right[ridx].current = right[ridx].begin;
            ridx += 1;
            if (ridx >= right.size())
            {
                ridx = 0;
                return false;
            }
        }
        return true;
    }
    std::vector<Range> left;
    std::vector<Range> right;

    size_t lidx = 0;
    size_t ridx = 0;
};


class AsofJoinState : boost::noncopyable
{
public:
    void set(const FullMergeJoinCursor & rcursor, size_t rpos);
    void reset();

    bool hasMatch(const FullMergeJoinCursor & cursor, ASOFJoinInequality asof_inequality) const
    {
        if (value.empty())
            return false;
        return key.asofMatch(cursor, asof_inequality);
    }

    JoinKeyRow key;
    Chunk value;
    size_t value_row = 0;
};

/*
 * Wrapper for SortCursorImpl
 */
class FullMergeJoinCursor : boost::noncopyable
{
public:
    explicit FullMergeJoinCursor(const Block & sample_block_, const SortDescription & description_, bool is_asof = false);

    bool fullyCompleted() const;
    void setChunk(Chunk && chunk);
    const Chunk & getCurrent() const;
    Chunk detach();

    SortCursorImpl * operator-> () { return &cursor; }
    const SortCursorImpl * operator-> () const { return &cursor; }

    SortCursorImpl & operator* () { return cursor; }
    const SortCursorImpl & operator* () const { return cursor; }

    SortCursorImpl cursor;

    const Block & sampleBlock() const { return sample_block; }
    Columns sampleColumns() const { return sample_block.getColumns(); }

    const IColumn * getAsofColumn() const
    {
        if (!asof_column_position)
            return nullptr;
        return cursor.all_columns[*asof_column_position];
    }

    String dump() const;

private:
    Block sample_block;
    SortDescription desc;

    Chunk current_chunk;
    bool recieved_all_blocks = false;

    std::optional<size_t> asof_column_position;
};

/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class MergeJoinAlgorithm final : public IMergingAlgorithm
{
public:
    MergeJoinAlgorithm(JoinKind kind_,
                       JoinStrictness strictness_,
                       const TableJoin::JoinOnClause & on_clause_,
                       const Blocks & input_headers,
                       size_t max_block_size_);

    MergeJoinAlgorithm(JoinPtr join_ptr, const Blocks & input_headers, size_t max_block_size_);

    const char * getName() const override { return "MergeJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    void setAsofInequality(ASOFJoinInequality asof_inequality_);

    void logElapsed(double seconds);
    MergedStats getMergedStats() const override;

private:
    std::optional<Status> handleAnyJoinState();
    Status anyJoin();

    std::optional<Status> handleAllJoinState();
    Status allJoin();

    std::optional<Status> handleAsofJoinState();
    Status asofJoin();

    MutableColumns getEmptyResultColumns() const;
    Chunk createBlockWithDefaults(size_t source_num);
    Chunk createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const;

    /// For `USING` join key columns should have values from right side instead of defaults
    std::unordered_map<size_t, size_t> left_to_right_key_remap;

    std::array<FullMergeJoinCursorPtr, 2> cursors;
    ASOFJoinInequality asof_inequality = ASOFJoinInequality::None;

    /// Keep some state to make handle data from different blocks
    AnyJoinState any_join_state;
    std::unique_ptr<AllJoinState> all_join_state;
    AsofJoinState asof_join_state;

    JoinKind kind;
    JoinStrictness strictness;

    size_t max_block_size;
    int null_direction_hint = 1;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};
        size_t num_bytes[2] = {0, 0};

        size_t max_blocks_loaded = 0;
    };

    Statistic stat;

    LoggerPtr log;
};

class MergeJoinTransform final : public IMergingTransform<MergeJoinAlgorithm>
{
    using Base = IMergingTransform<MergeJoinAlgorithm>;

public:
    MergeJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint = 0);

    MergeJoinTransform(
        JoinKind kind_,
        JoinStrictness strictness_,
        const TableJoin::JoinOnClause & on_clause_,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint_ = 0);

    String getName() const override { return "MergeJoinTransform"; }

    void setAsofInequality(ASOFJoinInequality asof_inequality_) { algorithm.setAsofInequality(asof_inequality_); }

protected:
    void onFinish() override;
};

}
