#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/core/noncopyable.hpp>

#include <Common/PODArray.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <IO/ReadBuffer.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

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
    std::vector<ColumnPtr> row;

    JoinKeyRow() = default;

    explicit JoinKeyRow(const SortCursorImpl & impl_, size_t pos)
    {
        row.reserve(impl_.sort_columns.size());
        for (const auto & col : impl_.sort_columns)
        {
            auto new_col = col->cloneEmpty();
            new_col->insertFrom(*col, pos);
            row.push_back(std::move(new_col));
        }
    }

    void reset()
    {
        row.clear();
    }

    bool equals(const SortCursorImpl & impl) const
    {
        if (row.empty())
            return false;

        assert(this->row.size() == impl.sort_columns_size);
        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            int cmp = this->row[i]->compareAt(0, impl.getRow(), *impl.sort_columns[i], impl.desc[i].nulls_direction);
            if (cmp != 0)
                return false;
        }
        return true;
    }
};

/// Remembers previous key if it was joined in previous block
class AnyJoinState : boost::noncopyable
{
public:
    AnyJoinState() = default;

    void set(size_t source_num, const SortCursorImpl & cursor)
    {
        assert(cursor.rows);
        keys[source_num] = JoinKeyRow(cursor, cursor.rows - 1);
    }

    void setValue(Chunk value_) { value = std::move(value_); }

    bool empty() const { return keys[0].row.empty() && keys[1].row.empty(); }

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

    AllJoinState(const SortCursorImpl & lcursor, size_t lpos,
                 const SortCursorImpl & rcursor, size_t rpos)
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

/*
 * Wrapper for SortCursorImpl
 */
class FullMergeJoinCursor : boost::noncopyable
{
public:
    explicit FullMergeJoinCursor(const Block & sample_block_, const SortDescription & description_)
        : sample_block(sample_block_.cloneEmpty())
        , desc(description_)
    {
    }

    bool fullyCompleted() const;
    void setChunk(Chunk && chunk);
    const Chunk & getCurrent() const;
    Chunk detach();

    SortCursorImpl * operator-> () { return &cursor; }
    const SortCursorImpl * operator-> () const { return &cursor; }

    SortCursorImpl cursor;

    const Block & sampleBlock() const { return sample_block; }
    Columns sampleColumns() const { return sample_block.getColumns(); }

private:
    Block sample_block;
    SortDescription desc;

    Chunk current_chunk;
    bool recieved_all_blocks = false;
};

/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class MergeJoinAlgorithm final : public IMergingAlgorithm
{
public:
    explicit MergeJoinAlgorithm(JoinPtr table_join, const Blocks & input_headers, size_t max_block_size_);

    virtual void initialize(Inputs inputs) override;
    virtual void consume(Input & input, size_t source_num) override;
    virtual Status merge() override;

    void logElapsed(double seconds, bool force)
    {
        /// Do not log more frequently than once per ten seconds
        if (seconds - stat.last_log_seconds < 10 && !force)
            return;

        LOG_TRACE(log,
            "Finished pocessing in {} seconds"
            ", left: {} blocks, {} rows; right: {} blocks, {} rows"
            ", max blocks loaded to memory: {}",
            seconds, stat.num_blocks[0], stat.num_rows[0], stat.num_blocks[1], stat.num_rows[1],
            stat.max_blocks_loaded);
        stat.last_log_seconds = seconds;
    }

private:
    std::optional<Status> handleAnyJoinState();
    Status anyJoin(ASTTableJoin::Kind kind);

    std::optional<Status> handleAllJoinState();
    Status allJoin(ASTTableJoin::Kind kind);

    Chunk createBlockWithDefaults(size_t source_num);
    Chunk createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const;

    /// For `USING` join key columns should have values from right side instead of defaults
    std::unordered_map<size_t, size_t> left_to_right_key_remap;

    std::vector<FullMergeJoinCursorPtr> cursors;

    /// Keep some state to make connection between data in different blocks
    AnyJoinState any_join_state;
    std::unique_ptr<AllJoinState> all_join_state;

    JoinPtr table_join;

    size_t max_block_size;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};

        size_t max_blocks_loaded = 0;

        double last_log_seconds = 0;
    };

    Statistic stat;

    Poco::Logger * log;
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

    String getName() const override { return "MergeJoinTransform"; }

protected:
    void onFinish() override;

    void work() override
    {
        algorithm.logElapsed(total_stopwatch.elapsedSeconds(), true);
        Base::work();
    }

    Poco::Logger * log;
};

}
