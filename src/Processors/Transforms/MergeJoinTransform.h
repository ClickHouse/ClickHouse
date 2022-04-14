#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "Interpreters/IJoin.h"
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <base/logger_useful.h>
#include <Core/SortCursor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <boost/core/noncopyable.hpp>

namespace Poco { class Logger; }

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class FullMergeJoinCursor;

using FullMergeJoinCursorPtr = std::unique_ptr<FullMergeJoinCursor>;


struct AnyJoinState : boost::noncopyable
{
    /// Used instead of storing previous block
    struct Row
    {
        std::vector<ColumnPtr> row_key;
        Chunk value;

        explicit Row(const SortCursorImpl & impl_, size_t pos, Chunk value_)
            : value(std::move(value_))
        {
            row_key.reserve(impl_.sort_columns.size());
            for (const auto & col : impl_.sort_columns)
            {
                auto new_col = col->cloneEmpty();
                new_col->insertFrom(*col, pos);
                row_key.push_back(std::move(new_col));
            }
        }

        bool equals(const SortCursorImpl & impl) const
        {
            assert(this->row_key.size() == impl.sort_columns_size);
            for (size_t i = 0; i < impl.sort_columns_size; ++i)
            {
                int cmp = this->row_key[i]->compareAt(0, impl.getRow(), *impl.sort_columns[i], 0);
                if (cmp != 0)
                    return false;
            }
            return true;
        }
    };

    void setLeft(const SortCursorImpl & impl_, size_t pos, Chunk value)
    {
        left = std::make_unique<Row>(impl_, pos, std::move(value));
    }

    void setRight(const SortCursorImpl & impl_, size_t pos, Chunk value)
    {
        right = std::make_unique<Row>(impl_, pos, std::move(value));
    }

    std::unique_ptr<Row> left;
    std::unique_ptr<Row> right;
};

/*
 * Wrapper for SortCursorImpl
 * It is used to keep cursor for list of blocks.
 */
class FullMergeJoinCursor : boost::noncopyable
{
public:
    struct CursorWithBlock : boost::noncopyable
    {
        CursorWithBlock() = default;

        CursorWithBlock(const Block & header, const SortDescription & desc_, Chunk && chunk)
            : input(std::move(chunk))
            , cursor(header, input.getColumns(), desc_)
        {
        }

        Chunk detach()
        {
            cursor = SortCursorImpl();
            return std::move(input);
        }

        SortCursorImpl * operator-> () { return &cursor; }
        const SortCursorImpl * operator-> () const { return &cursor; }

        Chunk input;
        SortCursorImpl cursor;
    };

    using CursorList = std::list<CursorWithBlock>;
    using CursorListIt = CursorList::iterator;

    explicit FullMergeJoinCursor(const Block & sample_block_, const SortDescription & description_)
        : sample_block(sample_block_.cloneEmpty())
        , desc(description_)
        , current(inputs.end())
    {
    }

    bool fullyCompleted();
    void addChunk(Chunk && chunk);
    CursorWithBlock & getCurrent();

private:
    void dropBlocksUntilCurrent();

    Block sample_block;
    SortDescription desc;

    CursorList inputs;
    CursorListIt current;
    CursorWithBlock empty_cursor;

    bool recieved_all_blocks = false;
};

/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class MergeJoinAlgorithm final : public IMergingAlgorithm
{
public:
    explicit MergeJoinAlgorithm(JoinPtr table_join, const Blocks & input_headers);

    virtual void initialize(Inputs inputs) override;
    virtual void consume(Input & input, size_t source_num) override;
    virtual Status merge() override
    {
        Status result = mergeImpl();
        LOG_TRACE(log, "XXXX: merge result: chunk: {}, required: {}, finished: {}",
        result.chunk.getNumRows(), result.required_source, result.is_finished);
        return result ;
    }

    void onFinish(double seconds)
    {
        LOG_TRACE(log, "Finished pocessing in {} seconds - left: {} blocks, {} rows; right: {} blocks, {} rows",
            seconds, stat.num_blocks[0], stat.num_rows[0], stat.num_blocks[1], stat.num_rows[1]);
    }

private:
    Status mergeImpl();

    Status anyJoin(ASTTableJoin::Kind kind);

    std::vector<FullMergeJoinCursorPtr> cursors;
    std::vector<Chunk> sample_chunks;

    std::optional<size_t> required_input = std::nullopt;
    std::unique_ptr<AnyJoinState> any_join_state;

    JoinPtr table_join;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};
    };
    Statistic stat;

    Poco::Logger * log;
};

class MergeJoinTransform final : public IMergingTransform<MergeJoinAlgorithm>
{
public:
    MergeJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        UInt64 limit_hint = 0);

    String getName() const override { return "MergeJoinTransform"; }

protected:
    void onFinish() override;

    Poco::Logger * log;
};

}
