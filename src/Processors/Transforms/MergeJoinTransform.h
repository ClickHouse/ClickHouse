#pragma once

#include <cassert>
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

/*
 * Wrapper for SortCursorImpl
 * It is used to keep cursor for list of blocks.
 */
class FullMergeJoinCursor : boost::noncopyable
{
public:
    struct CursorWithBlock
    {
        CursorWithBlock(const Block & header, const SortDescription & desc_, Chunk && chunk)
            : input(std::move(chunk))
            , impl(header, input.getColumns(), desc_)
        {
        }

        Chunk input;
        SortCursorImpl impl;
    };

    /*
    /// Used in any join, instead of storing previous block
    struct Row
    {
        std::vector<ColumnPtr> sort_columns;

        explicit Row(const SortCursorImpl & impl_)
        {
            assert(impl_.isValid());

            sort_columns.reserve(impl_.sort_columns.size());
            for (const auto & col : impl_.sort_columns)
            {
                auto new_col = col->cloneEmpty();
                new_col->insertFrom(*col, impl_.getRow());
                sort_columns.push_back(std::move(new_col));
            }
        }

    };
    */

    using CursorList = std::list<CursorWithBlock>;
    using CursorListIt = CursorList::iterator;

    explicit FullMergeJoinCursor(const Block & sample_block_, const SortDescription & description_)
        : sample_block(sample_block_)
        , desc(description_)
        , current(inputs.end())
    {
    }

    void next();

    size_t nextDistinct();

    bool haveAllCurrentRange() const;
    bool isValid() const;
    bool isLast() const;
    bool fullyCompleted() const;

    void addChunk(Chunk && chunk)
    {
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} addChunk {} {} {} {}", __FILE__, __LINE__,
            bool(chunk),
            chunk.hasRows(), chunk.getNumRows(),
            chunk.hasColumns());

        assert(!recieved_all_blocks);
        if (!chunk)
        {
            LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} recieved_all_blocks = true", __FILE__, __LINE__);
            recieved_all_blocks = true;
            return;
        }

        dropBlocksUntilCurrent();
        inputs.emplace_back(sample_block, desc, std::move(chunk));

        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} end {} prev {} size {}; [{}/{};{}]", __FILE__, __LINE__,
            current == inputs.end(),
            current == std::prev(inputs.end()),
            inputs.size(),
            inputs.back().impl.getRow(),
            inputs.back().impl.rows,
            inputs.back().input.getNumRows());

        if (current == inputs.end())
        {

            current = std::prev(inputs.end());
            LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} end {} prev {} size {}", __FILE__, __LINE__,
                current == inputs.end(),
                current == std::prev(inputs.end()),
                inputs.size());

        }
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} end {} prev {} size {}", __FILE__, __LINE__,
            current == inputs.end(),
            current == std::prev(inputs.end()),
            inputs.size());

    }

    Chunk detachCurrentChunk();

    const CursorWithBlock & getCurrent() const
    {
        assert(current != inputs.end());
        return *current;
    }

    SortCursorImpl & getCurrentMutable()
    {
        assert(isValid());
        return current->impl;
    }

    const ColumnRawPtrs & getSortColumns() const;
    void dropBlocksUntilCurrent();

private:

    Block sample_block;
    SortDescription desc;

    CursorList inputs;
    CursorListIt current;

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
    virtual Status merge() override;

    void onFinish(double seconds)
    {
        LOG_TRACE(log, "Finished pocessing in {} seconds - left: {} blocks, {} rows; right: {} blocks, {} rows",
            seconds, stat.num_blocks[0], stat.num_rows[0], stat.num_blocks[1], stat.num_rows[1]);
    }

private:
    Chunk anyJoin(ASTTableJoin::Kind kind);

    std::optional<size_t> required_input = std::nullopt;

    std::vector<FullMergeJoinCursorPtr> cursors;
    std::vector<Chunk> sample_chunks;

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
