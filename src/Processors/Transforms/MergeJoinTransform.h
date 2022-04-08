#pragma once

#include <mutex>
#include <optional>
#include <vector>
#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include "Interpreters/IJoin.h"
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <base/logger_useful.h>
#include <Core/SortCursor.h>

namespace Poco { class Logger; }

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;


class MultiCursor
{
    void next()
    {
    }

    bool isLast()
    {
        return false;
    }

    bool isValid()
    {
        return false;
    }

private:
    SortDescription desc;
    struct CursorWithBlock
    {
        SortCursorImpl impl;
        Chunk input;
    };

    using CursorList = std::list<CursorWithBlock>;
    using CursorListIt = CursorList::iterator;

    CursorListIt current;
    CursorList inputs;
};

/*
 * Wrapper for SortCursorImpl
 * It is used to store information about the current state of the cursor.
 */
class FullMergeJoinCursor
{
public:

    FullMergeJoinCursor(const Block & block, const SortDescription & desc);

    SortCursor getCursor();

    bool sameUnitlEnd() const;

    bool ALWAYS_INLINE sameNext() const;

    size_t nextDistinct();

    void reset();

    const Chunk & getCurrentChunk() const;

    void setInput(IMergingAlgorithm::Input && input);

    bool fullyCompleted() const { return !impl.isValid() && fully_completed; }

    SortCursorImpl * operator-> () { return &impl; }
    const SortCursorImpl * operator-> () const { return &impl; }

private:
    void resetInternalCursor();



    SortCursorImpl impl;

    IMergingAlgorithm::Input current_input;


    bool fully_completed = false;

    Block sample_block;
    // bool has_left_nullable = false;
    // bool has_right_nullable = false;
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
        LOG_TRACE(log, "Finished pocessing {} left and {} right blocks in {} seconds",
            stat.num_blocks[0],
            stat.num_blocks[1],
            seconds);
    }

private:
    std::optional<size_t> required_input = std::nullopt;

    std::vector<FullMergeJoinCursor> cursors;
    std::vector<Chunk> sample_chunks;

    JoinPtr table_join;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
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
