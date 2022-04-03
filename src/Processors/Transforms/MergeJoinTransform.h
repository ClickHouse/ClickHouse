#pragma once

#include <mutex>
#include <vector>
#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <base/logger_useful.h>
#include <Core/SortCursor.h>

namespace Poco { class Logger; }

namespace DB
{

class TableJoin;


/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class MergeJoinAlgorithm final : public IMergingAlgorithm
{
public:
    explicit MergeJoinAlgorithm(const TableJoin & table_join, const Blocks & input_headers);

    virtual void initialize(Inputs inputs) override;
    virtual void consume(Input & input, size_t source_num) override;
    virtual Status merge() override;

private:
    SortDescription left_desc;
    SortDescription right_desc;

    std::vector<Input> current_inputs;
    std::vector<SortCursorImpl> cursors;

    bool left_stream_finished = false;
    bool right_stream_finished = false;

    const TableJoin & table_join;
    Poco::Logger * log;
};

class MergeJoinTransform final : public IMergingTransform<MergeJoinAlgorithm>
{
public:
    MergeJoinTransform(
        const TableJoin & table_join,
        const Blocks & input_headers,
        const Block & output_header,
        UInt64 limit_hint = 0);

    String getName() const override { return "MergeJoinTransform"; }

protected:
    void onFinish() override;
    UInt64 elapsed_ns = 0;

    Poco::Logger * log;
};

}
