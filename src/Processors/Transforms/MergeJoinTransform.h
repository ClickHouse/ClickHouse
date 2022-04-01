#pragma once

#include <mutex>
#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <base/logger_useful.h>


namespace Poco { class Logger; }

namespace DB
{

/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class MergeJoinAlgorithm final : public IMergingAlgorithm
{
public:
    MergeJoinAlgorithm();

    virtual void initialize(Inputs inputs) override;
    virtual void consume(Input & input, size_t source_num) override;
    virtual Status merge() override;

private:
    Inputs current_inputs;

    Chunks chunks;
    Poco::Logger * log;
};

class MergeJoinTransform final : public IMergingTransform<MergeJoinAlgorithm>
{
public:
    MergeJoinTransform(
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
