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
    MergeJoinAlgorithm()
        : log(&Poco::Logger::get("MergeJoinAlgorithm"))
    {
    }

    virtual void initialize(Inputs inputs) override
    {
        LOG_DEBUG(log, "MergeJoinAlgorithm initialize, number of inputs: {}", inputs.size());
    }

    virtual void consume(Input & input, size_t source_num) override
    {
        LOG_DEBUG(log, "Consume from {}", source_num);
        if (source_num == 0)
        {
            chunk = std::move(input.chunk);
        }
    }

    virtual Status merge() override
    {
        LOG_DEBUG(log, "merge (chunk: {})", bool(chunk));

        if (chunk)
            return Status(std::move(chunk), true);

        return Status(0);
    }

private:
    Inputs current_inputs;

    Chunk chunk;
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
