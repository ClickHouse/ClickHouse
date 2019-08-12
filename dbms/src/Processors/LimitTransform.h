#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class LimitTransform : public IProcessor
{
private:
    InputPort & input;
    OutputPort & output;

    size_t limit;
    size_t offset;
    size_t rows_read = 0; /// including the last read block
    bool always_read_till_end;

    bool has_block = false;
    bool block_processed = false;
    Chunk current_chunk;

    UInt64 rows_before_limit_at_least = 0;

public:
    LimitTransform(
        const Block & header_, size_t limit_, size_t offset_,
        bool always_read_till_end_ = false);

    String getName() const override { return "Limit"; }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    UInt64 getRowsBeforeLimitAtLeast() const { return rows_before_limit_at_least; }
};

}
