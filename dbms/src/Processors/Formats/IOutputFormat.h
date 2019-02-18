#pragma once

#include <string>
#include <Processors/IProcessor.h>


namespace DB
{

class WriteBuffer;

/** Output format have three inputs and no outputs. It writes data from WriteBuffer.
  *
  * First input is for main resultset, second is for "totals" and third is for "extremes".
  * It's not necessarily to connect "totals" or "extremes" ports (they may remain dangling).
  *
  * Data from input ports are pulled in order: first, from main input, then totals, then extremes.
  *
  * By default, data for "totals" and "extremes" is ignored.
  */
class IOutputFormat : public IProcessor
{
public:
    enum PortKind { Main = 0, Totals = 1, Extremes = 2 };

protected:
    WriteBuffer & out;

    Chunk current_chunk;
    PortKind current_block_kind;
    bool has_input = false;

    virtual void consume(Chunk) = 0;
    virtual void consumeTotals(Chunk) {}
    virtual void consumeExtremes(Chunk) {}

public:
    IOutputFormat(Block header, WriteBuffer & out);

    Status prepare() override;
    void work() override;

    void flush();

    /** Content-Type to set when sending HTTP response.
      */
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    /// TODO onProgress, rows_before_limit_at_least

    InputPort & getPort(PortKind kind) { return inputs[kind]; }
};
}

