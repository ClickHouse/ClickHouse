#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Passes the chunks through, and keeps reading its input to the end after its output is closed, discarding the chunks.
///
/// A bounded streaming read reports its cursor (see `StreamingCursor`) once it has read everything up to the end of
/// the bounded stream. A step of the query may stop reading earlier, when it needs no more rows (e.g. `DISTINCT` or
/// `LIMIT BY` over constants): it closes its input, the source finishes without reporting the cursor, and the query
/// still succeeds. The next incremental refresh would start from the previous cursor and append the same rows again.
/// This transform is added after the source of such a read, so that the source always reaches the end of the stream.
class ReadToEndTransform final : public IProcessor
{
public:
    explicit ReadToEndTransform(SharedHeader header);

    String getName() const override { return "ReadToEnd"; }
    Status prepare() override;

private:
    InputPort & input;
    OutputPort & output;
};

}
