#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Passes the chunks through, and when its output is closed, keeps reading and discarding its input until it finishes.
///
/// A processor closes its input when it does not need more data for the result (`LIMIT`, `LIMIT BY`, `DISTINCT` of
/// constants, `GROUP BY` or `DISTINCT` with the overflow mode `break`, etc.), which finishes the source before it has
/// read everything. This transform is put right after a source that has to be read to the end regardless of that:
/// a bounded streaming read of an incremental refresh of a materialized view reports the cursor it advanced to only
/// when it reaches the end of its snapshot, see `IStorage::reportsStreamingCursor`.
class ReadToEndTransform final : public IProcessor
{
public:
    explicit ReadToEndTransform(SharedHeader header);

    String getName() const override { return "ReadToEnd"; }

    Status prepare() override;
};

}
