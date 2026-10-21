#pragma once

#include <Processors/QueryPlan/CapturedPlan.h>

namespace DB
{

class WriteBuffer;

/// Writes a captured plan out in one format.
///
/// The captured plan carries no formatting decisions of its own, so a rendering is a reading of it
/// rather than a part of it. Only the JSON rendering exists today, for the `query_plan` column of
/// `system.query_log`. The text that `EXPLAIN ANALYZE` prints is still produced by walking the live
/// plan; giving it a renderer here, so that both read the same captured value, is the point of this
/// interface and the subject of a later change.
class IPlanRenderer
{
public:
    virtual ~IPlanRenderer() = default;

    virtual void render(const CapturedPlan & captured, WriteBuffer & out) = 0;
};

}
