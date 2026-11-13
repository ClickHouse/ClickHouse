#pragma once

#include <Processors/QueryPlan/Profiling/Capture/CapturedPlan.h>

namespace DB
{

class WriteBuffer;

/// Writes a captured plan out in one format.
class IPlanRenderer
{
public:
    virtual ~IPlanRenderer() = default;

    virtual void render(const CapturedPlan & captured, WriteBuffer & out) = 0;
};

}
