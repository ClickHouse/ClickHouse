#pragma once

#include <Processors/QueryPlan/ReadFromPreparedSource.h>

namespace DB
{

class ReadFromQueryCacheStep : public ReadFromPreparedSource
{
public:
    explicit ReadFromQueryCacheStep(Pipe pipe_);

    String getName() const override { return "ReadFromQueryCacheStep"; }
};

}
