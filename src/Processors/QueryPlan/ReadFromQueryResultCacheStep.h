#pragma once

#include <Processors/QueryPlan/ReadFromPreparedSource.h>

namespace DB
{

class ReadFromQueryResultCacheStep : public ReadFromPreparedSource
{
public:
    explicit ReadFromQueryResultCacheStep(Pipe pipe_);

    String getName() const override { return "ReadFromQueryResultCacheStep"; }
};

}
