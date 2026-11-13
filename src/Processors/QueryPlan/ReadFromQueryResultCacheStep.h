#pragma once

#include <Processors/QueryPlan/ReadFromPreparedSource.h>

namespace DB
{

class ReadFromQueryResultCacheStep : public ReadFromPreparedSource
{
public:
    explicit ReadFromQueryResultCacheStep(Pipe pipe_);

    [[nodiscard]] String getName() const override { return "ReadFromQueryResultCacheStep"; }

    [[nodiscard]] bool hasTotals() const { return pipe.getTotalsPort() != nullptr; }
};

}
