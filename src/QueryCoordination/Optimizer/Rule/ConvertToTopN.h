#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class ConvertToTopN final : public Rule
{
public:
    ConvertToTopN(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
