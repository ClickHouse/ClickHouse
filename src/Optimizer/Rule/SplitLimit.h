#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class SplitLimit final : public Rule
{
public:
    SplitLimit(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
