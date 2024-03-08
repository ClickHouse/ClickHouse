#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class SplitTopN final : public Rule
{
public:
    SplitTopN(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
