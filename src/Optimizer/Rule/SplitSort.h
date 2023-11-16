#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class SplitSort final : public Rule
{
public:
    SplitSort(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
