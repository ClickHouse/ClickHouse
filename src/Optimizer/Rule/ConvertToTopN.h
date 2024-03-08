#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

/// Convert 'Sort -> Limit' to 'TopN'
/// TODO implement TopN processer
class ConvertToTopN final : public Rule
{
public:
    ConvertToTopN(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
