#pragma once

#include <Optimizer/Rule/ConvertToTopN.h>
#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/Rule/SplitAggregation.h>
#include <Optimizer/Rule/SplitLimit.h>
#include <Optimizer/Rule/SplitSort.h>
#include <Optimizer/Rule/SplitTopN.h>


namespace DB
{

namespace CostBasedOptimizerRules
{

static constexpr size_t RULES_SIZE = 5;

inline const auto & getRules()
{
    static const std::vector<RulePtr> rules = {
        {std::make_shared<SplitAggregation>(0)},
        {std::make_shared<ConvertToTopN>(1)},
        {std::make_shared<SplitLimit>(2)},
        {std::make_shared<SplitSort>(3)},
        {std::make_shared<SplitTopN>(4)},
    };

    return rules;
}

}

}
