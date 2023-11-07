#pragma once

#include <QueryCoordination/Optimizer/Rule/ConvertToTopN.h>
#include <QueryCoordination/Optimizer/Rule/Pattern.h>
#include <QueryCoordination/Optimizer/Rule/Rule.h>
#include <QueryCoordination/Optimizer/Rule/SplitAggregation.h>
#include <QueryCoordination/Optimizer/Rule/SplitLimit.h>
#include <QueryCoordination/Optimizer/Rule/SplitSort.h>
#include <QueryCoordination/Optimizer/Rule/SplitTopN.h>


namespace DB
{

namespace Optimizer
{

inline const auto & getRules()
{
    static const std::vector<RulePtr> rules = {
        {std::make_shared<SplitAggregation>()},
        {std::make_shared<ConvertToTopN>()},
        {std::make_shared<SplitLimit>()},
        {std::make_shared<SplitSort>()},
        {std::make_shared<SplitTopN>()},
    };

    return rules;
}

}

}
