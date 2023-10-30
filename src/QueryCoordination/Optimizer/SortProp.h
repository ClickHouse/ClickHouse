#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct SortProp
{
    SortDescription sort_description = {};
    DataStream::SortScope sort_scope = DataStream::SortScope::None;
};

}
