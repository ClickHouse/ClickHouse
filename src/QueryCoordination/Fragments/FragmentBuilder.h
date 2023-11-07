#pragma once

#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Optimizer/SubQueryPlan.h>

namespace DB
{

class FragmentBuilder
{
public:
    FragmentBuilder(QueryPlan & plan_, ContextMutablePtr context_);

    FragmentPtr build();

private:
    QueryPlan & plan;
    ContextMutablePtr context;
};

}
