#pragma once

#include <Optimizer/SubQueryPlan.h>
#include <QueryCoordination/Fragments/Fragment.h>

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
