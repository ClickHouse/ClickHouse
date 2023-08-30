#pragma once

#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Optimizer/StepTree.h>

namespace DB
{

class FragmentBuilder
{
public:
    FragmentBuilder(StepTree & plan_, ContextMutablePtr context_);

    FragmentPtr build();

private:
    StepTree & plan;
    ContextMutablePtr context;
};

}
