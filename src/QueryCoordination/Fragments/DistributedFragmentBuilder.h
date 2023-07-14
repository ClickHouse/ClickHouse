#pragma once

#include <QueryCoordination/IO/FragmentRequest.h>
#include <QueryCoordination/Fragments/DistributedFragment.h>

namespace DB
{

class DistributedFragmentBuilder
{
public:
    DistributedFragmentBuilder(std::vector<FragmentRequest> & plan_fragment_requests_) : plan_fragment_requests(plan_fragment_requests_) { }

    DistributedFragments build();

private:
    std::vector<FragmentRequest> & plan_fragment_requests;
    PlanFragmentPtrs all_fragments;
};

}
