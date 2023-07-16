#pragma once

#include <QueryCoordination/IO/FragmentRequest.h>
#include <QueryCoordination/Fragments/DistributedFragment.h>

namespace DB
{

class DistributedFragmentBuilder
{
public:
    DistributedFragmentBuilder(const PlanFragmentPtrs & all_fragments_, const std::vector<FragmentRequest> & plan_fragment_requests_)
    : all_fragments(all_fragments_), plan_fragment_requests(plan_fragment_requests_) {}

    DistributedFragments build();

private:
    const PlanFragmentPtrs & all_fragments;
    const std::vector<FragmentRequest> & plan_fragment_requests;
};

}
