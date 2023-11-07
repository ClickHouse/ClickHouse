#pragma once

#include <QueryCoordination/Fragments/DistributedFragment.h>
#include <QueryCoordination/IO/FragmentRequest.h>

namespace DB
{

class DistributedFragmentBuilder
{
public:
    DistributedFragmentBuilder(const FragmentPtrs & all_fragments_, const std::vector<FragmentRequest> & plan_fragment_requests_)
        : all_fragments(all_fragments_), plan_fragment_requests(plan_fragment_requests_)
    {
    }

    DistributedFragments build();

private:
    const FragmentPtrs & all_fragments;
    const std::vector<FragmentRequest> & plan_fragment_requests;
};

}
