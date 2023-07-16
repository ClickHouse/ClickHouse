#pragma once

#include <QueryCoordination/Fragments/PlanFragment.h>
#include <QueryCoordination/IO/FragmentRequest.h>


namespace DB
{

class Settings;

Pipelines fragmentsToPipelines(const PlanFragmentPtrs & all_fragments,
                               const std::vector <FragmentRequest> & plan_fragment_requests, const String & query_id,
                               const Settings & settings);


};

