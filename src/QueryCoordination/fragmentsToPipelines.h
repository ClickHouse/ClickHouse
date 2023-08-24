#pragma once

#include <QueryCoordination/Pipelines/Pipelines.h>
#include <QueryCoordination/IO/FragmentRequest.h>


namespace DB
{

class PlanFragment;

using PlanFragmentPtr = std::shared_ptr<PlanFragment>;
using PlanFragmentPtrs = std::vector<PlanFragmentPtr>;

struct Settings;

Pipelines fragmentsToPipelines(const PlanFragmentPtrs & all_fragments,
                               const std::vector <FragmentRequest> & plan_fragment_requests, const String & query_id,
                               const Settings & settings);


};

