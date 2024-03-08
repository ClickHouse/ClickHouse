#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/Fragments/FragmentRequest.h>
#include <QueryCoordination/Pipelines/Pipelines.h>


namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

struct Settings;

Pipelines fragmentsToPipelines(
    const FragmentPtrs & all_fragments,
    const std::vector<FragmentRequest> & plan_fragment_requests,
    const String & query_id,
    const Settings & settings,
    ClusterPtr cluster);


};
