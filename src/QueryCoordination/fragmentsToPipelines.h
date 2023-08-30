#pragma once

#include <QueryCoordination/Pipelines/Pipelines.h>
#include <QueryCoordination/IO/FragmentRequest.h>
#include <Interpreters/Cluster.h>


namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

struct Settings;

Pipelines fragmentsToPipelines(const FragmentPtrs & all_fragments,
                               const std::vector <FragmentRequest> & plan_fragment_requests, const String & query_id,
                               const Settings & settings,
                               ClusterPtr cluster);


};

