#pragma once

#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <QueryCoordination/Fragments/DistributedFragment.h>
#include <QueryCoordination/Pipelines/Pipelines.h>

namespace DB
{

class PipelinesBuilder
{
public:
    PipelinesBuilder(
        const String & query_id_, const Settings & settings_, ClusterPtr cluster_, const DistributedFragments & distributed_fragments_)
        : log(&Poco::Logger::get("PipelinesBuilder"))
        , query_id(query_id_)
        , settings(settings_)
        , cluster(cluster_)
        , distributed_fragments(distributed_fragments_)
    {
    }

    Pipelines build();

private:
    Poco::Logger * log;

    String query_id;
    const Settings & settings;
    ClusterPtr cluster;
    DistributedFragments distributed_fragments;
};

}
