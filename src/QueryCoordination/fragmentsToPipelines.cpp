#include <Core/Settings.h>
#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/Pipelines/PipelinesBuilder.h>
#include <QueryCoordination/fragmentsToPipelines.h>


namespace DB
{

Pipelines fragmentsToPipelines(
    const FragmentPtrs & all_fragments,
    const std::vector<FragmentRequest> & plan_fragment_requests,
    const String & query_id,
    const Settings & settings,
    ClusterPtr cluster)
{
    DistributedFragmentBuilder builder(all_fragments, plan_fragment_requests);
    const DistributedFragments & distributed_fragments = builder.build();

    PipelinesBuilder pipelines_builder(query_id, settings, cluster, distributed_fragments);
    return pipelines_builder.build();
}

}
