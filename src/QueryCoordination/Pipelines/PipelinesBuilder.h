#pragma once

#include <QueryCoordination/Fragments/DistributedFragment.h>
#include <QueryCoordination/Pipelines/Pipelines.h>
#include <Core/Settings.h>

namespace DB
{

class PipelinesBuilder
{
public:
    PipelinesBuilder(const String & query_id_, const Settings & settings_, const DistributedFragments & distributed_fragments_)
        : log(&Poco::Logger::get("PipelinesBuilder"))
        , query_id(query_id_)
        , settings(settings_)
        , distributed_fragments(distributed_fragments_)
    {
    }

    Pipelines build();

private:
    Poco::Logger * log;

    String query_id;
    const Settings & settings;
    DistributedFragments distributed_fragments;
};

}
