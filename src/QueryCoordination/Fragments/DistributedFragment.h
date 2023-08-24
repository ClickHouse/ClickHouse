#pragma once

#include <QueryCoordination/Fragments/PlanFragment.h>
#include <QueryCoordination/IO/FragmentRequest.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Core/Block.h>
#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/Pipelines/CompletedPipelinesExecutor.h>

namespace DB
{

class DistributedFragment
{
public:
    DistributedFragment(PlanFragmentPtr fragment_, Destinations & data_to_, Sources & data_from_)
        : fragment(fragment_), data_to(data_to_), data_from(data_from_)
    {
    }

    static String receiverKey(PlanID exchange_id, const String & source)
    {
        return source + "_" + toString(exchange_id);
    }

    PlanFragmentPtr getFragment() const
    {
        return fragment;
    }

    const Destinations & getDataTo() const
    {
        return data_to;
    }

    const Sources & getDataFrom() const
    {
        return data_from;
    }

private:
    PlanFragmentPtr fragment;
    Destinations data_to;
    Sources data_from;

//    ExchangeDataSources receivers;

};

using DistributedFragments = std::vector<DistributedFragment>;

}
