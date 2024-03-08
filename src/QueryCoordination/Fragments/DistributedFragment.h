#pragma once

#include <Core/Block.h>
#include <QueryCoordination/Exchange/ExchangeDataRequest.h>
#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentRequest.h>
#include <QueryCoordination/Pipelines/CompletedPipelinesExecutor.h>

namespace DB
{

class DistributedFragment
{
public:
    DistributedFragment(FragmentPtr fragment_, Destinations & data_to_, Sources & data_from_)
        : fragment(fragment_), data_to(data_to_), data_from(data_from_)
    {
    }

    static String receiverKey(UInt32 exchange_id, const String & source) { return source + "_" + toString(exchange_id); }

    FragmentPtr getFragment() const { return fragment; }

    const Destinations & getDataTo() const { return data_to; }
    const Sources & getDataFrom() const { return data_from; }

private:
    FragmentPtr fragment;
    Destinations data_to;
    Sources data_from;

    //    ExchangeDataSources receivers;
};

using DistributedFragments = std::vector<DistributedFragment>;

}
