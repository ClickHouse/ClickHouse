#pragma once

#include <unordered_map>
#include <string_view>

#include <QueryCoordination/PlanFragment.h>
#include <QueryCoordination/IO/FragmentRequest.h>
#include <QueryCoordination/IO/ExchangeDataRequest.h>
#include <Core/Block.h>

namespace DB
{

using QueryFragment = std::unordered_map<String, std::vector<PlanFragmentPtr>>;

struct ExchangeNodeKey
{
    String query_id;
    UInt32 fragment_id;
    UInt32 exchange_id;

    bool operator== (const ExchangeNodeKey & key) const
    {    return query_id == key.query_id &&
            fragment_id == key.fragment_id &&
            exchange_id == key.exchange_id;
    }

    struct Hash
    {
        size_t operator()(const ExchangeNodeKey & key) const
        {
            return std::hash<std::string_view>{}(key.query_id) + std::hash<UInt32>{}(key.fragment_id) + std::hash<UInt32>{}(key.exchange_id);
        }
    };
};


using ExchangeMap = std::unordered_map<ExchangeNodeKey, ExchangeStep, ExchangeNodeKey::Hash>;

class FragmentMgr
{
public:
    // for SECONDARY_QUERY from tcphandler, for INITIAL_QUERY from InterpreterSelectQueryFragments
    void addFragment(String query_id, PlanFragmentPtr fragment);

    // Keep fragments that need to be executed by themselves
    void keepToProcessFragments(String query_id, const std::vector<FragmentRequest> & self_fragment);

    void beginFragments(String query_id);

    void receiveData(const ExchangeDataRequest & exchange_data_request, Block & block);

    static FragmentMgr & getInstance()
    {
        static FragmentMgr fragment_mgr;
        return fragment_mgr;
    }

private:
    QueryFragment query_fragment;

    ExchangeMap exchange_map;
};

}
