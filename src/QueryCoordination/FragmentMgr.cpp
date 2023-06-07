
#include <QueryCoordination/FragmentMgr.h>

namespace DB
{

// for SECONDARY_QUERY from tcphandler, for INITIAL_QUERY from InterpreterSelectQueryFragments
void FragmentMgr::addFragment(String query_id, PlanFragmentPtr fragment)
{
    query_fragment[query_id].emplace_back(fragment);
}

// Keep fragments that need to be executed by themselves
void FragmentMgr::keepToProcessFragments(String query_id, const std::vector<FragmentRequest> & self_fragment)
{

}

void FragmentMgr::beginFragments(String query_id)
{
    for (PlanFragmentPtr fragment : query_fragment[query_id])
    {
        fragment->
    }
}

void FragmentMgr::receiveData(const ExchangeDataRequest & exchange_data_request, Block & block)
{

}


}
