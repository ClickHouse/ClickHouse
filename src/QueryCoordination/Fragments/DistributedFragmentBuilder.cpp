#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/Fragments/Fragment.h>


namespace DB
{

DistributedFragments DistributedFragmentBuilder::build()
{
    std::unordered_map<UInt32, FragmentRequest> id_fragments;
    for (const auto & request : plan_fragment_requests)
    {
        //        LOG_DEBUG(log, "Receive fragment to distributed, need execute {}", request.toString());
        id_fragments.emplace(request.fragment_id, request);
    }

    DistributedFragments res_fragments;

    for (const auto & fragment : all_fragments)
    {
        auto it = id_fragments.find(fragment->getFragmentID());
        if (it != id_fragments.end())
        {
            auto & request = it->second;
            res_fragments.emplace_back(DistributedFragment(fragment, request.data_to, request.data_from));
        }
    }

    return res_fragments;
}

}
