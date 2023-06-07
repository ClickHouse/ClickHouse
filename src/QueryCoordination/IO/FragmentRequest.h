#pragma once

#include <QueryCoordination/PlanFragment.h>
#include <QueryCoordination/IO/DestinationRequest.h>
#include <Common/typeid_cast.h>
#include <vector>

namespace DB
{

using Destinations = std::vector<DestinationRequest>;

class FragmentRequest
{
public:
    void write(WriteBuffer & out) const
    {
        Coordination::write(query, out);
        Coordination::write(fragment_id, out);

        Coordination::write(destinations.size(), out);
        for (const DestinationRequest & destination : destinations)
        {
            destination.write(out);
        }
    }


private:
    String query;
    UInt32 fragment_id;
    Destinations destinations;
};

}
