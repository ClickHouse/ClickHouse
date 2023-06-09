#pragma once

#include <QueryCoordination/PlanFragment.h>
#include <Common/typeid_cast.h>
#include <vector>

namespace DB
{

using Destinations = std::vector<String>;

class FragmentRequest
{
public:
    void write(WriteBuffer & out) const
    {
        Coordination::write(fragment_id, out);

        Coordination::write(destinations.size(), out);
        for (const String & destination : destinations)
        {
            Coordination::write(destination, out);
        }
    }

    UInt32 fragment_id;
    Destinations destinations;
};

}
