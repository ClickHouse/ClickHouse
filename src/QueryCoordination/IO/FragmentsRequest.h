#pragma once

#include <QueryCoordination/IO/FragmentRequest.h>
#include <base/types.h>
#include <vector>

namespace DB
{

class FragmentsRequest
{
public:
    void write(WriteBuffer & out) const
    {
        Coordination::write(query, out);

        Coordination::write(fragments_request.size(), out);
        for (const FragmentRequest & fragment_request : fragments_request)
        {
            fragment_request.write(out);
        }
    }

    const std::vector<FragmentRequest> & fragmentsRequest() const { return fragments_request; }

    String query;
    std::vector<FragmentRequest> fragments_request;
};

}
