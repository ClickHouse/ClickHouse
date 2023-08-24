#pragma once

#include <QueryCoordination/IO/FragmentRequest.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <vector>

namespace DB
{

class FragmentsRequest
{
public:
    void write(WriteBuffer & out) const
    {
        /// query has been sent

        size_t size = fragments_request.size();
        writeVarUInt(size, out);

        for (const FragmentRequest & fragment_request : fragments_request)
        {
            fragment_request.write(out);
        }
    }

    void read(ReadBuffer & in)
    {
        /// query has been read

        size_t fragment_size = 0;
        readVarUInt(fragment_size, in);
//        fragments_request.reserve(fragment_size);

        for (size_t i = 0; i < fragment_size; ++i)
        {
            FragmentRequest request;
            request.read(in);
            fragments_request.emplace_back(request);
        }
    }

    const std::vector<FragmentRequest> & fragmentsRequest() const { return fragments_request; }

    String query;
    std::vector<FragmentRequest> fragments_request;
};

}
