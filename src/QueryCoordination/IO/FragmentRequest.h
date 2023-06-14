#pragma once

#include <Common/typeid_cast.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <vector>

namespace DB
{

using Destinations = std::vector<String>;

class FragmentRequest
{
public:
    void write(WriteBuffer & out) const
    {
        writeVarInt(fragment_id, out);

        writeVarInt(destinations.size(), out);
        for (const String & destination : destinations)
        {
            writeStringBinary(destination, out);
        }
    }

    void read(ReadBuffer & in)
    {
        readVarInt(fragment_id, in);

        Int64 destination_size;
        readVarInt(destination_size, in);

        for (Int64 i = 0; i < destination_size; ++i)
        {
            String destination;
            readStringBinary(destination, in);

            destinations.emplace_back(destination);
        }
    }

    Int64 fragment_id;
    Destinations destinations;
};

}
