#pragma once

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>

namespace DB
{

struct ExchangeDataRequest
{
    String from_host;
    String query_id;
    UInt32 fragment_id;
    UInt32 exchange_id;

    void write(WriteBuffer & out) const
    {
        writeStringBinary(from_host, out);
        writeStringBinary(query_id, out);
        writeVarUInt(fragment_id, out);
        writeVarUInt(exchange_id, out);
    }

    void read(ReadBuffer & in)
    {
        readStringBinary(from_host, in);
        readStringBinary(query_id, in);
        readVarUInt(fragment_id, in);
        readVarUInt(exchange_id, in);
    }

    String toString() const
    {
        return query_id + ", from " + from_host + ", destination fragment " + std::to_string(fragment_id) + ", destination exchange "
            + std::to_string(exchange_id);
    }
};

}
