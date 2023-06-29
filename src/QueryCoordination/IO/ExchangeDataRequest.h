#pragma once

#include <base/types.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

struct ExchangeDataRequest
{
    String from_host;
    String query_id;
    Int32 fragment_id;
    Int32 exchange_id;

    void write(WriteBuffer & out) const
    {
        writeStringBinary(from_host, out);
        writeStringBinary(query_id, out);
        writeVarInt(fragment_id, out);
        writeVarInt(exchange_id, out);
    }

    void read(ReadBuffer & in)
    {
        readStringBinary(from_host, in);
        readStringBinary(query_id, in);
        readVarInt(fragment_id, in);
        readVarInt(exchange_id, in);
    }

    String toString() const
    {
        return query_id + ", from " + from_host + ", destination fragment " + std::to_string(fragment_id) + ", destination exchange " + std::to_string(exchange_id);
    }
};

}
