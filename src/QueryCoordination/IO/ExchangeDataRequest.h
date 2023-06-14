#pragma once

#include <base/types.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

struct ExchangeDataRequest
{
    String query_id;
    Int32 fragment_id;
    Int32 exchange_id;

    void write(WriteBuffer & out) const
    {
        writeStringBinary(query_id, out);
        writeVarInt(fragment_id, out);
        writeVarInt(exchange_id, out);
    }

    void read(ReadBuffer & in)
    {
        readStringBinary(query_id, in);
        readVarInt(fragment_id, in);
        readVarInt(exchange_id, in);
    }
};

}
