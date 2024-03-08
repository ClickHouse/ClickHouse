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

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);

    String toString() const;
};

}
