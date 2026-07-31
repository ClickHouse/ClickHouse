#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/Types.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace DB::Lance
{

struct TableStateSnapshot
{
    UInt64 version = 0;

    void serialize(WriteBuffer & out) const;
    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const { return version == other.version; }
};

}

#endif
