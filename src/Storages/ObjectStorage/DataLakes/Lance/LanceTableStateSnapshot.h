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
    UInt64 snapshot_id = 0;
    UInt64 schema_id = 0;

    void serialize(WriteBuffer & out) const;
    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const
    {
        return snapshot_id == other.snapshot_id && schema_id == other.schema_id;
    }
};

}

#endif
