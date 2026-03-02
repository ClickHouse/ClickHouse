#pragma once
#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DeltaLake
{

struct TableStateSnapshot
{
    size_t version = 0;

    void serialize(DB::WriteBuffer & out) const;
    static TableStateSnapshot deserialize(DB::ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const
    {
        return version == other.version;
    }
};

}

#endif
