#pragma once
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/IColumn.h>


namespace ProfileEvents
{

/// Dumps profile events to columns Map(String, UInt64)
void dumpToMapColumn(const Counters::Snapshot & counters, DB::IColumn * column, bool nonzero_only = true);

/// This is for ProfileEvents packets.
enum Type : int8_t
{
    INCREMENT = 1,
    GAUGE     = 2,
};

extern std::shared_ptr<DB::DataTypeEnum8> TypeEnum;

}
