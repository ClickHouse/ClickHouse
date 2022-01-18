#pragma once
#include <Common/ProfileEvents.h>
#include <Columns/IColumn.h>


namespace ProfileEvents
{

/// Dumps profile events to columns Map(String, UInt64)
void dumpToMapColumn(const Counters & counters, DB::IColumn * column, bool nonzero_only = true);

}
