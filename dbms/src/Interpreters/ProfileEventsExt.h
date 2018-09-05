#pragma once
#include <Common/ProfileEvents.h>
#include <Columns/IColumn.h>


namespace ProfileEvents
{

/// Dumps profile events to two column Array(String) and Array(UInt64)
void dumpToArrayColumns(const Counters & counters, DB::IColumn * column_names, DB::IColumn * column_value, bool nonzero_only = true);

}
