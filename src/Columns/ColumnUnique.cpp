#include <Columns/ColumnUnique.h>

namespace DB
{

/// Explicit template instantiations.
template class ColumnUnique<ColumnInt8>;
template class ColumnUnique<ColumnUInt8>;
template class ColumnUnique<ColumnInt16>;
template class ColumnUnique<ColumnUInt16>;
template class ColumnUnique<ColumnInt32>;
template class ColumnUnique<ColumnUInt32>;
template class ColumnUnique<ColumnInt64>;
template class ColumnUnique<ColumnUInt64>;
template class ColumnUnique<ColumnInt128>;
template class ColumnUnique<ColumnUInt128>;
template class ColumnUnique<ColumnInt256>;
template class ColumnUnique<ColumnUInt256>;
template class ColumnUnique<ColumnFloat32>;
template class ColumnUnique<ColumnFloat64>;
template class ColumnUnique<ColumnString>;
template class ColumnUnique<ColumnFixedString>;
template class ColumnUnique<ColumnDateTime64>;
template class ColumnUnique<ColumnIPv4>;
template class ColumnUnique<ColumnIPv6>;
template class ColumnUnique<ColumnUUID>;

}
