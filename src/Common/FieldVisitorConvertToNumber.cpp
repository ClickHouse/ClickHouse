#include <Common/FieldVisitorConvertToNumber.h>
#include "base/Decimal.h"

namespace DB
{

/// Explicit template instantiations.
template class FieldVisitorConvertToNumber<Int8>;
template class FieldVisitorConvertToNumber<UInt8>;
template class FieldVisitorConvertToNumber<Int16>;
template class FieldVisitorConvertToNumber<UInt16>;
template class FieldVisitorConvertToNumber<Int32>;
template class FieldVisitorConvertToNumber<UInt32>;
template class FieldVisitorConvertToNumber<Int64>;
template class FieldVisitorConvertToNumber<UInt64>;
template class FieldVisitorConvertToNumber<Int128>;
template class FieldVisitorConvertToNumber<UInt128>;
template class FieldVisitorConvertToNumber<Int256>;
template class FieldVisitorConvertToNumber<UInt256>;
template class FieldVisitorConvertToNumber<Float32>;
template class FieldVisitorConvertToNumber<Float64>;

}
