#include <Core/DecimalField.h>
#include <Core/DecimalComparison.h>

namespace DB
{

template <typename T>
bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}


template bool decimalEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLess<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLessOrEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

}
