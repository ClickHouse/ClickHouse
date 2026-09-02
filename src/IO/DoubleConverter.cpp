#include <IO/DoubleConverter.h>

namespace DB
{
template <bool emit_decimal_point>
const double_conversion::DoubleToStringConverter & DoubleConverter<emit_decimal_point>::instance()
{
    static const double_conversion::DoubleToStringConverter instance{
        DoubleToStringConverterFlags<emit_decimal_point>::flags, "inf", "nan", 'e', -6, 21, 6, 1};

    return instance;
}

template class DoubleConverter<true>;
template class DoubleConverter<false>;
}
