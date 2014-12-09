#pragma once

#include <src/double-conversion.h>

namespace DB
{

template <bool emit_decimal_point> struct DoubleToStringConverterFlags
{
	static constexpr auto flags = double_conversion::DoubleToStringConverter::NO_FLAGS;
};

template <> struct DoubleToStringConverterFlags<true>
{
	static constexpr auto flags = double_conversion::DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT;
};

template <bool emit_decimal_point = true>
const double_conversion::DoubleToStringConverter & getDoubleToStringConverter()
{
	static const double_conversion::DoubleToStringConverter instance{
		DoubleToStringConverterFlags<emit_decimal_point>::flags, "inf", "nan", 'e', -6, 21, 6, 1
	};

	return instance;
}

}
