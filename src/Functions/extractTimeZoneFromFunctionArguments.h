#pragma once

#include <string>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>


class DateLUTImpl;

namespace DB
{

class Block;

/// Determine working timezone either from optional argument with time zone name or from time zone in DateTime type of argument.
/// Returns empty string if default time zone should be used.
std::string extractTimeZoneNameFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);

const DateLUTImpl & extractTimeZoneFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);

}
