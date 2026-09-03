#pragma once

#include <Common/DateLUT.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <string>


namespace DB
{

class Block;

std::string extractTimeZoneNameFromColumn(const IColumn * column, const String & column_name);

/// Determine working timezone either from optional argument with time zone name or from time zone in DateTime type of argument.
/// Returns empty string if default time zone should be used.
///
/// Parameter allow_nonconst_timezone_arguments toggles if non-const timezone function arguments are accepted (legacy behavior) or not. The
/// problem with the old behavior is that the timezone is part of the type, and not part of the value. This lead to confusion and unexpected
/// results.
/// - For new functions, set allow_nonconst_timezone_arguments = false.
/// - For existing functions
///    - which disallow non-const timezone arguments anyways (e.g. getArgumentsThatAreAlwaysConstant()), set allow_nonconst_timezone_arguments = false,
///    - which allow non-const timezone arguments, set allow_nonconst_timezone_arguments according to the corresponding setting.
std::string extractTimeZoneNameFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num, bool allow_nonconst_timezone_arguments);

const DateLUTImpl & extractTimeZoneFromFunctionArguments(
    const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);

}
