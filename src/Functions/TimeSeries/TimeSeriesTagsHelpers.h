#pragma once


namespace DB::TimeSeriesTagsHelpers
{

/// Checks the types of specified function arguments with names and values of some tags.
/// One of these arguments is an argument containing an array of tuples Array(Tuple(String, String)),
/// while the other arguments are zero or even number of arguments contain separate pairs name & value.
void checkArgumentTypesForTagNamesAndValues(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t array_of_tuples_argument_index,
    size_t name_value_pairs_start_argument_index);

/// Extracts names and values of some tags from specified function arguments.
/// One of these arguments is an argument containing an array of tuples Array(Tuple(String, String)),
/// while the other arguments are zero or even number of arguments contain separate pairs name & value.
std::vector<TagNamesAndValuesPtr> extractTagNamesAndValuesFromArguments(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t array_of_tuples_argument_index,
    size_t name_value_pairs_start_argument_index);

/// Converts a vector of groups to a column.
ColumnPtr makeColumnForGroups(const std::vector<Group> & groups);

}
