#pragma once


namespace DB::TimeSeriesTagsHelpers
{

/// Checks the types of specified function arguments containing names and values of tags.
/// The first of these arguments is an argument containing an array of tuples Array(Tuple(String, String)),
/// and zero or even number of other arguments contain separate pairs name & value.
/// It looks like this: [('tag_name1', 'tag_value1'), ...], 'tag_name2', 'tag_value2', ...
void checkArgumentTypesForTagNamesAndValues(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index);

/// Extracts names and values of tags from specified function arguments.
/// The first of these arguments is an argument containing an array of tuples Array(Tuple(String, String)),
/// and zero or even number of other arguments contain separate pairs name & value.
/// It looks like this: [('tag_name1', 'tag_value1'), ...], 'tag_name2', 'tag_value2', ...
std::vector<TagNamesAndValuesPtr> extractTagNamesAndValuesFromArguments(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index);

/// Converts a vector of groups to a column.
ColumnPtr makeColumnForGroup(const std::vector<Group> & groups);

}
