#pragma once

#include <Columns/IColumn_fwd.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <base/types.h>


namespace DB
{
    struct ColumnWithTypeAndName;
    using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;
}


namespace DB::TimeSeriesTagsFunctionHelpers
{
using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
using Group = ContextTimeSeriesTagsCollector::Group;

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

/// Checks the type of a function argument containing a group.
/// Group is an integer (UInt64) which is used to identify a set of tags.
void checkArgumentTypeForGroup(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Extracts a group from a function argument.
/// Group is an integer (UInt64) which is used to identify a set of tags.
std::vector<Group> extractGroupFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index, bool return_single_element_if_const_column = false);

/// Checks the type of a function argument containing a constant tag name.
void checkArgumentTypeForConstTagName(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);
void checkArgumentTypeForConstString(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Extracts a constant tag name from a function argument.
String extractConstTagNameFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);
String extractConstStringFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Checks the type of a function argument containing a constant array of tag names.
void checkArgumentTypeForConstTagNames(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);
void checkArgumentTypeForConstStrings(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Checks the type of a function argument containing a constant array of tag names.
std::vector<String> extractConstTagNamesFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);
std::vector<String> extractConstStringsFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Checks the type of a function argument containing an identifier of time series.
const std::type_info & checkArgumentTypeForID(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index, bool allow_nullable = false);

/// Extracts an identifier of time series from a function argument.
template <typename IDType>
std::vector<IDType> extractIDFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index);

/// Converts a vector of groups to a column.
ColumnPtr makeColumnForGroup(const std::vector<Group> & groups);

/// Convert a vector of tags to a column.
ColumnPtr makeColumnForTagNamesAndValues(const std::vector<TagNamesAndValuesPtr> & tags_vector);

}
