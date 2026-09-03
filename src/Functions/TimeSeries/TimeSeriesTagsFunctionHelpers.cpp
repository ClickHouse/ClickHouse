#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <base/insertAtEnd.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace DB::TimeSeriesTagsFunctionHelpers
{

namespace
{
    /// Checks that the column's type is Array(Tuple(String, String)).
    void checkTypeOfTagsArrayArgument(std::string_view function_name, const DataTypePtr & type, size_t argument_index)
    {
        auto nested_type = removeNullableOrLowCardinalityNullable(type);

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(nested_type.get()))
            nested_type = map_type->getNestedType();

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(nested_type.get()))
        {
            if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()))
            {
                if (tuple_type->getElements().size() == 2)
                {
                    auto first_type = removeNullableOrLowCardinalityNullable(tuple_type->getElements()[0]);
                    auto second_type = removeNullableOrLowCardinalityNullable(tuple_type->getElements()[1]);
                    if (isStringOrFixedString(first_type) && isStringOrFixedString(second_type))
                        return;
                }
            }

            if (isNothing(array_type->getNestedType()))
                return;
        }

        if (isNothing(nested_type))
            return;

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument #{} of function {} has wrong type {}, it must be {}",
            argument_index + 1,
            function_name,
            type,
            "Array(Tuple(String, String))");
    }

    void reserveTagVectors(VectorWithMemoryTracking<TagNamesAndValues> & out_tags_vector, size_t num_tags)
    {
        if (num_tags == 0)
            return;

        for (auto & tags : out_tags_vector)
            tags.reserve(tags.size() + num_tags);
    }

    /// Extracts tags from a column of type Array(Tuple(String, String)) containing pairs (tag_name, tag_value).
    void extractTagsArrayFromColumn(
        std::string_view function_name,
        size_t argument_index,
        const IColumn & column_tags_array,
        VectorWithMemoryTracking<TagNamesAndValues> & out_tags_vector,
        size_t num_extra_tags)
    {
        size_t num_rows = column_tags_array.size();
        chassert(out_tags_vector.size() == num_rows);

        /// The argument can be Array(Tuple(String, String)) or Array(Nothing).
        if (const auto * array_column = checkAndGetColumn<ColumnArray>(&column_tags_array))
        {
            const auto * tuple_column = checkAndGetColumn<ColumnTuple>(&array_column->getData());
            if (tuple_column && (tuple_column->tupleSize() == 2))
            {
                const IColumn & tag_names = tuple_column->getColumn(0);
                const IColumn & tag_values = tuple_column->getColumn(1);
                const IColumn::Offsets & offsets = array_column->getOffsets();
                for (size_t i = 0; i != num_rows; ++i)
                {
                    auto & res = out_tags_vector[i];
                    auto start_offset = offsets[i - 1];
                    auto end_offset = offsets[i];
                    res.reserve(res.size() + (end_offset - start_offset) + num_extra_tags);
                    for (size_t j = start_offset; j != end_offset; ++j)
                    {
                        auto tag_name = std::string_view{tag_names.getDataAt(j)};
                        auto tag_value = std::string_view{tag_values.getDataAt(j)};
                        res.emplace_back(tag_name, tag_value);
                    }
                }
                return;
            }

            /// [] is of type Array(Nothing).
            if (checkColumn<ColumnNothing>(&array_column->getData()))
            {
                reserveTagVectors(out_tags_vector, num_extra_tags);
                return;
            }
        }

        /// The argument can be literal NULL.
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&column_tags_array))
        {
            const auto & nested_column = nullable_column->getNestedColumn();
            if (checkColumn<ColumnNothing>(&nested_column))
            {
                reserveTagVectors(out_tags_vector, num_extra_tags);
                return;
            }
        }

        /// The argument can be wrapped in a ColumnMap.
        if (const auto * map_column = checkAndGetColumn<ColumnMap>(&column_tags_array))
        {
            extractTagsArrayFromColumn(function_name, argument_index, map_column->getNestedColumn(), out_tags_vector, num_extra_tags);
            return;
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_tags_array.convertToFullIfWrapped()->convertToFullColumnIfLowCardinality(); full_column.get() != &column_tags_array)
        {
            extractTagsArrayFromColumn(function_name, argument_index, *full_column, out_tags_vector, num_extra_tags);
            return;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_tags_array.getName(), argument_index + 1, function_name, "Array(Tuple(String, String))");
    }

    /// Checks that the column's type is String or FixedString.
    void checkTypeOfTagNameArgument(std::string_view function_name, const DataTypePtr & type, size_t argument_index)
    {
        auto nested_type = removeNullableOrLowCardinalityNullable(type);

        if (isStringOrFixedString(nested_type) || isNothing(nested_type))
            return;

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, function_name, type, "String or FixedString");
    }

    void checkTypeOfTagValueArgument(std::string_view function_name, const DataTypePtr & type, size_t argument_index)
    {
        checkTypeOfTagNameArgument(function_name, type, argument_index);
    }

    /// Extracts strings from a column.
    VectorWithMemoryTracking<std::string_view> extractStringViewFromArgument(const IColumn & column)
    {
        size_t num_rows = column.size();
        VectorWithMemoryTracking<std::string_view> res{num_rows, ""};
        for (size_t i = 0; i != res.size(); ++i)
        {
            if (!column.isNullAt(i))
            {
                auto str = std::string_view{column.getDataAt(i)};
                trimRight(str, '\0'); /// Trim zero characters in case FixedString was used for this column.
                res[i] = str;
            }
        }
        return res;
    }

    /// Extracts tags from two columns: the first column contains names and the second column contains values.
    void extractTagNameAndValueFromTwoColumns(const IColumn & column_tag_name,
                                              const IColumn & column_tag_value,
                                              VectorWithMemoryTracking<TagNamesAndValues> & out_tags_vector)
    {
        size_t num_rows = column_tag_name.size();
        chassert(column_tag_name.size() == num_rows);
        chassert(out_tags_vector.size() == num_rows);

        VectorWithMemoryTracking<std::string_view> tag_names = extractStringViewFromArgument(column_tag_name);
        VectorWithMemoryTracking<std::string_view> tag_values = extractStringViewFromArgument(column_tag_value);
        for (size_t i = 0; i != num_rows; ++i)
        {
            auto tag_name = tag_names[i];
            auto tag_value = tag_values[i];
            out_tags_vector[i].emplace_back(tag_name, tag_value);
        }
    }

    /// Sorts each set of tags and removes duplicate tags.
    /// The function also removes tags with empty values.
    void sortTagsAndRemoveDuplicates(std::string_view function_name, VectorWithMemoryTracking<TagNamesAndValues> & tags_vector)
    {
        auto less_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first < right.first;
        };
        auto equal_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first == right.first;
        };
        auto is_tag_name_empty = [](const std::pair<String, String> & x)
        {
            return x.first.empty();
        };
        auto is_tag_value_empty = [](const std::pair<String, String> & x)
        {
            return x.second.empty();
        };
        for (auto & tags : tags_vector)
        {
            if (std::find_if(tags.begin(), tags.end(), is_tag_name_empty) != tags.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} must not be called with an empty tag name", function_name);

            std::sort(tags.begin(), tags.end(), less_by_tag_name);
            tags.erase(std::unique(tags.begin(), tags.end()), tags.end());

            /// A tag with an empty value means the tag is absent, so drop such tags before checking for
            /// conflicts. This lets a caller pass an empty value for a tag that is also present (with a
            /// non-empty value) in the tags array - e.g. an empty `metric_name` column when `__name__` is
            /// already in the tags - without it being treated as a conflict.
            std::erase_if(tags, is_tag_value_empty);

            auto adjacent = std::adjacent_find(tags.begin(), tags.end(), equal_by_tag_name);
            if (adjacent != tags.end())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Found two tags with the same name {} but different values {} and {} while executing function {}",
                    quoteString(adjacent->first), quoteString(adjacent->second), quoteString(std::next(adjacent)->second), function_name);
            }
        }
    }

    /// Wrap TagNamesAndValues in shared pointers.
    VectorWithMemoryTracking<TagNamesAndValuesPtr> makeTagsPtrVector(VectorWithMemoryTracking<TagNamesAndValues> && tags_vector)
    {
        VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
        res.reserve(tags_vector.size());
        for (auto & tags : tags_vector)
        {
            res.emplace_back(std::make_shared<TagNamesAndValues>(std::move(tags)));
        }
        return res;
    }

    /// Extracts a group from a column.
    VectorWithMemoryTracking<Group> extractGroupFromColumn(std::string_view function_name, size_t argument_index, const IColumn & column, bool return_single_element_if_const_column)
    {
        /// Group must be UInt64.
        if (checkColumn<ColumnUInt64>(&column))
        {
            std::string_view data = column.getRawData();
            chassert(data.size() == column.size() * sizeof(UInt64));
            const UInt64 * begin = reinterpret_cast<const UInt64 *>(data.data());
            return VectorWithMemoryTracking<Group>(begin, begin + column.size());
        }

        /// The argument can be wrapped in ColumnConst.
        if (return_single_element_if_const_column)
        {
            if (const auto * data_column = checkAndGetColumnConstData<ColumnUInt64>(&column))
            {
                chassert(data_column->size() == 1);
                return extractGroupFromColumn(function_name, argument_index, *data_column, false);
            }
        }

        /// The argument can be wrapped in ColumnLowCardinality.
        if (auto full_column = column.convertToFullIfWrapped()->convertToFullColumnIfLowCardinality(); full_column.get() != &column)
        {
            return extractGroupFromColumn(function_name, argument_index, *full_column, false);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column.getName(), argument_index + 1, function_name, "UInt64");
    }

}


void checkArgumentTypesForTagNamesAndValues(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index)
{
    if (start_argument_index >= arguments.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} should be called with at least {} arguments", function_name, start_argument_index + 1);

    checkTypeOfTagsArrayArgument(function_name, arguments[start_argument_index].type, start_argument_index);

    if (start_argument_index + 1 < arguments.size())
    {
        bool nargs_should_be_odd = (start_argument_index % 2) == 0;
        if ((arguments.size() % 2) != nargs_should_be_odd)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Expected {} number of arguments of function {}",
                            nargs_should_be_odd ? "odd" : "even",
                            function_name);
        }

        for (size_t i = start_argument_index + 1; i < arguments.size(); i += 2)
        {
            checkTypeOfTagNameArgument(function_name, arguments[i].type, i);
            checkTypeOfTagValueArgument(function_name, arguments[i + 1].type, i + 1);
        }
    }
}

VectorWithMemoryTracking<TagNamesAndValuesPtr> extractTagNamesAndValuesFromArguments(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index)
{
    chassert(start_argument_index < arguments.size());
    const auto & column_tags_array = *arguments[start_argument_index].column;

    VectorWithMemoryTracking<TagNamesAndValues> tags_vector;
    tags_vector.resize(column_tags_array.size());
    const size_t num_extra_tags = (arguments.size() - start_argument_index - 1) / 2;
    extractTagsArrayFromColumn(function_name, start_argument_index, column_tags_array, tags_vector, num_extra_tags);

    if (start_argument_index + 1 < arguments.size())
    {
        bool nargs_should_be_odd = (start_argument_index % 2) == 0;
        chassert((arguments.size() % 2) == nargs_should_be_odd);
        for (size_t i = start_argument_index + 1; i < arguments.size(); i += 2)
        {
            const auto & column_tag_name = *arguments[i].column;
            const auto & column_tag_value = *arguments[i + 1].column;
            extractTagNameAndValueFromTwoColumns(column_tag_name, column_tag_value, tags_vector);
        }
    }

    sortTagsAndRemoveDuplicates(function_name, tags_vector);
    return makeTagsPtrVector(std::move(tags_vector));
}


void checkArgumentTypeForGroup(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    if (argument_index >= arguments.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} can't be called with {} arguments", function_name, arguments.size());

    auto type = arguments[argument_index].type;
    auto nested_type = removeLowCardinality(type);

    if (!isUInt64(nested_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, function_name, type, "UInt64");
}


VectorWithMemoryTracking<Group> extractGroupFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index, bool return_single_element_if_const_column)
{
    chassert(argument_index < arguments.size());
    const auto & column = *arguments[argument_index].column;
    return extractGroupFromColumn(function_name, argument_index, column, return_single_element_if_const_column);
}


void checkArgumentTypeForConstString(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    if (argument_index >= arguments.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} can't be called with {} arguments", function_name, arguments.size());

    auto type = arguments[argument_index].type;
    auto nested_type = removeLowCardinality(type);

    if (!isString(nested_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, function_name, type, "String");
}


void checkArgumentTypeForConstTagName(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    checkArgumentTypeForConstString(function_name, arguments, argument_index);
}


String extractConstStringFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    chassert(argument_index < arguments.size());
    const auto & column = *arguments[argument_index].column;

    if (!checkColumnConst<ColumnString>(&column))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant string", argument_index + 1, function_name);

    return String{column.getDataAt(0)};
}


String extractConstTagNameFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    String tag_name = extractConstStringFromArgument(function_name, arguments, argument_index);
    if (tag_name.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} must not be called with an empty tag name", function_name);
    }
    return tag_name;
}


void checkArgumentTypeForConstStrings(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    if (argument_index >= arguments.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} can't be called with {} arguments", function_name, arguments.size());

    auto type = arguments[argument_index].type;
    auto nested_type = removeLowCardinality(type);

    if (!isArray(nested_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, function_name, type, "Array(String)");

    auto element_type = typeid_cast<const DataTypeArray &>(*nested_type).getNestedType();
    if (!isString(element_type) && !isNothing(element_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, function_name, type, "Array(String)");
}


void checkArgumentTypeForConstTagNames(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    checkArgumentTypeForConstStrings(function_name, arguments, argument_index);
}


Strings extractConstStringsFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    chassert(argument_index < arguments.size());
    const auto & column = *arguments[argument_index].column;

    const auto * array_column = checkAndGetColumnConstData<ColumnArray>(&column);
    if (!array_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant Array(String)", argument_index + 1, function_name);

    if (checkAndGetColumn<ColumnNothing>(&array_column->getData()))
        return {};

    size_t count = array_column->getOffsets()[0];
    const auto * string_column = checkAndGetColumn<ColumnString>(&array_column->getData());
    if (!string_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant Array(String)", argument_index + 1, function_name);

    Strings res;
    res.reserve(count);
    for (size_t i = 0; i != count; ++i)
        res.emplace_back(String{string_column->getDataAt(i)});

    return res;
}


Strings extractConstTagNamesFromArgument(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index)
{
    Strings tag_names = extractConstStringsFromArgument(function_name, arguments, argument_index);
    for (const auto & tag_name : tag_names)
    {
        if (tag_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} must not be called with an empty tag name", function_name);
    }
    return tag_names;
}


void checkArgumentTypeForID(std::string_view function_name, const ColumnsWithTypeAndName & arguments, size_t argument_index, bool allow_nullable)
{
    if (argument_index >= arguments.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} can't be called with {} arguments", function_name, arguments.size());

    auto type = removeLowCardinality(arguments[argument_index].type);

    if (allow_nullable)
        type = removeNullable(type);

    bool id_ok = !type->isNullable() && !isNothing(*type) && type->isComparable()
        && !isVariant(*type) && !type->hasDynamicSubcolumns();
    if (!id_ok)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument #{} of function {} has wrong type {}, it must be a comparable non-Nullable type",
                        argument_index + 1, function_name, arguments[argument_index].type);
}


ColumnPtr makeColumnForGroup(const VectorWithMemoryTracking<Group> & groups)
{
    auto res = ColumnUInt64::create();
    res->reserve(groups.size());
    for (auto group : groups)
        res->insertValue(group);
    return res;
}


ColumnPtr makeColumnForTagNamesAndValues(const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    size_t total_tags = 0;
    for (const auto & tags : tags_vector)
        total_tags += tags->size();

    auto tag_names = ColumnString::create();
    auto tag_values = ColumnString::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    tag_names->reserve(total_tags);
    tag_values->reserve(total_tags);
    offsets->reserve(tags_vector.size());

    for (const auto & tags : tags_vector)
    {
        for (const auto & [tag_name, tag_value] : *tags)
        {
            tag_names->insertData(tag_name.data(), tag_name.length());
            tag_values->insertData(tag_value.data(), tag_value.length());
        }
        offsets->insertValue(tag_names->size());
    }

    MutableColumns tuple_columns;
    tuple_columns.reserve(2);
    tuple_columns.push_back(std::move(tag_names));
    tuple_columns.push_back(std::move(tag_values));
    return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(offsets));
}

}
