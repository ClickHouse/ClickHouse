#include <Functions/TimeSeries/TimeSeriesTagsHelpers.h>


namespace DB
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

    /// Extracts tags from a column of type Array(Tuple(String, String)) containing pairs (tag_name, tag_value).
    void extractTagsArrayFromArgument(
        std::string_view function_name,
        const IColumn & column_tags_array,
        size_t argument_index,
        std::vector<TagNamesAndValues> & out_tags_vector)
    {
        size_t num_rows = column_tags_array.size();
        chassert(out_tags_vector.size() == num_rows);

        /// The argument can be Array(Tuple(String, String)).
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
                    for (size_t j = start_offset; j != end_offset; ++j)
                    {
                        auto tag_name = std::string_view{tag_names.getDataAt(j)};
                        auto tag_value = std::string_view{tag_values.getDataAt(j)};
                        if (tag_name.empty())
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} must not be called with an empty tag name", function_name);
                        res.emplace_back(tag_name, tag_value);
                    }
                }
                return;
            }
        }

        /// The argument can be nullable.
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&column_tags_array))
        {
            const auto & null_map = nullable_column->getNullMapData();
            const auto & nested_column = nullable_column->getNestedColumn();
            std::vector<TagNamesAndValues> tags_vector_from_nested_column;
            tags_vector_from_nested_column.resize(num_rows);
            extractTagsArrayFromArgument(function_name, nested_column, argument_index, tags_vector_from_nested_column);
            for (size_t i = 0; i != num_rows; ++i)
            {
                if (!null_map[i])
                    insertAtEnd(out_tags_vector[i], std::move(tags_vector_from_nested_column[i]));
            }
            return;
        }

        /// The argument can be literal NULL.
        if (checkColumn<ColumnNothing>(&column_tags_array))
            return;

        /// The argument can be wrapped in a ColumnMap.
        if (const auto * map_column = checkAndGetColumn<ColumnMap>(&column_tags_array))
        {
            extractTagsArrayFromArgument(function_name, map_column->getNestedColumn(), argument_index, out_tags_vector);
            return;
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_tags_array.convertToFullIfNeeded(); full_column.get() != &column_tags_array)
        {
            extractTagsArrayFromArgument(function_name, *full_column, argument_index, out_tags_vector);
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
    std::vector<std::string_view> extractStringViewFromArgument(const IColumn & column)
    {
        size_t num_rows = column.size();
        std::vector<std::string_view> res{num_rows, ""};
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
    void extractTagNameAndValueFromTwoArguments(std::string_view function_name,
                                                const IColumn & column_tag_names,
                                                const IColumn & column_tag_values,
                                                std::vector<TagNamesAndValues> & out_tags_vector)
    {
        size_t num_rows = column_tag_names.size();
        chassert(column_tag_values.size() == num_rows);
        chassert(out_tags_vector.size() == num_rows);

        std::vector<std::string_view> tag_names = extractStringViewFromArgument(column_tag_names);
        std::vector<std::string_view> tag_values = extractStringViewFromArgument(column_tag_values);
        for (size_t i = 0; i != num_rows; ++i)
        {
            auto tag_name = tag_names[i];
            auto tag_value = tag_values[i];
            if (tag_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} must not be called with an empty tag name", function_name);
            out_tags_vector[i].emplace_back(tag_name, tag_value);
        }
    }

    /// Sorts each set of tags and removes duplicate tags.
    /// The function also removes tags with empty values.
    void sortTagsAndRemoveDuplicates(std::string_view function_name, std::vector<TagNamesAndValues> & tags_vector)
    {
        auto less_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first < right.first;
        };
        auto equal_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first == right.first;
        };
        auto is_tag_value_empty = [](const std::pair<String, String> & x)
        {
            return x.second.empty();
        };
        for (auto & tags : tags_vector)
        {
            std::sort(tags.begin(), tags.end(), less_by_tag_name);
            tags.erase(std::unique(tags.begin(), tags.end()), tags.end());

            auto adjacent = std::adjacent_find(tags.begin(), tags.end(), equal_by_tag_name);
            if (adjacent != tags.end())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Found two tags with the same name {} but different values {} and {} while executing function {}",
                    quoteString(adjacent->first), quoteString(adjacent->second), quoteString(std::next(adjacent)->second), name);
            }

            std::erase_if(tags, is_tag_value_empty);
        }
    }

    /// Wrap TagNamesAndValues in shared pointers.
    std::vector<TagNamesAndValuesPtr> makeTagsPtrVector(std::vector<TagNamesAndValues> && tags_vector)
    {
        std::vector<TagNamesAndValuesPtr> res;
        res.reserve(tags_vector.size());
        for (auto & tags : tags_vector)
        {
            res.emplace_back(std::make_shared<TagNamesAndValues>(std::move(tags)));
        }
        return res;
    }
}


void checkArgumentTypesForTagNamesAndValues(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index)
{
    if (start_argument_index < arguments.size())
        checkTypeOfTagsArrayArgument(function_name, arguments[start_argument_index].type, start_argument_index, tags_vector);

    if (start_argument_index + 1 < arguments.size())
    {
        bool nargs_should_be_odd = (start_argument_index % 2) == 0;
        if ((arguments.size() % 2) != nargs_should_be_odd)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Expected {} number of arguments of function {}",
                            nargs_should_be_odd ? "odd" "even",
                            function_name);

        for (size_t i = start_argument_index + 1; i < arguments.size(); i += 2)
        {
            checkTypeOfTagNameArgument(function_name, arguments[i].type, i);
            checkTypeOfTagValueArgument(function_name, arguments[i + 1].type, i + 1);
        }
    }
}

std::vector<TagNamesAndValuesPtr> extractTagNamesAndValuesFromArguments(
    std::string_view function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t start_argument_index,
    size_t input_rows_count)
{
    std::vector<TagNamesAndValues> tags_vector;
    tags_vector.resize(input_rows_count);

    if (start_argument_index < arguments.size())
        extractTagsArraysFromArgument(function_name, *arguments[start_argument_index], start_argument_index);

    if (start_argument_index + 1 < arguments.size())
    {
        chassert(((arguments.size() - start_argument_index - 1) % 2) == 0);
        for (size_t i = start_argument_index + 1; i < arguments.size(); i += 2)
        {
            const auto & column_separate_tag_names = arguments[i].column;
            const auto & column_separate_tag_values = arguments[i + 1].column;
            extractTagNameAndValueFromTwoArguments(function_name, *column_separate_tag_names, *column_separate_tag_values, tags_vector);
        }
    }

    sortTagsAndRemoveDuplicates(tags_vector);
    return makeTagsPtrVector(std::move(tags_vector));
}


ColumnPtr makeColumnForGroup(const std::vector<Group> & groups)
{
    auto res = ColumnUInt64::create();
    res->reserve(groups.size());
    for (auto group : groups)
        res->insertValue(group);
    return res;
}

}
