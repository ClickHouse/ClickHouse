#include <Functions/TimeSeries/TimeSeriesTagsHelpers.h>


namespace DB
{

namespace
{
    static void checkDataTypeOfTagsArray(const DataTypePtr & data_type, size_t argument_index)
    {
        auto type = removeNullableOrLowCardinalityNullable(data_type);

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
            type = map_type->getNestedType();

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()))
            {
                if (tuple_type->getElements().size() == 2)
                {
                    auto tuple_first_type = removeNullableOrLowCardinalityNullable(tuple_type->getElements()[0]);
                    auto tuple_second_type = removeNullableOrLowCardinalityNullable(tuple_type->getElements()[1]);
                    if (isStringOrFixedString(tuple_first_type) && isStringOrFixedString(tuple_second_type))
                        return;
                }
            }
        }

        if (isNothing(type))
            return;

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "Array(Tuple(String, String))");
    }

    static void checkDataTypeOfSeparateTagNames(const DataTypePtr & data_type, size_t argument_index)
    {
        auto type = removeNullableOrLowCardinalityNullable(data_type);
        if (isStringOrFixedString(type) || isNothing(type))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "String or FixedString");
    }

    static void checkDataTypeOfSeparateTagValues(const DataTypePtr & data_type, size_t argument_index)
    {
        checkDataTypeOfSeparateTagNames(data_type, argument_index);
    }

    /// Extracts tags from the second argument of type Array(Tuple(String, String)) containing pairs {tag_name, tag_value}.
    void extractTagsFromTagsArrayColumn(std::vector<TagNamesAndValues> & out_tags_vector, const IColumn & column_tags_array, size_t argument_index)
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
                        if (!tag_name.empty() && !tag_value.empty())
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
            extractTagsFromTagsArrayColumn(tags_vector_from_nested_column, nested_column, argument_index);
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
            extractTagsFromTagsArrayColumn(out_tags_vector, map_column->getNestedColumn(), argument_index);
            return;
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_tags_array.convertToFullIfNeeded(); full_column.get() != &column_tags_array)
        {
            extractTagsFromTagsArrayColumn(out_tags_vector, *full_column, argument_index);
            return;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_tags_array.getName(), argument_index + 1, name, "Array(Tuple(String, String))");
    }

    /// Extracts tags from two arguments - the first argument contains the name of a tag and the second argument contains the value of the tag.
    void extractTagsFromSeparateTagNamesAndTagValuesColumns(std::vector<TagNamesAndValues> & out_tags_vector,
                                                                   const IColumn & column_separate_tag_names,
                                                                   const IColumn & column_separate_tag_values)
    {
        size_t num_rows = column_separate_tag_names.size();
        chassert(column_separate_tag_values.size() == num_rows);
        chassert(out_tags_vector.size() == num_rows);

        std::vector<std::string_view> tag_names = extractStringsFromColumn(column_separate_tag_names);
        std::vector<std::string_view> tag_values = extractStringsFromColumn(column_separate_tag_values);
        for (size_t i = 0; i != num_rows; ++i)
        {
            auto tag_name = tag_names[i];
            auto tag_value = tag_values[i];
            if (!tag_name.empty() && !tag_value.empty())
                out_tags_vector[i].emplace_back(tag_name, tag_value);
        }
    }

    /// Extracts strings from a column containing either separate tag names or their values.
    std::vector<std::string_view> extractStringsFromColumn(const IColumn & column)
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

    /// Sorts each set of tags and removes duplicate tags in each set of tags.
    void sortTagsAndRemoveDuplicates(std::vector<TagNamesAndValues> & tags_vector)
    {
        auto less_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first < right.first;
        };
        for (auto & tags : tags_vector)
        {
            std::sort(tags.begin(), tags.end(), less_by_tag_name);
            tags.erase(std::unique(tags.begin(), tags.end()), tags.end());
        }
    }

    /// Checks that our sorted set of tags is ok to store in the query context.
    void checkTags(const std::vector<TagNamesAndValues> & tags_vector)
    {
        auto empty_name_or_value = [](const std::pair<String, String> & x)
        {
            return x.first.empty() || x.second.empty();
        };
        auto equal_by_tag_name = [](const std::pair<String, String> & left, const std::pair<String, String> & right)
        {
            return left.first == right.first;
        };
        for (const auto & tags : tags_vector)
        {
            auto with_empty_name_or_value = std::find_if(tags.begin(), tags.end(), empty_name_or_value);
            if (with_empty_name_or_value != tags.end())
            {
                throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
                    "Found tag with empty name {} or value {} while executing function {}",
                    quoteString(with_empty_name_or_value->first), quoteString(with_empty_name_or_value->second), name);
            }
            auto adjacent = std::adjacent_find(tags.begin(), tags.end(), equal_by_tag_name);
            if (adjacent != tags.end())
            {
                throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
                    "Found two tags with the same name {} but different values {} and {} while executing function {}",
                    quoteString(adjacent->first), quoteString(adjacent->second), quoteString(std::next(adjacent)->second), name);
            }
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
    size_t array_of_tuples_argument_index,
    size_t name_value_pairs_start_argument_index)
{
    if (arguments.size() < 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with at least 1 arguments", name);

    if ((arguments.size() % 2) != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with odd number of arguments", name);

    checkDataTypeOfTagsArray(arguments[1].type, 1);

    for (size_t i = 2; i < arguments.size(); i += 2)
    {
        checkDataTypeOfSeparateTagNames(arguments[i].type, i);
        checkDataTypeOfSeparateTagValues(arguments[i + 1].type, i + 1);
    }
}

std::vector<TagNamesAndValuesPtr> extractTagNamesAndValuesFromArguments(
        const ColumnsWithTypeAndName & arguments,
        size_t array_of_tuples_argument_index,
        size_t name_value_pairs_start_argument_index)
{
    chassert((arguments.size() >= 1) && ((arguments.size() % 2) == 1));

    std::vector<TagNamesAndValues> tags_vector;
    tags_vector.resize(input_rows_count);

    const auto & column_tags_array = arguments[0].column;
    extractTagsFromTagsArrayColumn(tags_vector, *column_tags_array, 0);

    for (size_t i = 1; i != arguments.size(); i += 2)
    {
        const auto & column_separate_tag_names = arguments[i].column;
        const auto & column_separate_tag_values = arguments[i + 1].column;
        extractTagsFromSeparateTagNamesAndTagValuesColumns(tags_vector, *column_separate_tag_names, *column_separate_tag_values);
    }

    sortTagsAndRemoveDuplicates(tags_vector);
    checkTags(tags_vector);

    return makeTagsPtrVector(std::move(tags_vector));
}



    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with at least 1 arguments", name);

        if ((arguments.size() % 2) != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with odd number of arguments", name);

        checkDataTypeOfTagsArray(arguments[1].type, 1);

        for (size_t i = 2; i < arguments.size(); i += 2)
        {
            checkDataTypeOfSeparateTagNames(arguments[i].type, i);
            checkDataTypeOfSeparateTagValues(arguments[i + 1].type, i + 1);
        }
    }




/// Converts a vector of tags to a result column.
ColumnPtr tagsGroupsToColumn(const std::vector<Group> & groups)
{
    auto res = ColumnUInt64::create();
    res->reserve(groups.size());
    for (auto group : groups)
        res->insertValue(group);
    return res;
}

}
