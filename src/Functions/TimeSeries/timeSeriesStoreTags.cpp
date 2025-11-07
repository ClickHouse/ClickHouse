#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <base/insertAtEnd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesStoreTags(<id>, [('tag1_name', 'tag1_value'), ...], 'tag2_name', 'tag2_value', ...) returns <id>
/// and stores the mapping between the identifier of a time series and its tags in the query context so that
/// they can later be extracted by function timeSeriesIdToTags().
class FunctionTimeSeriesStoreTags : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesStoreTags";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesStoreTags>(context_); }
    explicit FunctionTimeSeriesStoreTags(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    /// There should be 2 or more arguments.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Function timeSeriesStoreTags(<id>, ...) always returns <id>, so it's deterministic.
    bool isDeterministic() const override { return true; }

    /// This function allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with at least 2 arguments", name);

        if ((arguments.size() % 2) != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with even number of arguments", name);

        checkDataTypeOfID(arguments[0].type, 0);
        checkDataTypeOfTagsArray(arguments[1].type, 1);

        for (size_t i = 2; i < arguments.size(); i += 2)
        {
            checkDataTypeOfSeparateTagNames(arguments[i].type, i);
            checkDataTypeOfSeparateTagValues(arguments[i + 1].type, i + 1);
        }

        return arguments[0].type;
    }

    static void checkDataTypeOfID(const DataTypePtr & data_type, size_t argument_index)
    {
        auto type = removeNullableOrLowCardinalityNullable(data_type);
        if (isUInt64(type) || isUInt128(type) || isUUID(type) || isNothing(type))
            return;
        if (const auto * fixed_string_data_type = typeid_cast<const DataTypeFixedString *>(type.get());
            fixed_string_data_type && (fixed_string_data_type->getN() == 16))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "UInt64 or UInt128 or UUID");
    }

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

    using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert((arguments.size() >= 2) && ((arguments.size() % 2) == 0));
        const auto & column_ids = arguments[0].column;

        std::vector<TagNamesAndValues> tags_vector;
        tags_vector.resize(input_rows_count);

        const auto & column_tags_array = arguments[1].column;
        extractTagsFromTagsArrayColumn(tags_vector, *column_tags_array, 1);

        for (size_t i = 2; i != arguments.size(); i += 2)
        {
            const auto & column_separate_tag_names = arguments[i].column;
            const auto & column_separate_tag_values = arguments[i + 1].column;
            extractTagsFromSeparateTagNamesAndTagValuesColumns(tags_vector, *column_separate_tag_names, *column_separate_tag_values);
        }

        sortTagsAndRemoveDuplicates(tags_vector);
        checkTags(tags_vector);

        extractIDsAndStoreTags(*column_ids, 0, std::move(tags_vector));

        return column_ids;
    }

    /// Converts the second argument of type Array(Tuple(String, String)) containing pairs {tag_name, tag_value} to full if it's necessary to extract tags from it.
    static ColumnPtr convertTagsArrayColumnToFullIfNeeded(ColumnPtr column_tags_array)
    {
        /// TagNamesAndValuesSV returned by extractTagsFromTagsArrayColumn() uses references to tag names and values, so it's necessary
        /// to convert the array column to full in the scope of executeImpl() and not in the scope of extractTagsFromTagsArrayColumn().
        if (checkColumn<ColumnConst>(column_tags_array.get()) || checkColumn<ColumnLowCardinality>(column_tags_array.get()) || checkColumn<ColumnSparse>(column_tags_array.get()))
        {
            return column_tags_array->convertToFullIfNeeded();
        }
        return column_tags_array;
    }

    /// Extracts tags from the second argument of type Array(Tuple(String, String)) containing pairs {tag_name, tag_value}.
    static void extractTagsFromTagsArrayColumn(std::vector<TagNamesAndValues> & out_tags_vector, const IColumn & column_tags_array, size_t argument_index)
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
    static void extractTagsFromSeparateTagNamesAndTagValuesColumns(std::vector<TagNamesAndValues> & out_tags_vector,
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
    static std::vector<std::string_view> extractStringsFromColumn(const IColumn & column)
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
    static void sortTagsAndRemoveDuplicates(std::vector<TagNamesAndValues> & tags_vector)
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
    static void checkTags(const std::vector<TagNamesAndValues> & tags_vector)
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

    /// Extracts identifiers from the first argument and stores the matches between these identifiers and specified tags in the query context.
    void extractIDsAndStoreTags(const IColumn & column_ids, size_t argument_index, std::vector<TagNamesAndValues> && tags_vector) const
    {
        size_t num_rows = column_ids.size();
        chassert(tags_vector.size() == num_rows);

        /// Identifier can be UInt64.
        if (checkColumn<ColumnUInt64>(&column_ids))
        {
            extractIDsAndStoreTagsImpl<UInt64>(column_ids, std::move(tags_vector));
            return;
        }

        /// Identifier can be UInt128 or UUID.
        if (checkColumn<ColumnUInt128>(&column_ids) || checkColumn<ColumnUUID>(&column_ids))
        {
            /// We can handle UUID as UInt128 as they have the same size.
            extractIDsAndStoreTagsImpl<UInt128>(column_ids, std::move(tags_vector));
            return;
        }

        /// Identifier can be FixedString(16).
        if (const auto * fixed_string_column = checkAndGetColumn<ColumnFixedString>(&column_ids);
                 fixed_string_column && (fixed_string_column->getN() == 16))
        {
            /// We can handle FixedString(16) as UInt128 as they have the same size.
            extractIDsAndStoreTagsImpl<UInt128>(column_ids, std::move(tags_vector));
            return;
        }

        /// The argument can be nullable.
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&column_ids))
        {
            const auto & null_map = nullable_column->getNullMapData();
            const auto & nested_column = nullable_column->getNestedColumn();
            std::vector<TagNamesAndValues> filtered_tags_vector;
            auto filtered_column_ids = nested_column.cloneEmpty();
            filtered_tags_vector.reserve(num_rows);
            filtered_column_ids->reserve(num_rows);
            for (size_t i = 0; i != num_rows; ++i)
            {
                if (!null_map[i])
                {
                    filtered_tags_vector.emplace_back(std::move(tags_vector[i]));
                    filtered_column_ids->insertFrom(nested_column, i);
                }
            }
            extractIDsAndStoreTags(*filtered_column_ids, argument_index, std::move(filtered_tags_vector));
            return;
        }

        /// The argument can be literal NULL.
        if (checkColumn<ColumnNothing>(&column_ids))
            return;

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_ids.convertToFullIfNeeded(); full_column.get() != &column_ids)
        {
            extractIDsAndStoreTags(*full_column, argument_index, std::move(tags_vector));
            return;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_ids.getName(), argument_index + 1, name, "UInt64 or UInt128 or UUID");
    }

    template <typename IDType>
    void extractIDsAndStoreTagsImpl(const IColumn & column_ids, std::vector<TagNamesAndValues> && tags_vector) const
    {
        auto ids = extractIDs<IDType>(column_ids);
        getContext()->getQueryContext()->getTimeSeriesTagsCollector().add(ids, makeTagsPtrVector(std::move(tags_vector)));
    }

    /// Extracts identifiers from the column.
    template <typename IDType>
    static std::vector<IDType> extractIDs(const IColumn & column_ids)
    {
        std::string_view data = column_ids.getRawData();
        chassert(data.size() == column_ids.size() * sizeof(IDType));  /// NOLINT(bugprone-sizeof-expression,cert-arr39-c)
        const IDType * begin = reinterpret_cast<const IDType *>(data.data());
        return std::vector<IDType>(begin, begin + column_ids.size());
    }

    /// Wrap TagNamesAndValues in shared pointers.
    static std::vector<TagNamesAndValuesPtr> makeTagsPtrVector(std::vector<TagNamesAndValues> && tags_vector)
    {
        std::vector<TagNamesAndValuesPtr> res;
        res.reserve(tags_vector.size());
        for (auto & tags : tags_vector)
        {
            res.emplace_back(std::make_shared<TagNamesAndValues>(std::move(tags)));
        }
        return res;
    }
};


REGISTER_FUNCTION(TimeSeriesStoreTags)
{
    FunctionDocumentation::Description description = R"(Stores mapping between the identifier of a time series and its tags in the query context, so that function timeSeriesIdToTags() can extract these tags later.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesStoreTags(id, tags_array, separate_tag_name_1, separate_tag_value_1, ...)";
    FunctionDocumentation::Arguments arguments = {{"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}},
                                                  {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
                                                  {"separate_tag_name_i", "The name of a tag.", {"String", "FixedString"}},
                                                  {"separate_tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the first argument, i.e. the identifier of a time series."};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count')", "8374283493092"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesStoreTags>(documentation);
}

}
