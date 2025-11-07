#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesIdToTags(<group>) returns Array(Tuple(String, String)) containing the names and values of tags associated with
/// a specified group.
class FunctionTimeSeriesTagsGroupToTags : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesTagsGroupToTags";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesTagsGroupToTags>(context_); }
    explicit FunctionTimeSeriesTagsGroupToTags(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesTagsGroupToTags(<group>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with one argument", name);

        checkDataTypeOfGroup(arguments[0].type, 0);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkDataTypeOfGroup(const DataTypePtr & data_type, size_t argument_index)
    {
        if (isUInt64(data_type))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "UInt64");
    }

    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 1);
        const auto & column_groups = arguments[0].column;

        auto tags_vector = getTags(*column_groups, 0);
        chassert(tags_vector.size() == input_rows_count);

        return makeResultColumn(tags_vector);
    }

    /// Converts a vector of tags to a result column.
    static ColumnPtr makeResultColumn(const std::vector<TagNamesAndValuesPtr> & tags_vector)
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

    /// Gets the names and values of the tags associated with specified identifiers.
    std::vector<TagNamesAndValuesPtr> getTags(const IColumn & column_groups, size_t argument_index) const
    {
        /// Group must be UInt64.
        if (checkColumn<ColumnUInt64>(&column_groups))
        {
            auto groups = extractGroups(column_groups);
            return getContext()->getQueryContext()->getTimeSeriesTagsCollector().getTagsByGroup(groups);
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_groups.convertToFullIfNeeded(); full_column.get() != &column_groups)
        {
            return getTags(*full_column, argument_index);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_groups.getName(), argument_index + 1, name, "UInt64");
    }

    /// Extracts groups from the column.
    static std::vector<size_t> extractGroups(const IColumn & column_groups)
    {
        std::string_view data = column_groups.getRawData();
        chassert(data.size() == column_groups.size() * sizeof(UInt64));  /// NOLINT(bugprone-sizeof-expression,cert-arr39-c)
        const UInt64 * begin = reinterpret_cast<const UInt64 *>(data.data());
        return std::vector<size_t>(begin, begin + column_groups.size());
    }
};


REGISTER_FUNCTION(TimeSeriesTagsGroupToTags)
{
    FunctionDocumentation::Description description = R"(Finds tags associated with a group index. Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesTagsGroupToTags(group)";
    FunctionDocumentation::Arguments arguments = {{"group", "Group index associated with a time series.", {"UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesTagsGroupToTags(group)", "8374283493092    0    [('__name__', ''http_requests_count''), ('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesTagsGroupToTags>(documentation);
}

}
