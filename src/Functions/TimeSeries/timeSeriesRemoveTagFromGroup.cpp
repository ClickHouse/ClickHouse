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

#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function FunctionTimeSeriesRemoveTagFromGroup(<group>, '<tag_name>') removes a specified tag from a tags group,
/// and returns the new tags group. The name of the specified tag is const.
class FunctionTimeSeriesRemoveTagFromGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesRemoveTagFromGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesRemoveTagFromGroup>(context_); }
    explicit FunctionTimeSeriesRemoveTagFromGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    /// Function timeSeriesTagsGroupToTags(<group>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with one argument", name);

        //checkDataTypeOfGroup(arguments[0].type, 0);
        
        return std::make_shared<DataTypeUInt64>();
    }

    /*
    static void checkDataTypeOfGroup(const DataTypePtr & data_type, size_t argument_index)
    {
        if (isUInt64(data_type))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "UInt64");
    }
    */

    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 2);
        auto old_groups = extractGroups(*arguments[0].column, 0);
        String tag_to_remove = extractTagName(*arguments[1].column, 1);

        auto new_groups = getContext()->getQueryContext()->getTimeSeriesTagsCollector().removeTagFromGroup(old_groups, tag_to_remove);
        chassert(new_groups.size() == input_rows_count);

        return makeResultColumn(new_groups);
    }

    /// Extracts groups from the column.
    static std::vector<Group> extractGroups(const IColumn & column_groups, size_t argument_index)
    {
        /// Group must be UInt64.
        if (checkColumn<ColumnUInt64>(&column_groups))
        {
            std::string_view data = column_groups.getRawData();
            chassert(data.size() == column_groups.size() * sizeof(UInt64));
            const UInt64 * begin = reinterpret_cast<const UInt64 *>(data.data());
            return std::vector<Group>(begin, begin + column_groups.size());
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_groups.convertToFullIfNeeded(); full_column.get() != &column_groups)
        {
            return extractGroups(*full_column, argument_index);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_groups.getName(), argument_index + 1, name, "UInt64");
    }

    static String extractTagName(const IColumn & column_tag_name, size_t argument_index)
    {
        if (!checkColumnConst<ColumnString>(&column_tag_name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant string", argument_index + 1, name);

        return String{column_tag_name.getDataAt(0)};
    }

    /// Converts a vector of tags to a result column.
    static ColumnPtr makeResultColumn(const std::vector<Group> & groups)
    {
        auto res = ColumnUInt64::create();
        res->reserve(groups.size());
        for (auto group : groups)
            res->insertValue(group);
        return res;
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveTagFromGroup)
{
    FunctionDocumentation::Description description = R"(Finds tags associated with a group index. Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTagFromGroup(group, 'tag_name')";
    FunctionDocumentation::Arguments arguments = {{"group", "Group index associated with a time series.", {"UInt64"}},
                                                  {"tag_to_remove", "The name of a tag to remove from the group.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The result tags group.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesRemoveTagFromGroup(group, '__name__') AS group1, timeSeriesTagsGroupToTags(group1)", "8374283493092    0    1    [('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTagFromGroup>(documentation);
}

}
