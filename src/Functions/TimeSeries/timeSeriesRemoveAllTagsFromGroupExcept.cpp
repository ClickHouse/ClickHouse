#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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

/// Function FunctionTimeSeriesRemoveAllTagsFromGroupExcept(<group>, ['<tag_name1>', '<tag_name2>', ...])
/// removes all tags from a tags group except specified ones, and returns the new tags group.
class FunctionTimeSeriesRemoveAllTagsFromGroupExcept : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesRemoveAllTagsFromGroupExcept";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesRemoveAllTagsFromGroupExcept>(context_); }
    explicit FunctionTimeSeriesRemoveAllTagsFromGroupExcept(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveAllTagsFromGroupExcept() uses the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkDataTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkDataTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with two arguments", name);

        if (!isUInt64(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            1, name, arguments[1].type, "UInt64");

        if (!isArray(arguments[1].type) || !isString(typeid_cast<const DataTypeArray &>(*arguments[1].type).getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            2, name, arguments[0].type, "Array(String)");
    }

    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 2);
        auto old_groups = extractGroups(*arguments[0].column, 0);
        Strings tags_to_keep = extractTagNames(*arguments[1].column, 1);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        std::vector<Group> new_groups;
    
        if (old_groups.size() == 1)
            new_groups.push_back(tags_collector.removeAllTagsFromGroupExcept(old_groups[0], tags_to_keep));
        else
            new_groups = tags_collector.removeAllTagsFromGroupExcept(old_groups, tags_to_keep);

        return makeResultColumn(new_groups, input_rows_count);
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

        /// The argument can be wrapped in ColumnConst.
        if (const auto * const_column = checkAndGetColumnConstData<ColumnUInt64>(&column_groups))
        {
            return extractGroups(*const_column, argument_index);
        }

        /// The argument can be wrapped in ColumnLowCardinality.
        if (auto full_column = column_groups.convertToFullIfNeeded(); full_column.get() != &column_groups)
        {
            return extractGroups(*full_column, argument_index);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_groups.getName(), argument_index + 1, name, "UInt64");
    }

    static Strings extractTagNames(const IColumn & column_tag_names, size_t argument_index)
    {
        const auto * array_column = checkAndGetColumnConstData<ColumnArray>(&column_tag_names);
        if (!array_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant Array(String)", argument_index + 1, name);

        size_t count = array_column->getOffsets()[0];
        const auto * string_column = checkAndGetColumn<ColumnString>(&array_column->getData());
        if (!string_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant Array(String)", argument_index + 1, name);

        Strings tag_names;
        tag_names.reserve(count);
        for (size_t i = 0; i != count; ++i)
            tag_names.emplace_back(String{string_column->getDataAt(i)});

        return tag_names;
    }

    /// Converts a vector of tags to a result column.
    static ColumnPtr makeResultColumn(const std::vector<Group> & groups, size_t output_rows_count)
    {
        auto res = ColumnUInt64::create();
        res->reserve(groups.size());
        for (auto group : groups)
            res->insertValue(group);

        if (output_rows_count != groups.size())
            return ColumnConst::create(std::move(res), output_rows_count);
        else
            return res;
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveAllTagsFromGroupExcept)
{
    FunctionDocumentation::Description description = R"(Removes all tags from a tags group except specified ones.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveAllTagsFromGroupExcept(group, tags_to_keep)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tags_to_keep", "The names of tags to keep in the group.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group with only the specified tags kept.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesRemoveAllTagsFromGroupExcept(group, ['env']) AS group1, timeSeriesTagsGroupToTags(group1", "8374283493092    0    1    [('env','dev')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveAllTagsFromGroupExcept>(documentation);
}

}
