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

/// Function FunctionTimeSeriesCopyTagsToGroup(<dest_group>, <src_group>, ['<tag_name1>', '<tag_name2>', ...])
/// copies specified tags from the `src` group to the `dest` group, and returns the new tags group.
class FunctionTimeSeriesCopyTagsToGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesCopyTagsToGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesCopyTagsToGroup>(context_); }
    explicit FunctionTimeSeriesCopyTagsToGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    /// Function timeSeriesCopyTagsToGroup() uses the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with two arguments", name);

        if (!isUInt64(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            1, name, arguments[1].type, "UInt64");

        if (!isUInt64(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            2, name, arguments[1].type, "UInt64");

        if (!isArray(arguments[2].type) || !isString(typeid_cast<const DataTypeArray &>(*arguments[2].type).getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            3, name, arguments[0].type, "Array(String)");
    }

    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 3);
        auto dest_groups = extractGroups(*arguments[0].column, 0);
        auto src_groups = extractGroups(*arguments[1].column, 1);
        Strings tags_to_copy = extractTagNames(*arguments[2].column, 2);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        std::vector<Group> new_groups;

        if ((dest_groups.size() == 1) && (src_groups.size() == 1))
            new_groups.push_back(tags_collector.copyTagsToGroup(dest_groups[0], src_groups[0], tags_to_copy));
        else if (dest_groups.size() == 1)
            new_groups = tags_collector.copyTagsToGroup(dest_groups[0], src_groups, tags_to_copy);
        else if (src_groups.size() == 1)
            new_groups = tags_collector.copyTagsToGroup(dest_groups, src_groups[0], tags_to_copy);
        else
            new_groups = tags_collector.copyTagsToGroup(dest_groups, src_groups, tags_to_copy);

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


REGISTER_FUNCTION(TimeSeriesCopyTagsToGroup)
{
    FunctionDocumentation::Description description = R"(Copies specified tags from the 'src' group to the 'dest' group.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesCopyTagsToGroup(dest_group, src_group, tags_to_copy)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tags_to_copy", "The names of tags to copy.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group with copied tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id1, timeSeriesStoreTags(524344498142, [('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS id2, timeSeriesIdToTagsGroup(id1) AS group1, timeSeriesIdToTagsGroup(id2) AS group2, timeSeriesCopyTagsToGroup(group1, group2, ['__name__', 'code', 'env']) AS res, timeSeriesTagsGroupToTags(res)", "8374283493092    524344498142    0    1    2    [('__name__','http_codes'),('code','404'),('region','eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesCopyTagsToGroup>(documentation);
}

}
