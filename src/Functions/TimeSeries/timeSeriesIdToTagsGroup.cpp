#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnFixedString.h>
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

/// Function timeSeriesIdToTagsGroup(<id>) converts the specified identifier of a time series to its group index.
/// Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.
class FunctionTimeSeriesIdToTagsGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesIdToTagsGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesIdToTagsGroup>(context_); }
    explicit FunctionTimeSeriesIdToTagsGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesIdToTagsGroup(<id>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with one argument", name);

        checkDataTypeOfID(arguments[0].type, 0);

        return std::make_shared<DataTypeUInt64>();
    }

    static void checkDataTypeOfID(const DataTypePtr & data_type, size_t argument_index)
    {
        if (isUInt64(data_type) || isUInt128(data_type) || isUUID(data_type))
            return;
        if (const auto * fixed_string_data_type = typeid_cast<const DataTypeFixedString *>(data_type.get());
            fixed_string_data_type && (fixed_string_data_type->getN() == 16))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                        argument_index + 1, name, data_type, "UInt64 or UInt128 or UUID");
    }

    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 1);
        const auto & column_ids = arguments[0].column;

        auto groups = getGroups(*column_ids, 0);
        chassert(groups.size() == input_rows_count);

        return makeResultColumn(groups);
    }

    /// Converts a vector of tags to a result column.
    static ColumnPtr makeResultColumn(const std::vector<size_t> & groups)
    {
        auto res = ColumnUInt64::create();
        res->reserve(groups.size());
        for (auto group : groups)
            res->insertValue(group);
        return res;
    }

    /// Gets the names and values of the tags associated with specified identifiers.
    std::vector<size_t> getGroups(const IColumn & column_ids, size_t argument_index) const
    {
        /// Identifier can be UInt64.
        if (checkColumn<ColumnUInt64>(&column_ids))
        {
            return getGroupsImpl<UInt64>(column_ids);
        }

        /// Identifier can be UInt128 or UUID.
        if (checkColumn<ColumnUInt128>(&column_ids) || checkColumn<ColumnUUID>(&column_ids))
        {
            /// We can handle UUID as UInt128 as they have the same size.
            return getGroupsImpl<UInt128>(column_ids);
        }

        /// Identifier can be FixedString(16).
        if (const auto * fixed_string_column = checkAndGetColumn<ColumnFixedString>(&column_ids);
                 fixed_string_column && (fixed_string_column->getN() == 16))
        {
            /// We can handle FixedString(16) as UInt128 as they have the same size.
            return getGroupsImpl<UInt128>(column_ids);
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_ids.convertToFullIfNeeded(); full_column.get() != &column_ids)
        {
            return getGroups(*full_column, argument_index);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_ids.getName(), argument_index + 1, name, "UInt64 or UInt128 or UUID");
    }

    template <typename IDType>
    std::vector<size_t> getGroupsImpl(const IColumn & column_ids) const
    {
        auto ids = extractIDs<IDType>(column_ids);
        return getContext()->getQueryContext()->getTimeSeriesTagsCollector().getGroupById(ids);
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
};


REGISTER_FUNCTION(TimeSeriesIdToTagsGroup)
{
    FunctionDocumentation::Description description = R"(Converts the specified identifier of a time series to its group index. Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesIdToTagsGroup(id)";
    FunctionDocumentation::Arguments arguments = {{"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a group index associated with this set of tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id)", "8374283493092    0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesIdToTagsGroup>(documentation);
}

}
