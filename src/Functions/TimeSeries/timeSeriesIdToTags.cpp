#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
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

/// Function timeSeriesIdToTags(<id>) returns Array(Tuple(String, String)) containing the names and values of tags associated with
/// a specified identifier <id>.
class FunctionTimeSeriesIdToTags : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesIdToTags";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesIdToTags>(context_); }
    explicit FunctionTimeSeriesIdToTags(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesIdToTags(<id>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with one argument", name);

        checkDataTypeOfID(arguments[0].type, 0);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
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

        auto tags_vector = getTags(*column_ids, 0);
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
    std::vector<TagNamesAndValuesPtr> getTags(const IColumn & column_ids, size_t argument_index) const
    {
        /// Identifier can be UInt64.
        if (checkColumn<ColumnUInt64>(&column_ids))
        {
            return getTagsImpl<UInt64>(column_ids);
        }

        /// Identifier can be UInt128 or UUID.
        if (checkColumn<ColumnUInt128>(&column_ids) || checkColumn<ColumnUUID>(&column_ids))
        {
            /// We can handle UUID as UInt128 as they have the same size.
            return getTagsImpl<UInt128>(column_ids);
        }

        /// Identifier can be FixedString(16).
        if (const auto * fixed_string_column = checkAndGetColumn<ColumnFixedString>(&column_ids);
                 fixed_string_column && (fixed_string_column->getN() == 16))
        {
            /// We can handle FixedString(16) as UInt128 as they have the same size.
            return getTagsImpl<UInt128>(column_ids);
        }

        /// The argument can be wrapped in ColumnConst or ColumnLowCardinality.
        if (auto full_column = column_ids.convertToFullIfNeeded(); full_column.get() != &column_ids)
        {
            return getTags(*full_column, argument_index);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument #{} of function {}, it must be {}",
            column_ids.getName(), argument_index + 1, name, "UInt64 or UInt128 or UUID");
    }

    template <typename IDType>
    std::vector<TagNamesAndValuesPtr> getTagsImpl(const IColumn & column_ids) const
    {
        auto ids = extractIDs<IDType>(column_ids);
        return getContext()->getQueryContext()->getTimeSeriesTagsCollector().getTagsById(ids);
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


REGISTER_FUNCTION(TimeSeriesIdToTags)
{
    FunctionDocumentation::Description description = R"(Finds tags associated with the specified identifier of a time series.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesIdToTags(id)";
    FunctionDocumentation::Arguments arguments = {{"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTags(id)", "8374283493092    [('__name__', ''http_requests_count''), ('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesIdToTags>(documentation);
}

}
