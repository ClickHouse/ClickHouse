#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>

#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct GroupFormatData
{
    MutableColumns columns;
};

UInt64 getSerializationProtocolVersion(const ContextPtr & context)
{
    const auto & client_info = context->getClientInfo();
    if (client_info.connection_tcp_protocol_version)
        return client_info.connection_tcp_protocol_version;
    if (client_info.client_tcp_protocol_version)
        return client_info.client_tcp_protocol_version;
    return DBMS_TCP_PROTOCOL_VERSION;
}

/// The context of the query this thread currently executes, or the global context.
/// The function object must not capture the context itself: it is embedded into
/// `DataTypeAggregateFunction` for `AggregateFunction(groupFormat(...), ...)` columns,
/// and the data type would outlive the query that created it, so a captured `ContextPtr`
/// would keep that query's context alive. The format settings are captured separately as
/// a plain value (see the factory function).
ContextPtr resolveFormatContext()
{
    ContextPtr context;
    if (CurrentThread::isInitialized())
        context = CurrentThread::get().tryGetQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context is not initialized");
    return context;
}

class AggregateFunctionGroupFormat final : public IAggregateFunctionDataHelper<GroupFormatData, AggregateFunctionGroupFormat>
{
public:
    AggregateFunctionGroupFormat(
        const DataTypes & argument_types_,
        const Array & parameters_,
        String format_name_,
        UInt64 serialization_protocol_version_,
        FormatSettings format_settings_)
        : IAggregateFunctionDataHelper<GroupFormatData, AggregateFunctionGroupFormat>(
              argument_types_, parameters_, std::make_shared<DataTypeString>())
        , format_name(std::move(format_name_))
        , format_settings(std::move(format_settings_))
        , serialization_protocol_version(serialization_protocol_version_)
    {
        size_t num_columns = argument_types.size();
        for (size_t i = 0; i < num_columns; ++i)
        {
            String name = "c" + std::to_string(i + 1);
            header.insert({argument_types[i], name});
        }
    }

    String getName() const override { return "groupFormat"; }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) GroupFormatData;
        auto & state = data(place);
        state.columns.reserve(argument_types.size());
        for (const auto & type : argument_types)
            state.columns.emplace_back(type->createColumn());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = data(place);
        for (size_t i = 0; i < state.columns.size(); ++i)
            state.columns[i]->insertFrom(*columns[i], row_num);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        chassert(row_begin <= row_end);
        if (row_begin == row_end)
            return;

        auto & state = data(place);

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t row = row_begin; row < row_end; ++row)
            {
                if (flags[row])
                {
                    for (size_t col = 0; col < state.columns.size(); ++col)
                        state.columns[col]->insertFrom(*columns[col], row);
                }
            }
            return;
        }

        const size_t length = row_end - row_begin;
        for (size_t col = 0; col < state.columns.size(); ++col)
            state.columns[col]->insertRangeFrom(*columns[col], row_begin, length);
    }

    void addBatchSparseSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena) const override
    {
        /// `groupFormat` is order-dependent, so rows must be processed in input order.
        /// The inherited implementation appends all non-default sparse values first and
        /// then the default rows via `addManyDefaults`, which would reorder the output.
        /// Iterate row-by-row instead, mirroring the generic `addBatchSparse` path.
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        auto offset_it = column_sparse.getIterator(row_begin);

        for (size_t row = row_begin; row < row_end; ++row, ++offset_it)
            add(place, &values, offset_it.getValueIndex(), arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = data(place);
        const auto & rhs_state = data(rhs);
        for (size_t i = 0; i < state.columns.size(); ++i)
            state.columns[i]->insertRangeFrom(*rhs_state.columns[i], 0, rhs_state.columns[i]->size());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & state = data(place);
        UInt64 num_columns = state.columns.size();
        UInt64 num_rows = num_columns ? state.columns.front()->size() : 0;

        writeVarUInt(num_columns, buf);
        writeVarUInt(num_rows, buf);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto & column = state.columns[i];
            const auto & type = argument_types[i];
            auto serialization = type->getDefaultSerialization();
            NativeWriter::writeData(*serialization, column->getPtr(), buf, std::nullopt, 0, num_rows, serialization_protocol_version);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & state = data(place);
        UInt64 num_columns = 0;
        UInt64 num_rows = 0;
        readVarUInt(num_columns, buf);
        readVarUInt(num_rows, buf);

        if (num_columns != argument_types.size())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "State of aggregate function {} contains {} columns but {} columns are expected",
                getName(),
                num_columns,
                argument_types.size());

        state.columns.clear();
        state.columns.reserve(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto & type = argument_types[i];
            auto serialization = type->getDefaultSerialization();
            ColumnPtr column = type->createColumn();
            NativeReader::readData(*serialization, column, buf, nullptr, num_rows, nullptr, nullptr);
            state.columns.emplace_back(column->assumeMutable());
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = data(place);
        auto & column_string = assert_cast<ColumnString &>(to);

        ColumnString::Chars & chars = column_string.getChars();
        ColumnString::Offsets & offsets = column_string.getOffsets();
        const size_t old_size = chars.size();

        WriteBufferFromVector<ColumnString::Chars> buffer(chars, AppendModeTag{});

        /// The format settings were captured once at creation time (see the factory
        /// function). The context is resolved on demand only because `getOutputFormat`
        /// requires one - it must not be stored in the function object.
        auto output = FormatFactory::instance().getOutputFormat(format_name, buffer, header, resolveFormatContext(), format_settings);

        if (!state.columns.empty())
        {
            Columns columns;
            columns.reserve(state.columns.size());
            for (const auto & column : state.columns)
                columns.emplace_back(column->getPtr());

            auto block = header.cloneWithColumns(columns);
            output->write(block);
        }

        output->finalize();
        buffer.finalize();
        offsets.push_back(old_size + buffer.count());
    }

private:
    String format_name;
    Block header;
    FormatSettings format_settings;
    UInt64 serialization_protocol_version = DBMS_TCP_PROTOCOL_VERSION;
};

AggregateFunctionPtr createAggregateFunctionGroupFormat(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * /* settings */)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one parameter: format name", name);

    if (parameters[0].getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First parameter for aggregate function {} should be a string format name", name);

    auto format_name = parameters[0].safeGet<String>();
    FormatFactory::instance().checkFormatName(format_name);

    /// The serialization protocol version and the format settings are captured at creation
    /// time. States are serialized within the query that creates them (e.g. when partial
    /// aggregation states are sent between servers of a cluster with different versions),
    /// and the format settings are taken from that query as well. The context itself is not
    /// captured: the function object is embedded into `DataTypeAggregateFunction` and can
    /// outlive the query that created it, so holding a `ContextPtr` would keep that query's
    /// context alive.
    auto context = resolveFormatContext();
    UInt64 serialization_protocol_version = getSerializationProtocolVersion(context);

    auto format_settings = getFormatSettings(context);
    format_settings.json.valid_output_on_exception = false;
    format_settings.xml.valid_output_on_exception = false;
    /// The nested formatter runs inside the live query context. Without this, formats such
    /// as `JSON` and `XML` would emit a `statistics` section whose `rows_read` / `bytes_read`
    /// come from the enclosing query rather than from the group being formatted, putting
    /// misleading metadata into the resulting string.
    format_settings.write_statistics = false;

    return std::make_shared<AggregateFunctionGroupFormat>(
        argument_types, parameters, std::move(format_name), serialization_protocol_version, std::move(format_settings));
}

}

void registerAggregateFunctionGroupFormat(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupFormat(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description = R"(
Formats the rows in each group using the specified output format and returns the result as a string.

The format name is passed as a parameter, and the arguments are the columns to format.
Column names are generated as c1, c2, ... in the formatted output.
)";
    FunctionDocumentation::Syntax syntax = "groupFormat(format)(x, y, ...)";
    FunctionDocumentation::Parameters parameters
        = {{"format", "Output format name. For example, JSONEachRow, CSV, TabSeparated.", {"String"}}};
    FunctionDocumentation::Arguments arguments = {{"x, y, ...", "Expressions to format as rows.", {"Any"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Formatted output for the group.", {"String"}};
    FunctionDocumentation::Examples examples
        = {{"Basic usage",
            R"(
SELECT groupFormat('JSONEachRow')(number, toString(number))
FROM numbers(3)
            )",
            R"(
{"c1":0,"c2":"0"}
{"c1":1,"c2":"1"}
{"c1":2,"c2":"2"}
            )"}};
    FunctionDocumentation::IntroducedIn introduced_in{};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("groupFormat", {createAggregateFunctionGroupFormat, documentation, properties});
}

}
