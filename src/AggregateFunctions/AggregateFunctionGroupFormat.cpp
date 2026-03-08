#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>

#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>


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
    if (!context)
        return DBMS_TCP_PROTOCOL_VERSION;

    const auto & client_info = context->getClientInfo();
    if (client_info.connection_tcp_protocol_version)
        return client_info.connection_tcp_protocol_version;
    if (client_info.client_tcp_protocol_version)
        return client_info.client_tcp_protocol_version;
    return DBMS_TCP_PROTOCOL_VERSION;
}

class AggregateFunctionGroupFormat final : public IAggregateFunctionDataHelper<GroupFormatData, AggregateFunctionGroupFormat>
{
public:
    AggregateFunctionGroupFormat(
        const DataTypes & argument_types_,
        const Array & parameters_,
        String format_name_,
        FormatSettings format_settings_,
        ContextPtr context_)
        : IAggregateFunctionDataHelper<GroupFormatData, AggregateFunctionGroupFormat>(
              argument_types_, parameters_, std::make_shared<DataTypeString>())
        , format_name(std::move(format_name_))
        , format_settings(std::move(format_settings_))
        , context(std::move(context_))
    {
        serialization_protocol_version = getSerializationProtocolVersion(context);
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
        if (row_begin >= row_end)
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

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 *,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        /// For this aggregate we want to preserve NULL rows too, so just reuse the
        /// regular batch path and ignore the null_map.
        addBatchSinglePlace(row_begin, row_end, place, columns, nullptr, if_argument_pos);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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
        auto output = FormatFactory::instance().getOutputFormat(format_name, buffer, header, context, format_settings);

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

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & /*nested_function*/,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<AggregateFunctionGroupFormat>(arguments, params, format_name, format_settings, context);
    }

private:
    String format_name;
    FormatSettings format_settings;
    ContextPtr context;
    Block header;
    UInt64 serialization_protocol_version = DBMS_TCP_PROTOCOL_VERSION;
};

AggregateFunctionPtr createAggregateFunctionGroupFormat(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
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

    ContextPtr context;
    if (CurrentThread::isInitialized())
        context = CurrentThread::get().getQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context is not initialized");

    const Settings & settings_ref = settings ? *settings : context->getSettingsRef();
    ContextPtr format_context = context;
    if (settings)
    {
        auto context_copy = Context::createCopy(context);
        context_copy->setSettings(settings_ref);
        format_context = std::move(context_copy);
    }

    auto format_settings = getFormatSettings(format_context, settings_ref);
    format_settings.json.valid_output_on_exception = false;
    format_settings.xml.valid_output_on_exception = false;

    return std::make_shared<AggregateFunctionGroupFormat>(
        argument_types, parameters, std::move(format_name), std::move(format_settings), std::move(format_context));
}

}

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

    factory.registerFunction("groupFormat", {createAggregateFunctionGroupFormat, properties, documentation});
}

}
