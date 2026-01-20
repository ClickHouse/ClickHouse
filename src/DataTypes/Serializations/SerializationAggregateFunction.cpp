#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationAggregateFunction.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <absl/container/inlined_vector.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

void SerializationAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const AggregateFunctionStateData & state = field.safeGet<AggregateFunctionStateData>();
    writeBinary(state.data, ostr);
}

void SerializationAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    field = AggregateFunctionStateData();
    AggregateFunctionStateData & s = field.safeGet<AggregateFunctionStateData>();
    readBinary(s.data, istr);
    s.name = type_name;
}

void SerializationAggregateFunction::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], ostr, version);
}

void SerializationAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);
    try
    {
        function->deserialize(place, istr, version, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void SerializationAggregateFunction::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnAggregateFunction & real_column = typeid_cast<const ColumnAggregateFunction &>(column);
    const ColumnAggregateFunction::Container & vec = real_column.getData();

    size_t end = vec.size();
    if (limit)
        end = std::min(end, offset + limit);

    function->serializeBatch(vec, offset, end, ostr, version);
}

void SerializationAggregateFunction::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double /*avg_value_size_hint*/) const
{
    if (rows_offset)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method deserializeBinaryBulk of SerializationAggregateFunction does not support cases where rows_offset {} is non-zero",
                        rows_offset);

    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(column);
    ColumnAggregateFunction::Container & vec = real_column.getData();

    Arena & arena = real_column.createOrGetArena();
    real_column.set(function, version);
    vec.reserve(vec.size() + limit);

    size_t size_of_state = function->sizeOfData();
    size_t align_of_state = function->alignOfData();

    /// Adjust the size of state to make all states aligned in vector.
    size_t total_size_of_state = (size_of_state + align_of_state - 1) / align_of_state * align_of_state;
    char * place = arena.alignedAlloc(total_size_of_state * limit, align_of_state);

    function->createAndDeserializeBatch(vec, place, total_size_of_state, limit, istr, version, &arena);
}

static String serializeToString(const AggregateFunctionPtr & function, const IColumn & column, size_t row_num, size_t version)
{
    WriteBufferFromOwnString buffer;
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], buffer, version);
    return buffer.str();
}

static void deserializeFromString(const AggregateFunctionPtr & function, IColumn & column, const String & s, size_t version)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        ReadBufferFromString istr(s);
        function->deserialize(place, istr, version, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

static void deserializeFromValue(const AggregateFunctionPtr & function, IColumn & column, const String & value_str, const FormatSettings & settings)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        // Get the argument types for the aggregate function
        const auto & argument_types = function->getArgumentTypes();
        if (argument_types.size() == 1)
        {
            // Single argument - parse the value directly
            auto temp_column = argument_types[0]->createColumn();
            ReadBufferFromString buf(value_str);
            argument_types[0]->getDefaultSerialization()->deserializeTextCSV(*temp_column, buf, settings);
            // Add the value to the aggregate state
            const IColumn * columns[] = {temp_column.get()};
            function->add(place, columns, 0, &arena);
        }
        else
        {
            // Multiple arguments - parse as tuple
            auto arg_types = DataTypeTuple(function->getArgumentTypes());
            auto tmp_column = arg_types.createColumn();
            ReadBufferFromString buf(value_str);
            arg_types.getDefaultSerialization()->deserializeWholeText(*tmp_column, buf, settings);
            std::vector<const IColumn *> columns_ptrs;
            for (const auto & col : assert_cast<ColumnTuple*>(tmp_column.get())->getColumns())
                columns_ptrs.push_back(col.get());
            function->add(place, columns_ptrs.data(), 0, &arena);
        }
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }
    column_concrete.getData().push_back(place);
}

static void deserializeFromArray(const AggregateFunctionPtr & function, IColumn & column, const String & array_str, const FormatSettings & settings)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        // Get the argument types for the aggregate function
        const auto & argument_types = function->getArgumentTypes();
        const auto elem_type = argument_types.size() == 1 ? argument_types[0] : std::make_shared<DataTypeTuple>(argument_types);
        const auto tmp_column = elem_type->createColumn();
        const auto elem_serialization = elem_type->getDefaultSerialization();
        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (argument_types.size() == 1)
            columns_ptrs.push_back(tmp_column.get());
        else
            for (const auto & col : assert_cast<const ColumnTuple*>(tmp_column.get())->getColumns())
                columns_ptrs.push_back(col.get());
        // Parse the array - expect format like [val1,val2,val3] or [(val1a,val1b),(val2a,val2b)]
        ReadBufferFromString buf(array_str);
        assertChar('[', buf);
        bool first = true;
        while (!buf.eof())
        {
            skipWhitespaceIfAny(buf);
            if (*buf.position() == ']')
                break;
            if (!first)
            {
                assertChar(',', buf);
                skipWhitespaceIfAny(buf);
            }
            first = false;
            if (argument_types.size() == 1)
                elem_serialization->deserializeTextCSV(*tmp_column, buf, settings);  // CSV handles both ' and "
            else
                elem_serialization->deserializeTextQuoted(*tmp_column, buf, settings);  // Quoted for tuples
            function->add(place, columns_ptrs.data(), 0, &arena);
            tmp_column->popBack(1);
        }
        assertChar(']', buf);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void SerializationAggregateFunction::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeBasedOnInput(IColumn & column, const FormatSettings & settings, const String & s) const
{
    if (settings.aggregate_function_input_format == DB::FormatSettings::AggregateFunctionInputFormat::State)
    {
        deserializeFromString(function, column, s, version);
    }
    else if (settings.aggregate_function_input_format == DB::FormatSettings::AggregateFunctionInputFormat::Value)
    {
        deserializeFromValue(function, column, s, settings);
    }
    else if (settings.aggregate_function_input_format == DB::FormatSettings::AggregateFunctionInputFormat::Array)
    {
        deserializeFromArray(function, column, s, settings);
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid value for aggregate_function_input_format: '{}'. Expected '{}', '{}', or '{}'",
            settings.aggregate_function_input_format,
            DB::FormatSettings::AggregateFunctionInputFormat::State,
            DB::FormatSettings::AggregateFunctionInputFormat::Value,
            DB::FormatSettings::AggregateFunctionInputFormat::Array);
    }
}
void SerializationAggregateFunction::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr);
    deserializeBasedOnInput(column, settings, s);
}


void SerializationAggregateFunction::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readQuotedStringWithSQLStyle(s, istr);
    deserializeBasedOnInput(column, settings, s);
}


void SerializationAggregateFunction::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readStringUntilEOF(s, istr);
    deserializeBasedOnInput(column, settings, s);
}


void SerializationAggregateFunction::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(function, column, row_num, version), ostr, settings);
}


void SerializationAggregateFunction::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readJSONString(s, istr, settings.json);
    deserializeBasedOnInput(column, settings, s);
}


void SerializationAggregateFunction::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSV(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    deserializeBasedOnInput(column, settings, s);
}

}
