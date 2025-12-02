#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
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
}

namespace
{
void deserializeFromString(const AggregateFunctionPtr & function, IColumn & column, ReadBuffer & read_buf, size_t version)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        function->deserialize(place, read_buf, version, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void createStateFromValues(const AggregateFunctionPtr & function, IColumn & column, const IColumn ** arg_columns)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        function->addBatchSinglePlace(0, arg_columns[0]->size(), place, arg_columns, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }
    column_concrete.getData().push_back(place);
}

using DeserializeMethod = void(SerializationPtr, IColumn &, ReadBuffer &, const FormatSettings &);
#define DESERIALIZE_METHOD(method) [] (SerializationPtr serde, auto column_, auto istr_, auto settings_) { \
    serde->method(column_, istr_, settings_); \
}

template<DeserializeMethod Method>
void deserializeFromValues(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const AggregateFunctionPtr & function)
{
    chassert(settings.aggregate_function_input_format != FormatSettings::AggregateFunctionInputFormat::State);

    const auto & argument_types = function->getArgumentTypes();
    const auto value_type = argument_types.size() == 1 ? argument_types[0] : std::make_shared<DataTypeTuple>(argument_types);

    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::Value)
    {
        const auto tmp_column = value_type->createColumn();
        Method(value_type->getDefaultSerialization(), *tmp_column, istr, settings);

        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (argument_types.size() == 1)
            columns_ptrs.push_back(tmp_column.get());
        else
            for (const auto & col : assert_cast<const ColumnTuple*>(tmp_column.get())->getColumns())
                columns_ptrs.push_back(col.get());

        createStateFromValues(function, column, columns_ptrs.data());
    }
    else
    {
        auto array_type = DataTypeArray(value_type);
        const auto tmp_column = array_type.createColumn();
        Method(array_type.getDefaultSerialization(), *tmp_column, istr, settings);

        const auto & array_column = assert_cast<const ColumnArray&>(*tmp_column);
        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (argument_types.size() == 1)
            columns_ptrs.push_back(array_column.getDataPtr().get());
        else
            for (const auto & col : assert_cast<const ColumnTuple&>(array_column.getData()).getColumns())
                columns_ptrs.push_back(col.get());

        createStateFromValues(function, column, columns_ptrs.data());
    }
}

}

void SerializationAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const AggregateFunctionStateData & state = field.safeGet<AggregateFunctionStateData>();
    writeBinary(state.data, ostr);
}

void SerializationAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    field = AggregateFunctionStateData();
    AggregateFunctionStateData & s = field.safeGet<AggregateFunctionStateData>();
    s.name = type_name;

    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        readBinary(s.data, istr);
        return;
    }

    const auto & argument_types = function->getArgumentTypes();
    const auto value_type = argument_types.size() == 1 ? argument_types[0] : std::make_shared<DataTypeTuple>(argument_types);

    Arena arena{};
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());
    function->create(place);

    try
    {
        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::Value)
        {
            const auto tmp_column = value_type->createColumn();
            Field tmp;
            value_type->getDefaultSerialization()->deserializeBinary(tmp, istr, settings);
            tmp_column->insert(tmp);

            if (argument_types.size() == 1)
                columns_ptrs.push_back(tmp_column.get());
            else
                for (const auto & col : assert_cast<const ColumnTuple*>(tmp_column.get())->getColumns())
                    columns_ptrs.push_back(col.get());
        } else
        {
            auto array_type = DataTypeArray(value_type);
            const auto tmp_column = array_type.createColumn();
            Field tmp;
            array_type.getDefaultSerialization()->deserializeBinary(tmp, istr, settings);
            tmp_column->insert(tmp);

            const auto & array_column = assert_cast<const ColumnArray&>(*tmp_column);
            if (argument_types.size() == 1)
                columns_ptrs.push_back(array_column.getDataPtr().get());
            else
                for (const auto & col : assert_cast<const ColumnTuple&>(array_column.getData()).getColumns())
                    columns_ptrs.push_back(col.get());
        }

        function->addBatchSinglePlace(0, columns_ptrs[0]->size(), place, columns_ptrs.data(), &arena);
        WriteBufferFromString buf(s.data);
        function->serialize(place, buf, version);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }
}

void SerializationAggregateFunction::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], ostr, version);
}

void SerializationAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        deserializeFromString(function, column, istr, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeBinary);
    deserializeFromValues<method>(column, istr, settings, function);
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


void SerializationAggregateFunction::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextEscaped);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readQuotedStringWithSQLStyle(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextQuoted);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        deserializeFromString(function, column, istr, version);
        istr.ignoreAll();
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeWholeText);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(function, column, row_num, version), ostr, settings);
}


void SerializationAggregateFunction::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readJSONString(s, istr, settings.json);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextJSON);
    deserializeFromValues<method>(column, istr, settings, function);
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
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readCSV(s, istr, settings.csv);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextCSV);
    deserializeFromValues<method>(column, istr, settings, function);
}

}
