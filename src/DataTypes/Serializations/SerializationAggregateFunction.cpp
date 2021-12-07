#include <DataTypes/Serializations/SerializationAggregateFunction.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnAggregateFunction.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/AlignedBuffer.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

void SerializationAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const AggregateFunctionStateData & state = get<const AggregateFunctionStateData &>(field);
    writeBinary(state.data, ostr);
}

void SerializationAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    field = AggregateFunctionStateData();
    AggregateFunctionStateData & s = get<AggregateFunctionStateData &>(field);
    readBinary(s.data, istr);
    s.name = type_name;
}

void SerializationAggregateFunction::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], ostr);
}

void SerializationAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);
    try
    {
        function->deserialize(place, istr, &arena);
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

    ColumnAggregateFunction::Container::const_iterator it = vec.begin() + offset;
    ColumnAggregateFunction::Container::const_iterator end = limit ? it + limit : vec.end();

    if (end > vec.end())
        end = vec.end();

    for (; it != end; ++it)
        function->serialize(*it, ostr);
}

void SerializationAggregateFunction::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(column);
    ColumnAggregateFunction::Container & vec = real_column.getData();

    Arena & arena = real_column.createOrGetArena();
    real_column.set(function);
    vec.reserve(vec.size() + limit);

    size_t size_of_state = function->sizeOfData();
    size_t align_of_state = function->alignOfData();

    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        AggregateDataPtr place = arena.alignedAlloc(size_of_state, align_of_state);

        function->create(place);

        try
        {
            function->deserialize(place, istr, &arena);
        }
        catch (...)
        {
            function->destroy(place);
            throw;
        }

        vec.push_back(place);
    }
}

static String serializeToString(const AggregateFunctionPtr & function, const IColumn & column, size_t row_num)
{
    WriteBufferFromOwnString buffer;
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], buffer);
    return buffer.str();
}

static void deserializeFromString(const AggregateFunctionPtr & function, IColumn & column, const String & s)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        ReadBufferFromString istr(s);
        function->deserialize(place, istr, &arena);
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
    writeString(serializeToString(function, column, row_num), ostr);
}


void SerializationAggregateFunction::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(serializeToString(function, column, row_num), ostr);
}


void SerializationAggregateFunction::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String s;
    readEscapedString(s, istr);
    deserializeFromString(function, column, s);
}


void SerializationAggregateFunction::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(serializeToString(function, column, row_num), ostr);
}


void SerializationAggregateFunction::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String s;
    readQuotedStringWithSQLStyle(s, istr);
    deserializeFromString(function, column, s);
}


void SerializationAggregateFunction::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String s;
    readStringUntilEOF(s, istr);
    deserializeFromString(function, column, s);
}


void SerializationAggregateFunction::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(function, column, row_num), ostr, settings);
}


void SerializationAggregateFunction::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String s;
    readJSONString(s, istr);
    deserializeFromString(function, column, s);
}


void SerializationAggregateFunction::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(serializeToString(function, column, row_num), ostr);
}


void SerializationAggregateFunction::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSV(serializeToString(function, column, row_num), ostr);
}


void SerializationAggregateFunction::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    deserializeFromString(function, column, s);
}

}
