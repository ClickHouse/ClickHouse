#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>

#include <ext/map.h>
#include <ext/enumerate.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_READ_MAP_FROM_TEXT;
}


DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"}))) {}

std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getName() << "," << value_type->getName() << ")";

    return s.str();
}

static const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

static IColumn & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnMap &>(column).getNestedColumn();
}

void DataTypeMap::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & map = get<const Map &>(field);
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple>();
        assert(tuple.size() == 2);
        key_type->serializeBinary(tuple[0], ostr);
        value_type->serializeBinary(tuple[1], ostr);
    }
}

void DataTypeMap::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    size_t size;
    readVarUInt(size, istr);
    field = Map(size);
    for (auto & elem : field.get<Map &>())
    {
        Tuple tuple(2);
        key_type->deserializeBinary(tuple[0], istr);
        value_type->deserializeBinary(tuple[1], istr);
        elem = std::move(tuple);
    }
}

void DataTypeMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr);
}

void DataTypeMap::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr);
}


template <typename Writer>
void DataTypeMap::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && writer) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);

    const auto & nested_array = column_map.getNestedColumn();
    const auto & nested_tuple = column_map.getNestedData();
    const auto & offsets = nested_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    writeChar('{', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        writer(key_type, nested_tuple.getColumn(0), i);
        writeChar(':', ostr);
        writer(value_type, nested_tuple.getColumn(1), i);
    }
    writeChar('}', ostr);
}

template <typename Reader>
void DataTypeMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, bool need_safe_get_int_key, Reader && reader) const
{
    auto & column_map = assert_cast<ColumnMap &>(column);

    auto & nested_array = column_map.getNestedColumn();
    auto & nested_tuple = column_map.getNestedData();
    auto & offsets = nested_array.getOffsets();

    auto & key_column = nested_tuple.getColumn(0);
    auto & value_column = nested_tuple.getColumn(1);

    size_t size = 0;
    assertChar('{', istr);

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw Exception("Cannot read Map from text", ErrorCodes::CANNOT_READ_MAP_FROM_TEXT);
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            if (need_safe_get_int_key)
            {
                ReadBuffer::Position tmp = istr.position();
                while (*tmp != ':' && *tmp != '}')
                    ++tmp;
                *tmp = ' ';
                reader(key_type, key_column);
            }
            else
            {
                reader(key_type, key_column);
                skipWhitespaceIfAny(istr);
                assertChar(':', istr);
            }

            ++size;
            skipWhitespaceIfAny(istr);
            reader(value_type, value_column);

            skipWhitespaceIfAny(istr);
        }

        offsets.push_back(offsets.back() + size);
        assertChar('}', istr);
    }
    catch (...)
    {
        throw;
    }
}

void DataTypeMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const DataTypePtr & subcolumn_type, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_type->serializeAsTextQuoted(subcolumn, pos, ostr, settings);
        });
}

void DataTypeMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    // need_safe_get_int_key is set for Integer to prevent to readIntTextUnsafe
    bool need_safe_get_int_key = isInteger(key_type);

    deserializeTextImpl(column, istr, need_safe_get_int_key,
        [&](const DataTypePtr & subcolumn_type, IColumn & subcolumn)
        {
            subcolumn_type->deserializeAsTextQuoted(subcolumn, istr, settings);
        });
}


void DataTypeMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const DataTypePtr & subcolumn_type, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_type->serializeAsTextJSON(subcolumn, pos, ostr, settings);
        });
}

void DataTypeMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    // need_safe_get_int_key is set for Integer to prevent to readIntTextUnsafe
    bool need_safe_get_int_key = isInteger(key_type);

    deserializeTextImpl(column, istr, need_safe_get_int_key,
        [&](const DataTypePtr & subcolumn_type, IColumn & subcolumn)
        {
            subcolumn_type->deserializeAsTextJSON(subcolumn, istr, settings);
        });
}

void DataTypeMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & offsets = column_map.getNestedColumn().getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const auto & nested_data = column_map.getNestedData();

    writeCString("<map>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        writeCString("<key>", ostr);
        key_type->serializeAsTextXML(nested_data.getColumn(0), i, ostr, settings);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value_type->serializeAsTextXML(nested_data.getColumn(1), i, ostr, settings);
        writeCString("</value>", ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</map>", ostr);
}

void DataTypeMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void DataTypeMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings);
}


void DataTypeMap::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    nested->enumerateStreams(callback, path);
}

void DataTypeMap::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(settings, state);
}

void DataTypeMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void DataTypeMap::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}


void DataTypeMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
}

void DataTypeMap::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), limit, settings, state);
}

void DataTypeMap::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    nested->serializeProtobuf(extractNestedColumn(column), row_num, protobuf, value_index);
}

void DataTypeMap::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    nested->deserializeProtobuf(extractNestedColumn(column), protobuf, allow_add_row, row_added);
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(nested->createColumn());
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return nested->equals(*rhs_map.nested);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Map data type family must have two arguments: key and value types", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}
}
