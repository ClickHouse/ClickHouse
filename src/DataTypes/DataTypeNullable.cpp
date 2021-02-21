#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ConcatReadBuffer.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypeNullable::DataTypeNullable(const DataTypePtr & nested_data_type_)
    : nested_data_type{nested_data_type_}
{
    if (!nested_data_type->canBeInsideNullable())
        throw Exception("Nested type " + nested_data_type->getName() + " cannot be inside Nullable type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


bool DataTypeNullable::onlyNull() const
{
    return typeid_cast<const DataTypeNothing *>(nested_data_type.get());
}


void DataTypeNullable::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::NullMap);
    callback(path);
    path.back() = Substream::NullableElements;
    nested_data_type->enumerateStreams(callback, path);
    path.pop_back();
}


void DataTypeNullable::serializeBinaryBulkStatePrefix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_data_type->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void DataTypeNullable::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_data_type->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void DataTypeNullable::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_data_type->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void DataTypeNullable::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
    col.checkConsistency();

    /// First serialize null map.
    settings.path.push_back(Substream::NullMap);
    if (auto * stream = settings.getter(settings.path))
        DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), *stream, offset, limit);

    /// Then serialize contents of arrays.
    settings.path.back() = Substream::NullableElements;
    nested_data_type->serializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), offset, limit, settings, state);
    settings.path.pop_back();
}


void DataTypeNullable::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);

    settings.path.push_back(Substream::NullMap);
    if (auto * stream = settings.getter(settings.path))
        DataTypeUInt8().deserializeBinaryBulk(col.getNullMapColumn(), *stream, limit, 0);

    settings.path.back() = Substream::NullableElements;
    nested_data_type->deserializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), limit, settings, state);
    settings.path.pop_back();
}


void DataTypeNullable::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    if (field.isNull())
    {
        writeBinary(true, ostr);
    }
    else
    {
        writeBinary(false, ostr);
        nested_data_type->serializeBinary(field, ostr);
    }
}

void DataTypeNullable::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    bool is_null = false;
    readBinary(is_null, istr);
    if (!is_null)
    {
        nested_data_type->deserializeBinary(field, istr);
    }
    else
    {
        field = Null();
    }
}

void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested_data_type->serializeBinary(col.getNestedColumn(), row_num, ostr);
}

/// Deserialize value into ColumnNullable.
/// We need to insert both to nested column and to null byte map, or, in case of exception, to not insert at all.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested, typename std::enable_if_t<std::is_same_v<ReturnType, void>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(
    IColumn & column, const IDataType & /*nested_data_type*/,
    CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);

    if (check_for_null())
    {
        col.insertDefault();
    }
    else
    {
        deserialize_nested(col.getNestedColumn());

        try
        {
            col.getNullMapData().push_back(0);
        }
        catch (...)
        {
            col.getNestedColumn().popBack(1);
            throw;
        }
    }
}

/// Deserialize value into non-nullable column. In case of NULL, insert default value and return false.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested, typename std::enable_if_t<std::is_same_v<ReturnType, bool>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(
        IColumn & column, const IDataType & nested_data_type,
        CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
    assert(!dynamic_cast<ColumnNullable *>(&column));
    assert(!dynamic_cast<const DataTypeNullable *>(&nested_data_type));
    bool insert_default = check_for_null();
    if (insert_default)
        nested_data_type.insertDefaultInto(column);
    else
        deserialize_nested(column);
    return !insert_default;
}


void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    safeDeserialize(column, *nested_data_type,
        [&istr] { bool is_null = false; readBinary(is_null, istr); return is_null; },
        [this, &istr] (IColumn & nested) { nested_data_type->deserializeBinary(nested, istr); });
}


void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeAsTextEscaped(col.getNestedColumn(), row_num, ostr, settings);
}


void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped<void>(column, istr, settings, nested_data_type);
}

template<typename ReturnType>
ReturnType DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const DataTypePtr & nested_data_type)
{
    /// Little tricky, because we cannot discriminate null from first character.

    if (istr.eof())
        throw Exception("Unexpected end of stream, while parsing value of Nullable type", ErrorCodes::CANNOT_READ_ALL_DATA);

    /// This is not null, surely.
    if (*istr.position() != '\\')
    {
        return safeDeserialize<ReturnType>(column, *nested_data_type,
            [] { return false; },
            [&nested_data_type, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeAsTextEscaped(nested, istr, settings); });
    }
    else
    {
        /// Now we know, that data in buffer starts with backslash.
        ++istr.position();

        if (istr.eof())
            throw Exception("Unexpected end of stream, while parsing value of Nullable type, after backslash", ErrorCodes::CANNOT_READ_ALL_DATA);

        return safeDeserialize<ReturnType>(column, *nested_data_type,
            [&istr]
            {
                if (*istr.position() == 'N')
                {
                    ++istr.position();
                    return true;
                }
                return false;
            },
            [&nested_data_type, &istr, &settings] (IColumn & nested)
            {
                if (istr.position() != istr.buffer().begin())
                {
                    /// We could step back to consume backslash again.
                    --istr.position();
                    nested_data_type->deserializeAsTextEscaped(nested, istr, settings);
                }
                else
                {
                    /// Otherwise, we need to place backslash back in front of istr.
                    ReadBufferFromMemory prefix("\\", 1);
                    ConcatReadBuffer prepended_istr(prefix, istr);

                    nested_data_type->deserializeAsTextEscaped(nested, prepended_istr, settings);

                    /// Synchronise cursor position in original buffer.

                    if (prepended_istr.count() > 1)
                        istr.position() = prepended_istr.position();
                }
            });
    }
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("NULL", ostr);
    else
        nested_data_type->serializeAsTextQuoted(col.getNestedColumn(), row_num, ostr, settings);
}


void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextQuoted<void>(column, istr, settings, nested_data_type);
}

template<typename ReturnType>
ReturnType DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                   const DataTypePtr & nested_data_type)
{
    return safeDeserialize<ReturnType>(column, *nested_data_type,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr); },
        [&nested_data_type, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeAsTextQuoted(nested, istr, settings); });
}


void DataTypeNullable::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    safeDeserialize(column, *nested_data_type,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr); },
        [this, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeAsWholeText(nested, istr, settings); });
}


void DataTypeNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeAsTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextCSV<void>(column, istr, settings, nested_data_type);
}

template<typename ReturnType>
ReturnType DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const DataTypePtr & nested_data_type)
{
    constexpr char const * null_literal = "NULL";
    constexpr size_t len = 4;
    size_t null_prefix_len = 0;

    auto check_for_null = [&istr, &settings, &null_prefix_len]
    {
        if (checkStringByFirstCharacterAndAssertTheRest("\\N", istr))
            return true;
        if (!settings.csv.unquoted_null_literal_as_null)
            return false;

        /// Check for unquoted NULL
        while (!istr.eof() && null_prefix_len < len && null_literal[null_prefix_len] == *istr.position())
        {
            ++null_prefix_len;
            ++istr.position();
        }
        if (null_prefix_len == len)
            return true;

        /// Value and "NULL" have common prefix, but value is not "NULL".
        /// Restore previous buffer position if possible.
        if (null_prefix_len <= istr.offset())
        {
            istr.position() -= null_prefix_len;
            null_prefix_len = 0;
        }
        return false;
    };

    auto deserialize_nested = [&nested_data_type, &settings, &istr, &null_prefix_len] (IColumn & nested)
    {
        if (likely(!null_prefix_len))
            nested_data_type->deserializeAsTextCSV(nested, istr, settings);
        else
        {
            /// Previous buffer position was not restored,
            /// so we need to prepend extracted characters (rare case)
            ReadBufferFromMemory prepend(null_literal, null_prefix_len);
            ConcatReadBuffer buf(prepend, istr);
            nested_data_type->deserializeAsTextCSV(nested, buf, settings);

            /// Check if all extracted characters were read by nested parser and update buffer position
            if (null_prefix_len < buf.count())
                istr.position() = buf.position();
            else if (null_prefix_len > buf.count())
            {
                /// It can happen only if there is an unquoted string instead of a number
                /// or if someone uses 'U' or 'L' as delimiter in CSV.
                /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
                if (settings.csv.delimiter == 'U' || settings.csv.delimiter == 'L')
                    throw DB::Exception("Enabled setting input_format_csv_unquoted_null_literal_as_null may not work correctly "
                                        "with format_csv_delimiter = 'U' or 'L' for large input.", ErrorCodes::CANNOT_READ_ALL_DATA);
                WriteBufferFromOwnString parsed_value;
                nested_data_type->serializeAsTextCSV(nested, nested.size() - 1, parsed_value, settings);
                throw DB::Exception("Error while parsing \"" + std::string(null_literal, null_prefix_len)
                                    + std::string(istr.position(), std::min(size_t{10}, istr.available())) + "\" as Nullable(" + nested_data_type->getName()
                                    + ") at position " + std::to_string(istr.count()) + ": expected \"NULL\" or " + nested_data_type->getName()
                                    + ", got \"" + std::string(null_literal, buf.count()) + "\", which was deserialized as \""
                                    + parsed_value.str() + "\". It seems that input data is ill-formatted.",
                                    ErrorCodes::CANNOT_READ_ALL_DATA);
            }
        }
    };

    return safeDeserialize<ReturnType>(column, *nested_data_type, check_for_null, deserialize_nested);
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    /// In simple text format (like 'Pretty' format) (these formats are suitable only for output and cannot be parsed back),
    ///  data is printed without escaping.
    /// It makes theoretically impossible to distinguish between NULL and some string value, regardless on how do we print NULL.
    /// For this reason, we output NULL in a bit strange way.
    /// This assumes UTF-8 and proper font support. This is Ok, because Pretty formats are "presentational", not for data exchange.

    if (col.isNullAt(row_num))
    {
        if (settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8)
            writeCString("ᴺᵁᴸᴸ", ostr);
        else
            writeCString("NULL", ostr);
    }
    else
        nested_data_type->serializeAsText(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("null", ostr);
    else
        nested_data_type->serializeAsTextJSON(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSON<void>(column, istr, settings, nested_data_type);
}

template<typename ReturnType>
ReturnType DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const DataTypePtr & nested_data_type)
{
    return safeDeserialize<ReturnType>(column, *nested_data_type,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRest("null", istr); },
        [&nested_data_type, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeAsTextJSON(nested, istr, settings); });
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeAsTextXML(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
    if (!col.isNullAt(row_num))
        nested_data_type->serializeProtobuf(col.getNestedColumn(), row_num, protobuf, value_index);
}

void DataTypeNullable::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    IColumn & nested_column = col.getNestedColumn();
    size_t old_size = nested_column.size();
    try
    {
        nested_data_type->deserializeProtobuf(nested_column, protobuf, allow_add_row, row_added);
        if (row_added)
            col.getNullMapData().push_back(0);
    }
    catch (...)
    {
        nested_column.popBack(nested_column.size() - old_size);
        col.getNullMapData().resize_assume_reserved(old_size);
        row_added = false;
        throw;
    }
}

MutableColumnPtr DataTypeNullable::createColumn() const
{
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
}

Field DataTypeNullable::getDefault() const
{
    return Null();
}

size_t DataTypeNullable::getSizeOfValueInMemory() const
{
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.", ErrorCodes::LOGICAL_ERROR);
}


bool DataTypeNullable::equals(const IDataType & rhs) const
{
    return rhs.isNullable() && nested_data_type->equals(*static_cast<const DataTypeNullable &>(rhs).nested_data_type);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Nullable data type family must have exactly one argument - nested type", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypePtr nested_type = DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<DataTypeNullable>(nested_type);
}


void registerDataTypeNullable(DataTypeFactory & factory)
{
    factory.registerDataType("Nullable", create);
}


DataTypePtr makeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();
    return type;
}


template bool DataTypeNullable::deserializeTextEscaped<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DataTypePtr & nested);
template bool DataTypeNullable::deserializeTextQuoted<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings &, const DataTypePtr & nested);
template bool DataTypeNullable::deserializeTextCSV<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DataTypePtr & nested);
template bool DataTypeNullable::deserializeTextJSON<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings &, const DataTypePtr & nested);

}
