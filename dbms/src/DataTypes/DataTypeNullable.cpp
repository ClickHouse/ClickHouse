#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnNullable.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ConcatReadBuffer.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
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


void DataTypeNullable::enumerateStreams(StreamCallback callback, SubstreamPath path) const
{
    path.push_back(Substream::NullMap);
    callback(path);
    path.back() = Substream::NullableElements;
    nested_data_type->enumerateStreams(callback, path);
}


void DataTypeNullable::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    OutputStreamGetter getter,
    size_t offset,
    size_t limit,
    bool position_independent_encoding,
    SubstreamPath path) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
    col.checkConsistency();

    /// First serialize null map.
    path.push_back(Substream::NullMap);
    if (auto stream = getter(path))
        DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), *stream, offset, limit);

    /// Then serialize contents of arrays.
    path.back() = Substream::NullableElements;
    nested_data_type->serializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), getter, offset, limit, position_independent_encoding, path);
}


void DataTypeNullable::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    InputStreamGetter getter,
    size_t limit,
    double avg_value_size_hint,
    bool position_independent_encoding,
    SubstreamPath path) const
{
    ColumnNullable & col = static_cast<ColumnNullable &>(column);

    path.push_back(Substream::NullMap);
    if (auto stream = getter(path))
        DataTypeUInt8().deserializeBinaryBulk(col.getNullMapColumn(), *stream, limit, 0);

    path.back() = Substream::NullableElements;
    nested_data_type->deserializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), getter, limit, avg_value_size_hint, position_independent_encoding, path);
}


void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested_data_type->serializeBinary(col.getNestedColumn(), row_num, ostr);
}


/// We need to insert both to nested column and to null byte map, or, in case of exception, to not insert at all.
template <typename CheckForNull, typename DeserializeNested>
static void safeDeserialize(
    IColumn & column,
    CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
    ColumnNullable & col = static_cast<ColumnNullable &>(column);

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


void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    safeDeserialize(column,
        [&istr] { bool is_null = 0; readBinary(is_null, istr); return is_null; },
        [this, &istr] (IColumn & nested) { nested_data_type->deserializeBinary(nested, istr); } );
}


void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeTextEscaped(col.getNestedColumn(), row_num, ostr, settings);
}


void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    /// Little tricky, because we cannot discriminate null from first character.

    if (istr.eof())
        throw Exception("Unexpected end of stream, while parsing value of Nullable type", ErrorCodes::CANNOT_READ_ALL_DATA);

    /// This is not null, surely.
    if (*istr.position() != '\\')
    {
        safeDeserialize(column,
            [] { return false; },
            [this, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeTextEscaped(nested, istr, settings); } );
    }
    else
    {
        /// Now we know, that data in buffer starts with backslash.
        ++istr.position();

        if (istr.eof())
            throw Exception("Unexpected end of stream, while parsing value of Nullable type, after backslash", ErrorCodes::CANNOT_READ_ALL_DATA);

        safeDeserialize(column,
            [&istr]
            {
                if (*istr.position() == 'N')
                {
                    ++istr.position();
                    return true;
                }
                return false;
            },
            [this, &istr, &settings] (IColumn & nested)
            {
                if (istr.position() != istr.buffer().begin())
                {
                    /// We could step back to consume backslash again.
                    --istr.position();
                    nested_data_type->deserializeTextEscaped(nested, istr, settings);
                }
                else
                {
                    /// Otherwise, we need to place backslash back in front of istr.
                    ReadBufferFromMemory prefix("\\", 1);
                    ConcatReadBuffer prepended_istr(prefix, istr);

                    nested_data_type->deserializeTextEscaped(nested, prepended_istr, settings);

                    /// Synchronise cursor position in original buffer.

                    if (prepended_istr.count() > 1)
                        istr.position() = prepended_istr.position();
                }
            });
    }
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("NULL", ostr);
    else
        nested_data_type->serializeTextQuoted(col.getNestedColumn(), row_num, ostr, settings);
}


void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    safeDeserialize(column,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr); },
        [this, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeTextQuoted(nested, istr, settings); } );
}

void DataTypeNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    safeDeserialize(column,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRest("\\N", istr); },
        [this, &settings, &istr] (IColumn & nested) { nested_data_type->deserializeTextCSV(nested, istr, settings); } );
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    /// In simple text format (like 'Pretty' format) (these formats are suitable only for output and cannot be parsed back),
    ///  data is printed without escaping.
    /// It makes theoretically impossible to distinguish between NULL and some string value, regardless on how do we print NULL.
    /// For this reason, we output NULL in a bit strange way.
    /// This assumes UTF-8 and proper font support. This is Ok, because Pretty formats are "presentational", not for data exchange.

    if (col.isNullAt(row_num))
        writeCString("ᴺᵁᴸᴸ", ostr);
    else
        nested_data_type->serializeText(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("null", ostr);
    else
        nested_data_type->serializeTextJSON(col.getNestedColumn(), row_num, ostr, settings);
}

void DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    safeDeserialize(column,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRest("null", istr); },
        [this, &istr, &settings] (IColumn & nested) { nested_data_type->deserializeTextJSON(nested, istr, settings); } );
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested_data_type->serializeTextXML(col.getNestedColumn(), row_num, ostr, settings);
}

MutableColumnPtr DataTypeNullable::createColumn() const
{
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
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

}
