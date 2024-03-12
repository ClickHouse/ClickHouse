#include <Processors/Formats/Impl/IonRowOutputFormat.h>

#if USE_ION

#    include <iostream>
#    include <Formats/FormatFactory.h>
#    include <Common/assert_cast.h>

#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>

#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeTuple.h>

#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnLowCardinality.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsNumber.h>
#    include "Formats/FormatSettings.h"


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}

IonRowOutputFormat::IonRowOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_)
    , format_settings(format_settings_)
    , writer(std::make_unique<IonWriter>(out, format_settings.ion.output_type == FormatSettings::IonOutputWriterType::BINARY))
    , names(header_.getNames())
{
}

void IonRowOutputFormat::serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8: {
            writer->writeInt(assert_cast<const ColumnUInt8 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Date: {
            UInt16 value = assert_cast<const ColumnVector<UInt16> &>(column).getData()[row_num];
            LocalDate ld = LocalDate(DayNum(value));
            UInt16 year = ld.year();
            UInt8 month = ld.month();
            UInt8 day = ld.day();
            writer->writeDate(year, month, day);
            return;
        }
        case TypeIndex::UInt16: {
            writer->writeInt(assert_cast<const ColumnUInt16 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::DateTime: {
            UInt32 value = assert_cast<const ColumnVector<UInt32> &>(column).getData()[row_num];
            String timezone = getDateTimeTimezone(*data_type);
            LocalDateTime ldt = LocalDateTime(value, DateLUT::instance(timezone));
            UInt16 year = ldt.year();
            UInt8 month = ldt.month();
            UInt8 day = ldt.day();
            UInt8 hour = ldt.hour();
            UInt8 minute = ldt.minute();
            UInt8 second = ldt.second();
            writer->writeDateTime(year, month, day, hour, minute, second);
            return;
        }
        case TypeIndex::UInt32: {
            writer->writeInt(assert_cast<const ColumnUInt32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::IPv4: {
            writer->writeIPv4(assert_cast<const ColumnIPv4 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::UInt64: {
            writer->writeBigUInt(assert_cast<const ColumnUInt64 &>(column).getDataAt(row_num).data, sizeof(UInt64));
            return;
        }
        case TypeIndex::Enum8:
            [[fallthrough]];
        case TypeIndex::Int8: {
            writer->writeInt(assert_cast<const ColumnInt8 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Enum16:
            [[fallthrough]];
        case TypeIndex::Int16: {
            writer->writeInt(assert_cast<const ColumnInt16 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Date32: {
            Int32 value = assert_cast<const ColumnVector<Int32> &>(column).getData()[row_num];
            LocalDate ld = LocalDate(ExtendedDayNum(value));
            UInt16 year = ld.year();
            UInt8 month = ld.month();
            UInt8 day = ld.day();
            writer->writeDate(year, month, day);
            return;
        }
        case TypeIndex::Int32: {
            writer->writeInt(assert_cast<const ColumnInt32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int64: {
            writer->writeInt(assert_cast<const ColumnInt64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Int128: {
            writer->writeBigInt(assert_cast<const ColumnInt128 &>(column).getDataAt(row_num).data, sizeof(Int128));
            return;
        }
        case TypeIndex::UInt128: {
            writer->writeBigUInt(assert_cast<const ColumnUInt128 &>(column).getDataAt(row_num).data, sizeof(UInt128));
            return;
        }
        case TypeIndex::Int256: {
            writer->writeBigInt(assert_cast<const ColumnInt256 &>(column).getDataAt(row_num).data, sizeof(Int256));
            return;
        }
        case TypeIndex::UInt256: {
            writer->writeBigUInt(assert_cast<const ColumnUInt256 &>(column).getDataAt(row_num).data, sizeof(UInt256));
            return;
        }
        case TypeIndex::Float32: {
            writer->writeFloat(assert_cast<const ColumnFloat32 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Float64: {
            writer->writeDouble(assert_cast<const ColumnFloat64 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::DateTime64: {
            // I really don't know how to make it better
            writer->writeBigUInt(assert_cast<const DataTypeDateTime64::ColumnType &>(column).getDataAt(row_num).data, sizeof(DateTime64));
            return;
        }
        case TypeIndex::Decimal32: {
            writer->writeDecimal32(assert_cast<const ColumnDecimal<Decimal32> &>(column).getElement(row_num).value);
            return;
        }
        case TypeIndex::Decimal64: {
            writer->writeDecimal64(assert_cast<const ColumnDecimal<Decimal64> &>(column).getElement(row_num).value);
            return;
        }
        case TypeIndex::Decimal128: {
            writer->writeDecimal128(assert_cast<const ColumnDecimal<Decimal128> &>(column).getDataAt(row_num).data, sizeof(Int128));
            return;
        }
        case TypeIndex::Decimal256: {
            writer->writeDecimal256(assert_cast<const ColumnDecimal<Decimal256> &>(column).getDataAt(row_num).data, sizeof(Int256));
            return;
        }
        case TypeIndex::String: {
            writer->writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num).toView());
            return;
        }
        case TypeIndex::FixedString: {
            writer->writeString(assert_cast<const ColumnFixedString &>(column).getDataAt(row_num).toView());
            return;
        }
        case TypeIndex::IPv6: {
            writer->writeIPv6(assert_cast<const ColumnIPv6 &>(column).getElement(row_num));
            return;
        }
        case TypeIndex::Array: {
            auto nested_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const IColumn & nested_column = column_array.getData();
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            writer->writeStartList();
            for (size_t i = 0; i < size; ++i)
                serializeField(nested_column, nested_type, offset + i);
            writer->writeFinishList();
            return;
        }
        case TypeIndex::Tuple: {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
            const auto & nested_types = tuple_type.getElements();
            const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
            const auto & nested_columns = column_tuple.getColumns();
            writer->writeStartList();
            for (size_t i = 0; i < nested_types.size(); ++i)
                serializeField(*nested_columns[i], nested_types[i], row_num);
            writer->writeFinishList();
            return;
        }
        case TypeIndex::Nullable: {
            auto nested_type = removeNullable(data_type);
            const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
            if (!column_nullable.isNullAt(row_num))
                serializeField(column_nullable.getNestedColumn(), nested_type, row_num);
            else if (isInteger(nested_type))
                writer->writeTypedNull(NullableIonDataType::Integer);
            else if (isFloat(nested_type))
                writer->writeTypedNull(NullableIonDataType::Float);
            else if (isDecimal(nested_type))
                writer->writeTypedNull(NullableIonDataType::Decimal);
            else if (isDateOrDate32OrDateTimeOrDateTime64(nested_type))
                writer->writeTypedNull(NullableIonDataType::DateTime);
            else if (isStringOrFixedString(nested_type) || isIPv4(nested_type) || isIPv6(nested_type))
                writer->writeTypedNull(NullableIonDataType::String);
            else
                writer->writeNull();
            return;
        }
        case TypeIndex::Nothing: {
            writer->writeNull();
            return;
        }
        case TypeIndex::Map: {
            const auto & map_column = assert_cast<const ColumnMap &>(column);
            const auto & nested_column = map_column.getNestedColumn();
            const auto & key_value_columns = map_column.getNestedData().getColumns();
            const auto & key_column = key_value_columns[0];
            const auto & value_column = key_value_columns[1];

            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & offsets = nested_column.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            writer->writeStartStruct();
            for (size_t i = 0; i < size; ++i)
            {
                if (map_type.getKeyType()->getTypeId() == TypeIndex::String)
                    writer->writeStructFieldName(assert_cast<const ColumnString &>(*key_column).getDataAt(offset + i).toView());
                else if (map_type.getKeyType()->getTypeId() == TypeIndex::FixedString)
                    writer->writeStructFieldName(assert_cast<const ColumnFixedString &>(*key_column).getDataAt(offset + i).toView());
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ion only supports string types for map keys");
                serializeField(*value_column, map_type.getValueType(), offset + i);
            }
            writer->writeFinishStruct();
            return;
        }
        case TypeIndex::LowCardinality: {
            const auto & lc_column = assert_cast<const ColumnLowCardinality &>(column);
            auto dict_type = assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType();
            auto dict_column = lc_column.getDictionary().getNestedColumn();
            size_t index = lc_column.getIndexAt(row_num);
            serializeField(*dict_column, dict_type, index);
            return;
        }
        case TypeIndex::UUID: {
            const auto & uuid_column = assert_cast<const ColumnUUID &>(column);
            WriteBufferFromOwnString buf;
            writeText(uuid_column.getElement(row_num), buf);
            writer->writeString(buf.stringView());
            return;
        }
        default:
            break;
    }
    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for MsgPack output format", data_type->getName());
}

void IonRowOutputFormat::writeRowStartDelimiter()
{
    writer->writeStartStruct();
}

void IonRowOutputFormat::writeRowEndDelimiter()
{
    writer->writeFinishStruct();
}

void IonRowOutputFormat::writePrefix()
{
    writer->writeStartList();
}

void IonRowOutputFormat::writeSuffix()
{
    writer->writeFinishList();
}

void IonRowOutputFormat::finalizeImpl()
{
    writer->final();
}

void IonRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writeRowStartDelimiter();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        writer->writeStructFieldName(names[i]);
        serializeField(*columns[i], types[i], row_num);
    }
    writeRowEndDelimiter();
}

void registerOutputFormatIon(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Ion",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings)
        { return std::make_shared<IonRowOutputFormat>(buf, sample, settings); });
    factory.markOutputFormatSupportsParallelFormatting("Ion");
}
}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatIon(FormatFactory &)
{
}
}

#endif
