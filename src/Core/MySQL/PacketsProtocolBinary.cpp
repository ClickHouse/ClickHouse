#include <Columns/IColumn.h>
#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>
#include <Core/MySQL/PacketsProtocolBinary.h>
#include "Common/LocalDate.h"
#include "Common/LocalDateTime.h"
#include "Columns/ColumnLowCardinality.h"
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnVector.h"
#include "Columns/ColumnsDateTime.h"
#include "Core/DecimalFunctions.h"
#include "DataTypes/DataTypeDateTime64.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Formats/FormatSettings.h"
#include "IO/WriteBufferFromString.h"
#include "base/DayNum.h"
#include "base/Decimal.h"
#include "base/types.h"

namespace DB
{
namespace MySQLProtocol
{
namespace ProtocolBinary
{
ResultSetRow::ResultSetRow(const Serializations & serializations_, const DataTypes & data_types_, const Columns & columns_, int row_num_)
    : row_num(row_num_), columns(columns_), data_types(data_types_), serializations(serializations_)
{
    payload_size = 1 + null_bitmap_size;
    FormatSettings format_settings;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        ColumnPtr col = getColumn(i);
        if (col->isNullAt(row_num))
        {
            // See https://dev.mysql.com/doc/dev/mysql-server/8.1.0/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
            size_t byte = (i + 2) / 8;
            int bit = 1 << ((i + 2) % 8);
            null_bitmap[byte] |= bit;
            continue; // NULLs are stored in the null bitmap only
        }

        DataTypePtr data_type = removeLowCardinality(removeNullable((data_types[i])));
        TypeIndex type_index = data_type->getTypeId();
        switch (type_index)
        {
            case TypeIndex::Int8:
                payload_size += 1;
                break;
            case TypeIndex::UInt8:
                if (data_type->getName() == "Bool")
                    payload_size += 2; // BIT MySQL type is string<lenenc> in binary
                else
                    payload_size += 1;
                break;
            case TypeIndex::Int16:
            case TypeIndex::UInt16:
                payload_size += 2;
                break;
            case TypeIndex::Int32:
            case TypeIndex::UInt32:
            case TypeIndex::Float32:
                payload_size += 4;
                break;
            case TypeIndex::Int64:
            case TypeIndex::UInt64:
            case TypeIndex::Float64:
                payload_size += 8;
                break;
            case TypeIndex::Date:
            case TypeIndex::Date32: {
                size_t size = 1 // number of bytes following
                    + 2 // year
                    + 1 // month
                    + 1; // day
                payload_size += size;
                break;
            }
            case TypeIndex::DateTime: {
                UInt32 value = assert_cast<const ColumnVector<UInt32> &>(*col).getData()[row_num];
                LocalDateTime ldt = LocalDateTime(value, DateLUT::instance(getDateTimeTimezone(*data_type)));

                bool has_time = !(ldt.hour() == 0 && ldt.minute() == 0 && ldt.second() == 0);
                size_t size = 1 // number of bytes following
                    + 2 // year
                    + 1 // month
                    + 1; // day
                payload_size += size;
                if (has_time)
                {
                    size_t additional_size = 1 // hour
                        + 1 // minute
                        + 1; // second
                    payload_size += additional_size;
                }
                break;
            }
            case TypeIndex::DateTime64: {
                auto [components, scale] = getDateTime64ComponentsWithScale(data_type, col);
                if (scale > 6)
                {
                    // MySQL Timestamp has max scale of 6
                    components.fractional /= static_cast<int>(pow(10, scale - 6));
                }

                LocalDateTime ldt = LocalDateTime(components.whole, DateLUT::instance(getDateTimeTimezone(*data_type)));

                bool has_microseconds = components.fractional != 0;
                bool has_time = !(ldt.hour() == 0 && ldt.minute() == 0 && ldt.second() == 0);
                size_t size = 1 // number of bytes following
                    + 2 // year
                    + 1 // month
                    + 1; // day
                payload_size += size;
                if (has_microseconds)
                {
                    size_t additional_size = 1 // hour
                        + 1 // minute
                        + 1 // second
                        + 4; // microsecond;
                    payload_size += additional_size;
                }
                else if (has_time)
                {
                    size_t additional_size = 1 // hour
                        + 1 // minute
                        + 1; // second
                    payload_size += additional_size;
                }
                break;
            }
            // All other types including all Decimal types are string<lenenc> in binary
            default:
                WriteBufferFromOwnString ostr;
                serializations[i]->serializeText(*columns[i], row_num, ostr, format_settings);
                payload_size += getLengthEncodedStringSize(ostr.str());
                serialized[i] = std::move(ostr.str());
                break;
        }
    }
}

size_t ResultSetRow::getPayloadSize() const
{
    return payload_size;
}

void ResultSetRow::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(static_cast<char>(0x00));
    buffer.write(null_bitmap.data(), null_bitmap_size);
    for (size_t i = 0; i < columns.size(); ++i)
    {
        ColumnPtr col = getColumn(i);
        if (col->isNullAt(row_num))
            continue;

        DataTypePtr data_type = removeLowCardinality(removeNullable((data_types[i])));
        TypeIndex type_index = data_type->getTypeId();
        switch (type_index)
        {
            case TypeIndex::Int8: {
                Int8 value = assert_cast<const ColumnVector<Int8> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 1);
                break;
            }
            case TypeIndex::UInt8: {
                UInt8 value = assert_cast<const ColumnVector<UInt8> &>(*col).getData()[row_num];
                if (data_type->getName() == "Bool")
                    buffer.write(static_cast<char>(1));
                buffer.write(reinterpret_cast<char *>(&value), 1);
                break;
            }
            case TypeIndex::Int16: {
                Int16 value = assert_cast<const ColumnVector<Int16> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 2);
                break;
            }
            case TypeIndex::UInt16: {
                UInt16 value = assert_cast<const ColumnVector<UInt16> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 2);
                break;
            }
            case TypeIndex::Int32: {
                Int32 value = assert_cast<const ColumnVector<Int32> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 4);
                break;
            }
            case TypeIndex::UInt32: {
                UInt32 value = assert_cast<const ColumnVector<UInt32> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 4);
                break;
            }
            case TypeIndex::Float32: {
                Float32 value = assert_cast<const ColumnVector<Float32> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 4);
                break;
            }
            case TypeIndex::Int64: {
                Int64 value = assert_cast<const ColumnVector<Int64> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 8);
                break;
            }
            case TypeIndex::UInt64: {
                UInt64 value = assert_cast<const ColumnVector<UInt64> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 8);
                break;
            }
            case TypeIndex::Float64: {
                Float64 value = assert_cast<const ColumnVector<Float64> &>(*col).getData()[row_num];
                buffer.write(reinterpret_cast<char *>(&value), 8);
                break;
            }
            case TypeIndex::Date: {
                UInt16 value = assert_cast<const ColumnVector<UInt16> &>(*col).getData()[row_num];
                LocalDate ld = LocalDate(DayNum(value));
                buffer.write(static_cast<char>(4)); // bytes_following
                UInt16 year = ld.year();
                UInt8 month = ld.month();
                UInt8 day = ld.day();
                buffer.write(reinterpret_cast<const char *>(&year), 2);
                buffer.write(reinterpret_cast<const char *>(&month), 1);
                buffer.write(reinterpret_cast<const char *>(&day), 1);
                break;
            }
            case TypeIndex::Date32: {
                Int32 value = assert_cast<const ColumnVector<Int32> &>(*col).getData()[row_num];
                LocalDate ld = LocalDate(ExtendedDayNum(value));
                buffer.write(static_cast<char>(4)); // bytes_following
                UInt16 year = ld.year();
                UInt8 month = ld.month();
                UInt8 day = ld.day();
                buffer.write(reinterpret_cast<const char *>(&year), 2);
                buffer.write(reinterpret_cast<const char *>(&month), 1);
                buffer.write(reinterpret_cast<const char *>(&day), 1);
                break;
            }
            case TypeIndex::DateTime: {
                UInt32 value = assert_cast<const ColumnVector<UInt32> &>(*col).getData()[row_num];
                String timezone = getDateTimeTimezone(*data_type);
                LocalDateTime ldt = LocalDateTime(value, DateLUT::instance(timezone));
                UInt16 year = ldt.year();
                UInt8 month = ldt.month();
                UInt8 day = ldt.day();
                UInt8 hour = ldt.hour();
                UInt8 minute = ldt.minute();
                UInt8 second = ldt.second();
                bool has_time = !(hour == 0 && minute == 0 && second == 0);
                size_t bytes_following = has_time ? 7 : 4;
                buffer.write(reinterpret_cast<const char *>(&bytes_following), 1);
                buffer.write(reinterpret_cast<const char *>(&year), 2);
                buffer.write(reinterpret_cast<const char *>(&month), 1);
                buffer.write(reinterpret_cast<const char *>(&day), 1);
                if (has_time)
                {
                    buffer.write(reinterpret_cast<const char *>(&hour), 1);
                    buffer.write(reinterpret_cast<const char *>(&minute), 1);
                    buffer.write(reinterpret_cast<const char *>(&second), 1);
                }
                break;
            }
            case TypeIndex::DateTime64: {
                auto [components, scale] = getDateTime64ComponentsWithScale(data_type, col);
                if (components.fractional != 0)
                {
                    if (scale > 6)
                    {
                        // MySQL Timestamp has max scale of 6
                        components.fractional /= static_cast<int>(pow(10, scale - 6));
                    }
                    else
                    {
                        // fractional == 1 is a different microsecond value depending on the scale
                        // Scale 1 = 100000
                        // Scale 2 = 010000
                        // Scale 3 = 001000
                        // Scale 4 = 000100
                        // Scale 5 = 000010
                        // Scale 6 = 000001
                        components.fractional *= static_cast<int>(pow(10, 6 - scale));
                    }
                }

                String timezone = getDateTimeTimezone(*data_type);
                LocalDateTime ldt = LocalDateTime(components.whole, DateLUT::instance(timezone));
                UInt16 year = ldt.year();
                UInt8 month = ldt.month();
                UInt8 day = ldt.day();
                UInt8 hour = ldt.hour();
                UInt8 minute = ldt.minute();
                UInt8 second = ldt.second();

                bool has_time = !(hour == 0 && minute == 0 && second == 0);
                bool has_microseconds = components.fractional != 0;

                if (has_microseconds)
                {
                    buffer.write(static_cast<char>(11)); // bytes_following
                    buffer.write(reinterpret_cast<const char *>(&year), 2);
                    buffer.write(reinterpret_cast<const char *>(&month), 1);
                    buffer.write(reinterpret_cast<const char *>(&day), 1);
                    buffer.write(reinterpret_cast<const char *>(&hour), 1);
                    buffer.write(reinterpret_cast<const char *>(&minute), 1);
                    buffer.write(reinterpret_cast<const char *>(&second), 1);
                    buffer.write(reinterpret_cast<const char *>(&components.fractional), 4);
                }
                else if (has_time)
                {
                    buffer.write(static_cast<char>(7)); // bytes_following
                    buffer.write(reinterpret_cast<const char *>(&year), 2);
                    buffer.write(reinterpret_cast<const char *>(&month), 1);
                    buffer.write(reinterpret_cast<const char *>(&day), 1);
                    buffer.write(reinterpret_cast<const char *>(&hour), 1);
                    buffer.write(reinterpret_cast<const char *>(&minute), 1);
                    buffer.write(reinterpret_cast<const char *>(&second), 1);
                }
                else
                {
                    buffer.write(static_cast<char>(4)); // bytes_following
                    buffer.write(reinterpret_cast<const char *>(&year), 2);
                    buffer.write(reinterpret_cast<const char *>(&month), 1);
                    buffer.write(reinterpret_cast<const char *>(&day), 1);
                }
                break;
            }
            // All other types including all Decimal types are string<lenenc> in binary
            default:
                writeLengthEncodedString(serialized[i], buffer);
                break;
        }
    }
}

ResultSetRow::DateTime64ComponentsWithScale ResultSetRow::getDateTime64ComponentsWithScale(DataTypePtr data_type, ColumnPtr col) const
{
    const auto * date_time_type = typeid_cast<const DataTypeDateTime64 *>(data_type.get());

    static constexpr UInt32 MaxScale = DecimalUtils::max_precision<DateTime64>;
    UInt32 scale = std::min(MaxScale, date_time_type->getScale());

    const auto value = assert_cast<const ColumnDateTime64 &>(*col).getData()[row_num];
    auto components = DecimalUtils::split(value, scale);

    using T = typename DateTime64::NativeType;
    if (value.value < 0 && components.fractional)
    {
        components.fractional = DecimalUtils::scaleMultiplier<T>(scale) + (components.whole ? T(-1) : T(1)) * components.fractional;
        --components.whole;
    }

    return {components, scale};
}

ColumnPtr ResultSetRow::getColumn(size_t i) const
{
    ColumnPtr col = columns[i]->convertToFullIfNeeded();
    if (col->isNullable())
        return assert_cast<const ColumnNullable &>(*col).getNestedColumnPtr();
    return col;
}
}
}
}
