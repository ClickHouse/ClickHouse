#include <Columns/IColumn.h>
#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>
#include <Core/MySQL/PacketsProtocolBinary.h>
#include <Poco/DateTime.h>
#include <Poco/Timestamp.h>
#include "Columns/ColumnLowCardinality.h"
#include "Columns/ColumnVector.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeNullable.h"
#include "Formats/FormatSettings.h"
#include "IO/WriteBufferFromString.h"
#include "base/types.h"

namespace DB
{

namespace MySQLProtocol
{

    namespace ProtocolBinary
    {
        ResultSetRow::ResultSetRow(
            const Serializations & serializations_, const DataTypes & data_types_, const Columns & columns_, int row_num_)
            : row_num(row_num_), columns(columns_), data_types(data_types_), serializations(serializations_)
        {
            /// See https://dev.mysql.com/doc/dev/mysql-server/8.1.0/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
            payload_size = 1 + null_bitmap_size;
            // LOG_TRACE(&Poco::Logger::get("ResultSetRow"), "Null bitmap size: {}", null_bitmap_size);
            FormatSettings format_settings;
            for (size_t i = 0; i < columns.size(); ++i)
            {
                ColumnPtr col = columns[i];

                if (col->isNullAt(row_num))
                {
                    null_bitmap[i / 8] |= 1 << i % 8;
                }

                TypeIndex type_index = removeNullable(removeLowCardinality(data_types[i]))->getTypeId();
                switch (type_index)
                {
                    case TypeIndex::Int8:
                    case TypeIndex::UInt8:
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
                    case TypeIndex::Date: {
                        UInt64 value = col->get64(row_num);
                        if (value == 0)
                        {
                            payload_size += 1; // length only, no other fields
                        }
                        else
                        {
                            payload_size += 5;
                        }
                        break;
                    }
                    case TypeIndex::DateTime: {
                        UInt64 value = col->get64(row_num);
                        if (value == 0)
                        {
                            payload_size += 1; // length only, no other fields
                        }
                        else
                        {
                            Poco::DateTime dt = Poco::DateTime(Poco::Timestamp(value * 1000 * 1000));
                            if (dt.second() == 0 && dt.minute() == 0 && dt.hour() == 0)
                            {
                                payload_size += 5;
                            }
                            else
                            {
                                payload_size += 8;
                            }
                        }
                        break;
                    }
                    default:
                        WriteBufferFromOwnString ostr;
                        serializations[i]->serializeText(*columns[i], row_num, ostr, format_settings);
                        payload_size += getLengthEncodedStringSize(ostr.str());
                        serialized[i] = std::move(ostr.str());
                        break;
                }
            }
        }

        void ResultSetRow::writePayloadImpl(WriteBuffer & buffer) const
        {
            buffer.write(static_cast<char>(0x00));
            buffer.write(null_bitmap.data(), null_bitmap_size);
            for (size_t i = 0; i < columns.size(); ++i)
            {
                ColumnPtr col = columns[i];
                if (col->isNullAt(row_num))
                {
                    continue; // NULLs are stored in the null bitmap only
                }

                TypeIndex type_index = removeNullable(removeLowCardinality(data_types[i]))->getTypeId();
                switch (type_index)
                {
                    case TypeIndex::UInt8: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 1);
                        break;
                    }
                    case TypeIndex::UInt16: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 2);
                        break;
                    }
                    case TypeIndex::UInt32: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 4);
                        break;
                    }
                    case TypeIndex::UInt64: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 8);
                        break;
                    }
                    case TypeIndex::Int8: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 1);
                        break;
                    }
                    case TypeIndex::Int16: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 2);
                        break;
                    }
                    case TypeIndex::Int32: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 4);
                        break;
                    }
                    case TypeIndex::Int64: {
                        UInt64 value = col->get64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 8);
                        break;
                    }
                    case TypeIndex::Float32: {
                        Float32 value = col->getFloat32(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 4);
                        break;
                    }
                    case TypeIndex::Float64: {
                        Float64 value = col->getFloat64(row_num);
                        buffer.write(reinterpret_cast<char *>(&value), 8);
                        break;
                    }
                    case TypeIndex::Date: {
                        UInt64 value = col->get64(row_num);
                        if (value != 0)
                        {
                            Poco::DateTime dt = Poco::DateTime(Poco::Timestamp(value * 1000 * 1000));
                            buffer.write(static_cast<char>(4)); // bytes_following
                            int year = dt.year();
                            int month = dt.month();
                            int day = dt.day();
                            buffer.write(reinterpret_cast<const char *>(&year), 2);
                            buffer.write(reinterpret_cast<const char *>(&month), 1);
                            buffer.write(reinterpret_cast<const char *>(&day), 1);
                        }
                        else
                        {
                            buffer.write(static_cast<char>(0));
                        }
                        break;
                    }
                    case TypeIndex::DateTime: {
                        UInt64 value = col->get64(row_num);
                        if (value != 0)
                        {
                            Poco::DateTime dt = Poco::DateTime(Poco::Timestamp(value * 1000 * 1000));
                            bool is_date_time = !(dt.hour() == 0 && dt.minute() == 0 && dt.second() == 0);
                            size_t bytes_following = is_date_time ? 7 : 4;
                            buffer.write(reinterpret_cast<const char *>(&bytes_following), 1);
                            int year = dt.year();
                            int month = dt.month();
                            int day = dt.day();
                            buffer.write(reinterpret_cast<const char *>(&year), 2);
                            buffer.write(reinterpret_cast<const char *>(&month), 1);
                            buffer.write(reinterpret_cast<const char *>(&day), 1);
                            if (is_date_time)
                            {
                                int hour = dt.hourAMPM();
                                int minute = dt.minute();
                                int second = dt.second();
                                buffer.write(reinterpret_cast<const char *>(&hour), 1);
                                buffer.write(reinterpret_cast<const char *>(&minute), 1);
                                buffer.write(reinterpret_cast<const char *>(&second), 1);
                            }
                        }
                        else
                        {
                            buffer.write(static_cast<char>(0));
                        }
                        break;
                    }
                    default:
                        writeLengthEncodedString(serialized[i], buffer);
                        break;
                }
            }
        }

        size_t ResultSetRow::getPayloadSize() const
        {
            return payload_size;
        };
    }
}
}
