#include "MySQLReplication.h"

#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <common/DateLUT.h>
#include <Common/FieldVisitors.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolText.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace MySQLReplication
{
    using namespace MySQLProtocol;
    using namespace MySQLProtocol::Generic;
    using namespace MySQLProtocol::ProtocolText;

    /// https://dev.mysql.com/doc/internals/en/binlog-event-header.html
    void EventHeader::parse(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&type), 1);
        payload.readStrict(reinterpret_cast<char *>(&server_id), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_size), 4);
        payload.readStrict(reinterpret_cast<char *>(&log_pos), 4);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);
    }

    void EventHeader::dump(std::ostream & out) const
    {
        out << "\n=== " << to_string(this->type) << " ===" << std::endl;
        out << "Timestamp: " << this->timestamp << std::endl;
        out << "Event Type: " << this->type << std::endl;
        out << "Server ID: " << this->server_id << std::endl;
        out << "Event Size: " << this->event_size << std::endl;
        out << "Log Pos: " << this->log_pos << std::endl;
        out << "Flags: " << this->flags << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/format-description-event.html
    void FormatDescriptionEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&binlog_version), 2);
        assert(binlog_version == EVENT_VERSION_V4);
        payload.readStrict(reinterpret_cast<char *>(server_version.data()), 50);
        payload.readStrict(reinterpret_cast<char *>(&create_timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_header_length), 1);
        assert(event_header_length == EVENT_HEADER_LENGTH);

        size_t len = header.event_size - (2 + 50 + 4 + 1 + EVENT_HEADER_LENGTH) - 1;
        event_type_header_length.resize(len);
        payload.readStrict(reinterpret_cast<char *>(event_type_header_length.data()), len);
    }

    void FormatDescriptionEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "Binlog Version: " << this->binlog_version << std::endl;
        out << "Server Version: " << this->server_version << std::endl;
        out << "Create Timestamp: " << this->create_timestamp << std::endl;
        out << "Event Header Len: " << std::to_string(this->event_header_length) << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/rotate-event.html
    void RotateEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&position), 8);
        size_t len = header.event_size - EVENT_HEADER_LENGTH - 8 - CHECKSUM_CRC32_SIGNATURE_LENGTH;
        next_binlog.resize(len);
        payload.readStrict(reinterpret_cast<char *>(next_binlog.data()), len);
    }

    void RotateEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "Position: " << this->position << std::endl;
        out << "Next Binlog: " << this->next_binlog << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/query-event.html
    void QueryEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&thread_id), 4);
        payload.readStrict(reinterpret_cast<char *>(&exec_time), 4);
        payload.readStrict(reinterpret_cast<char *>(&schema_len), 1);
        payload.readStrict(reinterpret_cast<char *>(&error_code), 2);
        payload.readStrict(reinterpret_cast<char *>(&status_len), 2);

        status.resize(status_len);
        payload.readStrict(reinterpret_cast<char *>(status.data()), status_len);

        schema.resize(schema_len);
        payload.readStrict(reinterpret_cast<char *>(schema.data()), schema_len);
        payload.ignore(1);

        size_t len
            = header.event_size - EVENT_HEADER_LENGTH - 4 - 4 - 1 - 2 - 2 - status_len - schema_len - 1 - CHECKSUM_CRC32_SIGNATURE_LENGTH;
        query.resize(len);
        payload.readStrict(reinterpret_cast<char *>(query.data()), len);
        if (query.rfind("BEGIN", 0) == 0)
        {
            typ = BEGIN;
        }
        else if (query.rfind("XA", 0) == 0)
        {
            if (query.rfind("XA ROLLBACK", 0) == 0)
                throw ReplicationError("ParseQueryEvent: Unsupported query event:" + query, ErrorCodes::UNKNOWN_EXCEPTION);
            typ = XA;
        }
        else if (query.rfind("SAVEPOINT", 0) == 0)
        {
            throw ReplicationError("ParseQueryEvent: Unsupported query event:" + query, ErrorCodes::UNKNOWN_EXCEPTION);
        }
    }

    void QueryEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "Thread ID: " << this->thread_id << std::endl;
        out << "Execution Time: " << this->exec_time << std::endl;
        out << "Schema Len: " << std::to_string(this->schema_len) << std::endl;
        out << "Error Code: " << this->error_code << std::endl;
        out << "Status Len: " << this->status_len << std::endl;
        out << "Schema: " << this->schema << std::endl;
        out << "Query: " << this->query << std::endl;
    }

    void XIDEvent::parseImpl(ReadBuffer & payload) { payload.readStrict(reinterpret_cast<char *>(&xid), 8); }

    void XIDEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "XID: " << this->xid << std::endl;
    }

    void TableMapEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&table_id), 6);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);

        payload.readStrict(reinterpret_cast<char *>(&schema_len), 1);
        schema.resize(schema_len);
        payload.readStrict(reinterpret_cast<char *>(schema.data()), schema_len);
        payload.ignore(1);

        payload.readStrict(reinterpret_cast<char *>(&table_len), 1);
        table.resize(table_len);
        payload.readStrict(reinterpret_cast<char *>(table.data()), table_len);
        payload.ignore(1);

        column_count = readLengthEncodedNumber(payload);
        for (auto i = 0U; i < column_count; i++)
        {
            UInt8 v = 0x00;
            payload.readStrict(reinterpret_cast<char *>(&v), 1);
            column_type.emplace_back(v);
        }

        String meta;
        readLengthEncodedString(meta, payload);
        parseMeta(meta);

        size_t null_bitmap_size = (column_count + 7) / 8;
        readBitmap(payload, null_bitmap, null_bitmap_size);

        /// Ignore MySQL 8.0 optional metadata fields.
        /// https://mysqlhighavailability.com/more-metadata-is-written-into-binary-log/
        payload.ignore(payload.available() - CHECKSUM_CRC32_SIGNATURE_LENGTH);
    }

    /// Types that do not used in the binlog event:
    /// MYSQL_TYPE_ENUM
    /// MYSQL_TYPE_SET
    /// MYSQL_TYPE_TINY_BLOB
    /// MYSQL_TYPE_MEDIUM_BLOB
    /// MYSQL_TYPE_LONG_BLOB
    void TableMapEvent::parseMeta(String meta)
    {
        auto pos = 0;
        column_meta.reserve(column_count);
        for (auto i = 0U; i < column_count; i++)
        {
            UInt16 typ = column_type[i];
            switch (typ)
            {
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_NULL:
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_TIME:
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_NEWDATE: {
                    /// No data here.
                    column_meta.emplace_back(0);
                    break;
                }

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_TIMESTAMP2:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIME2:
                case MYSQL_TYPE_JSON:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY: {
                    column_meta.emplace_back(UInt16(meta[pos]));
                    pos += 1;
                    break;
                }
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_STRING: {
                    auto b0 = UInt16(meta[pos] << 8);
                    auto b1 = UInt8(meta[pos + 1]);
                    column_meta.emplace_back(UInt16(b0 + b1));
                    pos += 2;
                    break;
                }

                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING: {
                    auto b0 = UInt8(meta[pos]);
                    auto b1 = UInt16(meta[pos + 1] << 8);
                    column_meta.emplace_back(UInt16(b0 + b1));
                    pos += 2;
                    break;
                }
                default:
                    throw ReplicationError("ParseMetaData: Unhandled data type:" + std::to_string(typ), ErrorCodes::UNKNOWN_EXCEPTION);
            }
        }
    }

    void TableMapEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "Table ID: " << this->table_id << std::endl;
        out << "Flags: " << this->flags << std::endl;
        out << "Schema Len: " << std::to_string(this->schema_len) << std::endl;
        out << "Schema: " << this->schema << std::endl;
        out << "Table Len: " << std::to_string(this->table_len) << std::endl;
        out << "Table: " << this->table << std::endl;
        out << "Column Count: " << this->column_count << std::endl;
        for (auto i = 0U; i < column_count; i++)
        {
            out << "Column Type [" << i << "]: " << std::to_string(column_type[i]) << ", Meta: " << column_meta[i] << std::endl;
        }
        out << "Null Bitmap: " << this->null_bitmap << std::endl;
    }

    void RowsEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&table_id), 6);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);

        /// This extra_data_len contains the 2 bytes length.
        payload.readStrict(reinterpret_cast<char *>(&extra_data_len), 2);
        payload.ignore(extra_data_len - 2);

        number_columns = readLengthEncodedNumber(payload);
        size_t columns_bitmap_size = (number_columns + 7) / 8;
        switch (header.type)
        {
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
                readBitmap(payload, columns_present_bitmap1, columns_bitmap_size);
                readBitmap(payload, columns_present_bitmap2, columns_bitmap_size);
                break;
            default:
                readBitmap(payload, columns_present_bitmap1, columns_bitmap_size);
                break;
        }

        while (payload.available() > CHECKSUM_CRC32_SIGNATURE_LENGTH)
        {
            parseRow(payload, columns_present_bitmap1);
            if (header.type == UPDATE_ROWS_EVENT_V1 || header.type == UPDATE_ROWS_EVENT_V2)
            {
                parseRow(payload, columns_present_bitmap2);
            }
        }
    }

    /// Types that do not used in the binlog event:
    /// MYSQL_TYPE_ENUM
    /// MYSQL_TYPE_SET
    /// MYSQL_TYPE_TINY_BLOB
    /// MYSQL_TYPE_MEDIUM_BLOB
    /// MYSQL_TYPE_LONG_BLOB
    void RowsEvent::parseRow(ReadBuffer & payload, Bitmap & bitmap)
    {
        Tuple row;
        UInt32 null_index = 0;

        UInt32 re_count = 0;
        for (auto i = 0U; i < number_columns; i++)
        {
            if (bitmap[i])
                re_count++;
        }
        re_count = (re_count + 7) / 8;
        boost::dynamic_bitset<> columns_null_set;
        readBitmap(payload, columns_null_set, re_count);

        for (auto i = 0U; i < number_columns; i++)
        {
            UInt32 field_len = 0;

            /// Column not presents.
            if (!bitmap[i])
                continue;

            if (columns_null_set[null_index])
            {
                row.push_back(Field{Null{}});
            }
            else
            {
                auto meta = table_map->column_meta[i];
                auto field_type = table_map->column_type[i];
                if (field_type == MYSQL_TYPE_STRING)
                {
                    if (meta >= 256)
                    {
                        UInt8 byte0 = meta >> 8;
                        UInt8 byte1 = meta & 0xff;

                        if ((byte0 & 0x30) != 0x30)
                        {
                            field_len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                            field_type = byte0 | 0x30;
                        }
                        else
                        {
                            field_len = byte1;
                            field_type = byte0;
                        }
                    }
                    else
                    {
                        field_len = meta;
                    }
                }

                switch (field_type)
                {
                    case MYSQL_TYPE_TINY: {
                        UInt8 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 1);
                        row.push_back(Field{UInt8{val}});
                        break;
                    }
                    case MYSQL_TYPE_SHORT: {
                        UInt16 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 2);
                        row.push_back(Field{UInt16{val}});
                        break;
                    }
                    case MYSQL_TYPE_INT24: {
                        Int32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 3);
                        row.push_back(Field{Int32{val}});
                        break;
                    }
                    case MYSQL_TYPE_LONG: {
                        UInt32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{UInt32{val}});
                        break;
                    }
                    case MYSQL_TYPE_LONGLONG: {
                        UInt64 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 8);
                        row.push_back(Field{UInt64{val}});
                        break;
                    }
                    case MYSQL_TYPE_FLOAT: {
                        Float32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{Float32{val}});
                        break;
                    }
                    case MYSQL_TYPE_DOUBLE: {
                        Float64 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 8);
                        row.push_back(Field{Float64{val}});
                        break;
                    }
                    case MYSQL_TYPE_TIMESTAMP: {
                        UInt32 val = 0;

                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{val});
                        break;
                    }
                    case MYSQL_TYPE_TIME: {
                        UInt32 i24 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&i24), 3);

                        String time_buff;
                        time_buff.resize(8);
                        sprintf(
                            time_buff.data(),
                            "%02d:%02d:%02d",
                            static_cast<int>(i24 / 10000),
                            static_cast<int>(i24 % 10000) / 100,
                            static_cast<int>(i24 % 100));
                        row.push_back(Field{String{time_buff}});
                        break;
                    }
                    case MYSQL_TYPE_DATE: {
                        UInt32 i24 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&i24), 3);

                        DayNum date_day_number = DateLUT::instance().makeDayNum(
                            static_cast<int>((i24 >> 9) & 0x7fff), static_cast<int>((i24 >> 5) & 0xf), static_cast<int>(i24 & 0x1f));

                        row.push_back(Field(date_day_number.toUnderType()));
                        break;
                    }
                    case MYSQL_TYPE_YEAR: {
                        Int32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 1);

                        String time_buff;
                        time_buff.resize(4);
                        sprintf(time_buff.data(), "%04d", (val + 1900));
                        row.push_back(Field{String{time_buff}});
                        break;
                    }
                    case MYSQL_TYPE_TIME2: {
                        UInt32 val = 0, frac_part = 0;

                        readBigEndianStrict(payload, reinterpret_cast<char *>(&val), 3);
                        if (readBits(val, 0, 1, 24) == 0)
                        {
                            val = ~val + 1;
                        }
                        UInt32 hour = readBits(val, 2, 10, 24);
                        UInt32 minute = readBits(val, 12, 6, 24);
                        UInt32 second = readBits(val, 18, 6, 24);
                        readTimeFractionalPart(payload, reinterpret_cast<char *>(&frac_part), meta);

                        if (frac_part != 0)
                        {
                            String time_buff;
                            time_buff.resize(15);
                            sprintf(
                                time_buff.data(),
                                "%02d:%02d:%02d.%06d",
                                static_cast<int>(hour),
                                static_cast<int>(minute),
                                static_cast<int>(second),
                                static_cast<int>(frac_part));
                            row.push_back(Field{String{time_buff}});
                        }
                        else
                        {
                            String time_buff;
                            time_buff.resize(8);
                            sprintf(
                                time_buff.data(),
                                "%02d:%02d:%02d",
                                static_cast<int>(hour),
                                static_cast<int>(minute),
                                static_cast<int>(second));
                            row.push_back(Field{String{time_buff}});
                        }
                        break;
                    }
                    case MYSQL_TYPE_DATETIME2: {
                        Int64 val = 0, fsp = 0;
                        readBigEndianStrict(payload, reinterpret_cast<char *>(&val), 5);
                        readTimeFractionalPart(payload, reinterpret_cast<char *>(&fsp), meta);

                        UInt32 year_month = readBits(val, 1, 17, 40);
                        time_t date_time = DateLUT::instance().makeDateTime(
                            year_month / 13, year_month % 13, readBits(val, 18, 5, 40)
                            , readBits(val, 23, 5, 40), readBits(val, 28, 6, 40), readBits(val, 34, 6, 40)
                        );

                        row.push_back(Field{UInt32(date_time)});
                        break;
                    }
                    case MYSQL_TYPE_TIMESTAMP2: {
                        UInt32 sec = 0, fsp = 0;
                        readBigEndianStrict(payload, reinterpret_cast<char *>(&sec), 4);
                        readTimeFractionalPart(payload, reinterpret_cast<char *>(&fsp), meta);
                        row.push_back(Field{sec});
                        break;
                    }
                    case MYSQL_TYPE_NEWDECIMAL: {
                        Int8 digits_per_integer = 9;
                        Int8 precision = meta >> 8;
                        Int8 decimals = meta & 0xff;
                        const char compressed_byte_map[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

                        Int8 integral = (precision - decimals);
                        UInt32 uncompressed_integers = integral / digits_per_integer;
                        UInt32 uncompressed_decimals = decimals / digits_per_integer;
                        UInt32 compressed_integers = integral - (uncompressed_integers * digits_per_integer);
                        UInt32 compressed_decimals = decimals - (uncompressed_decimals * digits_per_integer);

                        String buff;
                        UInt32 bytes_to_read = uncompressed_integers * 4 + compressed_byte_map[compressed_integers]
                            + uncompressed_decimals * 4 + compressed_byte_map[compressed_decimals];
                        buff.resize(bytes_to_read);
                        payload.readStrict(reinterpret_cast<char *>(buff.data()), bytes_to_read);

                        String format;
                        format.resize(0);

                        bool is_negative = ((buff[0] & 0x80) == 0);
                        if (is_negative)
                        {
                            format += "-";
                        }
                        buff[0] ^= 0x80;

                        ReadBufferFromString reader(buff);
                        /// Compressed part.
                        if (compressed_integers != 0)
                        {
                            Int64 val = 0;
                            UInt8 to_read = compressed_byte_map[compressed_integers];
                            readBigEndianStrict(reader, reinterpret_cast<char *>(&val), to_read);
                            format += std::to_string(val);
                        }

                        for (auto k = 0U; k < uncompressed_integers; k++)
                        {
                            UInt32 val = 0;
                            readBigEndianStrict(reader, reinterpret_cast<char *>(&val), 4);
                            format += std::to_string(val);
                        }
                        format += ".";
                        for (auto k = 0U; k < uncompressed_decimals; k++)
                        {
                            UInt32 val = 0;
                            reader.readStrict(reinterpret_cast<char *>(&val), 4);
                            format += std::to_string(val);
                        }

                        /// Compressed part.
                        if (compressed_decimals != 0)
                        {
                            Int64 val = 0;
                            String compressed_buff;
                            UInt8 to_read = compressed_byte_map[compressed_decimals];
                            switch (to_read)
                            {
                                case 1: {
                                    reader.readStrict(reinterpret_cast<char *>(&val), 1);
                                    break;
                                }
                                case 2: {
                                    readBigEndianStrict(reader, reinterpret_cast<char *>(&val), 2);
                                    break;
                                }
                                case 3: {
                                    readBigEndianStrict(reader, reinterpret_cast<char *>(&val), 3);
                                    break;
                                }
                                case 4: {
                                    readBigEndianStrict(reader, reinterpret_cast<char *>(&val), 4);
                                    break;
                                }
                                default:
                                    break;
                            }
                            format += std::to_string(val);
                        }
                        row.push_back(Field{String{format}});
                        break;
                    }
                    case MYSQL_TYPE_ENUM: {
                        Int32 val = 0;
                        Int32 len = (meta & 0xff);
                        switch (len)
                        {
                            case 1: {
                                payload.readStrict(reinterpret_cast<char *>(&val), 1);
                                break;
                            }
                            case 2: {
                                payload.readStrict(reinterpret_cast<char *>(&val), 2);
                                break;
                            }
                            default:
                                break;
                        }
                        row.push_back(Field{Int32{val}});
                        break;
                    }
                    case MYSQL_TYPE_BIT: {
                        UInt32 bits = ((meta >> 8) * 8) + (meta & 0xff);
                        UInt32 size = (bits + 7) / 8;

                        Bitmap bitmap1;
                        readBitmap(payload, bitmap1, size);
                        row.push_back(Field{UInt64{bitmap1.to_ulong()}});
                        break;
                    }
                    case MYSQL_TYPE_SET: {
                        UInt32 size = (meta & 0xff);

                        Bitmap bitmap1;
                        readBitmap(payload, bitmap1, size);
                        row.push_back(Field{UInt64{bitmap1.to_ulong()}});
                        break;
                    }
                    case MYSQL_TYPE_VARCHAR:
                    case MYSQL_TYPE_VAR_STRING: {
                        uint32_t size = 0;
                        if (meta < 256)
                        {
                            payload.readStrict(reinterpret_cast<char *>(&size), 1);
                        }
                        else
                        {
                            payload.readStrict(reinterpret_cast<char *>(&size), 2);
                        }

                        String val;
                        val.resize(size);
                        payload.readStrict(reinterpret_cast<char *>(val.data()), size);
                        row.push_back(Field{String{val}});
                        break;
                    }
                    case MYSQL_TYPE_STRING: {
                        UInt32 size = 0;
                        if (field_len < 256)
                        {
                            payload.readStrict(reinterpret_cast<char *>(&size), 1);
                        }
                        else
                        {
                            payload.readStrict(reinterpret_cast<char *>(&size), 2);
                        }

                        String val;
                        val.resize(size);
                        payload.readStrict(reinterpret_cast<char *>(val.data()), size);
                        row.push_back(Field{String{val}});
                        break;
                    }
                    case MYSQL_TYPE_GEOMETRY:
                    case MYSQL_TYPE_BLOB: {
                        UInt32 size = 0;
                        switch (meta)
                        {
                            case 1: {
                                payload.readStrict(reinterpret_cast<char *>(&size), 1);
                                break;
                            }
                            case 2: {
                                payload.readStrict(reinterpret_cast<char *>(&size), 2);
                                break;
                            }
                            case 3: {
                                payload.readStrict(reinterpret_cast<char *>(&size), 3);
                                break;
                            }
                            case 4: {
                                payload.readStrict(reinterpret_cast<char *>(&size), 4);
                                break;
                            }
                            default:
                                break;
                        }

                        String val;
                        val.resize(size);
                        payload.readStrict(reinterpret_cast<char *>(val.data()), size);
                        row.push_back(Field{String{val}});
                        break;
                    }
                    case MYSQL_TYPE_JSON: {
                        UInt32 size = 0;
                        payload.readStrict(reinterpret_cast<char *>(&size), meta);

                        String val;
                        val.resize(size);
                        payload.readStrict(reinterpret_cast<char *>(val.data()), size);
                        row.push_back(Field{String{val}});
                        break;
                    }
                    default:
                        throw ReplicationError(
                            "ParseRow: Unhandled MySQL field type:" + std::to_string(field_type), ErrorCodes::UNKNOWN_EXCEPTION);
                }
            }
            null_index++;
        }
        rows.push_back(row);
    }

    void RowsEvent::dump(std::ostream & out) const
    {
        FieldVisitorToString to_string;

        header.dump(out);
        out << "Schema: " << this->schema << std::endl;
        out << "Table: " << this->table << std::endl;
        for (auto i = 0U; i < rows.size(); i++)
        {
            out << "Row[" << i << "]: " << applyVisitor(to_string, rows[i]) << std::endl;
        }
    }

    void GTIDEvent::parseImpl(ReadBuffer & payload)
    {
        /// We only care uuid:seq_no parts assigned to GTID_NEXT.
        payload.readStrict(reinterpret_cast<char *>(&commit_flag), 1);

        // MySQL UUID is big-endian.
        UInt64 high = 0UL, low = 0UL;
        readBigEndianStrict(payload, reinterpret_cast<char *>(&low), 8);
        gtid.uuid.toUnderType().low = low;

        readBigEndianStrict(payload, reinterpret_cast<char *>(&high), 8);
        gtid.uuid.toUnderType().high = high;

        payload.readStrict(reinterpret_cast<char *>(&gtid.seq_no), 8);

        /// Skip others.
        payload.ignore(payload.available() - CHECKSUM_CRC32_SIGNATURE_LENGTH);
    }

    void GTIDEvent::dump(std::ostream & out) const
    {
        auto gtid_next = gtid.uuid.toUnderType().toHexString() + ":" + std::to_string(gtid.seq_no);

        header.dump(out);
        out << "GTID Next: " << gtid_next << std::endl;
    }

    void DryRunEvent::parseImpl(ReadBuffer & payload) { payload.ignore(header.event_size - EVENT_HEADER_LENGTH); }

    void DryRunEvent::dump(std::ostream & out) const
    {
        header.dump(out);
        out << "[DryRun Event]" << std::endl;
    }

    /// Update binlog name/position/gtid based on the event type.
    void Position::update(BinlogEventPtr event)
    {
        switch (event->header.type)
        {
            case FORMAT_DESCRIPTION_EVENT:
            case QUERY_EVENT:
            case XID_EVENT: {
                binlog_pos = event->header.log_pos;
                break;
            }
            case ROTATE_EVENT: {
                auto rotate = std::static_pointer_cast<RotateEvent>(event);
                binlog_name = rotate->next_binlog;
                binlog_pos = event->header.log_pos;
                break;
            }
            case GTID_EVENT: {
                auto gtid_event = std::static_pointer_cast<GTIDEvent>(event);
                binlog_pos = event->header.log_pos;
                gtid_sets.update(gtid_event->gtid);
                break;
            }
            default: {
                /// DryRun event.
                binlog_pos = event->header.log_pos;
                break;
            }
        }
    }

    void Position::update(UInt64 binlog_pos_, const String & binlog_name_, const String & gtid_sets_)
    {
        binlog_pos = binlog_pos_;
        binlog_name = binlog_name_;
        gtid_sets.parse(gtid_sets_);
    }

    void Position::dump(std::ostream & out) const
    {
        out << "\n=== Binlog Position ===" << std::endl;
        out << "Binlog: " << this->binlog_name << std::endl;
        out << "Position: " << this->binlog_pos << std::endl;
        out << "GTIDSets: " << this->gtid_sets.toString() << std::endl;
    }

    void MySQLFlavor::readPayloadImpl(ReadBuffer & payload)
    {
        UInt16 header = static_cast<unsigned char>(*payload.position());
        switch (header)
        {
            case PACKET_EOF:
                throw ReplicationError("Master maybe lost", ErrorCodes::UNKNOWN_EXCEPTION);
            case PACKET_ERR:
                ERRPacket err;
                err.readPayloadWithUnpacked(payload);
                throw ReplicationError(err.error_message, ErrorCodes::UNKNOWN_EXCEPTION);
        }
        // skip the header flag.
        payload.ignore(1);

        EventType event_type = static_cast<EventType>(*(payload.position() + 4));
        switch (event_type)
        {
            case FORMAT_DESCRIPTION_EVENT: {
                event = std::make_shared<FormatDescriptionEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.update(event);
                break;
            }
            case ROTATE_EVENT: {
                event = std::make_shared<RotateEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.update(event);
                break;
            }
            case QUERY_EVENT: {
                event = std::make_shared<QueryEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);

                auto query = std::static_pointer_cast<QueryEvent>(event);
                switch (query->typ)
                {
                    case BEGIN:
                    case XA: {
                        event = std::make_shared<DryRunEvent>();
                        break;
                    }
                    default:
                        position.update(event);
                }
                break;
            }
            case XID_EVENT: {
                event = std::make_shared<XIDEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.update(event);
                break;
            }
            case TABLE_MAP_EVENT: {
                event = std::make_shared<TableMapEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                table_map = std::static_pointer_cast<TableMapEvent>(event);
                break;
            }
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2: {
                if (do_replicate())
                    event = std::make_shared<WriteRowsEvent>(table_map);
                else
                    event = std::make_shared<DryRunEvent>();

                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2: {
                if (do_replicate())
                    event = std::make_shared<DeleteRowsEvent>(table_map);
                else
                    event = std::make_shared<DryRunEvent>();

                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2: {
                if (do_replicate())
                    event = std::make_shared<UpdateRowsEvent>(table_map);
                else
                    event = std::make_shared<DryRunEvent>();

                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            case GTID_EVENT: {
                event = std::make_shared<GTIDEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.update(event);
                break;
            }
            default: {
                event = std::make_shared<DryRunEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.update(event);
                break;
            }
        }
        payload.tryIgnore(CHECKSUM_CRC32_SIGNATURE_LENGTH);
    }
}

}
