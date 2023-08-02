#include "MySQLReplication.h"

#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/MySQLBinlogEventReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/DateLUT.h>
#include <Common/FieldVisitorToString.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolText.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
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

    void EventHeader::dump(WriteBuffer & out) const
    {
        out << "\n=== " << to_string(this->type) << " ===" << '\n';
        out << "Timestamp: " << this->timestamp << '\n';
        out << "Event Type: " << to_string(this->type) << '\n';
        out << "Server ID: " << this->server_id << '\n';
        out << "Event Size: " << this->event_size << '\n';
        out << "Log Pos: " << this->log_pos << '\n';
        out << "Flags: " << this->flags << '\n';
    }

    /// https://dev.mysql.com/doc/internals/en/format-description-event.html
    void FormatDescriptionEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&binlog_version), 2);
        assert(binlog_version == EVENT_VERSION_V4);
        server_version.resize(50);
        payload.readStrict(reinterpret_cast<char *>(server_version.data()), 50);
        payload.readStrict(reinterpret_cast<char *>(&create_timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_header_length), 1);
        assert(event_header_length == EVENT_HEADER_LENGTH);
        readStringUntilEOF(event_type_header_length, payload);
    }

    void FormatDescriptionEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "Binlog Version: " << this->binlog_version << '\n';
        out << "Server Version: " << this->server_version << '\n';
        out << "Create Timestamp: " << this->create_timestamp << '\n';
        out << "Event Header Len: " << std::to_string(this->event_header_length) << '\n';
    }

    /// https://dev.mysql.com/doc/internals/en/rotate-event.html
    void RotateEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&position), 8);
        readStringUntilEOF(next_binlog, payload);
    }

    void RotateEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "Position: " << this->position << '\n';
        out << "Next Binlog: " << this->next_binlog << '\n';
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

        readStringUntilEOF(query, payload);
        if (query.starts_with("BEGIN") || query.starts_with("COMMIT"))
        {
            typ = QUERY_EVENT_MULTI_TXN_FLAG;
            if (!query.starts_with("COMMIT"))
                transaction_complete = false;
        }
        else if (query.starts_with("XA"))
        {
            if (query.starts_with("XA ROLLBACK"))
                throw ReplicationError("ParseQueryEvent: Unsupported query event:" + query, ErrorCodes::LOGICAL_ERROR);
            typ = QUERY_EVENT_XA;
            if (!query.starts_with("XA COMMIT"))
                transaction_complete = false;
        }
        else if (query.starts_with("SAVEPOINT"))
        {
            throw ReplicationError("ParseQueryEvent: Unsupported query event:" + query, ErrorCodes::LOGICAL_ERROR);
        }
    }

    void QueryEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "Thread ID: " << this->thread_id << '\n';
        out << "Execution Time: " << this->exec_time << '\n';
        out << "Schema Len: " << std::to_string(this->schema_len) << '\n';
        out << "Error Code: " << this->error_code << '\n';
        out << "Status Len: " << this->status_len << '\n';
        out << "Schema: " << this->schema << '\n';
        out << "Query: " << this->query << '\n';
    }

    void XIDEvent::parseImpl(ReadBuffer & payload) { payload.readStrict(reinterpret_cast<char *>(&xid), 8); }

    void XIDEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "XID: " << this->xid << '\n';
    }

    void TableMapEventHeader::parse(ReadBuffer & payload)
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
    }

    /// https://dev.mysql.com/doc/internals/en/table-map-event.html
    void TableMapEvent::parseImpl(ReadBuffer & payload)
    {
        column_count = readLengthEncodedNumber(payload);
        for (auto i = 0U; i < column_count; ++i)
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
        payload.ignoreAll();
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
        for (auto i = 0U; i < column_count; ++i)
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
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_YEAR:
                {
                    /// No data here.
                    column_meta.emplace_back(0);
                    break;
                }

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_TIMESTAMP2:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIME2:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                {
                    column_meta.emplace_back(static_cast<UInt16>(meta[pos]));
                    pos += 1;
                    break;
                }
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_STRING:
                {
                    /// Big-Endian
                    auto b0 = static_cast<UInt16>(meta[pos] << 8);
                    auto b1 = static_cast<UInt8>(meta[pos + 1]);
                    column_meta.emplace_back(static_cast<UInt16>(b0 + b1));
                    pos += 2;
                    break;
                }
                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING: {
                    /// Little-Endian
                    auto b0 = static_cast<UInt8>(meta[pos]);
                    auto b1 = static_cast<UInt16>(meta[pos + 1] << 8);
                    column_meta.emplace_back(static_cast<UInt16>(b0 + b1));
                    pos += 2;
                    break;
                }
                default:
                    throw ReplicationError("ParseMetaData: Unhandled data type:" + std::to_string(typ), ErrorCodes::UNKNOWN_EXCEPTION);
            }
        }
    }

    void TableMapEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "Table ID: " << this->table_id << '\n';
        out << "Flags: " << this->flags << '\n';
        out << "Schema Len: " << std::to_string(this->schema_len) << '\n';
        out << "Schema: " << this->schema << '\n';
        out << "Table Len: " << std::to_string(this->table_len) << '\n';
        out << "Table: " << this->table << '\n';
        out << "Column Count: " << this->column_count << '\n';
        for (UInt32 i = 0; i < column_count; ++i)
        {
            out << "Column Type [" << i << "]: " << std::to_string(column_type[i]) << ", Meta: " << column_meta[i] << '\n';
        }
        String bitmap_str;
        boost::to_string(this->null_bitmap, bitmap_str);
        out << "Null Bitmap: " << bitmap_str << '\n';
    }

    void RowsEventHeader::parse(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&table_id), 6);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);

        UInt16 extra_data_len;
        /// This extra_data_len contains the 2 bytes length.
        payload.readStrict(reinterpret_cast<char *>(&extra_data_len), 2);
        payload.ignore(extra_data_len - 2);
    }

    void RowsEvent::parseImpl(ReadBuffer & payload)
    {
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

        while (!payload.eof())
        {
            parseRow(payload, columns_present_bitmap1);
            if (header.type == UPDATE_ROWS_EVENT_V1 || header.type == UPDATE_ROWS_EVENT_V2)
            {
                parseRow(payload, columns_present_bitmap2);
            }
        }
    }

    /// Types that do not used in the binlog event:
    /// MYSQL_TYPE_SET
    /// MYSQL_TYPE_TINY_BLOB
    /// MYSQL_TYPE_MEDIUM_BLOB
    /// MYSQL_TYPE_LONG_BLOB
    void RowsEvent::parseRow(ReadBuffer & payload, Bitmap & bitmap)
    {
        Tuple row;
        UInt32 null_index = 0;

        UInt32 re_count = 0;
        for (UInt32 i = 0; i < number_columns; ++i)
        {
            if (bitmap[i])
                re_count++;
        }
        re_count = (re_count + 7) / 8;
        boost::dynamic_bitset<> columns_null_set;
        readBitmap(payload, columns_null_set, re_count);

        for (UInt32 i = 0; i < number_columns; ++i)
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
                    case MYSQL_TYPE_TINY:
                    {
                        UInt8 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 1);
                        row.push_back(Field{UInt8{val}});
                        break;
                    }
                    case MYSQL_TYPE_SHORT:
                    {
                        UInt16 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 2);
                        row.push_back(Field{UInt16{val}});
                        break;
                    }
                    case MYSQL_TYPE_INT24:
                    {
                        Int32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 3);
                        row.push_back(Field{Int32{val}});
                        break;
                    }
                    case MYSQL_TYPE_LONG:
                    {
                        UInt32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{UInt32{val}});
                        break;
                    }
                    case MYSQL_TYPE_LONGLONG:
                    {
                        UInt64 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 8);
                        row.push_back(Field{UInt64{val}});
                        break;
                    }
                    case MYSQL_TYPE_FLOAT:
                    {
                        Float32 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{Float32{val}});
                        break;
                    }
                    case MYSQL_TYPE_DOUBLE:
                    {
                        Float64 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 8);
                        row.push_back(Field{Float64{val}});
                        break;
                    }
                    case MYSQL_TYPE_TIMESTAMP:
                    {
                        UInt32 val = 0;

                        payload.readStrict(reinterpret_cast<char *>(&val), 4);
                        row.push_back(Field{val});
                        break;
                    }
                    case MYSQL_TYPE_DATE:
                    {
                        UInt32 i24 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&i24), 3);

                        const ExtendedDayNum date_day_number(DateLUT::instance().makeDayNum(
                            static_cast<int>((i24 >> 9) & 0x7fff), static_cast<int>((i24 >> 5) & 0xf), static_cast<int>(i24 & 0x1f)).toUnderType());

                        row.push_back(Field(date_day_number.toUnderType()));
                        break;
                    }
                    case MYSQL_TYPE_YEAR: {
                        Int16 val = 0;
                        payload.readStrict(reinterpret_cast<char *>(&val), 1);
                        row.push_back(Field{UInt16{static_cast<UInt16>(val + 1900)}});
                        break;
                    }
                    case MYSQL_TYPE_TIME2:
                    {
                        UInt64 uintpart = 0UL;
                        Int32 frac = 0U;
                        Int64 ltime;
                        Int64 intpart;
                        switch (meta)
                        {
                            case 0:
                            {
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&uintpart), 3);
                                intpart = uintpart - 0x800000L;
                                ltime = intpart << 24;
                                break;
                            }
                            case 1:
                            case 2:
                            {
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&uintpart), 3);
                                intpart = uintpart - 0x800000L;
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&frac), 1);
                                if (intpart < 0 && frac > 0)
                                {
                                    intpart ++;
                                    frac -= 0x100;
                                }
                                frac = frac * 10000;
                                ltime = intpart << 24;
                                break;
                            }
                            case 3:
                            case 4:
                            {
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&uintpart), 3);
                                intpart = uintpart - 0x800000L;
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&frac), 2);
                                if (intpart < 0 && frac > 0)
                                {
                                    intpart ++;
                                    frac -= 0x10000;
                                }
                                frac = frac * 100;
                                ltime = intpart << 24;
                                break;
                            }
                            case 5:
                            case 6:
                            {
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&uintpart), 6);
                                intpart = uintpart - 0x800000000000L;
                                ltime = intpart;
                                frac = std::abs(intpart % (1L << 24));
                                break;
                            }
                            default:
                            {
                                readBigEndianStrict(payload, reinterpret_cast<char *>(&uintpart), 3);
                                intpart = uintpart - 0x800000L;
                                ltime = intpart << 24;
                                break;
                            }
                        }
                        Int64 hh, mm, ss;
                        bool negative = false;
                        if (intpart == 0)
                        {
                            hh = 0;
                            mm = 0;
                            ss = 0;
                        }
                        else
                        {
                            if (ltime < 0) negative= true;
                            UInt64 ultime = std::abs(ltime);
                            intpart = ultime >> 24;
                            hh = (intpart >> 12) % (1 << 10);
                            mm = (intpart >> 6) % (1 << 6);
                            ss = intpart % (1 << 6);
                        }

                        Int64 time_micro = 0;
                        time_micro = (hh * 3600  + mm * 60 + ss) * 1000000 + std::abs(frac);
                        if (negative) time_micro = - time_micro;
                        row.push_back(Field{Int64{time_micro}});
                        break;
                    }
                    case MYSQL_TYPE_DATETIME2:
                    {
                        Int64 val = 0;
                        UInt32 fsp = 0;
                        readBigEndianStrict(payload, reinterpret_cast<char *>(&val), 5);
                        readTimeFractionalPart(payload, fsp, meta);

                        UInt32 year_month = readBits(val, 1, 17, 40);
                        time_t date_time = DateLUT::instance().makeDateTime(
                            year_month / 13, year_month % 13, readBits(val, 18, 5, 40)
                            , readBits(val, 23, 5, 40), readBits(val, 28, 6, 40), readBits(val, 34, 6, 40)
                        );

                        if (!meta)
                            row.push_back(Field{static_cast<UInt32>(date_time)});
                        else
                        {
                            DB::DecimalUtils::DecimalComponents<DateTime64> components{
                                static_cast<DateTime64::NativeType>(date_time), 0};

                            components.fractional = fsp;
                            row.push_back(Field(DecimalUtils::decimalFromComponents<DateTime64>(components, meta)));
                        }

                        break;
                    }
                    case MYSQL_TYPE_TIMESTAMP2:
                    {
                        UInt32 sec = 0, fsp = 0;
                        readBigEndianStrict(payload, reinterpret_cast<char *>(&sec), 4);
                        readTimeFractionalPart(payload, fsp, meta);

                        if (!meta)
                            row.push_back(Field{sec});
                        else
                        {
                            DB::DecimalUtils::DecimalComponents<DateTime64> components{
                                static_cast<DateTime64::NativeType>(sec), 0};

                            components.fractional = fsp;
                            row.push_back(Field(DecimalUtils::decimalFromComponents<DateTime64>(components, meta)));
                        }

                        break;
                    }
                    case MYSQL_TYPE_NEWDECIMAL:
                    {
                        const auto & dispatch = [](size_t precision, size_t scale, const auto & function) -> Field
                        {
                            if (precision <= DecimalUtils::max_precision<Decimal32>)
                                return Field(function(precision, scale, Decimal32()));
                            else if (precision <= DecimalUtils::max_precision<Decimal64>) //-V547
                                return Field(function(precision, scale, Decimal64()));
                            else if (precision <= DecimalUtils::max_precision<Decimal128>) //-V547
                                return Field(function(precision, scale, Decimal128()));

                            return Field(function(precision, scale, Decimal256()));
                        };

                        const auto & read_decimal = [&](size_t precision, size_t scale, auto decimal)
                        {
                            using DecimalType = decltype(decimal);
                            static constexpr size_t digits_per_integer = 9;
                            static const size_t compressed_bytes_map[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
                            static const size_t compressed_integer_align_numbers[] = {
                                0x0, 0xFF, 0xFF, 0xFFFF, 0xFFFF, 0xFFFFFF, 0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF};

                            UInt32 mask = 0;
                            DecimalType res(0);

                            if (payload.eof())
                                throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

                            if ((*payload.position() & 0x80) == 0)
                                mask = static_cast<UInt32>(-1);

                            *payload.position() ^= 0x80;

                            {
                                size_t integral = (precision - scale);
                                size_t uncompressed_integers = integral / digits_per_integer;
                                size_t compressed_integers = integral - (uncompressed_integers * digits_per_integer);

                                /// Compressed part.
                                if (compressed_integers != 0)
                                {
                                    UInt32 val = 0;
                                    size_t to_read = compressed_bytes_map[compressed_integers];
                                    readBigEndianStrict(payload, reinterpret_cast<char *>(&val), to_read);
                                    res += (val ^ (mask & compressed_integer_align_numbers[compressed_integers]));
                                }

                                for (size_t k = 0; k < uncompressed_integers; ++k)
                                {
                                    UInt32 val = 0;
                                    readBigEndianStrict(payload, reinterpret_cast<char *>(&val), 4);
                                    res *= intExp10OfSize<DecimalType>(digits_per_integer);
                                    res += (val ^ mask);
                                }
                            }

                            {
                                size_t uncompressed_decimals = scale / digits_per_integer;
                                size_t compressed_decimals = scale - (uncompressed_decimals * digits_per_integer);

                                for (size_t k = 0; k < uncompressed_decimals; ++k)
                                {
                                    UInt32 val = 0;
                                    readBigEndianStrict(payload, reinterpret_cast<char *>(&val), 4);
                                    res *= intExp10OfSize<DecimalType>(digits_per_integer);
                                    res += (val ^ mask);
                                }

                                /// Compressed part.
                                if (compressed_decimals != 0)
                                {
                                    UInt32 val = 0;
                                    size_t to_read = compressed_bytes_map[compressed_decimals];

                                    if (to_read) //-V547
                                    {
                                        readBigEndianStrict(payload, reinterpret_cast<char *>(&val), to_read);
                                        res *= intExp10OfSize<DecimalType>(compressed_decimals);
                                        res += (val ^ (mask & compressed_integer_align_numbers[compressed_decimals]));
                                    }
                                }
                            }

                            if (mask != 0)
                                res *= -1;

                            return res;
                        };

                        row.push_back(dispatch((meta >> 8) & 0xFF, meta & 0xFF, read_decimal));
                        break;
                    }
                    case MYSQL_TYPE_ENUM:
                    {
                        if ((meta & 0xFF) == 1)
                        {
                            UInt8 val = 0;
                            payload.readStrict(reinterpret_cast<char *>(&val), 1);
                            row.push_back(Field{UInt8{val}});
                        }
                        else
                        {
                            UInt16 val = 0;
                            payload.readStrict(reinterpret_cast<char *>(&val), 2);
                            row.push_back(Field{UInt16{val}});
                        }
                        break;
                    }
                    case MYSQL_TYPE_SET:
                    {
                        UInt32 size = (meta & 0xff);
                        Bitmap bitmap1;
                        readBitmap(payload, bitmap1, size);
                        row.push_back(Field{UInt64{bitmap1.to_ulong()}});
                        break;
                    }
                    case MYSQL_TYPE_BIT:
                    {
                        UInt32 bits = ((meta >> 8) * 8) + (meta & 0xff);
                        UInt32 size = (bits + 7) / 8;
                        UInt64 val = 0UL;
                        readBigEndianStrict(payload, reinterpret_cast<char *>(&val), size);
                        row.push_back(val);
                        break;
                    }
                    case MYSQL_TYPE_VARCHAR:
                    case MYSQL_TYPE_VAR_STRING:
                    {
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
                    case MYSQL_TYPE_STRING:
                    {
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
                    case MYSQL_TYPE_BLOB:
                    {
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
                    default:
                        throw ReplicationError(
                            "ParseRow: Unhandled MySQL field type:" + std::to_string(field_type), ErrorCodes::UNKNOWN_EXCEPTION);
                }
            }
            null_index++;
        }
        rows.push_back(row);
    }

    void RowsEvent::dump(WriteBuffer & out) const
    {
        FieldVisitorToString to_string;

        header.dump(out);
        out << "Schema: " << this->schema << '\n';
        out << "Table: " << this->table << '\n';
        for (size_t i = 0; i < rows.size(); ++i)
        {
            out << "Row[" << i << "]: " << applyVisitor(to_string, rows[i]) << '\n';
        }
    }

    void GTIDEvent::parseImpl(ReadBuffer & payload)
    {
        /// We only care uuid:seq_no parts assigned to GTID_NEXT.
        payload.readStrict(reinterpret_cast<char *>(&commit_flag), 1);

        // MySQL UUID is big-endian.
        UInt64 high = 0UL;
        UInt64 low = 0UL;
        readBigEndianStrict(payload, reinterpret_cast<char *>(&low), 8);
        gtid.uuid.toUnderType().items[0] = low;

        readBigEndianStrict(payload, reinterpret_cast<char *>(&high), 8);
        gtid.uuid.toUnderType().items[1] = high;

        payload.readStrict(reinterpret_cast<char *>(&gtid.seq_no), 8);

        /// Skip others.
        payload.ignoreAll();
    }

    void GTIDEvent::dump(WriteBuffer & out) const
    {
        WriteBufferFromOwnString ws;
        writeUUIDText(gtid.uuid, ws);
        auto gtid_next = ws.str() + ":" + std::to_string(gtid.seq_no);

        header.dump(out);
        out << "GTID Next: " << gtid_next << '\n';
    }

    void DryRunEvent::parseImpl(ReadBuffer & payload) { payload.ignoreAll(); }

    void DryRunEvent::dump(WriteBuffer & out) const
    {
        header.dump(out);
        out << "[DryRun Event]" << '\n';
    }

    /// Update binlog name/position/gtid based on the event type.
    void Position::update(BinlogEventPtr event)
    {
        switch (event->header.type)
        {
            case FORMAT_DESCRIPTION_EVENT: {
                binlog_pos = event->header.log_pos;
                break;
            }
            case QUERY_EVENT: {
                auto query = std::static_pointer_cast<QueryEvent>(event);
                if (query->transaction_complete && pending_gtid)
                {
                    gtid_sets.update(*pending_gtid);
                    pending_gtid.reset();
                }
                binlog_pos = event->header.log_pos;
                break;
            }
            case XID_EVENT: {
                if (pending_gtid)
                {
                    gtid_sets.update(*pending_gtid);
                    pending_gtid.reset();
                }
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
                if (pending_gtid)
                    gtid_sets.update(*pending_gtid);
                auto gtid_event = std::static_pointer_cast<GTIDEvent>(event);
                binlog_pos = event->header.log_pos;
                pending_gtid = gtid_event->gtid;
                break;
            }
            default:
                throw ReplicationError("Position update with unsupported event", ErrorCodes::LOGICAL_ERROR);
        }
    }

    void Position::update(UInt64 binlog_pos_, const String & binlog_name_, const String & gtid_sets_)
    {
        binlog_pos = binlog_pos_;
        binlog_name = binlog_name_;
        gtid_sets.parse(gtid_sets_);
    }

    void Position::dump(WriteBuffer & out) const
    {
        out << "\n=== Binlog Position ===" << '\n';
        out << "Binlog: " << this->binlog_name << '\n';
        out << "Position: " << this->binlog_pos << '\n';
        out << "GTIDSets: " << this->gtid_sets.toString() << '\n';
    }

    void MySQLFlavor::readPayloadImpl(ReadBuffer & payload)
    {
        if (payload.eof())
            throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

        UInt16 header = static_cast<unsigned char>(*payload.position());
        switch (header)
        {
            case PACKET_EOF:
                throw ReplicationError("Master maybe lost", ErrorCodes::CANNOT_READ_ALL_DATA);
            case PACKET_ERR:
                ERRPacket err;
                err.readPayloadWithUnpacked(payload);
                throw ReplicationError(err.error_message, ErrorCodes::UNKNOWN_EXCEPTION);
        }
        // skip the generic response packets header flag.
        payload.ignore(1);

        MySQLBinlogEventReadBuffer event_payload(payload, checksum_signature_length);

        EventHeader event_header;
        event_header.parse(event_payload);

        switch (event_header.type)
        {
            case FORMAT_DESCRIPTION_EVENT:
            {
                event = std::make_shared<FormatDescriptionEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                position.update(event);
                break;
            }
            case ROTATE_EVENT:
            {
                event = std::make_shared<RotateEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                position.update(event);
                break;
            }
            case QUERY_EVENT:
            {
                event = std::make_shared<QueryEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                position.update(event);

                auto query = std::static_pointer_cast<QueryEvent>(event);
                switch (query->typ)
                {
                    case QUERY_EVENT_MULTI_TXN_FLAG:
                    case QUERY_EVENT_XA:
                    {
                        event = std::make_shared<DryRunEvent>(std::move(query->header));
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case XID_EVENT:
            {
                event = std::make_shared<XIDEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                position.update(event);
                break;
            }
            case TABLE_MAP_EVENT:
            {
                TableMapEventHeader map_event_header;
                map_event_header.parse(event_payload);
                if (doReplicate(map_event_header.schema, map_event_header.table))
                {
                    event = std::make_shared<TableMapEvent>(std::move(event_header), map_event_header);
                    event->parseEvent(event_payload);
                    auto table_map = std::static_pointer_cast<TableMapEvent>(event);
                    table_maps[table_map->table_id] = table_map;
                }
                else
                {
                    event = std::make_shared<DryRunEvent>(std::move(event_header));
                    event->parseEvent(event_payload);
                }
                break;
            }
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2: {
                RowsEventHeader rows_header(event_header.type);
                rows_header.parse(event_payload);
                if (doReplicate(rows_header.table_id))
                    event = std::make_shared<WriteRowsEvent>(table_maps.at(rows_header.table_id), std::move(event_header), rows_header);
                else
                    event = std::make_shared<DryRunEvent>(std::move(event_header));

                event->parseEvent(event_payload);
                if (rows_header.flags & ROWS_END_OF_STATEMENT)
                    table_maps.clear();
                break;
            }
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2: {
                RowsEventHeader rows_header(event_header.type);
                rows_header.parse(event_payload);
                if (doReplicate(rows_header.table_id))
                    event = std::make_shared<DeleteRowsEvent>(table_maps.at(rows_header.table_id), std::move(event_header), rows_header);
                else
                    event = std::make_shared<DryRunEvent>(std::move(event_header));

                event->parseEvent(event_payload);
                if (rows_header.flags & ROWS_END_OF_STATEMENT)
                    table_maps.clear();
                break;
            }
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2: {
                RowsEventHeader rows_header(event_header.type);
                rows_header.parse(event_payload);
                if (doReplicate(rows_header.table_id))
                    event = std::make_shared<UpdateRowsEvent>(table_maps.at(rows_header.table_id), std::move(event_header), rows_header);
                else
                    event = std::make_shared<DryRunEvent>(std::move(event_header));

                event->parseEvent(event_payload);
                if (rows_header.flags & ROWS_END_OF_STATEMENT)
                    table_maps.clear();
                break;
            }
            case GTID_EVENT:
            {
                event = std::make_shared<GTIDEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                position.update(event);
                break;
            }
            default:
            {
                event = std::make_shared<DryRunEvent>(std::move(event_header));
                event->parseEvent(event_payload);
                break;
            }
        }
    }

    bool MySQLFlavor::doReplicate(UInt64 table_id)
    {
        if (replicate_do_db.empty())
            return false;
        if (table_id == 0x00ffffff)
        {
            // Special "dummy event"
            return false;
        }
        if (table_maps.contains(table_id))
        {
            auto table_map = table_maps.at(table_id);
            return (table_map->schema == replicate_do_db) && (replicate_tables.empty() || replicate_tables.contains(table_map->table));
        }
        return false;
    }

    bool MySQLFlavor::doReplicate(const String & db, const String & table_name)
    {
        if (replicate_do_db.empty())
            return false;
        if (replicate_do_db != db)
            return false;
        return replicate_tables.empty() || table_name.empty() || replicate_tables.contains(table_name);
    }
}

}
