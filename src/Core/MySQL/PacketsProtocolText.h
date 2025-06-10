#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>

namespace DB
{

namespace MySQLProtocol
{

namespace ProtocolText
{

enum CharacterSet
{
    utf8_general_ci = 33,
    binary = 63
};

// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
enum ColumnDefinitionFlags
{
    UNSIGNED_FLAG = 32,
    BINARY_FLAG = 128
};

enum ColumnType
{
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_NEWDATE = 0x0e,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_TIMESTAMP2 = 0x11,
    MYSQL_TYPE_DATETIME2 = 0x12,
    MYSQL_TYPE_TIME2 = 0x13,
    MYSQL_TYPE_JSON = 0xf5,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff
};

class ResultSetRow : public IMySQLWritePacket
{
protected:
    const Columns & columns;
    size_t row_num;
    size_t payload_size = 0;
    std::vector<String> serialized;

    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ResultSetRow(const Serializations & serializations, const DataTypes & data_types, const Columns & columns_, size_t row_num_);
};

class ComFieldList : public LimitedReadPacket
{
public:
    String table, field_wildcard;

    void readPayloadImpl(ReadBuffer & payload) override;
};

class ColumnDefinition : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    String schema;
    String table;
    String org_table;
    String name;
    String org_name;
    size_t next_length = 0x0c;
    uint16_t character_set;
    uint32_t column_length;
    ColumnType column_type;
    uint16_t flags;
    uint8_t decimals = 0x00;
    /// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
    /// There are extra fields in the packet for column defaults
    bool is_comm_field_list_response = false;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ColumnDefinition();

    ColumnDefinition(
        String schema_,
        String table_,
        String org_table_,
        String name_,
        String org_name_,
        uint16_t character_set_,
        uint32_t column_length_,
        ColumnType column_type_,
        uint16_t flags_,
        uint8_t decimals_,
        bool with_defaults_ = false);

    /// Should be used when column metadata (original name, table, original table, database) is unknown.
    ColumnDefinition(
        String name_,
        uint16_t character_set_,
        uint32_t column_length_,
        ColumnType column_type_,
        uint16_t flags_,
        uint8_t decimals_);
};

ColumnDefinition getColumnDefinition(const String & column_name, const DataTypePtr & data_type);

}

}

}
