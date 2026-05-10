#pragma once

#include "config.h"

#if USE_SQLITE

#include <Core/Block.h>
#include <Core/ExternalResultDescription.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>

#include <sqlite3.h>

#include <optional>
#include <vector>

namespace DB
{

class IColumn;

class SQLiteStatementReader
{
public:
    enum class ValueReadMode
    {
        Native,
        Text,
    };

    SQLiteStatementReader(const Block & sample_block_, const FormatSettings & format_settings_, ValueReadMode value_read_mode_);

    Chunk readChunk(sqlite3 * db, sqlite3_stmt * statement, UInt64 max_block_size, bool & finished);

private:
    using ValueType = ExternalResultDescription::ValueType;

    struct ColumnReadInfo
    {
        DataTypePtr data_type;
        SerializationPtr serialization;
        std::optional<ValueType> native_value_type;
        bool is_nullable = false;
    };

    ColumnReadInfo createColumnReadInfo(const ColumnWithTypeAndName & column) const;

    void insertValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;
    void insertTextValue(IColumn & column, const ColumnReadInfo & info, sqlite3_stmt * statement, int idx) const;

    Block sample_block;
    FormatSettings format_settings;
    ValueReadMode value_read_mode;
    std::vector<ColumnReadInfo> columns_info;
};

}

#endif
