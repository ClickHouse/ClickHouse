#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_SQLITE
#include <Core/ExternalResultDescription.h>
#include <DataStreams/IBlockInputStream.h>

#include <sqlite3.h>


namespace DB
{
class SQLiteBlockInputStream : public IBlockInputStream
{
using SQLitePtr = std::shared_ptr<sqlite3>;

public:
    SQLiteBlockInputStream(SQLitePtr sqlite_db_,
                           const String & query_str_,
                           const Block & sample_block,
                           UInt64 max_block_size_);

    String getName() const override { return "SQLite"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    void insertDefaultSQLiteValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }

    using ValueType = ExternalResultDescription::ValueType;

    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt) { sqlite3_finalize(stmt); }
    };

    void readPrefix() override;

    Block readImpl() override;

    void readSuffix() override;

    void insertValue(IColumn & column, const ExternalResultDescription::ValueType type, size_t idx);

    String query_str;
    UInt64 max_block_size;

    ExternalResultDescription description;

    SQLitePtr sqlite_db;
    std::unique_ptr<sqlite3_stmt, StatementDeleter> compiled_statement;
};

}

#endif
