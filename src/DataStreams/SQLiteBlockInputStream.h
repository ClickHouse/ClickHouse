#pragma once

#include <sqlite3.h>

#include <Core/ExternalResultDescription.h>
#include <DataStreams/IBlockInputStream.h>


namespace DB
{
class SQLiteBlockInputStream : public IBlockInputStream
{
public:
    SQLiteBlockInputStream(
        std::shared_ptr<sqlite3> connection_, const std::string & query_str_, const Block & sample_block, UInt64 max_block_size_);

    String getName() const override { return "SQLite"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    using ValueType = ExternalResultDescription::ValueType;

    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt) { sqlite3_finalize(stmt); }
    };

    void readPrefix() override;
    Block readImpl() override;
    void readSuffix() override;

    String query_str;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    std::shared_ptr<sqlite3> connection;
    std::unique_ptr<sqlite3_stmt, StatementDeleter> compiled_statement;
};

}
