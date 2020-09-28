#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>

namespace DB
{

using MySQLBufferAndSortingColumns = std::pair<Block, std::vector<size_t>>;
using MySQLBufferAndSortingColumnsPtr = std::shared_ptr<MySQLBufferAndSortingColumns>;

struct IMySQLBuffer
{
    String database;

    /// thresholds
    size_t max_block_rows = 0;
    size_t max_block_bytes = 0;
    size_t total_blocks_rows = 0;
    size_t total_blocks_bytes = 0;

    std::unordered_map<String, MySQLBufferAndSortingColumnsPtr> data;

    IMySQLBuffer(const String & database_) : database(database_) {}

    void add(
        size_t block_rows,
        size_t block_bytes,
        size_t written_rows,
        size_t written_bytes);

    bool checkThresholds(
        size_t check_block_rows,
        size_t check_block_bytes,
        size_t check_total_rows,
        size_t check_total_bytes) const;

    virtual void commit(const Context & context) = 0;

    virtual MySQLBufferAndSortingColumnsPtr getTableDataBuffer(
        const String & table,
        const Context & context) = 0;

    virtual ~IMySQLBuffer() = default;
};

using IMySQLBufferPtr = std::shared_ptr<IMySQLBuffer>;

struct MySQLDatabaseBuffer : public IMySQLBuffer
{
    MySQLDatabaseBuffer(const String & database_) : IMySQLBuffer(database_) {}
    void commit(const Context & context) override;

    MySQLBufferAndSortingColumnsPtr getTableDataBuffer(
        const String & table,
        const Context & context) override;
};

using MySQLDatabaseBufferPtr = std::shared_ptr<MySQLDatabaseBuffer>;

}

#endif
