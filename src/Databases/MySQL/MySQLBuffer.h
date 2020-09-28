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

struct MySQLBuffer
{
    String database;

    /// thresholds
    size_t max_block_rows = 0;
    size_t max_block_bytes = 0;
    size_t total_blocks_rows = 0;
    size_t total_blocks_bytes = 0;

    using BufferAndSortingColumns = std::pair<Block, std::vector<size_t>>;
    using BufferAndSortingColumnsPtr = std::shared_ptr<BufferAndSortingColumns>;
    std::unordered_map<String, BufferAndSortingColumnsPtr> data;

    MySQLBuffer(const String & database_) : database(database_) {}

    void commit(const Context & context);

    void add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes);

    bool checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const;

    BufferAndSortingColumnsPtr getTableDataBuffer(const String & table, const Context & context);
};

}

#endif
