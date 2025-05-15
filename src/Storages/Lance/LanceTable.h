#pragma once

#include "config.h"

#if USE_LANCE

#    include <Storages/ColumnsDescription.h>

#    include <lance.h>

namespace DB
{

class LanceTableReader;
class LanceSink;
class LanceDB;

class LanceTable
{
private:
    friend class LanceTableReader;
    friend class LanceSink;
    friend class LanceDB;

    using IndexByName = std::unordered_map<String, size_t>;

private:
    LanceTable(lance::Table * table_);

public:
    LanceTable(const LanceTable &) = delete;
    LanceTable(LanceTable && table_);

    ColumnsDescription getSchema() { return schema; }

    size_t getColumnIndexByName(const String & name) { return index_by_name.at(name); }

    void renameColumn(const String & old_name, const String & new_name);
    void dropColumn(const String & column_name);

    ~LanceTable();

private:
    void readSchema();

private:
    lance::Table * table = nullptr;
    ColumnsDescription schema;
    std::unordered_map<String, size_t> index_by_name;
};

using LanceTablePtr = std::shared_ptr<LanceTable>;

} // namespace DB

#endif
