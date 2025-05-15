#pragma once

#include "config.h"

#if USE_LANCE

#    include "LanceTable.h"

#    include <lance.h>

namespace DB
{

class LanceDB;

using LanceDBPtr = std::shared_ptr<LanceDB>;

class LanceDB
{
private:
    LanceDB(lance::Database * database_);

public:
    LanceDB(const LanceDB &) = delete;
    LanceDB(LanceDB && db);

    static LanceDBPtr connect(const String & database_path);

    LanceTablePtr tryOpenTable(const String & table_name);
    LanceTablePtr tryCreateTableWithSchema(const String & table_name, const ColumnsDescription columns);

    bool dropTable(const String & table_name);

    ~LanceDB();

private:
    lance::Database * database;
};

} // namespace DB

#endif
