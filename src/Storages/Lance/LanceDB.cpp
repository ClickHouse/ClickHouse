#include "LanceDB.h"

#if USE_LANCE

#    include "LanceUtils.h"

namespace DB
{

LanceDB::LanceDB(lance::Database * database_) : database(database_)
{
}

LanceDB::LanceDB(LanceDB && db) : database(std::exchange(db.database, nullptr))
{
}

LanceDBPtr LanceDB::connect(const String & database_path)
{
    lance::Database * db = lance::connect_to_database(database_path.c_str());
    if (db == nullptr)
    {
        return nullptr;
    }
    return LanceDBPtr(new LanceDB(db));
}

LanceTablePtr LanceDB::tryOpenTable(const String & table_name)
{
    lance::Table * table = lance::try_open_table(database, table_name.c_str());
    if (table == nullptr)
    {
        return nullptr;
    }
    return LanceTablePtr(new LanceTable(table));
}

LanceTablePtr LanceDB::tryCreateTableWithSchema(const String & table_name, const ColumnsDescription columns)
{
    std::vector<lance::ColumnDescription> schema_vector;
    for (auto & column : columns)
    {
        schema_vector.emplace_back(column.name.c_str(), dataTypePtrToLanceColumnType(column.type), column.type->isNullable());
    }
    lance::Schema lance_schema{
        .data = schema_vector.data(),
        .len = schema_vector.size(),
        .capacity = schema_vector.capacity(),
    };
    lance::Table * table = lance::create_table_with_schema(database, table_name.c_str(), lance_schema);
    if (table == nullptr)
    {
        return nullptr;
    }
    return std::make_shared<LanceTable>(LanceTable(table));
}

bool LanceDB::dropTable(const String & table_name)
{
    return lance::drop_table(database, table_name.c_str());
}

LanceDB::~LanceDB()
{
    if (database != nullptr)
    {
        lance::free_database(database);
    }
}

} // namespace DB

#endif
