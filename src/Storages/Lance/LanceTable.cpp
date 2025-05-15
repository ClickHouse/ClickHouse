#include "LanceTable.h"

#if USE_LANCE

#    include "LanceUtils.h"

#    include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

LanceTable::LanceTable(lance::Table * table_) : table(table_)
{
    readSchema();
}

LanceTable::LanceTable(LanceTable && table_)
    : table(std::exchange(table_.table, nullptr)), schema(std::move(table_.schema)), index_by_name(std::move(table_.index_by_name))
{
}

void LanceTable::renameColumn(const String & old_name, const String & new_name)
{
    schema.rename(old_name, new_name);
    index_by_name[new_name] = index_by_name[old_name];
    index_by_name.erase(old_name);
    lance::rename_column(table, old_name.c_str(), new_name.c_str());
}

void LanceTable::dropColumn(const String & column_name)
{
    schema.remove(column_name);
    index_by_name.erase(column_name);
    lance::drop_column(table, column_name.c_str());
}

LanceTable::~LanceTable()
{
    if (table != nullptr)
    {
        lance::free_table(table);
    }
}

void LanceTable::readSchema()
{
    lance::Schema lance_schema = lance::read_schema(table);
    NamesAndTypes names_and_types;
    for (size_t i = 0; i < lance_schema.len; ++i)
    {
        String name(lance_schema.data[i].name);
        index_by_name[name] = i;
        DataTypePtr data_type = lanceColumnTypeToDataType(lance_schema.data[i].data_type);
        if (lance_schema.data[i].is_nullable)
        {
            names_and_types.emplace_back(name, std::make_shared<DataTypeNullable>(data_type));
        }
        else
        {
            names_and_types.emplace_back(name, data_type);
        }
    }
    lance::free_schema(lance_schema);
    schema = ColumnsDescription::fromNamesAndTypes(names_and_types);
}

} // namespace DB

#endif
