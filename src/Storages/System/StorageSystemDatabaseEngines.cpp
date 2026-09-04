#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Storages/System/StorageSystemDatabaseEngines.h>

namespace DB
{

ColumnsDescription StorageSystemDatabaseEngines::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of database engine."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the database engine does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the database engine is specified in the ENGINE clause of a CREATE DATABASE query."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the database engine was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related database engines."},
    };
}

void StorageSystemDatabaseEngines::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [engine, creator] : DatabaseFactory::instance().getDatabaseEngines())
    {
        const auto & documentation = creator.documentation;

        int i = 0;
        res_columns[i++]->insert(engine);
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & name : documentation.related)
            related.push_back(name);
        res_columns[i++]->insert(related);
    }
}

}
