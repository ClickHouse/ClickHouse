#include <Storages/System/StorageSystemHypotheticalProjections.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/HypotheticalObjectStore.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSetQuery.h>
#include <Core/Field.h>

namespace DB
{

ColumnsDescription StorageSystemHypotheticalProjections::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table",    std::make_shared<DataTypeString>(), "Table name"},
        {"name",     std::make_shared<DataTypeString>(), "Projection name"},
        {"type",     std::make_shared<DataTypeString>(), "Projection type (normal or aggregate)"},
        {"query",    std::make_shared<DataTypeString>(), "Projection SELECT query"},
        {"settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
                     "Projection settings from WITH SETTINGS"},
    };
}

void StorageSystemHypotheticalProjections::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & store = context->getHypotheticalObjectStore();
    auto entries = store.getAllProjections();

    for (const auto & entry : entries)
    {
        /// Hide entries whose table no longer exists (DROP TABLE).
        String database_name = entry.table_id.getDatabaseName();
        String table_name = entry.table_id.getTableName();
        if (entry.table_id.uuid != UUIDHelpers::Nil)
        {
            auto [db, storage] = DatabaseCatalog::instance().tryGetByUUID(entry.table_id.uuid);
            if (!db || !storage)
                continue;
            database_name = db->getDatabaseName();
            table_name = storage->getStorageID().getTableName();
        }

        size_t col = 0;
        res_columns[col++]->insert(database_name);
        res_columns[col++]->insert(table_name);
        res_columns[col++]->insert(entry.projection.name);
        res_columns[col++]->insert(
            entry.projection.type == ProjectionDescription::Type::Aggregate ? "aggregate" : "normal");

        const auto * declaration = entry.projection.definition_ast
            ? entry.projection.definition_ast->as<ASTProjectionDeclaration>()
            : nullptr;
        if (declaration && declaration->query)
            res_columns[col++]->insert(declaration->query->formatForLogging());
        else
            res_columns[col++]->insertDefault();

        Map settings_map;
        if (declaration && declaration->with_settings)
        {
            for (const auto & change : declaration->with_settings->changes)
            {
                Tuple pair;
                pair.push_back(change.name);
                pair.push_back(fieldToString(change.value));
                settings_map.push_back(std::move(pair));
            }
        }
        res_columns[col++]->insert(settings_map);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemHypotheticalProjections) }
