#include <Storages/System/StorageSystemHypotheticalProjections.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
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
    auto projection_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Normal",    static_cast<UInt8>(ProjectionDescription::Type::Normal)},
            {"Aggregate", static_cast<UInt8>(ProjectionDescription::Type::Aggregate)}
        });

    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table",    std::make_shared<DataTypeString>(), "Table name"},
        {"name",     std::make_shared<DataTypeString>(), "Projection name"},
        {"type",     std::move(projection_type_datatype), "Projection type"},
        {"sorting_key", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Projection sorting key"},
        {"query",    std::make_shared<DataTypeString>(), "Projection SELECT query, empty for the INDEX ... TYPE ... form"},
        {"definition", std::make_shared<DataTypeString>(), "Full projection declaration as written"},
        {"settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
                     "Projection settings from WITH SETTINGS"},
    };
}

void StorageSystemHypotheticalProjections::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & store = context->getHypotheticalObjectStore();
    auto entries = store.getAllProjections();

    const auto access = context->getAccess();
    const bool check_access = !access->isGranted(AccessType::SHOW_TABLES);

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

        /// a session entry outlives the grants it was made under, so hide it once the table it
        /// points at is no longer visible, including after the table is renamed
        if (check_access && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
            continue;

        size_t col = 0;
        res_columns[col++]->insert(database_name);
        res_columns[col++]->insert(table_name);
        res_columns[col++]->insert(entry.projection.name);
        res_columns[col++]->insert(entry.projection.type);

        Array sorting_key;
        for (const auto & column : entry.projection.metadata->getSortingKeyColumns())
            sorting_key.push_back(column);
        res_columns[col++]->insert(sorting_key);

        const auto * declaration = entry.projection.definition_ast
            ? entry.projection.definition_ast->as<ASTProjectionDeclaration>()
            : nullptr;
        if (declaration && declaration->query)
            res_columns[col++]->insert(declaration->query->formatForLogging());
        else
            res_columns[col++]->insertDefault();

        /// the query column is empty for the INDEX form, so always expose the whole declaration
        if (entry.projection.definition_ast)
            res_columns[col++]->insert(entry.projection.definition_ast->formatForLogging());
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
