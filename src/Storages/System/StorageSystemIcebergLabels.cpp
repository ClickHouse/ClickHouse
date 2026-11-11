#include <Storages/System/StorageSystemIcebergLabels.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/DatabaseDataLake.h>
#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/extractTablesFilter.h>
#include <Storages/VirtualColumnUtils.h>

#include "config.h"

namespace DB
{

namespace Setting
{
extern const SettingsBool database_datalake_require_metadata_access;
extern const SettingsBool show_data_lake_catalogs_in_system_tables;
}

namespace
{

/// What a label is attached to. The names follow the Iceberg REST specification,
/// where the labels of a table are `object-labels` and the labels of its fields are `fields`.
enum class LabelScope : int8_t
{
    Object = 0,
    Field = 1,
};

}

ColumnsDescription StorageSystemIcebergLabels::getColumnsDescription()
{
    auto scope_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"object", static_cast<Int8>(LabelScope::Object)},
        {"field", static_cast<Int8>(LabelScope::Field)},
    });

    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name, with its namespace."},
        {"scope", std::move(scope_type), "What the label is attached to: `object` for the table as a whole, `field` for one of its fields."},
        {"field_id",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
         "Id of the field the label is attached to. `NULL` for a label of the table as a whole."},
        {"field_name",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
         "Name of the field in the current schema of the table, e.g. `address.city` for a nested field. "
         "`NULL` for a label of the table as a whole, and for a field which the current schema does not have, "
         "for example a dropped column: the catalog identifies fields by id and may keep their labels."},
        {"key", std::make_shared<DataTypeString>(), "Key of the label."},
        {"value", std::make_shared<DataTypeString>(), "Value of the label."},
    };
}

void StorageSystemIcebergLabels::fillData(
    [[maybe_unused]] MutableColumns & res_columns,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] const ActionsDAG::Node * predicate,
    std::vector<UInt8>) const
{
#if USE_AVRO && USE_PARQUET
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);
    const bool check_access_for_columns = !access->isGranted(AccessType::SHOW_COLUMNS);

    const bool rethrow_listing_exception = context->getSettingsRef()[Setting::show_data_lake_catalogs_in_system_tables];
    const bool rethrow_table_exception = context->getSettingsRef()[Setting::database_datalake_require_metadata_access];
    const QueryStatusPtr query_status = context->getProcessListElement();
    auto log = getLogger("SystemIcebergLabels");

    auto add_label = [&](
        const String & database_name,
        const String & table_name,
        LabelScope scope,
        const DataLake::TableLabels::FieldLabels * field,
        const String & key,
        const String & value)
    {
        size_t column_index = 0;
        res_columns[column_index++]->insert(database_name);
        res_columns[column_index++]->insert(table_name);
        res_columns[column_index++]->insert(static_cast<Int8>(scope));

        if (field)
            res_columns[column_index++]->insert(field->field_id);
        else
            res_columns[column_index++]->insertDefault();

        if (field && field->field_name)
            res_columns[column_index++]->insert(*field->field_name);
        else
            res_columns[column_index++]->insertDefault();

        res_columns[column_index++]->insert(key);
        res_columns[column_index++]->insert(value);
    };

    /// Only the databases which are catalogs can have labels.
    /// The block filter below only sees `database`, so it cannot use a condition that names the
    /// database together with the table, such as `(database, table) IN ((db, t))`; the extraction
    /// reads that shape too, and shortlists the databases first.
    const auto database_name_filter = extractNameFilter(predicate, "database", context);
    MutableColumnPtr database_column = ColumnString::create();
    for (const auto & [name, database] :
         DatabaseCatalog::instance().getDatabases({.with_datalake_catalogs = true, .with_remote_databases = false}))
    {
        if (database->isDatalakeCatalog() && (!database_name_filter || database_name_filter(name)))
            database_column->insert(name);
    }

    Block databases_block{
        {std::move(database_column), std::make_shared<DataTypeString>(), "database"},
    };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, databases_block, context);
    const auto & databases = assert_cast<const ColumnString &>(*databases_block.getByName("database").column);

    /// Labels come with the load of a table and with nothing else, so every table costs a request to the catalog.
    /// The filter on the table name lets the catalog list only the namespaces the query asks for, and lets us
    /// load only the tables it asks for.
    const TablesFilter tables_filter = extractTablesFilter(predicate, "table", context);

    for (size_t database_index = 0; database_index < databases.size(); ++database_index)
    {
        const String database_name{databases.getDataAt(database_index)};

        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
            continue;

        const auto * datalake_database = typeid_cast<const DatabaseDataLake *>(database.get());
        if (!datalake_database)
            continue;

        /// A catalog which cannot be reached is treated the way the listing of its tables below treats it:
        /// a query over all the catalogs must not fail because of one of them, and the failure must not be
        /// logged at the error level, because it would be sent to the client of a query which succeeded.
        std::shared_ptr<DataLake::ICatalog> catalog;
        try
        {
            catalog = datalake_database->getCatalog();
        }
        catch (...)
        {
            if (rethrow_listing_exception)
                throw;

            LOG_DEBUG(
                log,
                "Cannot get the catalog of the DataLakeCatalog database {}: {}",
                database_name,
                getCurrentExceptionMessage(/* with_stacktrace = */ false));
            continue;
        }

        if (!catalog->supportsLabels())
            continue;

        MutableColumnPtr table_database_column = ColumnString::create();
        MutableColumnPtr table_column = ColumnString::create();
        for (const auto & table_details : database->getLightweightTablesIteratorWithHint(context, {}, true, tables_filter))
        {
            table_database_column->insert(database_name);
            table_column->insert(table_details.name);
        }

        Block tables_block{
            {std::move(table_database_column), std::make_shared<DataTypeString>(), "database"},
            {std::move(table_column), std::make_shared<DataTypeString>(), "table"},
        };
        VirtualColumnUtils::filterBlockWithPredicate(predicate, tables_block, context);
        const auto & tables = assert_cast<const ColumnString &>(*tables_block.getByName("table").column);

        for (size_t table_index = 0; table_index < tables.size(); ++table_index)
        {
            /// A catalog can have a lot of tables, and each of them is a request.
            if (query_status)
                query_status->checkTimeLimit();

            const String table_name{tables.getDataAt(table_index)};

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            DataLake::TableMetadata table_metadata;
            table_metadata.withLabels();

            try
            {
                const auto [namespace_name, name_in_namespace] = DataLake::parseTableName(table_name);
                if (!catalog->tryGetTableMetadata(namespace_name, name_in_namespace, table_metadata))
                    continue; /// The table was dropped after it was listed.
            }
            catch (...)
            {
                if (rethrow_table_exception)
                    throw;

                tryLogCurrentException(log, fmt::format("Ignoring table {}.{}", database_name, table_name));
                continue;
            }

            const auto & labels = table_metadata.getLabels();

            for (const auto & [key, value] : labels.object_labels)
                add_label(database_name, table_name, LabelScope::Object, nullptr, key, value);

            /// The labels of the fields tell which columns the table has.
            if (labels.field_labels.empty()
                || (check_access_for_columns && !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name)))
                continue;

            for (const auto & field : labels.field_labels)
                for (const auto & [key, value] : field.labels)
                    add_label(database_name, table_name, LabelScope::Field, &field, key, value);
        }
    }
#endif
}

Block StorageSystemIcebergLabels::getFilterSampleBlock() const
{
#if USE_AVRO && USE_PARQUET
    return {
        {{}, std::make_shared<DataTypeString>(), "database"},
        {{}, std::make_shared<DataTypeString>(), "table"},
    };
#else
    return {};
#endif
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemIcebergLabels) }
