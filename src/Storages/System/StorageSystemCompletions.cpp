#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/Macros.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Formats/FormatFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/CommonParsers.h>
#include <Storages/System/StorageSystemCompletions.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 readonly;
    extern const SettingsSeconds lock_acquire_timeout;
}

ColumnsDescription StorageSystemCompletions::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"word", std::make_shared<DataTypeString>(), "Completion token."}
    };
    return description;
}

void fillDataWithTableColumns(const String & database_name, const String & table_name, const StoragePtr & table, MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & access = context->getAccess();
    if (!access->isGranted(AccessType::SHOW_TABLES) || !access->isGranted(AccessType::SHOW_TABLES, database_name) || !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
        return;

    if (!table)
        return; // table was dropped or deleted, while adding columns for previous table

    res_columns[0]->insert(table_name);

    if (!access->isGranted(AccessType::SHOW_COLUMNS) || !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name))
        return;

    auto table_lock = table->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    if (table_lock == nullptr)
        return; // table was dropped while acquiring the lock

    const auto & snapshot = table->getInMemoryMetadataPtr();
    const auto & columns = snapshot->getColumns();
    for (const auto & column : columns)
    {
        if (!access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name, column.name))
            continue;

        res_columns[0]->insert(column.name);
    }
}

void fillDataWithDatabasesTablesColumns(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & access = context->getAccess();
    const auto & databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database_ptr] : databases)
    {
        if (!access->isGranted(AccessType::SHOW_DATABASES) || !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        res_columns[0]->insert(database_name);

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// skipping internal database temporary tables

        /// We are skipping "Lazy" database because we cannot afford initialization of all its tables.
        if (database_ptr->getEngineName() == "Lazy")
            continue;

        for (auto iterator = database_ptr->getLightweightTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table_name = iterator->name();
            const auto & table = iterator->table();
            fillDataWithTableColumns(database_name, table_name, table, res_columns, context);
        }

    }

    if (context->hasSessionContext())
    {
        Tables external_tables = context->getSessionContext()->getExternalTables();
        for (auto & [table_name, table] : external_tables)
        {
            const String database_name(1, '\0');
            fillDataWithTableColumns(database_name, table_name, table, res_columns, context);
            res_columns[0]->insert(table_name);
        }
    }
}

void fillDataWithFunctions(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
    {
        res_columns[0]->insert(function_name);
    }
    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        res_columns[0]->insert(function_name);
    }
    const auto & user_defined_function_names = UserDefinedExecutableFunctionFactory::getRegisteredNames(context);
    for (const auto & function_name : user_defined_function_names)
    {
        res_columns[0]->insert(function_name);
    }
}

void fillDataWithTableEngines(MutableColumns & res_columns)
{
    const auto & storage_factory = StorageFactory::instance();
    const auto & table_engines = storage_factory.getAllStorages();
    for (const auto & [table_engine_name, _] : table_engines)
    {
        res_columns[0]->insert(table_engine_name);
    }
}

void fillDataWithFormats(MutableColumns & res_columns)
{
    const auto & format_factory = FormatFactory::instance();
    const auto & formats = format_factory.getAllFormats();
    for (const auto & [format_name, _] : formats)
    {
        res_columns[0]->insert(format_name);
    }
}

void fillDataWithTableFunctions(MutableColumns & res_columns, const ContextPtr & context)
{
    bool non_readonly_allowed = context->getSettingsRef()[Setting::readonly] == 0;
    const auto & table_functions_factory = TableFunctionFactory::instance();
    const auto & table_functions = table_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : table_functions)
    {
        auto properties = table_functions_factory.tryGetProperties(function_name);
        if ((non_readonly_allowed) || (properties && properties->allow_readonly))
            res_columns[0]->insert(function_name);
    }
}

void fillDataWithDataTypeFamilies(MutableColumns & res_columns)
{
    const auto & data_type_factory = DataTypeFactory::instance();
    const auto & data_type_names = data_type_factory.getAllRegisteredNames();
    for (const auto & data_type_name : data_type_names)
    {
        res_columns[0]->insert(data_type_name);
    }
}

void fillDataWithMergeTreeSettings(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & merge_tree_settings = context->getMergeTreeSettings();
    const auto & replicated_merge_tree_settings = context->getReplicatedMergeTreeSettings();
    merge_tree_settings.dumpToSystemCompletionsColumns(res_columns);
    replicated_merge_tree_settings.dumpToSystemCompletionsColumns(res_columns);
}

void fillDataWithSettings(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    const auto & setting_names = settings.getAllRegisteredNames();
    for (const auto & setting_name : setting_names)
    {
        res_columns[0]->insert(setting_name);
    }
}

void fillDataWithKeywords(MutableColumns & res_columns)
{
    for (const auto & keyword : getAllKeyWords())
    {
        res_columns[0]->insert(keyword);
    }
}

void fillDataWithClusters(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & clusters = context->getClusters();
    for (const auto & [cluster_name, _] : clusters)
    {
        res_columns[0]->insert(cluster_name);
    }
}

void fillDataWithMacros(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & macros = context->getMacros();
    for (const auto & [macro_name, _] : macros->getMacroMap())
    {
        res_columns[0]->insert(macro_name);
    }
}

void fillDataWithPolicies(MutableColumns & res_columns, const ContextPtr & context)
{
    for (const auto & [policy_name, _] : context->getPoliciesMap())
    {
        res_columns[0]->insert(policy_name);
    }
}

void fillDataWithDictionaries(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & access = context->getAccess();
    if (access->isGranted(AccessType::SHOW_DICTIONARIES))
        return;

    const auto & external_dictionaries = context->getExternalDictionariesLoader();
    for (const auto & load_result : external_dictionaries.getLoadResults())
    {
        const auto dict_ptr = std::dynamic_pointer_cast<const IDictionary>(load_result.object);

        StorageID dict_id = StorageID::createEmpty();
        if (dict_ptr)
            dict_id = dict_ptr->getDictionaryID();
        else if (load_result.config)
            dict_id = StorageID::fromDictionaryConfig(*load_result.config->config, load_result.config->key_in_config);
        else
            dict_id.table_name = load_result.name;

        String db_or_tag = dict_id.database_name.empty() ? IDictionary::NO_DATABASE_TAG : dict_id.database_name;
        if (!access->isGranted(AccessType::SHOW_DICTIONARIES, db_or_tag, dict_id.table_name))
            continue;
        res_columns[0]->insert(dict_id.table_name);
    }
}

void StorageSystemCompletions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    fillDataWithDatabasesTablesColumns(res_columns, context);
    fillDataWithFunctions(res_columns, context);
    fillDataWithTableEngines(res_columns);
    fillDataWithFormats(res_columns);
    fillDataWithTableFunctions(res_columns, context);
    fillDataWithDataTypeFamilies(res_columns);
    fillDataWithMergeTreeSettings(res_columns, context);
    fillDataWithSettings(res_columns, context);
    fillDataWithKeywords(res_columns);
    fillDataWithClusters(res_columns, context);
    fillDataWithMacros(res_columns, context);
    fillDataWithPolicies(res_columns, context);
    fillDataWithDictionaries(res_columns, context);
}

}
