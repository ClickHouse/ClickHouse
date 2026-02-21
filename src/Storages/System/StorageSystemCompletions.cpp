#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/CommonParsers.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemCompletions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Macros.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 readonly;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
}

static constexpr const char * DATABASE_CONTEXT = "database";
static constexpr const char * TABLE_CONTEXT = "table";
static constexpr const char * COLUMN_CONTEXT = "column";
static constexpr const char * FUNCTION_CONTEXT = "function";
static constexpr const char * AGGREGATE_FUNCTION_COMBINATOR_PAIR_CONTEXT = "aggregate function combinator pair";
static constexpr const char * TABLE_ENGINE_CONTEXT = "table engine";
static constexpr const char * FORMAT_CONTEXT = "format";
static constexpr const char * TABLE_FUNCTION_CONTEXT = "table function";
static constexpr const char * DATA_TYPE_CONTEXT = "data type";
static constexpr const char * SETTING_CONTEXT = "setting";
static constexpr const char * KEYWORD_CONTEXT = "keyword";
static constexpr const char * CLUSTER_CONTEXT = "cluster";
static constexpr const char * MACRO_CONTEXT = "macro";
static constexpr const char * POLICY_CONTEXT = "policy";
static constexpr const char * DICTIONARY_CONTEXT = "dictionary";

ColumnsDescription StorageSystemCompletions::getColumnsDescription()
{
    auto description = ColumnsDescription{
        {"word", std::make_shared<DataTypeString>(), "Completion token."},
        {"context", std::make_shared<DataTypeString>(), "Token entity kind (e.g. table)."},
        {"belongs",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
         "Token for entity, this token belongs to (e.g. name of owning database)."}};
    return description;
}

void fillDataWithTableColumns(
    const String & database_name,
    const String & table_name,
    const StoragePtr & table,
    MutableColumns & res_columns,
    const ContextPtr & context)
{
    const auto & access = context->getAccess();
    if (!access->isGranted(AccessType::SHOW_TABLES) || !access->isGranted(AccessType::SHOW_TABLES, database_name)
        || !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
        return;

    if (!table)
        return; // table was dropped or deleted, while adding columns for previous table

    res_columns[0]->insert(table_name);
    res_columns[1]->insert(TABLE_CONTEXT);
    res_columns[2]->insert(database_name);

    if (!access->isGranted(AccessType::SHOW_COLUMNS) || !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name))
        return;

    auto table_lock = table->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    if (table_lock == nullptr)
        return; // table was dropped while acquiring the lock

    auto snapshot = table->tryGetInMemoryMetadataPtr();
    if (!snapshot)
        return;

    const auto & columns = (*snapshot)->getColumns();
    for (const auto & column : columns)
    {
        if (!access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name, column.name))
            continue;

        res_columns[0]->insert(column.name);
        res_columns[1]->insert(COLUMN_CONTEXT);
        res_columns[2]->insert(table_name);
    }
}

void fillDataWithDatabasesTablesColumns(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & access = context->getAccess();
    const auto & settings = context->getSettingsRef();
    const auto & databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = settings[Setting::show_data_lake_catalogs_in_system_tables]});
    for (const auto & [database_name, database_ptr] : databases)
    {
        if (!access->isGranted(AccessType::SHOW_DATABASES) || !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; // skipping internal database temporary tables

        res_columns[0]->insert(database_name);
        res_columns[1]->insert(DATABASE_CONTEXT);
        res_columns[2]->insertDefault();

        for (auto iterator = database_ptr->getTablesIterator(context); iterator->isValid(); iterator->next())
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
        }
    }
}

void fillDataWithFunctions(MutableColumns & res_columns, const ContextPtr & context)
{
    auto insert_function = [&](const String & name)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(FUNCTION_CONTEXT);
        res_columns[2]->insertDefault();
    };
    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
        insert_function(function_name);
    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
        insert_function(function_name);
    const auto & user_defined_function_names = UserDefinedExecutableFunctionFactory::getRegisteredNames(context);
    for (const auto & function_name : user_defined_function_names)
        insert_function(function_name);
}

void fillDataWithAggregateFunctionCombinatorPair(MutableColumns & res_columns)
{
    const auto & aggregate_functions = AggregateFunctionFactory::instance().getAllRegisteredNames();
    const auto & aggregate_function_combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & function_name : aggregate_functions)
    {
        for (const auto & [combinator_name, combinator] : aggregate_function_combinators)
        {
            if (combinator->isForInternalUsageOnly())
                continue;
            res_columns[0]->insert(function_name + combinator_name);
            res_columns[1]->insert(AGGREGATE_FUNCTION_COMBINATOR_PAIR_CONTEXT);
            res_columns[2]->insertDefault();
        }
    }
}

void fillDataWithTableEngines(MutableColumns & res_columns)
{
    const auto & storage_factory = StorageFactory::instance();
    const auto & table_engines = storage_factory.getAllStorages();
    for (const auto & [table_engine_name, _] : table_engines)
    {
        res_columns[0]->insert(table_engine_name);
        res_columns[1]->insert(TABLE_ENGINE_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void fillDataWithFormats(MutableColumns & res_columns)
{
    const auto & format_factory = FormatFactory::instance();
    const auto & formats = format_factory.getAllFormats();
    for (const auto & [_, creators] : formats)
    {
        res_columns[0]->insert(creators.name);
        res_columns[1]->insert(FORMAT_CONTEXT);
        res_columns[2]->insertDefault();
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
        {
            res_columns[0]->insert(function_name);
            res_columns[1]->insert(TABLE_FUNCTION_CONTEXT);
            res_columns[2]->insertDefault();
        }
    }
}

void fillDataWithDataTypeFamilies(MutableColumns & res_columns)
{
    const auto & data_type_factory = DataTypeFactory::instance();
    const auto & data_type_names = data_type_factory.getAllRegisteredNames();
    for (const auto & data_type_name : data_type_names)
    {
        res_columns[0]->insert(data_type_name);
        res_columns[1]->insert(DATA_TYPE_CONTEXT);
        res_columns[2]->insertDefault();
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
    const auto & setting_registered_names = settings.getAllRegisteredNames();
    const auto & setting_alias_names = settings.getAllAliasNames();
    auto insertNames = [&](const auto & names)
    {
        for (const auto & name : names)
        {
            res_columns[0]->insert(name);
            res_columns[1]->insert(SETTING_CONTEXT);
            res_columns[2]->insertDefault();
        }
    };
    insertNames(setting_registered_names);
    insertNames(setting_alias_names);
}

void fillDataWithKeywords(MutableColumns & res_columns)
{
    for (const auto & keyword : getAllKeyWords())
    {
        res_columns[0]->insert(keyword);
        res_columns[1]->insert(KEYWORD_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void fillDataWithClusters(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & clusters = context->getClusters();
    for (const auto & [cluster_name, _] : clusters)
    {
        res_columns[0]->insert(cluster_name);
        res_columns[1]->insert(CLUSTER_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void fillDataWithMacros(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & macros = context->getMacros();
    for (const auto & [macro_name, _] : macros->getMacroMap())
    {
        res_columns[0]->insert(macro_name);
        res_columns[1]->insert(MACRO_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void fillDataWithPolicies(MutableColumns & res_columns, const ContextPtr & context)
{
    for (const auto & [policy_name, _] : context->getPoliciesMap())
    {
        res_columns[0]->insert(policy_name);
        res_columns[1]->insert(POLICY_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void fillDataWithDictionaries(MutableColumns & res_columns, const ContextPtr & context)
{
    const auto & access = context->getAccess();
    if (!access->isGranted(AccessType::SHOW_DICTIONARIES))
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
        res_columns[1]->insert(DICTIONARY_CONTEXT);
        res_columns[2]->insertDefault();
    }
}

void StorageSystemCompletions::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
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
    fillDataWithAggregateFunctionCombinatorPair(res_columns);
}

}
