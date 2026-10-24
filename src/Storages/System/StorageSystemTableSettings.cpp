#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemTableSettings.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/HiddenSecret.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/SettingsChanges.h>
#include <Common/quoteString.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool show_remote_databases_in_system_tables;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnsDescription StorageSystemTableSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database",    std::make_shared<DataTypeString>(), "Database of the table."},
        {"table",       std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine_name", std::make_shared<DataTypeString>(), "Name of the table engine."},
        {"name",        std::make_shared<DataTypeString>(), "Setting name."},
        {"value",       std::make_shared<DataTypeString>(), "Setting value."},
        {"default",     std::make_shared<DataTypeString>(), "Setting default value."},
        {"changed",     std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"min",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."},
        {"max",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."},
        {"disallowed_values",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of disallowed values"},
        {"readonly",    std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
        {"type",        std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* PRIVATE PREVIEW: The feature is on a clear path to general availability. Its applicability is still limited and it is not recommended for production use.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
        {"inherited", std::make_shared<DataTypeUInt8>(),
            "1 if the table's definition does not state the setting and the table takes it from elsewhere: from the query or "
            "server settings (the format settings, for example), from the server configuration, or from the session that "
            "created the table. `value` is then the engine's own value, which the table does not use."},
    };
}

namespace
{

/// A change whose value is an expression, such as `disk = disk(type = ..., ...)`, defines something the engine
/// creates from it when the table is created. It is shown as written, with secrets hidden as `SHOW CREATE TABLE`
/// hides them, rather than created again.
SettingsChanges withExpressionsAsText(SettingsChanges changes, bool show_secrets)
{
    for (auto & change : changes)
    {
        CustomType custom;
        if (change.value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
            change.value = custom.toString(show_secrets);
    }
    return changes;
}

/// The engines whose creator loads settings from a named collection named by the first engine argument
/// (`loadFromNamedCollection`). For others, a first argument that is an identifier is not a collection.
bool loadsSettingsFromNamedCollection(std::string_view engine_name)
{
    return engine_name == "Kafka" || engine_name == "NATS" || engine_name == "RabbitMQ" || engine_name == "MySQL"
        || engine_name == "PostgreSQL" || engine_name == "YTsaurus";
}

/// The named collection a table's engine arguments start with, and the overrides the arguments give as
/// `key = literal`. Nothing is evaluated: an override that is an expression is left out.
struct NamedCollectionOfTable
{
    String name;
    NamedCollectionPtr collection;
    SettingsChanges overrides;
};

std::optional<NamedCollectionOfTable> namedCollectionOf(const ASTFunction & engine)
{
    if (!engine.arguments || engine.arguments->children.empty())
        return {};

    const auto & arguments = engine.arguments->children;
    const auto name = tryGetIdentifierName(arguments[0]);
    if (!name)
        return {};

    /// A collection dropped after the table was created gives no values.
    auto collection = NamedCollectionFactory::instance().tryGet(*name);
    if (!collection)
        return {};

    NamedCollectionOfTable result{.name = *name, .collection = std::move(collection), .overrides = {}};
    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const auto * function = arguments[i]->as<ASTFunction>();
        if (!function || function->name != "equals" || !function->arguments || function->arguments->children.size() != 2)
            continue;
        const auto key = tryGetIdentifierName(function->arguments->children[0]);
        const auto * literal = function->arguments->children[1]->as<ASTLiteral>();
        if (key && literal)
            result.overrides.emplace_back(*key, literal->value);
    }
    return result;
}

/// The value as this user may see it: a value that holds a secret by the rules `SHOW CREATE TABLE` uses is shown as
/// `[HIDDEN]`, as a whole even where `SHOW CREATE TABLE` hides only a part of it. A value equal to the compiled-in
/// default holds no secret, so an unset password is not reported as hidden.
String visibleValue(const SettingDescription & setting, bool show_secrets)
{
    if (show_secrets || setting.value == setting.default_value)
        return setting.value;

    /// A rule that hides only a part of a value (`nats_url`, `rabbitmq_address`, a connection string) renders a value
    /// without a secret unchanged.
    const auto rendered = renderSecretChangeValue(SettingChange(setting.name, Field(setting.value)));
    if (rendered && *rendered != quoteString(setting.value))
        return String(HIDDEN_SECRET);

    return setting.value;
}

}

Block StorageSystemTableSettings::getFilterSampleBlock() const
{
    /// Every column of the blocks `fillData` passes to `filterBlockWithPredicate`.
    return {
        {{}, std::make_shared<DataTypeString>(), "database"},
        {{}, std::make_shared<DataTypeString>(), "table"},
    };
}

void StorageSystemTableSettings::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);
    const bool show_secrets = canDisplaySecrets(context);
    const auto & storages = StorageFactory::instance().getAllStorages();

    /// Lake catalogs are left out, as in `system.s3_queue_settings`: their tables live in a remote catalog and state
    /// no settings of their own. Remote databases follow the setting `system.tables` follows.
    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{
        .with_datalake_catalogs = false,
        .with_remote_databases = context->getSettingsRef()[Setting::show_remote_databases_in_system_tables]});

    /// The query's condition on `database` and `table` is applied to the names first, so a table's definition is read
    /// only for the rows the query asks for. Only names this user may see are filtered, so the condition cannot probe
    /// the others; a database is visible as in `system.databases`.
    const bool show_databases_granted = access->isGranted(AccessType::SHOW_DATABASES);
    MutableColumnPtr database_names = ColumnString::create();
    for (const auto & [database_name, database] : databases)
        if (database_name != DatabaseCatalog::TEMPORARY_DATABASE
            && (show_databases_granted || access->isGranted(AccessType::SHOW_DATABASES, database_name)))
            database_names->insert(database_name);
    Block databases_block{ColumnWithTypeAndName(std::move(database_names), std::make_shared<DataTypeString>(), "database")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, databases_block, context);
    const auto & filtered_databases = databases_block.getByPosition(0).column;

    MutableColumnPtr table_databases = ColumnString::create();
    MutableColumnPtr table_names = ColumnString::create();
    for (size_t i = 0; i < filtered_databases->size(); ++i)
    {
        const String database_name{filtered_databases->getDataAt(i)};
        const bool show_tables_in_database = show_tables_granted || access->isGranted(AccessType::SHOW_TABLES, database_name);
        for (auto it = databases.at(database_name)->getTablesIterator(context); it->isValid(); it->next())
        {
            if (!show_tables_in_database && !access->isGranted(AccessType::SHOW_TABLES, database_name, it->name()))
                continue;
            table_databases->insert(database_name);
            table_names->insert(it->name());
        }
    }
    Block tables_block{
        ColumnWithTypeAndName(std::move(table_databases), std::make_shared<DataTypeString>(), "database"),
        ColumnWithTypeAndName(std::move(table_names), std::make_shared<DataTypeString>(), "table")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, tables_block, context);

    const auto & filtered_table_databases = tables_block.getByPosition(0).column;
    const auto & filtered_table_names = tables_block.getByPosition(1).column;
    for (size_t i = 0; i < filtered_table_names->size(); ++i)
    {
        const String database_name{filtered_table_databases->getDataAt(i)};
        const String table_name{filtered_table_names->getDataAt(i)};

        /// A view, a dictionary and a table created `AS` a table function have no engine definition of their own.
        const auto create_ast = databases.at(database_name)->tryGetCreateTableQuery(table_name, context);
        const auto * create = create_ast ? create_ast->as<ASTCreateQuery>() : nullptr;
        if (!create || !create->storage || !create->storage->engine)
            continue;

        const auto engine = storages.find(create->storage->engine->name);
        if (engine == storages.end() || !engine->second.features.enumerate_engine_settings_fn)
            continue;

        /// The creator's order: the named collection and its overrides in the engine arguments, or else the settings
        /// the engine arguments give, such as those of `Kafka`; then the `SETTINGS` clause.
        std::optional<NamedCollectionOfTable> named_collection;
        if (loadsSettingsFromNamedCollection(engine->first))
        {
            NamedCollectionFactory::instance().loadIfNot();
            named_collection = namedCollectionOf(*create->storage->engine);
        }

        SettingsChanges changes;
        const auto & engine_arguments = create->storage->engine->arguments;
        if (named_collection)
            changes = named_collection->overrides;
        else if (const auto & from_arguments = engine->second.features.settings_from_engine_arguments_fn;
                 from_arguments && engine_arguments)
            changes = from_arguments(engine_arguments->children);
        if (create->storage->settings)
            for (const auto & change : withExpressionsAsText(create->storage->settings->changes, show_secrets))
                changes.push_back(change);

        const auto & enumerate = engine->second.features.enumerate_engine_settings_fn;
        auto settings = enumerate(context, changes);

        /// A row the collection supplies is found by comparing with the rows without it, so aliases and each class's
        /// own handling of names apply. Like `loadFromNamedCollection`, only keys spelled as a setting's name count.
        std::vector<bool> from_named_collection(settings.size(), false);
        bool show_named_collection = false;
        if (named_collection)
        {
            SettingsChanges with_collection;
            for (const auto & setting : settings)
                if (named_collection->collection->has(setting.name))
                    with_collection.emplace_back(setting.name, named_collection->collection->get<String>(setting.name));
            for (const auto & change : changes)
                with_collection.push_back(change);

            /// The message of an error raised while applying the collection can hold one of its values, which this user
            /// may not see. Unlike the table's definition, a collection can be changed after the table was created.
            SettingDescriptions settings_with_collection;
            try
            {
                settings_with_collection = enumerate(context, with_collection);
            }
            catch (const Exception & e)
            {
                throw Exception(
                    e.code(),
                    "Named collection {} of table {}.{} holds a value that cannot be applied to the engine's settings",
                    backQuoteIfNeed(named_collection->name), backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
            }
            if (settings_with_collection.size() != settings.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine {} enumerated a different number of settings", engine->first);
            for (size_t j = 0; j < settings.size(); ++j)
                from_named_collection[j] = settings_with_collection[j].value != settings[j].value
                    || settings_with_collection[j].changed != settings[j].changed;
            settings = std::move(settings_with_collection);

            /// As `system.named_collections` shows the collection's values.
            show_named_collection = access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS, named_collection->name)
                && access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS_SECRETS) && show_secrets;
        }

        for (size_t j = 0; j < settings.size(); ++j)
        {
            const auto & setting = settings[j];

            /// The engines with settings a table takes from elsewhere start from compiled defaults, so `changed` means the
            /// definition, its engine arguments or its named collection state the setting, or the engine sets it itself.
            const bool inherited = !setting.changed && !setting.set_by_engine
                && StorageFactory::isInheritedSetting(engine->second.features, setting.name);

            Array disallowed_values;
            for (const auto & value : setting.disallowed_values)
                disallowed_values.emplace_back(value);

            size_t col = 0;
            res_columns[col++]->insert(database_name);
            res_columns[col++]->insert(table_name);
            res_columns[col++]->insert(engine->first);
            res_columns[col++]->insert(setting.name);
            res_columns[col++]->insert(
                from_named_collection[j] && !show_named_collection ? String("[HIDDEN]") : visibleValue(setting, show_secrets));
            res_columns[col++]->insert(setting.default_value);
            res_columns[col++]->insert(setting.changed);
            res_columns[col++]->insert(setting.comment);
            res_columns[col++]->insert(setting.min_value ? Field(*setting.min_value) : Field());
            res_columns[col++]->insert(setting.max_value ? Field(*setting.max_value) : Field());
            res_columns[col++]->insert(disallowed_values);
            res_columns[col++]->insert(setting.readonly);
            res_columns[col++]->insert(setting.type);
            res_columns[col++]->insert(setting.tier == SettingsTierType::OBSOLETE);
            res_columns[col++]->insert(setting.tier);
            res_columns[col++]->insert(inherited);
        }
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTableSettings) }
