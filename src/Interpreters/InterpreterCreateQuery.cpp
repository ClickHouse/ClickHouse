#include <array>
#include <memory>

#include <filesystem>

#include <Access/AccessControl.h>
#include <Access/User.h>

#include <Core/Settings.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/stripQuerySettings.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/PoolId.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/atomicRename.h>
#include <Common/filesystemHelpers.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>

#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <Core/ServerSettings.h>
#include <Core/UUID.h>

#include <IO/WriteHelpers.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryConstructionSettings.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/replaceLegacyToTime.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/TemporaryReplaceTableName.h>

#include <Access/Common/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/dataTypeToAST.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/hasNullable.h>

#include <Databases/DatabaseBackup.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/TablesLoader.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/NormalizeAndEvaluateConstantsVisitor.h>

#include <Dictionaries/getDictionaryConfigurationFromAST.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryMetadataCache.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>


namespace CurrentMetrics
{
    extern const Metric AttachedTable;
    extern const Metric AttachedReplicatedTable;
    extern const Metric AttachedDictionary;
    extern const Metric AttachedView;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_database_materialized_postgresql;
    extern const SettingsBool enable_full_text_index;
    extern const SettingsBool allow_statistics;
    extern const SettingsBool allow_materialized_view_with_bad_select;
    extern const SettingsBool compatibility_ignore_collation_in_create_table;
    extern const SettingsBool compatibility_ignore_auto_increment_in_create_table;
    extern const SettingsBool create_if_not_exists;
    extern const SettingsFloat create_replicated_merge_tree_fault_injection_probability;
    extern const SettingsBool database_atomic_wait_for_drop_and_detach_synchronously;
    extern const SettingsUInt64 database_replicated_allow_explicit_uuid;
    extern const SettingsBool database_replicated_allow_heavy_create;
    extern const SettingsBool database_replicated_allow_only_replicated_engine;
    extern const SettingsBool data_type_default_nullable;
    extern const SettingsSQLSecurityType default_materialized_view_sql_security;
    extern const SettingsSQLSecurityType default_normal_view_sql_security;
    extern const SettingsDefaultTableEngine default_table_engine;
    extern const SettingsDefaultTableEngine default_temporary_table_engine;
    extern const SettingsString default_view_definer;
    extern const SettingsUInt64 distributed_ddl_entry_format_version;
    extern const SettingsBool flatten_nested;
    extern const SettingsBool fsync_metadata;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool materialized_views_populate_atomically;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsBool restore_replace_external_engines_to_null;
    extern const SettingsBool restore_replace_external_table_functions_to_null;
    extern const SettingsBool restore_replace_external_dictionary_source_to_null;
    extern const SettingsBool stop_refreshable_materialized_views_on_startup;
    extern const SettingsBool use_legacy_to_time;
}

namespace ServerSetting
{
    extern const ServerSettingsBool ignore_empty_sql_security_in_create_view_query;
}

namespace FailPoints
{
    extern const char create_or_replace_before_rename[];
    extern const char atomic_populate_fail_before_subscription[];
    extern const char atomic_populate_pause_before_subscription[];
    extern const char atomic_populate_pause_after_view_publication[];
    extern const char atomic_populate_pause_before_source_guard[];
}

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_DATABASE_FOR_TEMPORARY_TABLE;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int PATH_ACCESS_DENIED;
    extern const int ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int ENGINE_REQUIRED;
    extern const int UNKNOWN_STORAGE;
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_TABLES;
    extern const int TOO_MANY_DATABASES;
    extern const int THERE_IS_NO_COLUMN;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int FAULT_INJECTED;
}

namespace fs = std::filesystem;

namespace
{

/// Substitutes SQL UDFs the way `createTable` does, but never into an engine: an engine is an
/// `ASTFunction` too, and a UDF may carry an engine's name, so substituting there would replace the
/// engine with a function body. Key expressions live in several places (storage, a view's inner
/// engine, a projection's own `ORDER BY`), so the walk covers the query rather than a list of slots.
void substituteUserDefinedFunctionsOutsideEngines(ASTPtr & ast, const ContextPtr & context)
{
    for (auto & child : ast->children)
    {
        if (!child)
            continue;

        const auto * storage = ast->as<ASTStorage>();
        if (storage && child.get() == storage->engine)
            continue;

        const IAST * old_ptr = child.get();
        substituteUserDefinedFunctionsOutsideEngines(child, context);
        if (child.get() != old_ptr)
            ast->updatePointerToChild(old_ptr, child);
    }

    if (ast->as<ASTFunction>() && !ast->as<ASTStorage>())
    {
        ASTPtr expression = ast;
        UserDefinedSQLFunctionVisitor::visit(expression, context);
        ast = expression;
    }
}

void normalizeLegacyToTimeInCreateQuery(ASTPtr & query, const ContextPtr & context)
{
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        substituteUserDefinedFunctionsOutsideEngines(query, context);
    replaceLegacyToTime(*query);
}

}

InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    auto component_guard = Coordination::setCurrentComponent("InterpreterCreateQuery::createDatabase");
    String database_name = create.getDatabase();

    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "", nullptr);

    /// Database can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard
    if (DatabaseCatalog::instance().isDatabaseExist(database_name))
    {
        if (create.if_not_exists)
            return {};
        throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Database {} already exists.", database_name);
    }

    auto db_num_limit = getContext()->getGlobalContext()->getMaxDatabaseNumToThrow();
    if (db_num_limit > 0 && !internal)
    {
        size_t db_count = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true}).size();
        std::initializer_list<std::string_view> system_databases =
        {
            DatabaseCatalog::TEMPORARY_DATABASE,
            DatabaseCatalog::SYSTEM_DATABASE,
            DatabaseCatalog::INFORMATION_SCHEMA,
            DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE,
        };

        for (const auto & system_database : system_databases)
        {
            if (db_count > 0 && DatabaseCatalog::instance().isDatabaseExist(std::string(system_database)))
                --db_count;
        }

        if (db_count >= db_num_limit)
            throw Exception(ErrorCodes::TOO_MANY_DATABASES,
                            "Too many databases. "
                            "The limit (server configuration parameter `max_database_num_to_throw`) is set to {}, the current number of databases is {}",
                            db_num_limit, db_count);
    }

    auto default_db_disk = getContext()->getDatabaseDisk();

    /// Will write file with database metadata, if needed.
    default_db_disk->createDirectories(DatabaseCatalog::getMetadataDirPath());
    auto metadata_file_path = DatabaseCatalog::getMetadataFilePath(database_name);
    auto metadata_tmp_file_path = DatabaseCatalog::getMetadataTmpFilePath(database_name);

    fs::path metadata_path;
    if (!create.storage && create.attach)
    {
        if (!default_db_disk->existsFile(metadata_file_path))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Database engine must be specified for ATTACH DATABASE query");
        /// Short syntax: try read database definition from file
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, getContext(), default_db_disk, metadata_file_path);
        create = ast->as<ASTCreateQuery &>();
        if (create.table || !create.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Metadata file {} contains incorrect CREATE DATABASE query", metadata_file_path.string());
        create.attach = true;
        create.attach_short_syntax = true;
        create.setDatabase(database_name);
    }
    else if (!create.storage || !create.storage->engine)
    {
        /// For new-style databases engine is explicitly specified in .sql
        /// When attaching old-style database during server startup, we must always use Ordinary engine
        if (create.attach)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Database engine must be specified for ATTACH DATABASE query");
        if (!create.storage)
        {
            auto storage = make_intrusive<ASTStorage>();
            create.set(create.storage, storage);
        }
        auto engine = make_intrusive<ASTFunction>();
        engine->name = "Atomic";
        engine->setNoEmptyArgs(true);
        create.storage->set(create.storage->engine, engine);
    }
    else if ((create.columns_list
              && ((create.columns_list->indices && !create.columns_list->indices->children.empty())
                  || (create.columns_list->projections && !create.columns_list->projections->children.empty()))))
    {
        /// Currently, there are no database engines, that support any arguments.
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", create.storage->formatForErrorMessage());
    }

    if (create.storage && !create.storage->engine)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Database engine must be specified");

    if (create.storage->engine->name == "Atomic"
        || create.storage->engine->name == "Replicated"
        || create.storage->engine->name == "MaterializedPostgreSQL")
    {
        if (create.attach && create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "UUID must be specified for ATTACH. "
                            "If you want to attach existing database, use just ATTACH DATABASE {};", create.getDatabase());
        if (create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();

        metadata_path = DatabaseCatalog::getStoreDirPath(create.uuid);

        if (!create.attach && default_db_disk->existsDirectory(metadata_path) && !default_db_disk->isDirectoryEmpty(metadata_path))
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists and is not empty", metadata_path.string());
    }
    else
    {
        bool is_on_cluster = getContext()->isDDLOrOnClusterInternal();
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster && !internal)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Ordinary database engine does not support UUID");

        /// The database doesn't support UUID so we'll ignore it. The UUID could be set here because of either
        /// a) the initiator of `ON CLUSTER` query generated it to ensure the same UUIDs are used on different hosts; or
        /// b) `RESTORE from backup` query generated it to ensure the same UUIDs are used on different hosts.
        create.uuid = UUIDHelpers::Nil;
        metadata_path = DatabaseCatalog::getMetadataDirPath(database_name);
    }

    if (create.storage->engine->name == "MaterializedPostgreSQL"
        && !getContext()->getSettingsRef()[Setting::allow_experimental_database_materialized_postgresql] && !internal && !create.attach)
    {
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                        "MaterializedPostgreSQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialized_postgresql to use it");
    }

    bool need_write_metadata = !create.attach || !default_db_disk->existsFile(metadata_file_path);
    bool need_lock_uuid = internal || need_write_metadata;
    auto mode = getLoadingStrictnessLevel(create.attach, force_attach, has_force_restore_data_flag, /*secondary*/ false);

    /// Lock uuid, so we will known it's already in use.
    /// We do it when attaching databases on server startup (internal) and on CREATE query (!create.attach);
    TemporaryLockForUUIDDirectory uuid_lock;
    if (need_lock_uuid)
        uuid_lock = TemporaryLockForUUIDDirectory{create.uuid};
    else if (create.uuid != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(create.uuid))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create.uuid);

    DatabasePtr database = DatabaseFactory::instance().get(create, metadata_path / "", getContext(), mode, internal);

    if (create.uuid != UUIDHelpers::Nil)
        create.setDatabase(TABLE_WITH_UUID_NAME_PLACEHOLDER);

    if (need_write_metadata)
    {
        create.attach = true;
        create.if_not_exists = false;

        WriteBufferFromOwnString statement_buf;
        IAST::FormatSettings format_settings(/*one_line=*/false);
        create.format(statement_buf, format_settings);
        writeChar('\n', statement_buf);
        String statement = statement_buf.str();

        /// Needed to make database creation retriable if it fails after the file is created
        default_db_disk->removeFileIfExists(metadata_tmp_file_path);

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        writeMetadataFile(
            default_db_disk,
            /*file_path=*/metadata_tmp_file_path,
            /*content=*/statement,
            /*fsync_metadata=*/getContext()->getSettingsRef()[Setting::fsync_metadata]);
    }

    /// We attach database before loading it's tables, so do not allow concurrent DDL queries
    auto db_guard = DatabaseCatalog::instance().getExclusiveDDLGuardForDatabase(database_name);

    bool added = false;
    bool renamed = false;
    try
    {
        /// TODO Attach db only after it was loaded. Now it's not possible because of view dependencies
        DatabaseCatalog::instance().attachDatabase(database_name, database);
        added = true;

        if (need_write_metadata)
        {
            /// Prevents from overwriting metadata of detached database
            default_db_disk->moveFile(metadata_tmp_file_path, metadata_file_path);
            renamed = true;
        }

        if (!load_database_without_tables)
        {
            /// We use global context here, because storages lifetime is bigger than query context lifetime
            TablesLoader loader{getContext()->getGlobalContext(), {{database_name, database}}, mode};
            auto load_tasks = loader.loadTablesAsync();
            auto startup_tasks = loader.startupTablesAsync();
            /// First prioritize, schedule and wait all the load table tasks
            waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), load_tasks);
            /// Only then prioritize, schedule and wait all the startup tasks
            waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), startup_tasks);
        }
    }
    catch (...)
    {
        if (renamed)
        {
            chassert(default_db_disk->existsFile(metadata_file_path));
            default_db_disk->removeFileIfExists(metadata_file_path);
        }
        if (added)
            DatabaseCatalog::instance().detachDatabase(getContext(), database_name, false, false);

        throw;
    }

    return {};
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
    auto columns_list = make_intrusive<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = make_intrusive<ASTColumnDeclaration>();

        column_declaration->name = column.name;
        column_declaration->setType(dataTypeToAST(column.type));

        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns)
{
    boost::intrusive_ptr<ASTExpressionList> columns_list = boost::static_pointer_cast<ASTExpressionList>(formatColumns(columns));

    for (const auto & alias_column : alias_columns)
    {
        const auto column_declaration = make_intrusive<ASTColumnDeclaration>();

        column_declaration->name = alias_column.name;
        column_declaration->setType(dataTypeToAST(alias_column.type));

        column_declaration->default_specifier = ColumnDefaultSpecifier::Alias;

        const auto & alias = alias_column.expression;
        const char * alias_pos = alias.data();
        const char * alias_end = alias_pos + alias.size();
        ParserExpression expression_parser;
        column_declaration->setDefaultExpression(parseQuery(expression_parser, alias_pos, alias_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));

        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const ColumnsDescription & columns)
{
    auto columns_list = make_intrusive<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = make_intrusive<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;
        column_declaration->setType(dataTypeToAST(column.type));

        if (column.default_desc.expression)
        {
            column_declaration->default_specifier = toColumnDefaultSpecifier(column.default_desc.kind);
            column_declaration->setDefaultExpression(column.default_desc.expression->clone());
        }

        column_declaration->ephemeral_default = column.default_desc.ephemeral_default;

        if (!column.comment.empty())
        {
            column_declaration->setComment(make_intrusive<ASTLiteral>(Field(column.comment)));
        }

        if (column.codec)
        {
            column_declaration->setCodec(column.codec->clone());
        }

        if (column.statistics.hasExplicitStatistics())
        {
            column_declaration->setStatisticsDesc(column.statistics.getAST());
        }

        if (column.ttl)
        {
            column_declaration->setTTL(column.ttl->clone());
        }

        if (!column.settings.empty())
        {
            auto settings = make_intrusive<ASTSetQuery>();
            settings->is_standalone = false;
            settings->changes = column.settings;
            column_declaration->setSettings(std::move(settings));
        }

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatIndices(const IndicesDescription & indices)
{
    auto res = make_intrusive<ASTExpressionList>();

    for (const auto & index : indices)
        if (!index.isImplicitlyCreated())
            res->children.push_back(index.definition_ast->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatConstraints(const ConstraintsDescription & constraints)
{
    auto res = make_intrusive<ASTExpressionList>();

    for (const auto & constraint : constraints.getConstraints())
        res->children.push_back(constraint->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatProjections(const ProjectionsDescription & projections)
{
    auto res = make_intrusive<ASTExpressionList>();

    for (const auto & projection : projections)
        res->children.push_back(projection.definition_ast->clone());

    return res;
}

DataTypePtr InterpreterCreateQuery::getColumnType(
    const ASTColumnDeclaration & col_decl, const LoadingStrictnessLevel mode, const bool make_columns_nullable)
{
    auto col_type = col_decl.getType();
    if (!col_type)
    {
        /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr column_type = DataTypeFactory::instance().get(col_type);

    if (LoadingStrictnessLevel::ATTACH <= mode)
        setVersionToAggregateFunctions(column_type, true);
    else
        /// Spell the state version the column is going to be written with out in the type, so that
        /// it gets into the table metadata and the data stays readable when a newer server changes
        /// the default: an unversioned name in stored metadata denotes the layout from before the
        /// function became versioned (the ATTACH branch above pins it to 0).
        pinCurrentStateVersionToAggregateFunctions(column_type);

    if (col_decl.null_modifier)
    {
        if (column_type->isNullable())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Can't use [NOT] NULL modifier with Nullable type");
        if (*col_decl.null_modifier)
            column_type = makeNullable(column_type);
    }
    else if (make_columns_nullable)
    {
        column_type = makeNullable(column_type);
    }
    else if (auto default_expr = col_decl.getDefaultExpression();
        !hasNullable(column_type) && col_decl.default_specifier == ColumnDefaultSpecifier::Default && default_expr
        && default_expr->as<ASTLiteral>() && default_expr->as<ASTLiteral>()->value.isNull())
    {
        if (column_type->lowCardinality())
        {
            const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column_type.get());
            chassert(low_cardinality_type);
            column_type = std::make_shared<DataTypeLowCardinality>(makeNullable(low_cardinality_type->getDictionaryType()));
        }
        else
            column_type = makeNullable(column_type);
    }
    return column_type;
}

ColumnsDescription InterpreterCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns_ast, ContextPtr context_, LoadingStrictnessLevel mode, bool is_restore_from_backup, bool check_defaults_over_virtual_columns)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    DefaultExpressionsInfo default_expr_info;
    default_expr_info.expr_list = make_intrusive<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;

    /// On a DDL worker (ON CLUSTER / Replicated database) the query was already normalized on the initiator.
    /// Known limitation: with distributed_ddl_entry_format_version < NORMALIZE_CREATE_ON_INITIATOR_VERSION
    /// the initiator does not normalize the query, and the transforms are wrongly skipped here too.
    const bool already_normalized_on_initiator = context_->isDDLOrOnClusterInternal();

    bool make_columns_nullable = mode < LoadingStrictnessLevel::SECONDARY_CREATE
        && !already_normalized_on_initiator
        && !is_restore_from_backup
        && context_->getSettingsRef()[Setting::data_type_default_nullable];

    for (const auto & ast : columns_ast.children)
    {
        const auto & col_decl = ast->as<ASTColumnDeclaration &>();

        if (col_decl.getCollation() && !context_->getSettingsRef()[Setting::compatibility_ignore_collation_in_create_table])
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Cannot support collation, please set compatibility_ignore_collation_in_create_table=true");
        }


        column_names_and_types.emplace_back(col_decl.name, getColumnType(col_decl, mode, make_columns_nullable));

        /// add column to postprocessing if there is a default_expression specified
        getDefaultExpressionInfoInto(col_decl, column_names_and_types.back().type, default_expr_info);
    }

    Block defaults_sample_block;
    /// Set missing types and wrap default_expression's in a conversion-function if necessary.
    /// We try to avoid that validation while restoring from a backup because it might be slow or troublesome
    /// (for example, a default expression can contain dictGet() and that dictionary can access remote servers or
    /// require different users to authenticate).
    if (!default_expr_info.expr_list->children.empty()
        && (default_expr_info.has_columns_with_default_without_type || (mode <= LoadingStrictnessLevel::CREATE)))
    {
        /// Ordinary views never evaluate column defaults over an insert block, so a default over a
        /// virtual column is inert there and must not be rejected.
        NameSet insert_time_default_columns;
        if (check_defaults_over_virtual_columns)
            insert_time_default_columns = default_expr_info.insert_time_default_columns;
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_info.expr_list, column_names_and_types, context_, insert_time_default_columns);
    }

    bool skip_checks = LoadingStrictnessLevel::SECONDARY_CREATE <= mode;
    CodecValidationSettings codec_validation_settings = skip_checks ? CodecValidationSettings::trusted() : CodecValidationSettings(context_->getSettingsRef());

    ColumnsDescription res;
    auto name_type_it = column_names_and_types.begin();
    for (auto ast_it = columns_ast.children.begin(); ast_it != columns_ast.children.end(); ++ast_it, ++name_type_it)
    {
        ColumnDescription column;

        auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();

        column.name = col_decl.name;

        /// ignore or not other database extensions depending on compatibility settings
        if (col_decl.default_specifier == ColumnDefaultSpecifier::AutoIncrement
            && !context_->getSettingsRef()[Setting::compatibility_ignore_auto_increment_in_create_table])
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                            "AUTO_INCREMENT is not supported. To ignore the keyword "
                            "in column declaration, set `compatibility_ignore_auto_increment_in_create_table` to true");
        }

        if (auto default_expression = col_decl.getDefaultExpression())
        {
            if (context_->hasQueryContext() && context_->getQueryContext().get() == context_.get())
            {
                /// Normalize query only for original CREATE query, not on metadata loading.
                /// And for CREATE query we can pass local context, because result will not change after restart.
                NormalizeAndEvaluateConstantsVisitor::Data visitor_data{context_};
                NormalizeAndEvaluateConstantsVisitor visitor(visitor_data);
                visitor.visit(default_expression);
            }

            ASTPtr default_expr = default_expression->clone();

            if (col_decl.getType())
                column.type = name_type_it->type;
            else
            {
                column.type = defaults_sample_block.getByName(column.name).type;
                /// set nullability for case of column declaration w/o type but with default expression
                if ((col_decl.null_modifier && *col_decl.null_modifier) || make_columns_nullable)
                    column.type = makeNullable(column.type);
            }

            column.default_desc.kind = toColumnDefaultKind(col_decl.default_specifier);
            column.default_desc.expression = default_expr;
            column.default_desc.ephemeral_default = col_decl.ephemeral_default;
        }
        else if (col_decl.getType())
            column.type = name_type_it->type;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Neither default value expression nor type is provided for a column");

        if (auto comment = col_decl.getComment())
            column.comment = comment->as<ASTLiteral &>().value.safeGet<String>();

        if (auto codec = col_decl.getCodec())
        {
            if (col_decl.default_specifier == ColumnDefaultSpecifier::Alias)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
            column.codec
                = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, column.type, codec_validation_settings);
        }

        if (auto statistics_desc = col_decl.getStatisticsDesc())
        {
            if (!skip_checks && !context_->getSettingsRef()[Setting::allow_statistics])
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY, "Create table with statistics is disabled. Turn on allow_statistics");

            column.statistics = ColumnStatisticsDescription::fromStatisticsDescriptionAST(statistics_desc, column.name, column.type);
        }

        if (auto ttl = col_decl.getTTL())
            column.ttl = ttl;

        if (auto settings = col_decl.getSettings())
        {
            column.settings = settings->as<ASTSetQuery &>().changes;
            MergeTreeColumnSettings::validate(column.settings);
        }

        res.add(std::move(column));
    }

    if (mode < LoadingStrictnessLevel::SECONDARY_CREATE && !already_normalized_on_initiator
        && !is_restore_from_backup && context_->getSettingsRef()[Setting::flatten_nested])
        res.flattenNested();

    if (res.getAllPhysical().empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Cannot CREATE table without physical columns");

    if (mode <= LoadingStrictnessLevel::CREATE && !is_restore_from_backup && res.getInsertable().empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Cannot CREATE table without insertable columns");

    return res;
}


ConstraintsDescription InterpreterCreateQuery::getConstraintsDescription(
    const ASTExpressionList * constraints, const ColumnsDescription & columns, ContextPtr local_context)
{
    ASTs constraints_data;
    const auto column_names_and_types = columns.getAllPhysical();
    if (constraints)
        for (const auto & constraint : constraints->children)
        {
            auto clone = constraint->clone();
            TreeRewriter(local_context).analyze(clone, column_names_and_types);
            constraints_data.push_back(constraint->clone());
        }
    return ConstraintsDescription{constraints_data};
}


InterpreterCreateQuery::TableProperties InterpreterCreateQuery::getTablePropertiesAndNormalizeCreateQuery(
    ASTCreateQuery & create, LoadingStrictnessLevel mode)
{
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    /// We have to check access rights again (in case engine was changed).
    if (create.storage && create.storage->engine)
        getContext()->checkAccess(AccessType::TABLE_ENGINE, create.storage->engine->name);

    /// If this is a TimeSeries table then we need to normalize list of columns (add missing columns and reorder), and also set inner table engines.
    if (create.is_time_series_table && (mode <= LoadingStrictnessLevel::SECONDARY_CREATE))
        normalizeTimeSeriesDefinition(create, getContext(), mode, is_restore_from_backup);

    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (create.columns_list)
    {
        if (create.as_table_function && (create.columns_list->indices || create.columns_list->constraints))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Indexes and constraints are not supported for table functions");

        /// Dictionaries have dictionary_attributes_list instead of columns_list
        chassert(!create.is_dictionary);

        if (create.columns_list->columns)
        {
            /// An ordinary view and an external-target (`TO`) materialized view never evaluate their own
            /// column defaults over an insert block (a `TO` MV forwards inserts to the target using the
            /// target metadata), so a default over a virtual column is inert there and must not be rejected.
            const bool check_defaults_over_virtual_columns
                = !(create.is_ordinary_view || create.is_materialized_view_with_external_target());
            properties.columns = getColumnsDescription(
                *create.columns_list->columns, getContext(), mode, is_restore_from_backup, check_defaults_over_virtual_columns);
        }

        if (create.columns_list->indices)
        {
            for (const auto & index : create.columns_list->indices->children)
            {
                constexpr bool is_implicitly_created = false;
                constexpr bool escape_index_filenames = true; /// We don't care about this value because it won't be used
                IndexDescription index_desc = IndexDescription::getIndexFromAST(
                    index->clone(), properties.columns, is_implicitly_created, escape_index_filenames, getContext());
                if (properties.indices.has(index_desc.name))
                    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Duplicated index name {} is not allowed. Please use a different index name", backQuoteIfNeed(index_desc.name));

                const auto & settings = getContext()->getSettingsRef();
                if (index_desc.type == TEXT_INDEX_NAME && !settings[Setting::enable_full_text_index])
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The text index feature is disabled. Enable the setting 'enable_full_text_index' to use it");

                properties.indices.push_back(index_desc);
            }
        }

        if (create.columns_list->projections)
            for (const auto & projection_ast : create.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, properties.columns, nullptr, getContext(), mode);
                properties.projections.add(std::move(projection));
            }

        properties.constraints = getConstraintsDescription(create.columns_list->constraints, properties.columns, getContext());
    }
    else if (!create.as_table.empty())
    {
        String as_database_name = getContext()->resolveDatabase(create.as_database);
        getContext()->checkAccess(AccessType::SHOW_COLUMNS, as_database_name, create.as_table);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, getContext());

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr(getContext(), false);
        properties.columns = as_storage_metadata->getColumns();

        if (!create.comment && !as_storage_metadata->comment.empty())
            create.set(create.comment, make_intrusive<ASTLiteral>(as_storage_metadata->comment));

        /// Secondary indices and projections make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
        {
            /// Copy secondary indexes but only the ones which were not implicitly created. These will be re-generated later again and need
            /// not be copied.
            const auto & indices = as_storage_metadata->getSecondaryIndices();
            for (const auto & index : indices)
                if (!index.isImplicitlyCreated())
                    properties.indices.push_back(index);

            /// Copy projections.
            properties.projections = as_storage_metadata->getProjections().clone();

            /// CREATE TABLE AS should copy PRIMARY KEY, ORDER BY, and similar clauses.
            /// Note: only supports the source table engine is using the new syntax.
            if (const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(as_storage.get()))
            {
                if (merge_tree_data->format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                {
                    if (!create.storage->primary_key && as_storage_metadata->isPrimaryKeyDefined() && as_storage_metadata->hasPrimaryKey())
                        create.storage->set(create.storage->primary_key, as_storage_metadata->getPrimaryKeyAST()->clone());

                    if (!create.storage->partition_by && as_storage_metadata->isPartitionKeyDefined() && as_storage_metadata->hasPartitionKey())
                        create.storage->set(create.storage->partition_by, as_storage_metadata->getPartitionKeyAST()->clone());

                    if (!create.storage->order_by && as_storage_metadata->isSortingKeyDefined() && as_storage_metadata->hasSortingKey())
                        create.storage->set(create.storage->order_by, as_storage_metadata->getSortingKeyAST()->clone());

                    if (!create.storage->sample_by && as_storage_metadata->isSamplingKeyDefined() && as_storage_metadata->hasSamplingKey())
                        create.storage->set(create.storage->sample_by, as_storage_metadata->getSamplingKeyAST()->clone());
                }
            }
        }
        else
        {
            /// Only MergeTree support TTL
            properties.columns.resetColumnTTLs();
        }

        properties.constraints = as_storage_metadata->getConstraints();

        if (create.is_clone_as)
        {
            if (!endsWith(as_storage->getName(), "MergeTree"))
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only support CLONE AS from tables of the MergeTree family");

            if (create.storage)
            {
                if (!endsWith(create.storage->engine->name, "MergeTree"))
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only support CLONE AS with tables of the MergeTree family");

                /// Ensure that as_storage and the new storage has the same primary key, sorting key and partition key
                auto query_to_string = [](const IAST * ast) { return ast ? ast->formatWithSecretsOneLine() : ""; };

                const String as_storage_sorting_key_str = query_to_string(as_storage_metadata->getSortingKeyAST().get());
                const String as_storage_primary_key_str = query_to_string(as_storage_metadata->getPrimaryKeyAST().get());
                const String as_storage_partition_key_str = query_to_string(as_storage_metadata->getPartitionKeyAST().get());

                const String storage_sorting_key_str = query_to_string(create.storage->order_by);
                const String storage_primary_key_str = query_to_string(create.storage->primary_key);
                const String storage_partition_key_str = query_to_string(create.storage->partition_by);

                if (as_storage_sorting_key_str != storage_sorting_key_str)
                {
                    /// It is possible that the storage only has primary key and an empty sorting key, and as_storage has both primary key and sorting key with the same value.
                    if (as_storage_sorting_key_str != as_storage_primary_key_str || as_storage_sorting_key_str != storage_primary_key_str)
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different ordering");
                    }
                }
                if (as_storage_partition_key_str != storage_partition_key_str)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different partition key");

                if (as_storage_primary_key_str != storage_primary_key_str)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different primary key");
            }
        }
    }
    else if (create.select)
    {
        if (create.isParameterizedView())
            return properties;

        if (create.aliases_list)
        {
            auto & aliases_children = create.aliases_list->children;
            const auto * select_with_union_query = create.select->as<ASTSelectWithUnionQuery>();

            if (!select_with_union_query)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ASTSelectWithUnionQuery");

            std::function<void(const ASTPtr &)> apply_aliases = [&](const ASTPtr & node)
            {
                /// Must check ASTSelectIntersectExceptQuery before ASTSelectQuery
                if (const auto * intersect_except = node->as<ASTSelectIntersectExceptQuery>())
                {
                    for (const auto & child : intersect_except->getListOfSelects())
                        apply_aliases(child);
                }
                else if (const auto * select_query = node->as<ASTSelectQuery>())
                {
                    auto select_expression_list = select_query->select();
                    if (!select_expression_list)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "No select expressions in SELECT query");

                    auto & select_expressions = select_expression_list->children;

                    /// Check for asterisks and COLUMNS matchers — we cannot set aliases on them at AST level.
                    for (const auto & expr : select_expressions)
                    {
                        if (expr->as<ASTAsterisk>() || expr->as<ASTQualifiedAsterisk>()
                            || expr->as<ASTColumnsRegexpMatcher>() || expr->as<ASTColumnsListMatcher>()
                            || expr->as<ASTQualifiedColumnsRegexpMatcher>() || expr->as<ASTQualifiedColumnsListMatcher>())
                            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Cannot use column aliases with asterisk (*) or COLUMNS matcher in SELECT list of a view definition. "
                                "Please list the columns explicitly");
                    }

                    if (select_expressions.size() != aliases_children.size())
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Number of aliases does not match number of expressions in SELECT list");
                    }

                    for (size_t i = 0; i < select_expressions.size(); ++i)
                    {
                        auto & expr = select_expressions[i];
                        const auto & alias_ast = aliases_children[i]->as<ASTIdentifier &>();
                        expr->setAlias(alias_ast.name());
                    }
                }
                else if (const auto * nested_union = node->as<ASTSelectWithUnionQuery>())
                {
                    for (const auto & child : nested_union->list_of_selects->children)
                        apply_aliases(child);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST node inside ASTSelectWithUnionQuery: {}", node->getID());
            };

            const auto & selects = select_with_union_query->list_of_selects->children;
            for (const auto & select : selects)
                apply_aliases(select);
        }

        /// For refreshable materialized views, use the MV's database as context for the view's SELECT analysis.
        /// This ensures unqualified table/view references resolve in the MV's database, not the session's database.
        ContextPtr select_context = getContext();
        bool is_refreshable_mv = create.is_materialized_view && create.refresh_strategy;
        if (is_refreshable_mv)
        {
            auto mv_context = Context::createCopy(getContext());
            mv_context->setCurrentDatabase(create.getDatabase());
            select_context = mv_context;
        }

        SharedHeader as_select_sample;

        if (getContext()->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            as_select_sample = InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(),
                select_context,
                SelectQueryOptions{}.analyze().checkSubqueryTableAccess());
        }
        else
        {
            /// For refreshable materialized views, allow parameterized views in the query.
            /// This prevents the old analyzer from trying to execute table functions during analysis.
            as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(),
                select_context,
                false /* is_subquery */,
                is_refreshable_mv /* is_create_parameterized_view */);
        }

        auto columns_from_select = as_select_sample->getNamesAndTypesList();
        if (mode < LoadingStrictnessLevel::ATTACH)
        {
            /// A fresh `...State(...)` result type already spells its state version out, but an
            /// inferred type can also come from an unversioned source (a `CREATE TABLE ... AS SELECT`
            /// over an old table), so the version is pinned into the inferred types the same way it
            /// is pinned into explicitly declared ones (see `getColumnType`) for it to reach the
            /// stored metadata. On ATTACH the types are re-inferred rather than read from legacy
            /// metadata, so they keep denoting the default version, as before.
            for (auto & column : columns_from_select)
                pinCurrentStateVersionToAggregateFunctions(column.type);
        }
        properties.columns = ColumnsDescription(std::move(columns_from_select));
        properties.columns_inferred_from_select_query = true;
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function_ast = create.as_table_function->ptr();
        auto table_function = TableFunctionFactory::instance().get(table_function_ast, getContext());
        properties.columns = table_function->getActualTableStructureWithAccess(getContext(), /*is_insert_query*/ true);
    }
    else if (create.is_dictionary)
    {
        if (!create.dictionary || !create.dictionary->source)
            return {};

        /// Evaluate expressions (like currentDatabase() or tcpPort()) in dictionary source definition.
        NormalizeAndEvaluateConstantsVisitor::Data visitor_data{getContext()};
        NormalizeAndEvaluateConstantsVisitor visitor(visitor_data);
        visitor.visit(create.dictionary->source->ptr());

        return {};
    }
    else if (!create.storage || !create.storage->engine)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected application state. CREATE query is missing either its storage or engine.");
    /// We can have queries like "CREATE TABLE <table> ENGINE=<engine>" if <engine>
    /// supports schema inference (will determine table structure in it's constructor).
    else if (!StorageFactory::instance().getStorageFeatures(create.storage->engine->name).supports_schema_inference)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect CREATE query: required list of column descriptions or AS section or SELECT.");

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    if (!create.columns_list)
        create.set(create.columns_list, make_intrusive<ASTColumns>());

    ASTPtr new_columns = formatColumns(properties.columns);
    ASTPtr new_indices = formatIndices(properties.indices);
    ASTPtr new_constraints = formatConstraints(properties.constraints);
    ASTPtr new_projections = formatProjections(properties.projections);

    create.columns_list->setOrReplace(create.columns_list->columns, new_columns);
    create.columns_list->setOrReplace(create.columns_list->indices, new_indices);
    create.columns_list->setOrReplace(create.columns_list->constraints, new_constraints);
    create.columns_list->setOrReplace(create.columns_list->projections, new_projections);

    validateTableStructure(create, properties);

    chassert(as_database_saved.empty() && as_table_saved.empty());
    std::swap(create.as_database, as_database_saved);
    std::swap(create.as_table, as_table_saved);
    if (!as_table_saved.empty())
        create.is_create_empty = false;

    return properties;
}

void InterpreterCreateQuery::validateTableStructure(const ASTCreateQuery & create,
                                                    const InterpreterCreateQuery::TableProperties & properties) const
{
    /// Check for duplicates
    std::set<String> all_columns;
    for (const auto & column : properties.columns)
    {
        if (!all_columns.emplace(column.name).second)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column {} already exists", backQuoteIfNeed(column.name));
    }

    const auto & settings = getContext()->getSettingsRef();

    /// If it's not attach and not materialized view to existing table,
    /// we need to validate data types (check for experimental or suspicious types).
    if (!create.attach && !create.is_materialized_view)
    {
        DataTypeValidationSettings validation_settings(settings);
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
            validateDataType(name_and_type_pair.type, validation_settings);
    }
}

void InterpreterCreateQuery::validateMaterializedViewColumnsAndEngine(const ASTCreateQuery & create, const TableProperties & properties, const DatabasePtr & database)
{
    /// This is not strict validation, just catches common errors that would make the view not work.
    /// It's possible to circumvent these checks by ALTERing the view or target table after creation;
    /// we should probably do some of these checks on ALTER as well.

    NamesAndTypesList all_output_columns;
    bool check_columns = false;
    if (create.hasTargetTableID(ViewTarget::To))
    {
        StoragePtr to_table;
        try
        {
            to_table = DatabaseCatalog::instance().getTable(
                create.getTargetTableID(ViewTarget::To), getContext());
        }
        catch (...)
        {
            if (!getContext()->getSettingsRef()[Setting::allow_materialized_view_with_bad_select])
                throw;
        }

        if (to_table)
        {
            auto to_table_metadata = to_table->getInMemoryMetadataPtr(getContext(), false);
            all_output_columns = to_table_metadata->getSampleBlockInsertable().getNamesAndTypesList();
            check_columns = true;
        }
    }
    else if (!properties.columns_inferred_from_select_query)
    {
        all_output_columns = properties.columns.getInsertable();
        check_columns = true;
    }

    if (create.refresh_strategy && !create.refresh_strategy->append)
    {
        if (database && database->getEngineName() != "Atomic" && database->getEngineName() != "Replicated")
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Refreshable materialized views (except with APPEND) only support Atomic and Replicated database engines, but database {} has engine {}", create.getDatabase(), database->getEngineName());

        std::string message;
        if (!supportsAtomicRename(&message))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Can't create refreshable materialized view because exchanging files is not supported by the OS ({})", message);
    }

    SharedHeader input_block;

    if (check_columns)
    {
        try
        {
            if (getContext()->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                /// We should treat SELECT as an initial query in order to properly analyze it.
                auto context = Context::createCopy(getContext());
                context->setQueryKindInitial();

                /// For refreshable materialized views, use the MV's database as context.
                /// This ensures unqualified references resolve in the MV's database, not session's database.
                if (create.refresh_strategy)
                    context->setCurrentDatabaseUnchecked(create.getDatabase());

                input_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(),
                    context,
                    SelectQueryOptions{}.analyze().createView().checkSubqueryTableAccess());
            }
            else
            {
                /// For refreshable materialized views with old analyzer, use MV's database context.
                ContextPtr select_context = getContext();
                bool is_refreshable_mv = create.refresh_strategy != nullptr;
                if (is_refreshable_mv)
                {
                    auto mv_context = Context::createCopy(getContext());
                    mv_context->setCurrentDatabaseUnchecked(create.getDatabase());
                    select_context = mv_context;
                }

                /// For refreshable materialized views, allow parameterized views in the query.
                /// This prevents the old analyzer from trying to execute table functions during analysis.
                auto options = SelectQueryOptions().analyze();
                if (is_refreshable_mv)
                    options = options.createParameterizedView();

                input_block = InterpreterSelectWithUnionQuery(create.select->clone(),
                    select_context,
                    options).getSampleBlock();
            }
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::ACCESS_DENIED)
                throw;

            if (!getContext()->getSettingsRef()[Setting::allow_materialized_view_with_bad_select])
                throw;
            check_columns = false;
        }
    }

    if (check_columns)
    {
        std::unordered_map<std::string_view, DataTypePtr> output_types;
        for (const NameAndTypePair & nt : all_output_columns)
            output_types[nt.name] = nt.type;

        ColumnsWithTypeAndName input_columns;
        ColumnsWithTypeAndName output_columns;
        for (const auto & input_column : *input_block)
        {
            auto it = output_types.find(input_column.name);
            if (it != output_types.end())
            {
                input_columns.push_back(input_column.cloneEmpty());
                output_columns.push_back(ColumnWithTypeAndName(it->second->createColumn(), it->second, input_column.name));
            }
            else if (create.refresh_strategy || !getContext()->getSettingsRef()[Setting::allow_materialized_view_with_bad_select])
            {
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "SELECT query outputs column with name '{}', which is not found in the target table. Use 'AS' to assign alias that matches a column name", input_column.name);
            }
        }

        if (input_columns.empty())
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "None of the columns produced by the SELECT query are present in the target table. Use 'AS' to assign aliases that match column names");

        ActionsDAG::makeConvertingActions(
            input_columns,
            output_columns,
            ActionsDAG::MatchColumnsMode::Position,
            getContext()
        );
    }
}

namespace
{
    void checkTemporaryTableEngineName(const String & name)
    {
        if (name.starts_with("Replicated") || name.starts_with("Shared") || name == "KeeperMap")
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Temporary tables cannot be created with Replicated, Shared or KeeperMap table engines");
    }

    void setDefaultTableEngine(ASTStorage & storage, DefaultTableEngine engine)
    {
        if (engine == DefaultTableEngine::None)
            throw Exception(ErrorCodes::ENGINE_REQUIRED, "Table engine is not specified in CREATE query");

        auto engine_ast = make_intrusive<ASTFunction>();
        engine_ast->name = SettingFieldDefaultTableEngine(engine).toString();
        engine_ast->setNoEmptyArgs(true);
        storage.set(storage.engine, engine_ast);
    }

    /// Merge the storage settings of the source table (in `CREATE TABLE x AS y`) into the settings
    /// explicitly specified for the new table. The explicitly specified settings take precedence;
    /// the rest are inherited from the source table.
    void mergeStorageSettings(ASTStorage & storage, const ASTSetQuery * source_settings)
    {
        if (!source_settings || source_settings->changes.empty())
            return;

        if (!storage.settings)
        {
            storage.set(storage.settings, source_settings->clone());
            return;
        }

        for (const auto & change : source_settings->changes)
            storage.settings->changes.insertSetting(change.name, change.value);
    }

    /// Inherit the storage definition of the source table (in `CREATE TABLE x AS y <storage_clauses>` without
    /// an explicit ENGINE) into the partial storage definition of the new table. The engine and every storage
    /// clause (PARTITION BY, PRIMARY KEY, ORDER BY, SAMPLE BY, TTL, UNIQUE KEY) that was not explicitly
    /// specified for the new table is taken from the source; explicitly specified clauses (and individual
    /// SETTINGS) take precedence. This preserves the full inheritance of plain `CREATE TABLE x AS y` (engine,
    /// keys, TTL, ...) while still allowing individual clauses and settings to be overridden, and it also
    /// works when the source is a materialized view whose inherited engine lives in its inner storage.
    void inheritStorageFromSource(ASTStorage & storage, const ASTStorage & source)
    {
        /// We only reach this for `CREATE TABLE x AS y <storage_clauses>` without an explicit ENGINE.
        chassert(!storage.engine);
        if (source.engine)
            storage.set(storage.engine, source.engine->clone());

        if (!storage.partition_by && source.partition_by)
            storage.set(storage.partition_by, source.partition_by->clone());
        if (!storage.primary_key && source.primary_key)
            storage.set(storage.primary_key, source.primary_key->clone());
        if (!storage.order_by && source.order_by)
            storage.set(storage.order_by, source.order_by->clone());
        if (!storage.sample_by && source.sample_by)
            storage.set(storage.sample_by, source.sample_by->clone());
        if (!storage.ttl_table && source.ttl_table)
            storage.set(storage.ttl_table, source.ttl_table->clone());
        if (!storage.unique_key && source.unique_key)
            storage.set(storage.unique_key, source.unique_key->clone());

        mergeStorageSettings(storage, source.settings);
    }

    void setNullTableEngine(ASTStorage & storage)
    {
        storage.forEachPointerToChild([](IAST ** ptr, boost::intrusive_ptr<IAST> *)
        {
            *ptr = nullptr;
        });

        auto engine_ast = make_intrusive<ASTFunction>();
        engine_ast->name = "Null";
        engine_ast->setNoEmptyArgs(true);
        storage.set(storage.engine, engine_ast);
    }

    /// For external tables with the `restore_replace_external_engines_to_null` setting we replace external
    /// engines with the `Null` table engine. This must run after the engine has been resolved, whether it
    /// was specified explicitly or inherited from the source table of `CREATE TABLE x AS y` (both the partial
    /// storage clause and the plain `AS` forms inherit the source engine, so both must be replaced).
    void replaceExternalEngineWithNullIfNeeded(ASTStorage & storage, bool enabled)
    {
        if (enabled
            && storage.engine
            && StorageFactory::instance().getStorageFeatures(storage.engine->name).source_access_type)
        {
            setNullTableEngine(storage);
        }
    }

    void setNullDictionarySourceIfExternal(ASTCreateQuery & create_query)
    {
        ASTDictionary & dict = *create_query.dictionary;
        if (Poco::toLower(dict.source->name) == "clickhouse")
        {
            auto config = getDictionaryConfigurationFromAST(create_query, Context::getGlobalContextInstance());
            auto info = getInfoIfClickHouseDictionarySource(config, Context::getGlobalContextInstance());
            if (info && info->is_local)
                return;
        }
        auto source_ast = make_intrusive<ASTFunctionWithKeyValueArguments>();
        source_ast->name = "null";
        source_ast->elements = make_intrusive<ASTExpressionList>();
        source_ast->children.push_back(source_ast->elements);
        dict.set(dict.source, source_ast);
    }

    ASTs * getEngineArgsFromCreateQuery(ASTCreateQuery & create_query)
    {
        ASTStorage * storage_def = create_query.storage;
        if (!storage_def)
            return nullptr;

        if (!storage_def->engine)
            return nullptr;

        const ASTFunction & engine_def = *storage_def->engine;
        if (!engine_def.arguments)
            return nullptr;

        return &engine_def.arguments->children;
    }

    bool hasColumnsWithDynamicStructure(const ColumnsDescription & columns)
    {
        return std::any_of(columns.begin(), columns.end(),
            [](const auto & column)
            {
               return column.type->hasDynamicStructure();
            });
    }

}

void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.as_table_function)
    {
        if (getContext()->getSettingsRef()[Setting::restore_replace_external_table_functions_to_null])
        {
            const auto & factory = TableFunctionFactory::instance();

            auto properties = factory.tryGetProperties(create.as_table_function->as<ASTFunction>()->name);
            if (properties && properties->allow_readonly)
                return;
            if (!create.storage)
            {
                auto storage_ast = make_intrusive<ASTStorage>();
                create.set(create.storage, storage_ast);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage should not be created yet, it's a bug.");
            create.reset(create.as_table_function);
            setNullTableEngine(*create.storage);
        }
        return;
    }

    if (create.is_dictionary && getContext()->getSettingsRef()[Setting::restore_replace_external_dictionary_source_to_null])
        setNullDictionarySourceIfExternal(create);

    if (create.is_dictionary || create.is_ordinary_view)
        return;

    if (create.isTemporary())
    {
        /// Some part of storage definition is specified, but ENGINE is not: just set the one from default_temporary_table_engine setting.

        if (!create.cluster.empty())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Temporary tables cannot be created with ON CLUSTER clause");

        if (!create.storage)
        {
            auto storage_ast = make_intrusive<ASTStorage>();
            create.set(create.storage, storage_ast);
        }

        if (!create.storage->engine)
            setDefaultTableEngine(*create.storage, getContext()->getSettingsRef()[Setting::default_temporary_table_engine].value);

        checkTemporaryTableEngineName(create.storage->engine->name);
        return;
    }

    if (create.is_materialized_view)
    {
        /// A materialized view with an external target doesn't need a table engine.
        if (create.is_materialized_view_with_external_target())
            return;

        if (auto * to_engine = create.getTargetInnerEngine(ViewTarget::To))
        {
            /// This materialized view already has a storage definition.
            if (!to_engine->engine)
            {
                /// Some part of storage definition (such as PARTITION BY) is specified, but ENGINE is not: just set default one.
                setDefaultTableEngine(*to_engine, getContext()->getSettingsRef()[Setting::default_table_engine].value);
            }
            return;
        }
    }

    /// We'll try to extract a storage definition from clause `AS`:
    ///     CREATE TABLE table_name AS other_table_name [storage_clauses]
    /// It is needed both when no storage clause is specified at all and when storage clauses such as
    /// PARTITION BY, ORDER BY or SETTINGS are specified without an explicit ENGINE: in the latter case
    /// the engine and the settings are inherited from `other_table_name`.
    boost::intrusive_ptr<ASTStorage> storage_def;
    if (!create.as_table.empty() && (!create.storage || !create.storage->engine))
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = getContext()->resolveDatabase(create.as_database);
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name)->getCreateTableQuery(as_table_name, getContext());

        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        const String qualified_name = backQuoteIfNeed(as_database_name) + "." + backQuoteIfNeed(as_table_name);

        if (as_create.is_ordinary_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a View", qualified_name);

        if (as_create.is_materialized_view_with_external_target())
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a table AS {}, it is a Materialized View without storage. Use \"AS {}\" instead",
                qualified_name,
                as_create.getTargetTableID(ViewTarget::To).getFullTableName());
        }

        if (as_create.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Dictionary", qualified_name);

        if (as_create.is_materialized_view)
        {
            storage_def = as_create.getTargetInnerEngine(ViewTarget::To);
        }
        else if (as_create.as_table_function)
        {
            /// The source table is backed by a table function. Forward the table function only when no storage
            /// clauses were specified for the new table; otherwise keep the explicit storage definition.
            if (!create.storage)
            {
                create.set(create.as_table_function, as_create.as_table_function->ptr());
                return;
            }
        }
        else if (as_create.storage)
        {
            storage_def = boost::static_pointer_cast<ASTStorage>(as_create.storage->ptr());
            create.is_time_series_table = as_create.is_time_series_table;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set engine, it's a bug.");
        }
    }

    if (create.storage)
    {
        /// This table already has a (possibly partial) storage definition.
        if (!create.storage->engine)
        {
            if (storage_def && storage_def->engine)
            {
                /// `CREATE TABLE x AS y [storage_clauses]` without an explicit ENGINE: inherit the engine of `y`
                /// together with every storage clause (keys, TTL, ...) that was not explicitly specified, and
                /// merge its settings under the explicitly specified ones (the latter take precedence).
                inheritStorageFromSource(*create.storage, *storage_def);
            }
            else
            {
                /// Some part of storage definition (such as PARTITION BY) is specified, but ENGINE is not: just set default one.
                setDefaultTableEngine(*create.storage, getContext()->getSettingsRef()[Setting::default_table_engine].value);
            }
        }

        /// For external tables with the restore_replace_external_engines_to_null setting we replace external
        /// engines with the Null table engine, whether the engine was specified explicitly or inherited.
        replaceExternalEngineWithNullIfNeeded(
            *create.storage, getContext()->getSettingsRef()[Setting::restore_replace_external_engines_to_null]);
        return;
    }

    if (!storage_def)
    {
        /// Set ENGINE by default.
        storage_def = make_intrusive<ASTStorage>();
        setDefaultTableEngine(*storage_def, getContext()->getSettingsRef()[Setting::default_table_engine].value);
    }

    /// Use the found table engine to modify the create query.
    if (create.is_materialized_view)
        create.setTargetInnerEngine(ViewTarget::To, storage_def);
    else
    {
        /// A plain `CREATE TABLE x AS y` without any storage clause reaches here with the engine inherited from
        /// the source table. The external-engine replacement must run for it too, consistently with the partial
        /// storage clause path above; otherwise `CREATE TABLE x AS url_src` would keep the external engine while
        /// `CREATE TABLE x AS url_src ORDER BY ...` becomes Null.
        replaceExternalEngineWithNullIfNeeded(
            *storage_def, getContext()->getSettingsRef()[Setting::restore_replace_external_engines_to_null]);
        create.set(create.storage, storage_def);
    }
}

void InterpreterCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";
    bool is_replicated_database_internal = database->getEngineName() == "Replicated" && getContext()->getClientInfo().is_replicated_database_internal;
    bool from_path = create.has_attach_from_path;
    bool is_on_cluster = getContext()->isDDLOrOnClusterInternal();

    if (database->getEngineName() == "Replicated" && create.uuid != UUIDHelpers::Nil && !is_replicated_database_internal && !internal && !is_on_cluster && !create.attach)
    {
        if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "It's not allowed to explicitly specify UUIDs for tables in Replicated databases, "
                                                       "see database_replicated_allow_explicit_uuid");
        }
        if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 1)
        {
            LOG_WARNING(
                &Poco::Logger::get("InterpreterCreateQuery"),
                "It's not recommended to explicitly specify UUIDs for tables in Replicated databases");
        }
        else if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 2)
        {
            UUID old_uuid = create.uuid;
            create.uuid = UUIDHelpers::Nil;
            create.generateRandomUUIDs();
            LOG_WARNING(
                &Poco::Logger::get("InterpreterCreateQuery"),
                "Replaced a user-provided UUID ({}) with a random one ({}) "
                "to make sure it's unique",
                old_uuid,
                create.uuid);
        }
    }

    if (is_replicated_database_internal && !internal)
    {
        if (create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table UUID is not specified in DDL log");
    }

    if (database->getUUID() != UUIDHelpers::Nil)
    {
        if (create.attach && !from_path && create.uuid == UUIDHelpers::Nil)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Incorrect ATTACH {} query for Atomic database engine. "
                            "Use one of the following queries instead:\n"
                            "1. ATTACH {} {};\n"
                            "2. CREATE {} {} <table definition>;\n"
                            "3. ATTACH {} {} FROM '/path/to/data/' <table definition>;\n"
                            "4. ATTACH {} {} UUID '<uuid>' <table definition>;",
                            kind_upper,
                            kind_upper, create.table->formatForErrorMessage(),
                            kind_upper, create.table->formatForErrorMessage(),
                            kind_upper, create.table->formatForErrorMessage(),
                            kind_upper, create.table->formatForErrorMessage());
        }

        create.generateRandomUUIDs();
    }
    else
    {
        bool has_uuid = (create.uuid != UUIDHelpers::Nil) || create.hasInnerUUIDs();
        if (has_uuid && !is_on_cluster && !internal)
        {
            /// We don't show the following error message either
            /// 1) if it's a secondary query (an initiator of a CREATE TABLE ON CLUSTER query
            /// doesn't know the exact database engines on replicas and generates an UUID, and then the replicas are free to ignore that UUID); or
            /// 2) if it's an internal query (for example RESTORE uses internal queries to create tables and it generates an UUID
            /// before creating a table to be possibly ignored if the database engine doesn't need it).
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} UUID specified, but engine of database {} is not Atomic", kind, create.getDatabase());
        }

        /// The database doesn't support UUID so we'll ignore it. The UUID could be set here because of either
        /// a) the initiator of `ON CLUSTER` query generated it to ensure the same UUIDs are used on different hosts; or
        /// b) `RESTORE from backup` query generated it to ensure the same UUIDs are used on different hosts.
        create.resetUUIDs();
    }
}


namespace
{

void addTableDependencies(const ASTCreateQuery & create, const ASTPtr & query_ptr, const ContextPtr & context)
{
    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};

    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr);
    DatabaseCatalog::instance().addDependencies(qualified_name, ref_dependencies.dependencies, loading_dependencies, ref_dependencies.mv_from_dependency ? TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()} : TableNamesSet{});
}

void checkTableCanBeAddedWithNoCyclicDependencies(const ASTCreateQuery & create, const ASTPtr & query_ptr, const ContextPtr & context)
{
    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase(), /*can_throw*/true);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, /*can_throw*/true);
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(qualified_name, ref_dependencies.dependencies, loading_dependencies);
}

bool isReplicated(const ASTStorage & storage)
{
    if (!storage.engine)
        return false;
    const auto & storage_name = storage.engine->name;
    return storage_name.starts_with("Replicated") || storage_name.starts_with("Shared");
}

}

BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    auto component_guard = Coordination::setCurrentComponent("InterpreterCreateQuery::createTable");
    /// Temporary tables are created out of databases.
    if (create.isTemporary() && create.attach)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "ATTACH of TEMPORARY tables are not supported");

    if (create.isTemporary() && create.database)
        throw Exception(ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE,
                        "Temporary objects (tables/views) cannot be inside a database. "
                        "You should not specify a database for a temporary objects.");

    if (create.isTemporary() && !create.cluster.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Temporary objects (tables/views) cannot be created ON CLUSTER."
            "You should not specify a cluster for a temporary objects.");

    String current_database = getContext()->getCurrentDatabase();
    auto database_name = create.database ? create.getDatabase() : current_database;

    bool is_secondary_query = getContext()->getZooKeeperMetadataTransaction() && !getContext()->getZooKeeperMetadataTransaction()->isInitialQuery();
    auto mode = getLoadingStrictnessLevel(create.attach, /*force_attach*/ false, /*has_force_restore_data_flag*/ false, is_secondary_query || is_restore_from_backup);

    if (!create.sql_security && create.supportSQLSecurity() && (create.refresh_strategy || !getContext()->getServerSettings()[ServerSetting::ignore_empty_sql_security_in_create_view_query]))
        create.set(create.sql_security, make_intrusive<ASTSQLSecurity>());

    if (create.sql_security)
        processSQLSecurityOption(getContext(), create.sql_security->as<ASTSQLSecurity &>(), create.is_materialized_view, mode);

    DDLGuardPtr ddl_guard;

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && (!create.storage || !create.storage->engine) && !create.columns_list)
    {
        /// First, reject any user-supplied storage clauses or top-level fields that the
        /// short-ATTACH path below would silently drop by overwriting `create` with the
        /// stored table metadata. `InterpreterSetQuery::applySettingsFromQuery` (called from
        /// `executeQueryImpl` before this interpreter) has already hoisted session settings
        /// out of `create.storage->settings`, so anything still here is either engine-specific
        /// (`ORDER BY`, `PARTITION BY`, `PRIMARY KEY`, `SAMPLE BY`, `TTL`, `UNIQUE KEY`, MergeTree
        /// `SETTINGS`) or a top-level `ASTCreateQuery` field (`COMMENT`, `REFRESH`, `SQL SECURITY`,
        /// `TO target`, `EMPTY`, `CLONE`, `AS SELECT`, `UUID`, etc.).
        bool has_dropped_clauses = false;

        if (create.storage)
        {
            const auto & storage = *create.storage;
            has_dropped_clauses
                = storage.partition_by != nullptr
                || storage.primary_key != nullptr
                || storage.order_by != nullptr
                || storage.sample_by != nullptr
                || storage.ttl_table != nullptr
                || storage.unique_key != nullptr
                || storage.settings != nullptr;
        }

        has_dropped_clauses = has_dropped_clauses
            || create.comment != nullptr
            || create.refresh_strategy != nullptr
            || create.sql_security != nullptr
            || create.select != nullptr
            || create.targets != nullptr
            || create.as_table_function != nullptr
            || create.aliases_list != nullptr
            || create.is_create_empty
            || create.is_clone_as
            || !create.as_database.empty()
            || !create.as_table.empty()
            || create.has_attach_from_path
            || create.has_uuid_clause
            || create.has_inner_uuid_clause;

        if (has_dropped_clauses)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ATTACH applies the table definition from stored metadata and can't be changed in the query itself. "
                "Use 'ATTACH TABLE {0};' to re-attach with stored metadata, or 'ALTER TABLE {0} MODIFY SETTING ...' "
                "(or 'MODIFY ORDER BY ...') after ATTACH to change settings.",
                backQuoteIfNeed(create.getTable()));
        }

        // In case of an ON CLUSTER query, the database may not be present on the initiator node
        auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (database && database->shouldReplicateQuery(getContext(), query_ptr))
        {
            auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.getTable(), database.get());
            create.setDatabase(database_name);
            guard->releaseTableLock();
            return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), QueryFlags{ .internal = internal, .distributed_backup_restore = is_restore_from_backup }, std::move(guard));
        }

        if (!create.cluster.empty())
            return executeQueryOnCluster(create);

        if (!database)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(database_name));

        /// For short syntax of ATTACH query we have to lock table name here, before reading metadata
        /// and hold it until table is attached
        if (likely(need_ddl_guard))
            ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.getTable(), database.get());

        bool if_not_exists = create.if_not_exists;

        // Table SQL definition is available even if the table is detached (even permanently)
        auto query = database->getCreateTableQuery(create.getTable(), getContext());
        FunctionNameNormalizer::visit(query.get());
        auto create_query = query->as<ASTCreateQuery &>();

        /// Set replicated or not replicated MergeTree engine in metadata and query
        if (create.attach_as_replicated.has_value())
        {
            if (database->isTableExist(create.getTable(), getContext()))
                throw Exception(
                    ErrorCodes::TABLE_ALREADY_EXISTS,
                    "Table {}.{} already exists",
                    backQuoteIfNeed(create.getDatabase()),
                    backQuoteIfNeed(create.getTable()));
            convertMergeTreeTableIfPossible(create_query, database, create.attach_as_replicated.value());
        }

        if (!create.is_dictionary && create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH TABLE {}.{}, it is a Dictionary",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.getTable()));

        if (create.is_dictionary && !create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH DICTIONARY {}.{}, it is a Table",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.getTable()));

        create = create_query; // Copy the saved create query, but use ATTACH instead of CREATE

        create.attach = true;
        create.attach_short_syntax = true;
        create.if_not_exists = if_not_exists;

        /// Compatibility setting which should be enabled by default on attach
        /// Otherwise server will be unable to start for some old-format of IPv6/IPv4 types
        getContext()->setSetting("cast_ipv4_ipv6_default_on_conversion_error", 1);
    }

    /// TODO throw exception if !create.attach_short_syntax && !create.attach_from_path && !internal
    if (!create.attach_short_syntax && create.attach_as_replicated.has_value())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Attaching table as [not] replicated is supported only for short attach queries");

    if (create.has_attach_from_path)
    {
        chassert(!ddl_guard);

        fs::path user_files = fs::path(getContext()->getUserFilesPath()).lexically_normal();
        fs::path root_path = fs::path(getContext()->getPath()).lexically_normal();

        if (!getContext()->isDDLOrOnClusterInternal())
        {
            fs::path data_path = fs::path(create.attach_from_path).lexically_normal();
            if (data_path.is_relative())
                data_path = (user_files / data_path).lexically_normal();
            if (!fileOrSymlinkPathStartsWith(data_path.string(), user_files.string()))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));

            /// Data path must be relative to root_path
            create.attach_from_path = fs::relative(data_path, root_path) / "";
        }
        else
        {
            fs::path data_path = (root_path / create.attach_from_path).lexically_normal();
            if (!fileOrSymlinkPathStartsWith(data_path.string(), user_files.string()))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));
        }
    }
    else if (create.attach && !create.attach_short_syntax && !getContext()->isDDLOrOnClusterInternal())
    {
        auto log = getLogger("InterpreterCreateQuery");
        LOG_WARNING(log, "ATTACH TABLE query with full table definition is not recommended: "
                         "use either ATTACH TABLE {}; to attach existing table "
                         "or CREATE TABLE {} <table definition>; to create new table "
                         "or ATTACH TABLE {} FROM '/path/to/data/' <table definition>; to create new table and attach data.",
                         create.getTable(), create.getTable(), create.getTable());
    }

    if (!create.isTemporary() && !create.database)
        create.setDatabase(current_database);

    if (create.targets)
        create.targets->setCurrentDatabase(current_database);

    if (create.select && create.isView())
    {
        /// Query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
        /// `page`) shape a result and are materialized by wrapping the query as a derived table during
        /// direct execution. A stored view definition cannot support them equivalently: its columns are
        /// inferred (below, before any wrapping) so `select` would change the result schema versus the
        /// stored metadata; the per-`UNION`-arm pass is not applied; and a refreshable materialized view
        /// refreshes through `InterpreterInsertQuery`, not `executeQuery`. Reject them in a view
        /// definition rather than shaping inconsistently — put them on the query that reads the view.
        ///
        /// Only a fresh, user-initiated CREATE is rejected. `ATTACH` (metadata load on startup,
        /// upgrade, restore) and secondary replays (Replicated database DDL, ON CLUSTER, restore
        /// from backup) must keep loading definitions that were stored before this rule existed:
        /// `limit` and `offset` are pre-existing setting names, so `SETTINGS limit = 10` can
        /// legitimately occur in old view metadata.
        if (mode <= LoadingStrictnessLevel::CREATE && hasConstructionSettings(*create.select))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Query-construction settings (`select`/`filter`/`order`/`sort`/`limit`/`offset`/`page`) "
                "are not supported in a {} definition. Specify them on the query that reads the view instead.",
                create.is_materialized_view ? "MATERIALIZED VIEW" : "VIEW");

        // Expand CTE before filling default database
        ApplyWithSubqueryVisitor::visit(*create.select);
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.select);
    }

    if (create.refresh_strategy)
    {
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.refresh_strategy);
    }

    if (create.columns_list)
    {
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.columns_list);
    }

    // substitute possible UDFs with their definitions
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr, getContext());

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = getTablePropertiesAndNormalizeCreateQuery(create, mode);

    /// The definition persisted below must not depend on the session setting, because reloads and
    /// replicas re-derive the key type from the stored text. This must happen after normalization:
    /// `CREATE TABLE ... AS` materializes copied columns and key expressions only there. A replayed
    /// definition (short attach, metadata load, backup restore) already records its spelling.
    if (!create.is_clone_as && !create.attach_short_syntax && !is_restore_from_backup
        && getContext()->getSettingsRef()[Setting::use_legacy_to_time]
        && replaceLegacyToTime(*query_ptr))
    {
        /// `properties` was derived before the rewrite, and the live table below is built from it while
        /// the metadata written to disk comes from the rewritten query. `CREATE TABLE ... AS src` copies
        /// the source column expressions verbatim, so a `DEFAULT`, `MATERIALIZED`, `ALIAS` or column
        /// `TTL` mentioning `toTime` would keep the source spelling in memory while the metadata records
        /// `toTimeWithFixedDate`, and the same insert would produce different values before and after a
        /// reload. The expressions are rewritten in place: re-deriving the whole `ColumnsDescription`
        /// is not idempotent for the `AS SELECT` / `AS src` branches — `getColumnsDescription` would
        /// flatten `Nested` columns that those branches deliberately keep intact.
        for (const auto & column : properties.columns)
        {
            if (column.default_desc.expression)
                replaceLegacyToTime(*column.default_desc.expression);
            if (column.ttl)
                replaceLegacyToTime(*column.ttl);
        }

        /// Constraints need the same treatment: `MergeTree` reparses them from the rewritten AST, but most
        /// engines take `properties.constraints` verbatim, so a `CHECK` or `ASSUME` mentioning `toTime`
        /// would be enforced with the session spelling in memory and with `toTimeWithFixedDate` after a
        /// reload, accepting and rejecting the same row on the two sides of a restart.
        if (create.columns_list)
            properties.constraints
                = getConstraintsDescription(create.columns_list->constraints, properties.columns, getContext());
    }

    DatabasePtr database;
    bool need_add_to_database = !create.isTemporary();
    // In case of an ON CLUSTER query, the database may not be present on the initiator node
    if (need_add_to_database)
        database = DatabaseCatalog::instance().tryGetDatabase(database_name);

    /// Check type compatible for materialized dest table and select columns
    if (create.select && create.is_materialized_view && mode <= LoadingStrictnessLevel::CREATE)
    {
        // An MV with a flattened nested column in an inner table can never be filled
        if (create.is_materialized_view_with_inner_table())
            getContext()->setSetting("flatten_nested", false);
        validateMaterializedViewColumnsAndEngine(create, properties, database);
    }

    bool is_storage_replicated = false;
    if (create.storage && isReplicated(*create.storage))
        is_storage_replicated = true;

    if (create.targets)
    {
        for (const auto & inner_table_engine : create.targets->getInnerEngines())
        {
            if (isReplicated(*inner_table_engine))
                is_storage_replicated = true;
        }
    }

    bool allow_heavy_populate = getContext()->getSettingsRef()[Setting::database_replicated_allow_heavy_create] && create.is_populate;
    if (!allow_heavy_populate && database && database->getEngineName() == "Replicated" && (create.select || create.is_populate))
    {
        const bool allow_create_select_for_replicated
            = (create.isView() && !create.is_populate) || create.is_create_empty || !is_storage_replicated;
        if (!allow_create_select_for_replicated)
        {
            /// POPULATE can be enabled with setting, provide hint in error message
            if (create.is_populate)
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "CREATE with POPULATE is not supported with Replicated databases. Consider using separate CREATE and INSERT "
                    "queries. "
                    "Alternatively, you can enable 'database_replicated_allow_heavy_create' setting to allow this operation, use with "
                    "caution");

            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "CREATE AS SELECT is not supported with Replicated databases. Consider using separate CREATE and INSERT queries.");
        }
    }

    if (create.is_clone_as)
    {
        if (database && database->getEngineName() == "Replicated")
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "CREATE CLONE AS is not supported with Replicated databases. Consider using separate CREATE and INSERT queries.");
    }

    if (database && database->shouldReplicateQuery(getContext(), query_ptr))
    {
        chassert(!ddl_guard);
        auto guard = DatabaseCatalog::instance().getDDLGuard(create.getDatabase(), create.getTable(), database.get());
        assertOrSetUUID(create, database);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), QueryFlags{ .internal = internal, .distributed_backup_restore = is_restore_from_backup }, std::move(guard));
    }

    if (!create.cluster.empty())
    {
        chassert(!ddl_guard);
        return executeQueryOnCluster(create);
    }

    if (need_add_to_database && !database)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(database_name));

    if (create.isTemporary() && create.replace_table)
    {
        chassert(!ddl_guard);
        return doCreateOrReplaceTemporaryTable(create, properties, mode);
    }

    if (create.replace_table
        || (create.replace_view && (database->getEngineName() == "Atomic" || database->getEngineName() == "Replicated")))
    {
        chassert(!ddl_guard);
        return doCreateOrReplaceTable(create, properties, mode);
    }

    /// A plain `CREATE TABLE ... AS SELECT` (no REPLACE) on an Atomic database is executed by creating a
    /// temporary table, running the populating INSERT SELECT into it, and only then atomically publishing it
    /// under the final name with a RENAME. If the SELECT is denied (or fails for any other reason), the
    /// temporary table is dropped, so a denied query leaves no empty orphan table behind (issue #26746: a
    /// retry used to report `TABLE_ALREADY_EXISTS` instead of the access error). This reuses the same
    /// create-temporary-then-publish machinery as CREATE OR REPLACE (doCreateOrReplaceTable). On non-Atomic
    /// databases (getUUID() == Nil, e.g. Ordinary) we keep the previous behavior: the table is created first
    /// and an orphan is left if the INSERT SELECT fails. Materialized views are excluded (they can own
    /// an inner table and carry source-view dependencies, so they keep the previous behavior for now).
    ///
    /// As a consequence, the final table name is only registered by the publishing RENAME, so the populating
    /// `SELECT` runs while the destination does not yet exist. A `SELECT` that references the destination
    /// itself -- directly (`CREATE TABLE dst ... AS SELECT * FROM dst`) or indirectly (e.g. via
    /// `system.tables`) -- therefore no longer observes it as an already-created empty table the way the
    /// previous create-then-populate order did. This narrow, intentional visibility change (the table becomes
    /// visible only once fully populated) is covered by
    /// `04547_create_as_select_destination_not_visible_during_populate`.
    if (create.isCreateQueryWithImmediateInsertSelect()
        && !create.is_materialized_view
        && database && database->getUUID() != UUIDHelpers::Nil)
    {
        chassert(!ddl_guard);
        return doCreateOrReplaceTable(create, properties, mode);
    }

    /// An atomically populated materialized view also needs the DDL guard of its *source table's name*
    /// held across the cut (see fillMaterializedViewAtomically): the subscription registered there is
    /// keyed by name, so a concurrent RENAME or EXCHANGE of the source between resolving it and
    /// registering the subscription would wire the view to whatever table owns the name afterwards while
    /// the backfill reads the table that owned it before - or leave the subscription on a name nobody
    /// owns. The source guard also has to be held from *before* doCreateTable publishes the view: a
    /// RENAME of the source landing between the publication and the guard acquisition would make
    /// getValidatedAtomicPopulateSource fail the CREATE on the vanished name (rolling the view back)
    /// even though the source is still there under its new name. Queries that take several
    /// DDL guards (RENAME, EXCHANGE) acquire them in ascending (database, table) order, so to avoid
    /// deadlocks both guards are taken here, before the publication, in that canonical order
    /// (doCreateTable then sees the view's guard already held and does not re-acquire it).
    DDLGuardPtr source_ddl_guard;
    bool populate_atomically = shouldPopulateMaterializedViewAtomically(create);
    if (populate_atomically && likely(need_ddl_guard))
    {
        if (auto populate_source_name = tryGetAtomicPopulateSourceName(create))
        {
            /// Validating the view's SELECT above took a storage snapshot of the source table and, under
            /// `enable_shared_storage_snapshot_in_query`, cached it in the query-scoped snapshot cache,
            /// where it would hold a reference to the source storage until this query ends. Blocking on
            /// the source-name DDL guard below while holding that reference deadlocks with a concurrent
            /// synchronous `DROP` of the source (`DROP TABLE ... SYNC`, or any `DROP` when
            /// `database_atomic_wait_for_drop_and_detach_synchronously` is set, as in the test harness):
            /// the `DROP` holds the guard this thread wants, and after detaching the table it waits for
            /// every reference to the storage to be released before returning. The validation snapshot is
            /// of no further use - the population pins its own snapshot under the source's exclusive lock
            /// (see fillMaterializedViewAtomically) - so drop it before taking the guard.
            if (auto metadata_cache = getContext()->getQueryMetadataCache())
            {
                if (auto source = DatabaseCatalog::instance().tryGetTable(
                        StorageID{populate_source_name->database, populate_source_name->table}, getContext()))
                {
                    auto [snapshot_cache, snapshot_cache_lock] = metadata_cache->getStorageSnapshotCache();
                    snapshot_cache->erase(source.get());
                }
            }

            /// Covers the window between validating the view's SELECT (which resolved the source) and
            /// acquiring the source-name guard below: a DROP of the source landing here must fail the
            /// CREATE and roll the view back, not leave a half-created view behind
            /// (see 04824_atomic_populate_materialized_view_source_dropped_before_guard).
            FailPointInjection::pauseFailPoint(FailPoints::atomic_populate_pause_before_source_guard);

            UniqueTableName view_name{create.getDatabase(), create.getTable()};
            UniqueTableName source_name{populate_source_name->database, populate_source_name->table};

            auto lock_view_name = [&]
            {
                chassert(!ddl_guard);
                ddl_guard = DatabaseCatalog::instance().getDDLGuard(view_name.database_name, view_name.table_name, nullptr);
            };
            auto lock_source_name = [&]
            {
                source_ddl_guard = DatabaseCatalog::instance().getDDLGuard(source_name.database_name, source_name.table_name, nullptr);
            };

            if (source_name < view_name)
            {
                lock_source_name();
                lock_view_name();
            }
            else if (view_name < source_name)
            {
                lock_view_name();
                lock_source_name();
            }
            else
            {
                /// The view cannot select from itself - the name it is being created under owns no table
                /// yet - so equal names mean the source does not exist and the population will fail with
                /// its natural error. Locking the same name twice would self-deadlock, so the view's own
                /// guard, which doCreateTable takes below, covers both roles of the name.
            }
        }
    }

    /// Actually creates table
    bool created = doCreateTable(create, properties, ddl_guard, mode);

    if (!created)   /// Table already exists
    {
        ddl_guard.reset();
        return {};
    }

    /// A materialized view with POPULATE subscribes to new inserts of its source table and, at the same
    /// time, must be filled with the data that already exists in the source. Doing these two steps
    /// independently is racy: a row inserted concurrently can be routed to the view and also appear in the
    /// population snapshot (duplicated), or be routed nowhere and miss the snapshot (lost). To make it
    /// atomic we register the subscription and pin a snapshot of the source together under a brief
    /// exclusive lock, then populate from the pinned snapshot. See fillMaterializedViewAtomically.
    ///
    /// The view's own DDL guard is still held here and is passed down: `fillMaterializedViewAtomically`
    /// releases it once the view is subscribed to the source. Without it, a concurrent `DROP` or `RENAME` of
    /// the just-published view could run while we wait for the source's exclusive lock, and the subscription
    /// registered afterwards would name a view that no longer exists there - which stops
    /// `DatabaseCatalog::getReadyDependentViews` from returning any view of that source.
    if (populate_atomically)
    {
        /// Covers the window between the view's publication above and the cut: the source's guard is
        /// already held here, so a concurrent RENAME or EXCHANGE of the source must wait until the view
        /// is subscribed (see 04813_atomic_populate_materialized_view_rename_after_publication).
        FailPointInjection::pauseFailPoint(FailPoints::atomic_populate_pause_after_view_publication);

        if (auto result = fillMaterializedViewAtomically(create, ddl_guard, source_ddl_guard))
            return std::move(*result);
    }

    ddl_guard.reset();
    source_ddl_guard.reset();

    /// If table has dependencies - add them to the graph
    addTableDependencies(create, query_ptr, getContext());
    return fillTableIfNeeded(create);
}

namespace
{

void checkForUnsupportedColumns(IStorage & storage, LoadingStrictnessLevel mode, ContextPtr context, bool is_temporary)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr(context, false);

    /// Re-check inferred column types only for a fresh, persisted table: the pre-construction check
    /// does not see inferred columns, and ATTACH/RESTORE, temporary tables and views/dictionaries are
    /// not subject to this check on load.
    if (mode <= LoadingStrictnessLevel::CREATE && !is_temporary && !storage.isView() && !storage.isDictionary())
        checkAllTypesAreAllowedInTable(metadata_snapshot->getColumns().getAll());

    if (mode <= LoadingStrictnessLevel::CREATE && hasColumnsWithDynamicStructure(metadata_snapshot->getColumns()) && !storage.supportsColumnsWithDynamicStructure())
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Cannot create table with column of type Dynamic or JSON, "
            "because storage {} doesn't support columns with dynamic structure",
            storage.getName());
    }
}

void validateVirtualColumns(IStorage & storage, ContextPtr context)
{
    const auto metadata = storage.getInMemoryMetadataPtr(context, false);
    for (const auto & storage_column : metadata->columns)
    {
        if (metadata->virtuals.tryGet(storage_column.name, VirtualsKind::Persistent, VirtualsMaterializationPlace::All))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Cannot create table with column '{}' for {} engines because it is reserved for persistent virtual column",
                storage_column.name, storage.getName());
        }

        /// An EPHEMERAL user column has no physical storage and no read-time expression,
        /// so it cannot properly shadow a virtual column of the same name.
        /// This leads to a type mismatch: the Block header uses the user column's type
        /// while the data comes from the virtual column (which may have a different type).
        if (storage_column.default_desc.kind == ColumnDefaultKind::Ephemeral && metadata->virtuals.tryGet(storage_column.name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::All))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Cannot create table with ephemeral column '{}' for {} engines "
                "because it conflicts with a virtual column of the same name",
                storage_column.name, storage.getName());
        }
    }
}

void validateStorage(IStorage & storage, LoadingStrictnessLevel mode, ContextPtr context, bool is_temporary)
try
{
    validateVirtualColumns(storage, context);
    checkForUnsupportedColumns(storage, mode, context, is_temporary);
}
catch (...)
{
    if (mode <= LoadingStrictnessLevel::CREATE)
    {
        try
        {
            storage.drop();
        }
        catch (...)
        {
            tryLogCurrentException("validateStorage");
        }
    }
    throw;
}

}

bool InterpreterCreateQuery::doCreateTable(ASTCreateQuery & create,
                                           const InterpreterCreateQuery::TableProperties & properties,
                                           DDLGuardPtr & ddl_guard, LoadingStrictnessLevel mode)
{
    if (create.isTemporary())
    {
        if (create.if_not_exists && getContext()->tryResolveStorageID({"", create.getTable()}, Context::ResolveExternal))
            return false;

        DatabasePtr database = DatabaseCatalog::instance().getDatabase(DatabaseCatalog::TEMPORARY_DATABASE);

        String temporary_table_name = create.getTable();
        auto creator = [&](const StorageID & table_id)
        {
            auto res = StorageFactory::instance().get(create,
                database->getTableDataPath(table_id.getTableName()),
                getContext(),
                getContext()->getGlobalContext(),
                properties.columns,
                properties.constraints,
                mode,
                is_restore_from_backup);
            validateStorage(*res, mode, getContext(), /*is_temporary=*/true);
            return res;
        };
        auto temporary_table = TemporaryTableHolder(getContext(), creator, query_ptr);

        getContext()->getSessionContext()->addExternalTable(temporary_table_name, std::move(temporary_table));
        return true;
    }

    if (!ddl_guard && likely(need_ddl_guard))
        ddl_guard = DatabaseCatalog::instance().getDDLGuard(create.getDatabase(), create.getTable(), nullptr);

    String data_path;
    DatabasePtr database;

    database = DatabaseCatalog::instance().getDatabase(create.getDatabase());
    assertOrSetUUID(create, database);

    String storage_name = create.is_dictionary ? "Dictionary" : "Table";
    auto storage_already_exists_error_code = create.is_dictionary ? ErrorCodes::DICTIONARY_ALREADY_EXISTS : ErrorCodes::TABLE_ALREADY_EXISTS;

    /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
    if (database->isTableExist(create.getTable(), getContext()))
    {
        /// TODO Check structure of table
        if (create.if_not_exists)
            return false;
        if (create.replace_view)
        {
            /// when executing CREATE OR REPLACE VIEW, drop current existing view
            auto drop_ast = make_intrusive<ASTDropQuery>();
            drop_ast->setDatabase(create.getDatabase());
            drop_ast->setTable(create.getTable());
            drop_ast->no_ddl_lock = true;

            auto drop_context = Context::createCopy(context);
            /// Don't check dependencies during DROP of the view, because we will recreate
            /// it with the same name and all dependencies will remain valid.
            drop_context->setSetting("check_table_dependencies", false);
            drop_context->setDDLOrOnClusterInternal(true);
            InterpreterDropQuery interpreter(drop_ast, drop_context);
            interpreter.execute();
        }
        else
        {
            if (database->getTable(create.getTable(), getContext())->isDictionary())
                throw Exception(
                    ErrorCodes::DICTIONARY_ALREADY_EXISTS,
                    "Dictionary {}.{} already exists",
                    backQuoteIfNeed(create.getDatabase()),
                    backQuoteIfNeed(create.getTable()));
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS,
                "Table {}.{} already exists",
                backQuoteIfNeed(create.getDatabase()),
                backQuoteIfNeed(create.getTable()));
        }
    }
    else if (!create.attach)
    {
        /// Checking that table may exists in detached/detached permanently state
        try
        {
            database->checkMetadataFilenameAvailability(create.getTable());
        }
        catch (const Exception &)
        {
            if (create.if_not_exists)
                return false;
            throw;
        }

        /// If this is an initial create, we also need to check the table name's length.
        /// We are not checking this for secondary creates to avoid backward compatibility issues.
        if (mode <= LoadingStrictnessLevel::CREATE)
            database->checkTableNameLength(create.getTable());
    }

    data_path = database->getTableDataPath(create);
    // When creating a table, when checking if the data path exists, it should use the local disk to check, not the database disk. Because the database disk stores metadata files only.
    auto full_data_path = fs::path{getContext()->getPath()} / data_path;

    if (!create.attach && !data_path.empty() && fs::exists(full_data_path))
    {
        if (getContext()->getZooKeeperMetadataTransaction() &&
            !getContext()->getZooKeeperMetadataTransaction()->isInitialQuery() &&
            !DatabaseCatalog::instance().hasUUIDMapping(create.uuid) &&
            Context::getGlobalContextInstance()->isServerCompletelyStarted() &&
            Context::getGlobalContextInstance()->getConfigRef().getBool("allow_moving_table_directory_to_trash", false))
        {
            /// This is a secondary query from a Replicated database. It cannot be retried with another UUID, we must execute it as is.
            /// We don't have a table with this UUID (and all metadata is loaded),
            /// so the existing directory probably contains some leftovers from previous unsuccessful attempts to create the table

            fs::path trash_path = fs::path{getContext()->getPath()} / "trash" / data_path / getHexUIntLowercase(thread_local_rng());
            LOG_WARNING(getLogger("InterpreterCreateQuery"), "Directory for {} data {} already exists. Will move it to {}",
                        Poco::toLower(storage_name), String(data_path), trash_path);
            fs::create_directories(trash_path.parent_path());
            renameNoReplace(full_data_path, trash_path);
        }
        else
        {
            throw Exception(storage_already_exists_error_code,
                "Directory for {} data {} already exists", Poco::toLower(storage_name), String(data_path));
        }
    }

    bool from_path = create.has_attach_from_path;
    String actual_data_path = data_path;
    if (from_path)
    {
        if (data_path.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ATTACH ... FROM ... query is not supported for {} database engine", database->getEngineName());
        /// We will try to create Storage instance with provided data path
        data_path = create.attach_from_path;
        create.has_attach_from_path = false;
        create.attach_from_path.clear();
    }

    if (create.attach)
    {
        /// If table was detached it's not possible to attach it back while some threads are using
        /// old instance of the storage. For example, AsynchronousMetrics may cause ATTACH to fail,
        /// so we allow waiting here. If database_atomic_wait_for_drop_and_detach_synchronously is disabled
        /// and old storage instance still exists it will throw exception.
        if (getContext()->getSettingsRef()[Setting::database_atomic_wait_for_drop_and_detach_synchronously])
        {
            QueryStatusPtr query_status = getContext()->getProcessListElementSafe();
            database->waitDetachedTableNotInUse(create.uuid, [&]()
            {
                if (query_status)
                    query_status->throwIfKilled();
            });
        }
        else
            database->checkDetachedTableNotInUse(create.uuid);
    }

    /// We should lock UUID on CREATE query (because for ATTACH it must be already locked previously).
    /// But ATTACH without create.attach_short_syntax flag works like CREATE actually, that's why we check it.
    bool need_lock_uuid = !create.attach_short_syntax;
    TemporaryLockForUUIDDirectory uuid_lock;
    if (need_lock_uuid)
        uuid_lock = TemporaryLockForUUIDDirectory{create.uuid};
    else if (create.uuid != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(create.uuid))
    {
        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
        if (database->getEngineName() != "MaterializedPostgreSQL")
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create.uuid);
    }

    /// Before actually creating the table, check if it will lead to cyclic dependencies.
    checkTableCanBeAddedWithNoCyclicDependencies(create, query_ptr, getContext());

    /// Initial queries in Replicated database at this point have is_ddl_or_on_cluster_internal = true,
    /// so we need to check whether the query is initial through getZooKeeperMetadataTransaction()->isInitialQuery()
    bool is_initial_query = !getContext()->isDDLOrOnClusterInternal() ||
                            (getContext()->getZooKeeperMetadataTransaction() && getContext()->getZooKeeperMetadataTransaction()->isInitialQuery());
    bool is_predefined_database = DatabaseCatalog::isPredefinedDatabase(create.getDatabase());
    if (!internal && is_initial_query && !is_predefined_database)
        throwIfTooManyEntities(create);

    StoragePtr res;
    /// NOTE: CREATE query may be rewritten by Storage creator or table function
    if (create.as_table_function)
    {
        if (create.sql_security)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SQL SECURITY is not supported for tables created from a table function");

        auto table_function_ast = create.as_table_function->ptr();
        auto table_function = TableFunctionFactory::instance().get(table_function_ast, getContext());

        if (!table_function->canBeUsedToCreateTable())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' cannot be used to create a table", table_function->getName());

        /// In case of CREATE AS table_function() query we should use global context
        /// in storage creation because there will be no query context on server startup
        /// and because storage lifetime is bigger than query context lifetime.
        res = table_function->execute(table_function_ast, getContext(), create.getTable(), properties.columns, /*use_global_context=*/true, /*is_insert_query=*/true);
        res->renameInMemory({create.getDatabase(), create.getTable(), create.uuid});

        /// The table is permanent, so it must hold its named collection (if any) the same way a table
        /// engine does: `DROP NAMED COLLECTION` is blocked while the table exists.
        if (const auto collection_name = table_function->getUsedNamedCollectionName(); !collection_name.empty())
            NamedCollectionFactory::instance().addDependency(collection_name, res->getStorageID());
    }
    else
    {
        res = StorageFactory::instance().get(create,
            data_path,
            getContext(),
            getContext()->getGlobalContext(),
            properties.columns,
            properties.constraints,
            mode,
            is_restore_from_backup);

        /// If schema was inferred while storage creation, add columns description to create query.
        auto & create_query = query_ptr->as<ASTCreateQuery &>();
        addColumnsDescriptionToCreateQueryIfNecessary(create_query, res);
        /// Add any inferred engine args if needed. For example, data format for engines File/S3/URL/etc
        if (auto * engine_args = getEngineArgsFromCreateQuery(create_query))
            res->addInferredEngineArgsToCreateQuery(*engine_args, getContext());
    }

    validateStorage(*res, mode, getContext(), create.isTemporary());

    if (!create.attach && getContext()->getSettingsRef()[Setting::database_replicated_allow_only_replicated_engine])
    {
        bool is_replicated_storage = typeid_cast<const StorageReplicatedMergeTree *>(res.get()) != nullptr;
        if (!is_replicated_storage && res->storesDataOnDisk() && database && database->getEngineName() == "Replicated")
            throw Exception(ErrorCodes::UNKNOWN_STORAGE,
                            "Only tables with a Replicated engine "
                            "or tables which do not store data on disk are allowed in a Replicated database");
    }

    if (from_path && !res->storesDataOnDisk())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "ATTACH ... FROM ... query is not supported for {} table engine, "
                        "because such tables do not store any data on disk. Use CREATE instead.", res->getName());

    auto * replicated_storage = typeid_cast<StorageReplicatedMergeTree *>(res.get());
    if (replicated_storage)
    {
        const auto probability = getContext()->getSettingsRef()[Setting::create_replicated_merge_tree_fault_injection_probability];
        std::bernoulli_distribution fault(static_cast<double>(probability));
        if (fault(thread_local_rng))
        {
            /// We emulate the case when the exception was thrown in StorageReplicatedMergeTree constructor
            if (!create.attach)
                replicated_storage->dropIfEmpty();

            throw Coordination::Exception(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (during table creation)");
        }
    }

    database->createTable(getContext(), create.getTable(), res, query_ptr);

    /// Move table data to the proper place. Wo do not move data earlier to avoid situations
    /// when data directory moved, but table has not been created due to some error.
    if (from_path)
        res->rename(actual_data_path, {create.getDatabase(), create.getTable(), create.uuid});

    /// We must call "startup" and "shutdown" while holding DDLGuard.
    /// Because otherwise method "shutdown" (from InterpreterDropQuery) can be called before startup
    /// (in case when table was created and instantly dropped before started up)
    ///
    /// Method "startup" may create background tasks and method "shutdown" will wait for them.
    /// But if "shutdown" is called before "startup", it will exit early, because there are no background tasks to wait.
    /// Then background task is created by "startup" method. And when destructor of a table object is called, background task is still active,
    /// and the task will use references to freed data.

    /// Also note that "startup" method is exception-safe. If exception is thrown from "startup",
    /// we can safely destroy the object without a call to "shutdown", because there is guarantee
    /// that no background threads/similar resources remain after exception from "startup".

    res->startup();
    return true;
}


void InterpreterCreateQuery::throwIfTooManyEntities(ASTCreateQuery & create) const
{
    auto check_and_throw = [&](UInt64 num_limit, CurrentMetrics::Metric metric, String setting_name, String entity_name)
    {
        UInt64 attached_count = CurrentMetrics::get(metric);
        if (num_limit > 0 && attached_count >= num_limit)
            throw Exception(
                ErrorCodes::TOO_MANY_TABLES,
                "Too many {}. "
                "The limit (server configuration parameter `{}`) is set to {}, the current number is {}",
                entity_name,
                setting_name,
                num_limit,
                attached_count);
    };

    String engine_name = create.storage && create.storage->engine ? create.storage->engine->name : "";
    bool is_replicated = engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree");

    auto global_context = getContext()->getGlobalContext();
    if (create.is_dictionary)
        check_and_throw(
            global_context->getMaxDictionaryNumToThrow(),
            CurrentMetrics::AttachedDictionary,
            "max_dictionary_num_to_throw",
            "dictionaries");
    else if (create.isView())
        check_and_throw(global_context->getMaxViewNumToThrow(), CurrentMetrics::AttachedView, "max_view_num_to_throw", "views");
    else if (is_replicated)
        check_and_throw(
            global_context->getMaxReplicatedTableNumToThrow(),
            CurrentMetrics::AttachedReplicatedTable,
            "max_replicated_table_num_to_throw",
            "replicated tables");
    else
        check_and_throw(global_context->getMaxTableNumToThrow(), CurrentMetrics::AttachedTable, "max_table_num_to_throw", "tables");
}


BlockIO InterpreterCreateQuery::doCreateOrReplaceTable(ASTCreateQuery & create,
                                                       const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode)
{
    /// This function creates the table under a temporary name, populates it, and then atomically publishes it
    /// under the final name. It serves both REPLACE / CREATE OR REPLACE (publish via EXCHANGE / rename) and a
    /// plain `CREATE TABLE ... AS SELECT` (no REPLACE; publish via a plain RENAME that fails if the target
    /// exists). The plain-create case is routed here (only for Atomic databases) so a denied or failing
    /// populating INSERT SELECT leaves no empty orphan table behind (issue #26746).
    const bool is_plain_create = !create.replace_table && !create.create_or_replace && !create.replace_view;

    /// Replicated database requires separate contexts for each DDL query
    ContextPtr current_context = getContext();
    if (auto txn = current_context->getZooKeeperMetadataTransaction())
        txn->setIsCreateOrReplaceQuery();
    ContextMutablePtr create_context = Context::createCopy(current_context);
    create_context->setQueryContext(std::const_pointer_cast<Context>(current_context));

    /// Before actually creating/replacing the table, check if it will lead to cyclic dependencies.
    /// For a plain create this check runs later, after the existence fast path below: a
    /// `CREATE TABLE IF NOT EXISTS` over an existing table must be a no-op even when the (new, unused)
    /// definition would fail create-only validation, mirroring the check order of `doCreateTable`.
    if (!is_plain_create)
        checkTableCanBeAddedWithNoCyclicDependencies(create, query_ptr, create_context);

    auto make_drop_context = [&](bool bypass_size_guard) -> ContextMutablePtr
    {
        ContextMutablePtr drop_context = Context::createCopy(current_context);
        drop_context->setQueryContext(std::const_pointer_cast<Context>(current_context));
        drop_context->setDDLOrOnClusterInternal(true);
        /// Bypass = "the size guard was already enforced upstream; do not re-check or consume `force_drop_table` twice".
        if (bypass_size_guard)
        {
            drop_context->setSetting("max_table_size_to_drop", Field(UInt64{0}));
            drop_context->setSetting("max_partition_size_to_drop", Field(UInt64{0}));
        }
        return drop_context;
    };

    /// The temporary table is an implementation detail: renaming it to the final name and dropping it on
    /// failure are internal operations that must not require the user to hold RENAME/DROP privileges on it.
    /// A plain `CREATE TABLE ... AS SELECT` only requires CREATE + INSERT (+ SELECT on the sources, still
    /// checked by the populating INSERT SELECT below, which runs as the user), so those internal steps run
    /// with a full-access context derived from the global context -- mirroring how inner tables of a
    /// materialized view are dropped (see `InterpreterDropQuery::executeDropQuery`). Settings and any
    /// Replicated-database ZooKeeper transaction are propagated so the operations behave and replicate
    /// correctly. This is used only for the plain-create case; REPLACE keeps running as the user (its
    /// required access already includes DROP).
    ///
    /// `bypass_size_guard` additionally zeroes `max_table_size_to_drop` / `max_partition_size_to_drop`. The
    /// size guard is meaningful only for user-visible tables; a populated-then-abandoned temporary table can
    /// exceed it, and its cleanup DROP must always succeed or the temporary table would be stranded. Pass it
    /// for the cleanup DROPs; the publishing RENAME does not consult these settings, so it does not need it
    /// (this mirrors the `bypass_size_guard` argument of `make_drop_context` used by REPLACE).
    auto make_internal_context = [&](bool bypass_size_guard) -> ContextMutablePtr
    {
        ContextMutablePtr internal_context = Context::createCopy(current_context->getGlobalContext());
        internal_context->makeQueryContext();
        internal_context->setSettings(current_context->getSettingsRef());
        /// The settings copied above can make the internal DROPs below wait; the element is the only way out.
        internal_context->setProcessListElement(current_context->getProcessListElementSafe());
        internal_context->setDDLOrOnClusterInternal(true);
        if (bypass_size_guard)
        {
            internal_context->setSetting("max_table_size_to_drop", Field(UInt64{0}));
            internal_context->setSetting("max_partition_size_to_drop", Field(UInt64{0}));
        }
        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            internal_context->setQueryKindReplicatedDatabaseInternal();
            internal_context->setQueryContext(std::const_pointer_cast<Context>(current_context));
            internal_context->initZooKeeperMetadataTransaction(txn, /*attach_existing=*/true);
        }
        return internal_context;
    };

    auto ast_drop = make_intrusive<ASTDropQuery>();
    String table_to_replace_name = create.getTable();

    {
        auto database = DatabaseCatalog::instance().getDatabase(create.getDatabase());
        if (database->getUUID() == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} query is supported only for Atomic databases",
                            is_plain_create
                ? "CREATE ... AS SELECT via a temporary table"
                : (create.create_or_replace
                    ? (create.is_materialized_view ? "CREATE OR REPLACE MATERIALIZED VIEW"
                        : (create.isView() ? "CREATE OR REPLACE VIEW" : "CREATE OR REPLACE TABLE"))
                    : "REPLACE TABLE"));

        /// For a plain create the final name must not already exist (as an active table, as a dictionary, or
        /// reserved by a detached table). Check it up front, before the create-only validations below (table
        /// name length, cyclic dependencies) and before authorizing or running the populating SELECT, so that
        /// (a) an `IF NOT EXISTS` create on a taken name is a no-op that does not run (and does not require
        /// access to) the SELECT and does not fail validations that only matter when a table is actually
        /// created (mirroring `doCreateTable`, where the existence check precedes them), and (b) a plain
        /// create over a taken name fails fast, with the same error `doCreateTable` reports, before the
        /// (potentially expensive) populate. This mirrors the full existence handling of `doCreateTable`
        /// (`isTableExist` plus the detached-name `checkMetadataFilenameAvailability` branch), not just the
        /// active-table probe. This check is not under the table DDL guard, so it can race with a concurrent
        /// create; the final RENAME below re-establishes correctness (it fails if the target appeared
        /// meanwhile, which for `IF NOT EXISTS` is a no-op).
        if (is_plain_create)
        {
            if (database->isTableExist(table_to_replace_name, current_context))
            {
                if (create.if_not_exists)
                    return {};
                /// Preserve the established error contract: a name already used by a dictionary reports
                /// `DICTIONARY_ALREADY_EXISTS`, not `TABLE_ALREADY_EXISTS` (mirrors `doCreateTable` and
                /// `02973_dictionary_table_exception_fix`).
                if (database->getTable(table_to_replace_name, current_context)->isDictionary())
                    throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS, "Dictionary {}.{} already exists",
                        backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(table_to_replace_name));
                throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists",
                    backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(table_to_replace_name));
            }
            else if (!create.attach)
            {
                /// The final name may still be reserved by a table in a detached / detached-permanently state:
                /// its metadata file is present even though `isTableExist` is false. Mirror `doCreateTable` so
                /// that `IF NOT EXISTS` is a no-op and a plain create fails with the metadata-name availability
                /// error -- both before the populate. Otherwise a no-op could run the SELECT, raise a source
                /// `ACCESS_DENIED`, or trigger scalar-subquery side effects, and a plain create could surface a
                /// source-query failure instead of the existing detached-name error, before the RENAME finally
                /// discovers the collision.
                try
                {
                    database->checkMetadataFilenameAvailability(table_to_replace_name);
                }
                catch (const Exception &)
                {
                    if (create.if_not_exists)
                        return {};
                    throw;
                }
            }
        }

        if (mode <= LoadingStrictnessLevel::CREATE)
            database->checkTableNameLength(table_to_replace_name);
    }

    if (is_plain_create)
        checkTableCanBeAddedWithNoCyclicDependencies(create, query_ptr, create_context);

    /// A non-APPEND refreshable materialized view exclusively owns its target table. The replacement is
    /// built while the view being replaced still owns it, so reject only when a different view owns it.
    /// Gate this like the constructor-side guard, which only applies to non-APPEND refreshable views.
    if (create.is_materialized_view && create.refresh_strategy && !create.refresh_strategy->append)
    {
        auto target_table_id = create.getTargetTableID(ViewTarget::To);
        if (!target_table_id.empty())
        {
            if (target_table_id.database_name.empty())
                target_table_id.database_name = create.getDatabase();
            if (auto task = getContext()->getRefreshSet().tryGetTaskForInnerTable(target_table_id))
            {
                auto owner_view_id = task->getInfo().view_id;
                if (owner_view_id != StorageID{create.getDatabase(), table_to_replace_name})
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Table {} is already a target of another refreshable materialized view: {}",
                        target_table_id.getFullTableName(), owner_view_id.getFullTableName());
            }
        }
    }

    {
        const String name_hash = TemporaryReplaceTableName::calculateHash(create.getDatabase(), create.getTable());
        const String random_suffix = [&]()
        {
            if (auto txn = current_context->getZooKeeperMetadataTransaction())
            {
                /// Avoid different table name on database replicas
                UInt64 hashed_zk_path = sipHash64(txn->getTaskZooKeeperPath());
                return getHexUIntLowercase(hashed_zk_path);
            }
            if (!current_context->getCurrentQueryId().empty())
            {
                const UInt32 hashed_query_id = static_cast<UInt32>(sipHash64(current_context->getCurrentQueryId()));
                return getRandomASCIIString(/*length=*/8) + getHexUIntLowercase(hashed_query_id);
            }
            return getRandomASCIIString(/*length=*/16);
        }();

        const String tmp_replace_table_name = TemporaryReplaceTableName{.name_hash = name_hash, .random_suffix = random_suffix}.toString();
        create.setTable(tmp_replace_table_name);

        ast_drop->setTable(create.getTable());
        ast_drop->is_dictionary = create.is_dictionary;
        ast_drop->setDatabase(create.getDatabase());
        ast_drop->kind = ASTDropQuery::Drop;
    }

    /// The populating INSERT SELECT runs against the internal temporary table, so its (random) name is
    /// recorded in this query's access info and would surface in `system.query_log` `tables`. The temporary
    /// table is an implementation detail whose random name is meaningless to the user and would make the log
    /// non-deterministic, so scrub it. This must run on every exit after the populate may have touched the
    /// temporary table -- the successful publish, the `IF NOT EXISTS` lost-race no-op, and any failure or
    /// rethrow -- because `executeQuery.cpp` copies the access info into the log for failed queries too, so a
    /// denied or failing `CREATE ... AS SELECT` would otherwise leak the internal name. The final table name is
    /// added to the log independently, from the query's target (see `IInterpreter::extendQueryLogElem`).
    auto scrub_temp_table_from_query_log = [&]()
    {
        if (create.isCreateQueryWithImmediateInsertSelect() && getContext()->hasQueryContext())
            getContext()->getQueryContext()->removeQueryAccessInfoTable(
                StorageID{create.getDatabase(), create.getTable()}.getFullTableName());
    };

    bool created = false;
    bool renamed = false;
    try
    {
        /// Create temporary table (random name will be generated)
        DDLGuardPtr ddl_guard;
        [[maybe_unused]] bool done = InterpreterCreateQuery(query_ptr, create_context).doCreateTable(create, properties, ddl_guard, mode);
        ddl_guard.reset();
        chassert(done);
        created = true;

        /// If table has dependencies - add them to the graph
        addTableDependencies(create, query_ptr, getContext());

        /// For a plain `CREATE TABLE ... AS SELECT` the populate below runs against the internal temporary
        /// table, so `InterpreterInsertQuery` would authorize `INSERT` on the random `_tmp_replace_*` name
        /// rather than the final name. That would regress table-scoped grants: before this PR the plain-create
        /// path checked `INSERT` on the final name directly, so `CREATE TABLE` + `INSERT ON db.dst` (not a
        /// wildcard grant) was sufficient. To preserve that contract, authorize `INSERT` on the final name up
        /// front -- as the user, over the columns that will be inserted -- and then skip the redundant
        /// target-`INSERT` check on the temporary name inside the populate. The source `SELECT` access is
        /// still checked by the populate as the user. REPLACE keeps its prior behavior: it never required
        /// `INSERT` on the final name (only DROP/CREATE), so it does not get the up-front check or the skip.
        if (is_plain_create && create.isCreateQueryWithImmediateInsertSelect())
        {
            auto temp_table = DatabaseCatalog::instance().getTable(
                StorageID{create.getDatabase(), create.getTable(), create.uuid}, current_context);
            auto temp_metadata = temp_table->getInMemoryMetadataPtr(current_context, false);
            const Names insert_columns = temp_metadata->getSampleBlockNonMaterialized().getNames();
            current_context->checkAccess(
                AccessType::INSERT, StorageID{create.getDatabase(), table_to_replace_name}, insert_columns);
        }

        /// Try fill temporary table. Note: POPULATE here uses the legacy, non-atomic population - the
        /// atomic path is only wired into the plain CREATE flow, not the create-or-replace flow (which
        /// populates a temporary table and then atomically swaps it in via EXCHANGE/RENAME).
        BlockIO fill_io = fillTableIfNeeded(create, /*skip_target_insert_access_check=*/is_plain_create);
        /// For queries like 'CREATE OR REPLACE TABLE ... AS SELECT * INSERT' might take a long time,
        /// passing this callback allows tcp sessions to send progress, stats and logs.
        /// It prevents getting socket timeout as well.
        bool with_interactive_cancel = create.isCreateQueryWithImmediateInsertSelect();
        executeTrivialBlockIO(fill_io, getContext(), with_interactive_cancel);

        /// Replace target table with created one
        ASTRenameQuery::Element elem
        {
            ASTRenameQuery::Table
            {
                create.getDatabase().empty() ? nullptr : make_intrusive<ASTIdentifier>(create.getDatabase()),
                make_intrusive<ASTIdentifier>(create.getTable())
            },
            ASTRenameQuery::Table
            {
                create.getDatabase().empty() ? nullptr : make_intrusive<ASTIdentifier>(create.getDatabase()),
                make_intrusive<ASTIdentifier>(table_to_replace_name)
            }
        };

        auto ast_rename = make_intrusive<ASTRenameQuery>(ASTRenameQuery::Elements{std::move(elem)});
        ast_rename->dictionary = create.is_dictionary;
        if (is_plain_create)
        {
            /// Plain CREATE ... AS SELECT: the target must not exist. A plain RENAME asserts this and fails
            /// with TABLE_ALREADY_EXISTS if a concurrent query created the target while we were populating the
            /// temporary table (handled below for IF NOT EXISTS).
            ast_rename->exchange = false;
            ast_rename->rename_if_cannot_exchange = false;
        }
        else if (create.create_or_replace || create.replace_view)
        {
            /// CREATE OR REPLACE TABLE/VIEW
            /// Will execute ordinary RENAME instead of EXCHANGE if the target table does not exist
            ast_rename->rename_if_cannot_exchange = true;
            ast_rename->exchange = false;
        }
        else
        {
            /// REPLACE TABLE/VIEW
            /// Will execute EXCHANGE query and fail if the target table does not exist
            ast_rename->exchange = true;
        }

        FailPointInjection::pauseFailPoint(FailPoints::create_or_replace_before_rename);

        /// The size check runs once inside the rename's `DDLGuard`s via `setPreSwapCheck`.
        /// If it throws, no rename happens and the catch block below drops the temp. For a plain create the
        /// rename publishes an internal temporary table under the final name, so it runs with a full-access
        /// context (the user is not required to hold RENAME/DROP on the temporary table); REPLACE keeps
        /// running as the user.
        ContextPtr rename_context = is_plain_create ? ContextPtr{make_internal_context(/*bypass_size_guard=*/false)} : current_context;
        InterpreterRenameQuery interpreter_rename{ast_rename, rename_context};
        interpreter_rename.setPreSwapCheck(
            [&current_context](const StorageID & to_drop_id)
            {
                if (auto to_drop = DatabaseCatalog::instance().tryGetTable(to_drop_id, current_context))
                {
                    /// The replaced table is dropped after the swap, under an internal temporary name that
                    /// grants cannot cover, so check the drop privilege for its kind here, on its real name.
                    AccessType drop_access = AccessType::DROP_TABLE;
                    if (to_drop->isView())
                        drop_access = AccessType::DROP_VIEW;
                    else if (to_drop->isDictionary())
                        drop_access = AccessType::DROP_DICTIONARY;
                    current_context->checkAccess(drop_access, to_drop_id);
                    to_drop->checkTableSizeBelowDropLimit(current_context);
                }
            });
        try
        {
            interpreter_rename.execute();
        }
        catch (const Exception & e)
        {
            /// A concurrent query created the target while we were populating the temporary table. For a plain
            /// `CREATE TABLE IF NOT EXISTS ... AS SELECT` this is a no-op: drop the temporary table and return
            /// without error. For a plain create without IF NOT EXISTS (and for REPLACE) the error propagates.
            if (is_plain_create && create.if_not_exists && e.code() == ErrorCodes::TABLE_ALREADY_EXISTS)
            {
                InterpreterDropQuery(ast_drop, make_internal_context(/*bypass_size_guard=*/true)).execute();
                scrub_temp_table_from_query_log();
                create.setTable(table_to_replace_name);
                return {};
            }
            throw;
        }
        renamed = true;

        if (!is_plain_create && !interpreter_rename.renamedInsteadOfExchange())
        {
            /// After the exchange the temporary name holds the replaced table, which may be of a different
            /// kind than the new one (e.g. a dictionary replaced by a view), so the drop must match its kind.
            if (auto replaced = DatabaseCatalog::instance().tryGetTable(StorageID{create.getDatabase(), create.getTable()}, current_context))
                ast_drop->is_dictionary = replaced->isDictionary();
            /// `pre_swap_check` already authorized this drop against the replaced table's real name.
            /// The temporary name cannot be covered by grants, so skip the access check on it.
            ast_drop->no_access_check = true;
            /// `pre_swap_check` also gated the size; bypass to avoid double-consuming
            /// the `force_drop_table` flag inside `Context::checkCanBeDropped`.
            auto drop_context = make_drop_context(/*bypass_size_guard=*/true);
            InterpreterDropQuery(ast_drop, drop_context).execute();
        }

        /// The replacement view's refresher was created paused so it could not touch the target
        /// before the rename. Resume it now, unless stop_refreshable_materialized_views_on_startup
        /// keeps refreshable views stopped, in which case it stays stopped like a plain CREATE.
        if (!current_context->getGlobalContext()->getSettingsRef()[Setting::stop_refreshable_materialized_views_on_startup])
            for (const auto & task : current_context->getRefreshSet().findTasks({create.getDatabase(), table_to_replace_name}))
                task->start();

        scrub_temp_table_from_query_log();

        create.setTable(table_to_replace_name);

        return {};
    }
    catch (...)
    {
        /// Drop the temp table we just created if it was not renamed to the target name.
        /// Bypassing the size guard is safe here: the temp name is unique to this call. For a plain create
        /// use a full-access context (also size-guard-bypassed): the user is not required to hold DROP on the
        /// internal temporary table (its cleanup must not turn a denied source SELECT into an ACCESS_DENIED on
        /// the temporary table), and the cleanup must succeed even after the temporary table has grown past
        /// `max_table_size_to_drop`, or a late failure would strand it.
        if (created && !renamed)
        {
            auto drop_context = is_plain_create ? make_internal_context(/*bypass_size_guard=*/true) : make_drop_context(/*bypass_size_guard=*/true);
            try
            {
                InterpreterDropQuery(ast_drop, drop_context).execute();
            }
            catch (...)
            {
                tryLogCurrentException("InterpreterCreateQuery", "Cannot DROP temporary table");
            }
        }
        /// The temporary name is still set on `create` here (it is reset to the final name only on the success
        /// and lost-race-no-op paths), so scrub it before the error propagates and gets logged.
        scrub_temp_table_from_query_log();
        throw;
    }
}

BlockIO InterpreterCreateQuery::doCreateOrReplaceTemporaryTable(ASTCreateQuery & create,
                                                                const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(DatabaseCatalog::TEMPORARY_DATABASE);

    String temporary_table_name = create.getTable();

    auto creator = [&](const StorageID & table_id)
    {
        auto res = StorageFactory::instance().get(create,
            database->getTableDataPath(table_id.getTableName()),
            getContext(),
            getContext()->getGlobalContext(),
            properties.columns,
            properties.constraints,
            mode,
            is_restore_from_backup);
        validateStorage(*res, mode, getContext(), /*is_temporary=*/true);
        return res;
    };

    auto temporary_table = TemporaryTableHolder(getContext(), creator, query_ptr);
    /// Bare `REPLACE` requires the target to exist (`updateExternalTable` throws `UNKNOWN_TABLE`);
    /// `CREATE OR REPLACE` accepts either state. Both calls are thread-safe.
    auto session_context = getContext()->getSessionContext();
    if (create.create_or_replace)
        session_context->addOrUpdateExternalTable(temporary_table_name, std::move(temporary_table));
    else
        session_context->updateExternalTable(temporary_table_name, std::move(temporary_table));
    /// Note, until BlockIO will be "executed" - the table is empty, so it is not atomic, but this is OK, since concurrent access from the same session to a temporary table is not possible
    return fillTableIfNeeded(create);
}

BlockIO InterpreterCreateQuery::fillTableIfNeeded(const ASTCreateQuery & create, bool skip_target_insert_access_check)
{
    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.isCreateQueryWithImmediateInsertSelect())
    {
        auto insert = make_intrusive<ASTInsertQuery>();
        insert->table_id = {create.getDatabase(), create.getTable(), create.uuid};
        insert->select = create.select->clone();

        InterpreterInsertQuery interpreter(
            insert,
            getContext(),
            getContext()->getSettingsRef()[Setting::insert_allow_materialized_columns],
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false);
        interpreter.setSkipTargetInsertAccessCheck(skip_target_insert_access_check);
        return interpreter.execute();
    }

    /// If the query is a CREATE TABLE .. CLONE AS ..., attach all partitions of the source table to the newly created table.
    if (create.is_clone_as && !as_table_saved.empty() && !create.is_create_empty && !create.is_ordinary_view
        && (!create.is_materialized_view || create.is_populate))
    {
        String as_database_name = getContext()->resolveDatabase(as_database_saved);

        auto partition = make_intrusive<ASTPartition>();
        partition->all = true;

        auto command = make_intrusive<ASTAlterCommand>();
        command->replace = false;
        command->type = ASTAlterCommand::REPLACE_PARTITION;
        command->partition = command->children.emplace_back(std::move(partition)).get();
        command->from_database = as_database_name;
        command->from_table = as_table_saved;
        command->to_database = create.getDatabase();
        command->to_table = create.getTable();

        auto command_list = make_intrusive<ASTExpressionList>();
        command_list->children.push_back(command);

        auto query = make_intrusive<ASTAlterQuery>();
        query->setDatabase(create.getDatabase());
        query->setTable(create.getTable());
        query->uuid = create.uuid;
        auto * alter = query->as<ASTAlterQuery>();

        alter->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
        alter->set(alter->command_list, command_list);

        /// The result of this internal ALTER is not visible to the user,
        /// so disable verbose output to avoid creating a pulling pipeline
        /// that executeTrivialBlockIO cannot handle.
        auto alter_context = Context::createCopy(getContext());
        alter_context->setSetting("alter_partition_verbose_result", Field(false));
        return InterpreterAlterQuery(query, alter_context).execute();
    }

    return {};
}

bool InterpreterCreateQuery::shouldPopulateMaterializedViewAtomically(const ASTCreateQuery & create) const
{
    /// `CREATE OR REPLACE` / `REPLACE` go through doCreateOrReplaceTable, which populates a temporary table
    /// and then atomically swaps it in via EXCHANGE/RENAME. Coordinating the exclusive-lock cut with that
    /// swap (and with the old view's still-live subscription) is not handled here, so those queries keep
    /// the legacy non-atomic population; only the plain CREATE flow is atomic.
    bool applies = create.isCreateQueryWithImmediateInsertSelect()
        && create.is_materialized_view && !create.is_clone_as && !internal
        && !create.replace_table && !create.replace_view
        && getContext()->getSettingsRef()[Setting::materialized_views_populate_atomically];

    if (!applies)
        return false;

    /// A CREATE executed as an entry of a `Replicated` database's DDL log (`POPULATE` is allowed there
    /// with `database_replicated_allow_heavy_create`) cannot honor the atomic path's "a failed CREATE
    /// leaves nothing behind" contract: by the time the population fails, the entry's metadata transaction
    /// has already been committed by the creation of the view - `DatabaseReplicated::dropTable` would try
    /// to add a `ZooKeeper` operation to an already executed transaction (a logical error) - and even a
    /// successful unilateral drop would diverge this replica from the replicas where the same entry
    /// succeeded. Rather than advertising rollback-and-retry semantics that cannot be provided, fall back
    /// to the legacy non-atomic population, the pre-existing behavior of `POPULATE` under this override.
    if (getContext()->getZooKeeperMetadataTransaction())
    {
        LOG_INFO(getLogger("InterpreterCreateQuery"),
            "Populating materialized view {}.{} non-atomically because it is created by an entry of a "
            "replicated database DDL log, where a failed atomic population could not be rolled back. "
            "Rows inserted into the source during the population may be missed or duplicated.",
            backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable()));
        return false;
    }

    return true;
}

std::optional<QualifiedTableName> InterpreterCreateQuery::tryGetAtomicPopulateSourceName(const ASTCreateQuery & create) const
{
    auto context = getContext();
    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase());

    /// The view is fed by inserts into a single source table (its `FROM` table); that table is what we must
    /// subscribe to and snapshot atomically. If the view has no such single source (e.g. it selects from a
    /// subquery, a table function, or a join without a clear driving table), there is nothing to race
    /// against, so atomic population does not apply.
    if (!ref_dependencies.mv_from_dependency)
        return std::nullopt;

    return ref_dependencies.mv_from_dependency->getQualifiedName();
}

StoragePtr InterpreterCreateQuery::getValidatedAtomicPopulateSource(const ASTCreateQuery & create)
{
    auto context = getContext();

    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase());

    if (!ref_dependencies.mv_from_dependency)
        return nullptr;

    /// The caller holds the DDL guard of the source's name, so from here until the guard is released the
    /// name cannot be renamed, exchanged or dropped. But the per-query storage cache may already hold a
    /// mapping for the name that was resolved *before* the guard was acquired - validating the view's
    /// SELECT resolves the source - and a RENAME or EXCHANGE may have changed the owner of the name in
    /// between. Drop the cache entry so the resolution below sees the current owner; it re-fills the
    /// cache, so every later read of this query (in particular the population's SELECT) resolves the name
    /// to the same table that is locked, subscribed and snapshotted here.
    if (context->hasQueryContext())
        context->getQueryContext()->dropStorageCacheEntry(*ref_dependencies.mv_from_dependency);

    auto source = DatabaseCatalog::instance().tryGetTable(*ref_dependencies.mv_from_dependency, context);

    /// The view's SELECT was validated against the source before the view was published, so the source
    /// existed then; not finding it now means it was dropped, renamed or exchanged away in the window
    /// between that validation and the acquisition of the source-name DDL guard the caller holds. The
    /// view is already published, so falling back to the legacy population is not an option: its
    /// INSERT ... SELECT would fail on the vanished name *outside* the rollback scope of
    /// `fillMaterializedViewAtomically`, leaving the just-created view behind (and subscribed to a name
    /// nobody owns), so a retry would get TABLE_ALREADY_EXISTS. Throw instead - we are inside the
    /// rollback scope, so the view is dropped and the failed CREATE leaves nothing behind.
    if (!source)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Table {} does not exist. It was dropped, renamed or exchanged concurrently with"
            " CREATE MATERIALIZED VIEW ... POPULATE reading from it",
            ref_dependencies.mv_from_dependency->getNameForLogs());

    /// Some sources (views, `Distributed`, `Merge`, `Buffer`, `Log` family, ...) cannot provide a pinned
    /// point-in-time snapshot, or are not in an `Atomic` database so the snapshot cannot be addressed by
    /// UUID. We cannot populate atomically from them, so fall back to the legacy non-atomic population
    /// (the previous behavior) instead of failing - this keeps existing `POPULATE` queries working. The
    /// fallback is best-effort, so record in the log that rows inserted during the population may be
    /// missed or duplicated.
    if (!source->supportsPinnedSnapshot() || source->getStorageID().uuid == UUIDHelpers::Nil)
    {
        LOG_INFO(getLogger("InterpreterCreateQuery"),
            "Populating materialized view {} non-atomically because its source table {} (engine {}) does not "
            "support reading a pinned point-in-time snapshot. Rows inserted into the source during the "
            "population may be missed or duplicated in the view.",
            qualified_name.getFullName(), source->getStorageID().getNameForLogs(), source->getName());
        return nullptr;
    }

    return source;
}

namespace
{

/// Settings that would take the population read off the pinned local snapshot, see
/// `fillMaterializedViewAtomically`. They are forced on the population context, but query-local `SETTINGS`
/// are reapplied on top of the context later - by the analyzer in `QueryTreeBuilder::buildSelectExpression`
/// and by the old interpreter in `InterpreterSelectQuery::initSettings` - so a view defined as
/// `... POPULATE AS SELECT ... SETTINGS enable_parallel_replicas = 1` would otherwise re-enable remote
/// reads for the population. Both the setting name and its alias have to be listed, because a `SETTINGS`
/// clause keeps the name as it was written and resolves the alias only when the change is applied.
///
/// `removeSettingsFromQuery` drops both the `name = value` and the `name = DEFAULT` forms - the latter
/// matters just as much, because `InterpreterSetQuery::resetSettingsToDefaultValue` restores the built-in
/// default, and for two of these settings that default is the dangerous value
/// (`parallel_distributed_insert_select = 2`, `enable_shared_storage_snapshot_in_query = true`). It also
/// detaches a `SETTINGS` clause that ends up empty, so the population query never formats to a bare
/// `SETTINGS` keyword, which would throw on re-parse.
constexpr std::array<std::string_view, 4> settings_incompatible_with_pinned_snapshot
{
    "allow_experimental_parallel_reading_from_replicas",
    "enable_parallel_replicas",
    "parallel_distributed_insert_select",
    "enable_shared_storage_snapshot_in_query",
};

}

std::optional<BlockIO> InterpreterCreateQuery::fillMaterializedViewAtomically(const ASTCreateQuery & create, DDLGuardPtr & ddl_guard, DDLGuardPtr & source_ddl_guard)
{
    try
    {
        return fillMaterializedViewAtomicallyImpl(create, ddl_guard, source_ddl_guard);
    }
    catch (...)
    {
        /// The rollback `DROP` below takes the DDL guard of this very view, so the guard we still hold (when
        /// the failure happened before the cut) has to be released first, otherwise the drop deadlocks on it.
        /// The source's guard has to go too: holding it while acquiring the view's guard inside the drop
        /// could invert the canonical (database, table) acquisition order and deadlock against a concurrent
        /// RENAME or EXCHANGE, and the rollback does not need the source name to be stable.
        ddl_guard.reset();
        source_ddl_guard.reset();

        /// doCreateTable has already created and started the view, but the atomic cut failed - most
        /// realistically `lockExclusively` timed out on a busy source, before `addDependencies` subscribed
        /// the view to it. Letting the exception escape as-is would leave behind a view that exists but is
        /// not registered as a dependent of the source, so future inserts would silently never populate it.
        /// Drop the just-created view instead, so the failed CREATE leaves behind nothing of what it
        /// created and can simply be retried (the same no-orphan contract as the temporary-table path of
        /// CREATE TABLE ... AS SELECT). The contract covers the objects this CREATE created: the view, its
        /// subscription and - for the plain ENGINE form, where the view owns its data - the populated rows,
        /// which the DROP removes with the view. For the `TO target` form the target table is a pre-existing
        /// table that is not ours to roll back: rows the failed population already appended to it stay
        /// there, exactly as after a failed `INSERT ... SELECT` into that table (ClickHouse inserts are not
        /// transactional across blocks), so retrying the CREATE backfills them again. That caveat is
        /// documented, and a test pins it down.
        /// A failure after the subscription is rolled back the same way - the DROP also removes the
        /// registered dependencies. That covers both building the population pipeline and running it: the
        /// population executes eagerly inside the `try` (see fillMaterializedViewAtomicallyImpl), so a
        /// runtime failure of the view's SELECT or of the target write - or a KILL of the CREATE - lands
        /// here too, after `executeTrivialBlockIO` has already torn the failed pipeline down and released
        /// its table locks. The drop runs under the global context, like the internal drop of a view's
        /// inner table: the user needed only CREATE to get here.
        ///
        /// The drop is asynchronous. Everything the rollback needs - unsubscribing the view from the source,
        /// removing it from the catalog and renaming away its metadata, so that the name is free again for a
        /// retry - happens synchronously inside `DatabaseAtomic::dropTable`; only the removal of the (empty)
        /// data is deferred to the background drop task, exactly as for a plain `DROP TABLE`. Waiting for
        /// that here would buy nothing and can hang the failed `CREATE` indefinitely: `clickhouse-local`
        /// never finishes `waitTableFinallyDropped`, so a synchronous drop turns a rollback into a hang.
        ///
        /// In a `Replicated` database the view would not be ours to drop - the entry's metadata transaction
        /// is already committed and a unilateral drop would diverge this replica - which is why
        /// `shouldPopulateMaterializedViewAtomically` never takes the atomic path for a CREATE executed as
        /// an entry of a replicated database DDL log.
        chassert(!getContext()->getZooKeeperMetadataTransaction());

        try
        {
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop,
                getContext()->getGlobalContext(),
                getContext(),
                StorageID{create.getDatabase(), create.getTable(), create.uuid},
                /* sync */ false,
                /* ignore_sync_setting */ true);
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("InterpreterCreateQuery"),
                fmt::format(
                    "Cannot drop materialized view {}.{} while rolling back its failed atomic population; "
                    "the view exists but may not be subscribed to its source table",
                    backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable())));
        }
        throw;
    }
}

std::optional<BlockIO> InterpreterCreateQuery::fillMaterializedViewAtomicallyImpl(const ASTCreateQuery & create, DDLGuardPtr & ddl_guard, DDLGuardPtr & source_ddl_guard)
{
    auto source = getValidatedAtomicPopulateSource(create);
    if (!source)
        return {};

    auto context = getContext();
    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr);
    auto source_uuid = source->getStorageID().uuid;

    /// Subscribe the view to new inserts and capture a snapshot of the existing source data together, under
    /// a brief exclusive lock on the source. An INSERT into the source holds a shared lock on it from before
    /// it decides which views to push to until after its part is committed. So the exclusive lock drains all
    /// in-flight inserts (their data is now in the snapshot and they did not see the view yet) and blocks new
    /// ones (which, once we release, will see the view and will not be in the snapshot). Every concurrently
    /// inserted row therefore lands on exactly one side of the cut.
    ///
    /// The snapshot is captured via `getStorageSnapshot`, which does not take the table's shared lock, so it
    /// does not conflict with the exclusive lock we hold here. The population pipeline is built and executed
    /// afterwards, without holding the lock, and reads the pinned snapshot.
    ///
    /// We must disable `enable_shared_storage_snapshot_in_query`: with it on, an earlier `getStorageSnapshot`
    /// of the source (taken while validating the view's SELECT, before we acquire the lock) is cached on the
    /// query and would be returned here, defeating the point of capturing under the lock. With it off the
    /// capture below is fresh - taken under the lock, after in-flight inserts have drained. The population
    /// read still uses the pinned snapshot (it takes priority over both the cache and a fresh capture).
    auto populate_context = Context::createCopy(context);
    populate_context->setSetting("enable_shared_storage_snapshot_in_query", false);

    /// The pinned snapshot lives only in this server's contexts (`populate_context` and its query context,
    /// pinned below). The population must therefore read the source locally, from that pinned snapshot. If
    /// the internal `INSERT ... SELECT` is instead dispatched to remote replicas (parallel replicas) or
    /// through a distributed write, those remote executions do not carry the pin: they read a fresh
    /// snapshot of the source - which breaks the exactly-once cut, because rows inserted concurrently with
    /// the population are then read remotely and also delivered to the view live - and they would re-send
    /// `INSERT INTO` the just-created view on other replicas. Force the local pinned-snapshot path by
    /// disabling `parallel_distributed_insert_select` and parallel-replica reading for this insert.
    populate_context->setSetting("parallel_distributed_insert_select", Field{0});
    populate_context->setSetting("enable_parallel_replicas", Field{0});

    StorageSnapshotPtr snapshot;
    {
        /// Models the window in which the view is already published but not yet subscribed to its source -
        /// in production it is the wait for the exclusive lock right below. A test uses it to run DDL on the
        /// view concurrently and check that the view's DDL guard, still held here, serializes it after the
        /// subscription.
        FailPointInjection::pauseFailPoint(FailPoints::atomic_populate_pause_before_subscription);

        auto source_lock = source->lockExclusively(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);

        /// Models a failure of the cut before the view is subscribed to the source (the realistic cause is
        /// a `lockExclusively` timeout right above, which a test cannot trigger deterministically). The
        /// rollback in `fillMaterializedViewAtomically` must drop the just-created view.
        fiu_do_on(FailPoints::atomic_populate_fail_before_subscription,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Failpoint atomic_populate_fail_before_subscription is triggered");
        });

        DatabaseCatalog::instance().addDependencies(
            qualified_name,
            ref_dependencies.dependencies,
            loading_dependencies,
            TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()});

        auto source_metadata = source->getInMemoryMetadataPtr(populate_context, false);
        snapshot = source->getStorageSnapshot(source_metadata, populate_context);
    }

    /// The cut is done: the view is published, subscribed to the source and the snapshot is pinned. Release
    /// the view's DDL guard, which the caller kept for us across the exclusive-lock wait above, so that a
    /// concurrent `DROP` or `RENAME` of the view could not squeeze in between publishing it and subscribing
    /// it - that would have left a subscription naming a view that is no longer there, and
    /// `DatabaseCatalog::getReadyDependentViews` treats a single missing dependent as "no views are ready",
    /// silently stopping the population of *every* view of that source. The population below can take
    /// arbitrarily long, and it does not need the guard: it is an ordinary `INSERT` into the view, so a
    /// concurrent `DROP` of the view during the population is handled exactly as for any other insert.
    ///
    /// The source's DDL guard is released for the same reason: it kept the owner of the source's name
    /// stable from resolving the source (in `getValidatedAtomicPopulateSource`) until the name-keyed
    /// subscription right above, so the subscription is guaranteed to be registered on the table that was
    /// locked and snapshotted. From now on a `RENAME` of the source carries the subscription along with
    /// the name change, like for any other materialized view, so the population does not need the guard.
    ddl_guard.reset();
    source_ddl_guard.reset();

    /// Pin the snapshot so the population's SELECT reads exactly the captured data. The population's
    /// SELECT is analyzed and executed under contexts derived from the shared query context
    /// (`getQueryContext()`), so the pin must live there: a pin stored only on `populate_context` (a
    /// copy) is not seen by those reads, which would then take a fresh snapshot of the source and read
    /// rows inserted concurrently with the population - rows that are also delivered to the view live,
    /// duplicating them. Set it on `populate_context` too (harmless) for any read that uses it directly.
    populate_context->setPinnedStorageSnapshot(source_uuid, snapshot);
    if (populate_context->hasQueryContext())
        populate_context->getQueryContext()->setPinnedStorageSnapshot(source_uuid, snapshot);

    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = {create.getDatabase(), create.getTable(), create.uuid};
    insert->select = create.select->clone();

    /// The settings above are forced on `populate_context`, but query-local `SETTINGS` of the view's SELECT
    /// are applied on top of the context afterwards and could re-enable remote reads, which do not carry the
    /// pin. Scrub them from the population query - it is a copy, so the view's stored definition (used for
    /// the live pushes, which are not bound to a pinned snapshot) keeps them.
    removeSettingsFromQuery(insert->select, settings_incompatible_with_pinned_snapshot);

    auto populate_io = InterpreterInsertQuery(
                           insert,
                           populate_context,
                           populate_context->getSettingsRef()[Setting::insert_allow_materialized_columns],
                           /* no_squash */ false,
                           /* no_destination */ false,
                           /* async_insert */ false)
                           .execute();

    /// Run the population right here, eagerly, like `doCreateOrReplaceTable` runs its populating insert.
    /// `InterpreterInsertQuery::execute` only builds the pipeline; returning the lazy `BlockIO` to the
    /// caller would let the outer `executeQuery` drive it after the rollback scope of
    /// `fillMaterializedViewAtomically` has already exited, so a runtime failure of the population (the
    /// view's SELECT or the target write) would escape the rollback and leave the failed CREATE with the
    /// view published and subscribed - breaking the no-orphan contract. Executing here, an execution-time
    /// exception (including a KILL of the CREATE) first tears the pipeline down - `executeTrivialBlockIO`
    /// calls `onException`, releasing the pipeline's table locks, so the rollback's DROP cannot contend
    /// with them - and then unwinds into the rollback, which drops the just-created view.
    ///
    /// The interactive-cancel callback keeps TCP sessions sending progress and able to cancel the
    /// (potentially long) population, and prevents socket timeouts, exactly as for `CREATE ... AS SELECT`.
    /// The process-list element makes `KILL QUERY` cancel the population: on the lazy path the outer
    /// `executeQuery` attached it to the returned pipeline, so it has to be attached by hand here.
    populate_io.pipeline.setProcessListElement(getContext()->getProcessListElement());
    executeTrivialBlockIO(populate_io, getContext(), /*with_interactive_cancel=*/true);
    return BlockIO{};
}

void InterpreterCreateQuery::prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr local_context, const String & cluster_name)
{
    if (create.attach)
        return;

    /// For CREATE query generate UUID on initiator, so it will be the same on all hosts.
    /// It will be ignored if database does not support UUIDs.
    create.generateRandomUUIDs();

    /// For cross-replication cluster we cannot use UUID in replica path.
    String cluster_name_expanded = local_context->getMacros()->expand(cluster_name);
    ClusterPtr cluster = local_context->getCluster(cluster_name_expanded);

    if (cluster->maybeCrossReplication())
    {
        auto on_cluster_version = local_context->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value;
        if (DDLLogEntry::NORMALIZE_CREATE_ON_INITIATOR_VERSION <= on_cluster_version)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Value {} of setting distributed_ddl_entry_format_version "
                                                         "is incompatible with cross-replication", on_cluster_version);

        /// Check that {uuid} macro is not used in zookeeper_path for ReplicatedMergeTree.
        /// Otherwise replicas will generate different paths.
        if (!create.storage)
            return;
        if (!create.storage->engine)
            return;
        if (!startsWith(create.storage->engine->name, "Replicated"))
            return;

        bool has_explicit_zk_path_arg = create.storage->engine->arguments &&
                                        create.storage->engine->arguments->children.size() >= 2 &&
                                        create.storage->engine->arguments->children[0]->as<ASTLiteral>() &&
                                        create.storage->engine->arguments->children[0]->as<ASTLiteral>()->value.getType() == Field::Types::String;

        if (has_explicit_zk_path_arg)
        {
            String zk_path = create.storage->engine->arguments->children[0]->as<ASTLiteral>()->value.safeGet<String>();
            Macros::MacroExpansionInfo info;
            info.table_id.uuid = create.uuid;
            info.ignore_unknown = true;
            local_context->getMacros()->expand(zk_path, info);
            if (!info.expanded_uuid)
                return;
        }

        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Seems like cluster is configured for cross-replication, "
                        "but zookeeper_path for ReplicatedMergeTree is not specified or contains {{uuid}} macro. "
                        "It's not supported for cross replication, because tables must have different UUIDs. "
                        "Please specify unique zookeeper_path explicitly.");
    }
}

BlockIO InterpreterCreateQuery::executeQueryOnCluster(ASTCreateQuery & create)
{
    prepareOnClusterQuery(create, getContext(), create.cluster);
    DDLQueryOnClusterParams params;
    params.access_to_check = getRequiredAccess();
    return executeDDLQueryOnCluster(query_ptr, getContext(), params);
}

BlockIO InterpreterCreateQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    auto & create = query_ptr->as<ASTCreateQuery &>();

    create.if_not_exists |= getContext()->getSettingsRef()[Setting::create_if_not_exists];

    bool is_create_database = create.database && !create.table;
    if (!create.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        if (create.attach_as_replicated.has_value())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "ATTACH AS [NOT] REPLICATED is not supported for ON CLUSTER queries");

        auto on_cluster_version = getContext()->getSettingsRef()[Setting::distributed_ddl_entry_format_version].value;
        if (is_create_database || on_cluster_version < DDLLogEntry::NORMALIZE_CREATE_ON_INITIATOR_VERSION)
        {
            /// Authorize here: this is the last point that still runs as the real user, and worker legs
            /// run with no user by default.
            if (is_create_database && create.storage && create.storage->engine
                && create.storage->engine->name == "Backup" && create.storage->engine->arguments)
                DatabaseBackup::parseAndAuthorizeLocator(create.storage->engine->arguments->children, getContext());

            /// This branch ships the query text as written, and `OLDEST_VERSION` also ships no settings,
            /// so a worker there would resolve `toTime` with its own default.
            if (!is_create_database && !create.attach_short_syntax && !is_restore_from_backup
                && getContext()->getSettingsRef()[Setting::use_legacy_to_time])
            {
                /// The source definition of `AS` is materialized on the worker, so the initiator cannot
                /// rewrite it here. Starting with `SETTINGS_IN_ZK_VERSION` the entry carries the query
                /// settings, hence the worker sees `use_legacy_to_time` and materializes exactly what a
                /// local `CREATE` would; only `OLDEST_VERSION` drops the setting. `CLONE AS` stays rejected
                /// for every version of this branch, because the worker-side rewrite skips clones on
                /// purpose (a re-spelled key would make the partition copy see a different structure), so
                /// carrying the setting does not make the stored spelling unambiguous.
                if (!create.as_table.empty()
                    && (create.is_clone_as || on_cluster_version == DDLLogEntry::OLDEST_VERSION))
                {
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "CREATE TABLE ... {} ON CLUSTER with distributed_ddl_entry_format_version = {} "
                        "and use_legacy_to_time = 1 is not supported",
                        create.is_clone_as ? "CLONE AS" : "AS",
                        on_cluster_version);
                }

                normalizeLegacyToTimeInCreateQuery(query_ptr, getContext());
            }

            return executeQueryOnCluster(create);
        }
    }

    getContext()->checkAccess(getRequiredAccess());

    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (is_create_database)
        return createDatabase(create);
    return createTable(create);
}


AccessRightsElements InterpreterCreateQuery::getRequiredAccess() const
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return {};

    AccessRightsElements required_access;
    const auto & create = query_ptr->as<const ASTCreateQuery &>();

    if (!create.table)
    {
        required_access.emplace_back(AccessType::CREATE_DATABASE, create.getDatabase());
    }
    else if (create.is_dictionary)
    {
        required_access.emplace_back(AccessType::CREATE_DICTIONARY, create.getDatabase(), create.getTable());
    }
    else if (create.isView())
    {
        if (create.replace_view)
            required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, create.getDatabase(), create.getTable());
        else if (create.isTemporary())
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_VIEW);
        else
            required_access.emplace_back(AccessType::CREATE_VIEW, create.getDatabase(), create.getTable());
    }
    else
    {
        if (create.isTemporary())
        {
            /// Currently default table engine for temporary tables is Memory. default_table_engine does not affect temporary tables.
            if (create.storage && create.storage->engine && create.storage->engine->name != "Memory")
                required_access.emplace_back(AccessType::CREATE_ARBITRARY_TEMPORARY_TABLE);
            else
                required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
        }
        else
        {
            if (create.replace_table)
                required_access.emplace_back(AccessType::DROP_TABLE, create.getDatabase(), create.getTable());
            required_access.emplace_back(AccessType::CREATE_TABLE, create.getDatabase(), create.getTable());
        }
    }

    if (create.targets)
    {
        for (const auto & target : create.targets->targets)
        {
            const auto & target_id = target.table_id;
            if (target_id)
                required_access.emplace_back(AccessType::SELECT | AccessType::INSERT, target_id.database_name, target_id.table_name);
        }
    }

    if (create.storage && create.storage->engine)
        required_access.emplace_back(AccessType::TABLE_ENGINE, create.storage->engine->name);

    return required_access;
}

void InterpreterCreateQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    if (!as_table_saved.empty())
    {
        String database = backQuoteIfNeed(as_database_saved.empty() ? getContext()->getCurrentDatabase() : as_database_saved);
        elem.query_databases.insert(database);
        elem.query_tables.insert(database + "." + backQuoteIfNeed(as_table_saved));
    }
}

void InterpreterCreateQuery::addColumnsDescriptionToCreateQueryIfNecessary(ASTCreateQuery & create, const StoragePtr & storage)
{
    if (create.is_dictionary || (create.columns_list && create.columns_list->columns && !create.columns_list->columns->children.empty()))
        return;

    auto ast_storage = make_intrusive<ASTStorage>();
    unsigned max_parser_depth_v = static_cast<unsigned>(getContext()->getSettingsRef()[Setting::max_parser_depth]);
    unsigned max_parser_backtracks_v = static_cast<unsigned>(getContext()->getSettingsRef()[Setting::max_parser_backtracks]);
    auto query_from_storage = DB::getCreateQueryFromStorage(storage, ast_storage, false, max_parser_depth_v, max_parser_backtracks_v, true, getContext());
    auto & create_query_from_storage = query_from_storage->as<ASTCreateQuery &>();

    if (!create.columns_list)
    {
        ASTPtr columns_list = make_intrusive<ASTColumns>(*create_query_from_storage.columns_list);
        create.set(create.columns_list, columns_list);
    }
    else
    {
        ASTPtr columns = make_intrusive<ASTExpressionList>(*create_query_from_storage.columns_list->columns);
        create.columns_list->set(create.columns_list->columns, columns);
    }
}

void InterpreterCreateQuery::processSQLSecurityOption(ContextMutablePtr context_, ASTSQLSecurity & sql_security, bool is_materialized_view, LoadingStrictnessLevel mode)
{
    /// If no SQL security is specified, apply default from default_*_view_sql_security setting.
    if (!sql_security.type)
    {
        SQLSecurityType default_security = {};

        if (is_materialized_view)
            default_security = context_->getSettingsRef()[Setting::default_materialized_view_sql_security];
        else
            default_security = context_->getSettingsRef()[Setting::default_normal_view_sql_security];

        if (default_security == SQLSecurityType::DEFINER)
        {
            String default_definer = context_->getSettingsRef()[Setting::default_view_definer];
            if (default_definer == "CURRENT_USER")
                sql_security.is_definer_current_user = true;
            else
                sql_security.definer = make_intrusive<ASTUserNameWithHost>(default_definer);
        }

        sql_security.type = default_security;
    }

    /// Resolves `DEFINER = CURRENT_USER`. Can change the SQL security type if we try to resolve the user during the attachment.
    const auto current_user_name = context_->getUserName();
    if (sql_security.is_definer_current_user)
    {
        if (current_user_name.empty())
            /// This can happen only when attaching a view for the first time after migration and with `CURRENT_USER` default.
            if (is_materialized_view)
                sql_security.type = SQLSecurityType::NONE;
            else
                sql_security.type = SQLSecurityType::INVOKER;
        else if (sql_security.definer)
            sql_security.definer->replace(current_user_name);
        else
            sql_security.definer = make_intrusive<ASTUserNameWithHost>(current_user_name);
    }

    /// Checks the permissions for the specified definer user.
    if (sql_security.definer)
    {
        auto definer_name = sql_security.definer->toString();
        if (definer_name != current_user_name)
            context_->checkAccess(AccessType::SET_DEFINER, definer_name);

        if (mode <= LoadingStrictnessLevel::CREATE)
        {
            auto & access_control = context_->getAccessControl();
            const auto user = access_control.read<User>(definer_name);
            if (access_control.isEphemeral(access_control.getID<User>(definer_name)))
            {
                definer_name = user->getName() + ":definer";
                sql_security.definer = make_intrusive<ASTUserNameWithHost>(definer_name);
                auto new_user = typeid_cast<std::shared_ptr<User>>(user->clone());
                new_user->setName(definer_name);
                new_user->authentication_methods.clear();
                new_user->authentication_methods.emplace_back(AuthenticationType::NO_AUTHENTICATION);
                access_control.insertOrReplace(new_user);
            }
        }
    }

    if (sql_security.type == SQLSecurityType::NONE)
        context_->checkAccess(AccessType::ALLOW_SQL_SECURITY_NONE);
}

void InterpreterCreateQuery::convertMergeTreeTableIfPossible(ASTCreateQuery & create, DatabasePtr database, bool to_replicated)
{
    /// Check engine can be changed
    if (database->getEngineName() != "Atomic")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table engine conversion to replicated is supported only for Atomic databases");

    if (!create.storage || !create.storage->engine || !create.storage->engine->name.contains("MergeTree"))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table engine conversion is supported only for MergeTree family engines");

    String engine_name = create.storage->engine->name;
    if (engine_name.starts_with("Replicated"))
    {
        if (to_replicated)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Can not attach table as replicated, table is already replicated");
    }
    else if (!to_replicated)
       throw Exception(ErrorCodes::INCORRECT_QUERY, "Can not attach table as not replicated, table is already not replicated");

    /// Must precede every side effect below: neither the transaction metadata removal nor the
    /// metadata rewrite can be rolled back. The other direction takes no Keeper path at all.
    if (to_replicated)
        DatabaseOrdinary::checkReplicaPathIsSafe(create, getContext());

    /// Ensure the old detached table instance is destroyed before we remove
    /// transaction metadata files. Otherwise the old table's parts still hold
    /// in-memory version metadata referencing those files, and the debug
    /// assertion in removeIfNeeded() → assertHasValidVersionMetadata() will
    /// fail when the old storage is destroyed later.
    if (create.uuid != UUIDHelpers::Nil)
    {
        if (getContext()->getSettingsRef()[Setting::database_atomic_wait_for_drop_and_detach_synchronously])
        {
            QueryStatusPtr query_status = getContext()->getProcessListElementSafe();
            database->waitDetachedTableNotInUse(create.uuid, [&]()
            {
                if (query_status)
                    query_status->throwIfKilled();
            });
        }
        else
            database->checkDetachedTableNotInUse(create.uuid);
    }

    /// When converting to replicated, remove all transaction metadata files
    if (to_replicated && !engine_name.starts_with("Replicated"))
    {
        String table_data_path = database->getTableDataPath(create);
        clearTransactionMetadata(table_data_path, getContext());
    }

    /// Set new engine
    DatabaseOrdinary::setMergeTreeEngine(create, getContext(), to_replicated);

    /// Save new metadata
    auto db_disk = database->getDisk();
    String table_metadata_path = database->getObjectMetadataPath(create.getTable());
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement = DB::getObjectDefinitionFromCreateQuery(create.clone());
    writeMetadataFile(
        db_disk,
        /*file_path=*/table_metadata_tmp_path,
        /*content=*/statement,
        /*fsync_metadata=*/getContext()->getSettingsRef()[Setting::fsync_metadata]);
    db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
}

void InterpreterCreateQuery::clearTransactionMetadata(const String & table_data_path, ContextPtr local_context)
{
    LOG_INFO(getLogger("InterpreterCreateQuery"), "Clearing transaction metadata for table, relative path: {} when ATTACH AS REPLICATED.", table_data_path);

    /// Use disk API to remove transaction metadata files from all disks
    auto disks = local_context->getDisksMap();
    size_t total_removed = 0;

    for (const auto & [disk_name, disk] : disks)
    {
        try
        {
            /// Skip if the table data path doesn't exist on this disk
            if (!disk->existsDirectory(table_data_path))
                continue;

            /// Iterate through all parts in the table data directory
            for (auto it = disk->iterateDirectory(table_data_path); it->isValid(); it->next())
            {
                String part_name = it->name();
                String part_path = fs::path(table_data_path) / part_name;

                /// Check if it's a directory (part directory)
                if (!disk->existsDirectory(part_path))
                    continue;

                /// Remove the committed metadata file (`txn_version.txt`) and any leftover
                /// temporary file (`txn_version.txt.tmp`). A `.tmp` file can legitimately linger
                /// on a part (for example, hardlinked onto a mutated part from its source during
                /// a merge/mutation race on object storage). If it is left behind here, the part
                /// is later misread as a rolled-back transaction (see
                /// `VersionMetadataOnDisk::loadMetadata`) and wrongly discarded as `Outdated`,
                /// which resurrects pre-mutation data after `ATTACH AS REPLICATED`.
                /// Remove the temporary file first so the cleanup is fail-closed: if removing the
                /// main file then throws, the part is left with a valid `txn_version.txt` (still a
                /// committed part) rather than the dangerous tmp-only state described above.
                for (const auto * file_name : {VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME,
                                               VersionMetadata::TXN_VERSION_METADATA_FILE_NAME})
                {
                    String txn_file = fs::path(part_path) / file_name;
                    if (disk->existsFile(txn_file))
                    {
                        disk->removeFile(txn_file);
                        total_removed++;
                    }
                }
            }
        }
        catch (...)
        {
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                           "Cannot ATTACH AS REPLICATED: failed to clear transaction metadata on disk {}, due to {}",
                           disk_name, getCurrentExceptionMessage(false));
        }
    }

    LOG_INFO(getLogger("InterpreterCreateQuery"), "Removed {} transaction metadata files for table, relative path: {}.", total_removed, table_data_path);
}

void registerInterpreterCreateQuery(InterpreterFactory & factory);
void registerInterpreterCreateQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateQuery", create_fn);
}

}
