#include <memory>

#include <filesystem>

#include <Access/AccessControl.h>
#include <Access/User.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include <Common/randomSeed.h>
#include <Common/atomicRename.h>
#include <Common/PoolId.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Parsers/ASTSetQuery.h>

#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <Core/ServerSettings.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/WindowView/StorageWindowView.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Access/Common/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/hasNullable.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/TablesLoader.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/NormalizeAndEvaluateConstantsVisitor.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>

#include <TableFunctions/TableFunctionFactory.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/QueryParameterVisitor.h>


namespace CurrentMetrics
{
    extern const Metric AttachedTable;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_codecs;
    extern const SettingsBool allow_experimental_database_materialized_mysql;
    extern const SettingsBool allow_experimental_database_materialized_postgresql;
    extern const SettingsBool allow_experimental_full_text_index;
    extern const SettingsBool allow_experimental_inverted_index;
    extern const SettingsBool allow_experimental_refreshable_materialized_view;
    extern const SettingsBool allow_experimental_statistics;
    extern const SettingsBool allow_experimental_vector_similarity_index;
    extern const SettingsBool allow_materialized_view_with_bad_select;
    extern const SettingsBool allow_suspicious_codecs;
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
    extern const SettingsBool enable_deflate_qpl_codec;
    extern const SettingsBool enable_zstd_qat_codec;
    extern const SettingsBool flatten_nested;
    extern const SettingsBool fsync_metadata;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsBool restore_replace_external_engines_to_null;
    extern const SettingsBool restore_replace_external_table_functions_to_null;
}

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
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
    extern const int NOT_IMPLEMENTED;
    extern const int ENGINE_REQUIRED;
    extern const int UNKNOWN_STORAGE;
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_TABLES;
    extern const int TOO_MANY_DATABASES;
    extern const int THERE_IS_NO_COLUMN;
}

namespace fs = std::filesystem;

InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    String database_name = create.getDatabase();

    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    /// Database can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard
    if (DatabaseCatalog::instance().isDatabaseExist(database_name))
    {
        if (create.if_not_exists)
            return {};
        else
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Database {} already exists.", database_name);
    }

    auto db_num_limit = getContext()->getGlobalContext()->getServerSettings().max_database_num_to_throw;
    if (db_num_limit > 0 && !internal)
    {
        size_t db_count = DatabaseCatalog::instance().getDatabases().size();
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

    /// Will write file with database metadata, if needed.
    String database_name_escaped = escapeForFileName(database_name);
    fs::path metadata_path = fs::weakly_canonical(getContext()->getPath());
    fs::create_directories(metadata_path / "metadata");
    fs::path metadata_file_tmp_path = metadata_path / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = metadata_path / "metadata" / (database_name_escaped + ".sql");

    if (!create.storage && create.attach)
    {
        if (!fs::exists(metadata_file_path))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Database engine must be specified for ATTACH DATABASE query");
        /// Short syntax: try read database definition from file
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, getContext(), metadata_file_path);
        create = ast->as<ASTCreateQuery &>();
        if (create.table || !create.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Metadata file {} contains incorrect CREATE DATABASE query", metadata_file_path.string());
        create.attach = true;
        create.attach_short_syntax = true;
        create.setDatabase(database_name);
    }
    else if (!create.storage)
    {
        /// For new-style databases engine is explicitly specified in .sql
        /// When attaching old-style database during server startup, we must always use Ordinary engine
        if (create.attach)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Database engine must be specified for ATTACH DATABASE query");
        auto engine = std::make_shared<ASTFunction>();
        auto storage = std::make_shared<ASTStorage>();
        engine->name = "Atomic";
        engine->no_empty_args = true;
        storage->set(storage->engine, engine);
        create.set(create.storage, storage);
    }
    else if ((create.columns_list
              && ((create.columns_list->indices && !create.columns_list->indices->children.empty())
                  || (create.columns_list->projections && !create.columns_list->projections->children.empty()))))
    {
        /// Currently, there are no database engines, that support any arguments.
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", serializeAST(*create.storage));
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
        else if (create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();

        metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);

        if (!create.attach && fs::exists(metadata_path) && !fs::is_empty(metadata_path))
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists and is not empty", metadata_path.string());
    }
    else if (create.storage->engine->name == "MaterializeMySQL"
        || create.storage->engine->name == "MaterializedMySQL")
    {
        /// It creates nested database with Ordinary or Atomic engine depending on UUID in query and default engine setting.
        /// Do nothing if it's an internal ATTACH on server startup or short-syntax ATTACH query from user,
        /// because we got correct query from the metadata file in this case.
        /// If we got query from user, then normalize it first.
        bool attach_from_user = create.attach && !internal && !create.attach_short_syntax;
        bool create_from_user = !create.attach;

        if (create_from_user)
        {
            if (create.uuid == UUIDHelpers::Nil)
                create.uuid = UUIDHelpers::generateV4();    /// Will enable Atomic engine for nested database
        }
        else if (attach_from_user && create.uuid == UUIDHelpers::Nil)
        {
            /// Ambiguity is possible: should we attach nested database as Ordinary
            /// or throw "UUID must be specified" for Atomic? So we suggest short syntax for Ordinary.
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Use short attach syntax ('ATTACH DATABASE name;' without engine) "
                            "to attach existing database or specify UUID to attach new database with Atomic engine");
        }

        /// Set metadata path according to nested engine
        if (create.uuid == UUIDHelpers::Nil)
            metadata_path = metadata_path / "metadata" / database_name_escaped;
        else
            metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);
    }
    else
    {
        bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster && !internal)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Ordinary database engine does not support UUID");

        /// The database doesn't support UUID so we'll ignore it. The UUID could be set here because of either
        /// a) the initiator of `ON CLUSTER` query generated it to ensure the same UUIDs are used on different hosts; or
        /// b) `RESTORE from backup` query generated it to ensure the same UUIDs are used on different hosts.
        create.uuid = UUIDHelpers::Nil;
        metadata_path = metadata_path / "metadata" / database_name_escaped;
    }

    if (create.storage->engine->name == "Replicated" && !internal && !create.attach && create.storage->engine->arguments)
    {
        /// Fill in default parameters
        if (create.storage->engine->arguments->children.size() == 1)
            create.storage->engine->arguments->children.push_back(std::make_shared<ASTLiteral>("{shard}"));

        if (create.storage->engine->arguments->children.size() == 2)
            create.storage->engine->arguments->children.push_back(std::make_shared<ASTLiteral>("{replica}"));
    }

    if ((create.storage->engine->name == "MaterializeMySQL" || create.storage->engine->name == "MaterializedMySQL")
        && !getContext()->getSettingsRef()[Setting::allow_experimental_database_materialized_mysql] && !internal && !create.attach)
    {
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                        "MaterializedMySQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialized_mysql to use it");
    }

    if (create.storage->engine->name == "MaterializedPostgreSQL"
        && !getContext()->getSettingsRef()[Setting::allow_experimental_database_materialized_postgresql] && !internal && !create.attach)
    {
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                        "MaterializedPostgreSQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialized_postgresql to use it");
    }

    bool need_write_metadata = !create.attach || !fs::exists(metadata_file_path);
    bool need_lock_uuid = internal || need_write_metadata;
    auto mode = getLoadingStrictnessLevel(create.attach, force_attach, has_force_restore_data_flag, /*secondary*/ false);

    /// Lock uuid, so we will known it's already in use.
    /// We do it when attaching databases on server startup (internal) and on CREATE query (!create.attach);
    TemporaryLockForUUIDDirectory uuid_lock;
    if (need_lock_uuid)
        uuid_lock = TemporaryLockForUUIDDirectory{create.uuid};
    else if (create.uuid != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(create.uuid))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create.uuid);

    DatabasePtr database = DatabaseFactory::instance().get(create, metadata_path / "", getContext());

    if (create.uuid != UUIDHelpers::Nil)
        create.setDatabase(TABLE_WITH_UUID_NAME_PLACEHOLDER);

    if (need_write_metadata)
    {
        create.attach = true;
        create.if_not_exists = false;

        WriteBufferFromOwnString statement_buf;
        formatAST(create, statement_buf, false);
        writeChar('\n', statement_buf);
        String statement = statement_buf.str();

        /// Needed to make database creation retriable if it fails after the file is created
        fs::remove(metadata_file_tmp_path);

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();
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

        if (need_write_metadata)
        {
            /// Prevents from overwriting metadata of detached database
            renameNoReplace(metadata_file_tmp_path, metadata_file_path);
            renamed = true;
        }
    }
    catch (...)
    {
        if (renamed)
        {
            [[maybe_unused]] bool removed = fs::remove(metadata_file_path);
            assert(removed);
        }
        if (added)
            DatabaseCatalog::instance().detachDatabase(getContext(), database_name, false, false);

        throw;
    }

    return {};
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column.name;

        ParserDataType type_parser;
        String type_name = column.type->getName();
        const char * pos = type_name.data();
        const char * end = pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, pos, end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns)
{
    std::shared_ptr<ASTExpressionList> columns_list = std::static_pointer_cast<ASTExpressionList>(formatColumns(columns));

    for (const auto & alias_column : alias_columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = alias_column.name;

        ParserDataType type_parser;
        String type_name = alias_column.type->getName();
        const char * type_pos = type_name.data();
        const char * type_end = type_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_pos, type_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        column_declaration->default_specifier = "ALIAS";

        const auto & alias = alias_column.expression;
        const char * alias_pos = alias.data();
        const char * alias_end = alias_pos + alias.size();
        ParserExpression expression_parser;
        column_declaration->default_expression = parseQuery(expression_parser, alias_pos, alias_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        column_declaration->children.push_back(column_declaration->default_expression);

        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const ColumnsDescription & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;

        ParserDataType type_parser;
        String type_name = column.type->getName();
        const char * type_name_pos = type_name.data();
        const char * type_name_end = type_name_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_name_pos, type_name_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        if (column.default_desc.expression)
        {
            column_declaration->default_specifier = toString(column.default_desc.kind);
            column_declaration->default_expression = column.default_desc.expression->clone();
            column_declaration->children.push_back(column_declaration->default_expression);
        }

        column_declaration->ephemeral_default = column.default_desc.ephemeral_default;

        if (!column.comment.empty())
        {
            column_declaration->comment = std::make_shared<ASTLiteral>(Field(column.comment));
            column_declaration->children.push_back(column_declaration->comment);
        }

        if (column.codec)
        {
            column_declaration->codec = column.codec;
            column_declaration->children.push_back(column_declaration->codec);
        }

        if (!column.statistics.empty())
        {
            column_declaration->statistics_desc = column.statistics.getAST();
            column_declaration->children.push_back(column_declaration->statistics_desc);
        }

        if (column.ttl)
        {
            column_declaration->ttl = column.ttl;
            column_declaration->children.push_back(column_declaration->ttl);
        }

        if (!column.settings.empty())
        {
            auto settings = std::make_shared<ASTSetQuery>();
            settings->is_standalone = false;
            settings->changes = column.settings;
            column_declaration->settings = std::move(settings);
        }

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatIndices(const IndicesDescription & indices)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices)
        res->children.push_back(index.definition_ast->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatConstraints(const ConstraintsDescription & constraints)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & constraint : constraints.getConstraints())
        res->children.push_back(constraint->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatProjections(const ProjectionsDescription & projections)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & projection : projections)
        res->children.push_back(projection.definition_ast->clone());

    return res;
}

ColumnsDescription InterpreterCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns_ast, ContextPtr context_, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;
    bool make_columns_nullable = mode <= LoadingStrictnessLevel::SECONDARY_CREATE && !is_restore_from_backup
        && context_->getSettingsRef()[Setting::data_type_default_nullable];
    bool has_columns_with_default_without_type = false;

    for (const auto & ast : columns_ast.children)
    {
        const auto & col_decl = ast->as<ASTColumnDeclaration &>();

        if (col_decl.collation && !context_->getSettingsRef()[Setting::compatibility_ignore_collation_in_create_table])
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot support collation, please set compatibility_ignore_collation_in_create_table=true");
        }

        DataTypePtr column_type = nullptr;
        if (col_decl.type)
        {
            column_type = DataTypeFactory::instance().get(col_decl.type);

            if (LoadingStrictnessLevel::ATTACH <= mode)
                setVersionToAggregateFunctions(column_type, true);

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
            else if (!hasNullable(column_type) &&
                     col_decl.default_specifier == "DEFAULT" &&
                     col_decl.default_expression &&
                     col_decl.default_expression->as<ASTLiteral>() &&
                     col_decl.default_expression->as<ASTLiteral>()->value.isNull())
            {
                if (column_type->lowCardinality())
                {
                    const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column_type.get());
                    assert(low_cardinality_type);
                    column_type = std::make_shared<DataTypeLowCardinality>(makeNullable(low_cardinality_type->getDictionaryType()));
                }
                else
                    column_type = makeNullable(column_type);
            }

            column_names_and_types.emplace_back(col_decl.name, column_type);
        }
        else
        {
            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            column_names_and_types.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());
        }

        /// add column to postprocessing if there is a default_expression specified
        if (col_decl.default_expression)
        {
            /** For columns with explicitly-specified type create two expressions:
              * 1. default_expression aliased as column name with _tmp suffix
              * 2. conversion of expression (1) to explicitly-specified type alias as column name
              */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
                const auto * data_type_ptr = column_names_and_types.back().type.get();

                default_expr_list->children.emplace_back(
                    setAlias(addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                        final_column_name));

                default_expr_list->children.emplace_back(
                    setAlias(col_decl.default_expression->clone(), tmp_column_name));
            }
            else
            {
                has_columns_with_default_without_type = true;
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
            }
        }
    }

    Block defaults_sample_block;
    /// Set missing types and wrap default_expression's in a conversion-function if necessary.
    /// We try to avoid that validation while restoring from a backup because it might be slow or troublesome
    /// (for example, a default expression can contain dictGet() and that dictionary can access remote servers or
    /// require different users to authenticate).
    if (!default_expr_list->children.empty() && (has_columns_with_default_without_type || (mode <= LoadingStrictnessLevel::CREATE)))
    {
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_list, column_names_and_types, context_);
    }

    bool skip_checks = LoadingStrictnessLevel::SECONDARY_CREATE <= mode;
    bool sanity_check_compression_codecs = !skip_checks && !context_->getSettingsRef()[Setting::allow_suspicious_codecs];
    bool allow_experimental_codecs = skip_checks || context_->getSettingsRef()[Setting::allow_experimental_codecs];
    bool enable_deflate_qpl_codec = skip_checks || context_->getSettingsRef()[Setting::enable_deflate_qpl_codec];
    bool enable_zstd_qat_codec = skip_checks || context_->getSettingsRef()[Setting::enable_zstd_qat_codec];

    ColumnsDescription res;
    auto name_type_it = column_names_and_types.begin();
    for (const auto * ast_it = columns_ast.children.begin(); ast_it != columns_ast.children.end(); ++ast_it, ++name_type_it)
    {
        ColumnDescription column;

        auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();

        column.name = col_decl.name;

        /// ignore or not other database extensions depending on compatibility settings
        if (col_decl.default_specifier == "AUTO_INCREMENT"
            && !context_->getSettingsRef()[Setting::compatibility_ignore_auto_increment_in_create_table])
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                            "AUTO_INCREMENT is not supported. To ignore the keyword "
                            "in column declaration, set `compatibility_ignore_auto_increment_in_create_table` to true");
        }

        if (col_decl.default_expression)
        {
            if (context_->hasQueryContext() && context_->getQueryContext().get() == context_.get())
            {
                /// Normalize query only for original CREATE query, not on metadata loading.
                /// And for CREATE query we can pass local context, because result will not change after restart.
                NormalizeAndEvaluateConstantsVisitor::Data visitor_data{context_};
                NormalizeAndEvaluateConstantsVisitor visitor(visitor_data);
                visitor.visit(col_decl.default_expression);
            }

            ASTPtr default_expr = col_decl.default_expression->clone();

            if (col_decl.type)
                column.type = name_type_it->type;
            else
            {
                column.type = defaults_sample_block.getByName(column.name).type;
                /// set nullability for case of column declaration w/o type but with default expression
                if ((col_decl.null_modifier && *col_decl.null_modifier) || make_columns_nullable)
                    column.type = makeNullable(column.type);
            }

            column.default_desc.kind = columnDefaultKindFromString(col_decl.default_specifier);
            column.default_desc.expression = default_expr;
            column.default_desc.ephemeral_default = col_decl.ephemeral_default;
        }
        else if (col_decl.type)
            column.type = name_type_it->type;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Neither default value expression nor type is provided for a column");

        if (col_decl.comment)
            column.comment = col_decl.comment->as<ASTLiteral &>().value.safeGet<String>();

        if (col_decl.codec)
        {
            if (col_decl.default_specifier == "ALIAS")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                col_decl.codec, column.type, sanity_check_compression_codecs, allow_experimental_codecs, enable_deflate_qpl_codec, enable_zstd_qat_codec);
        }

        if (col_decl.statistics_desc)
        {
            if (!skip_checks && !context_->getSettingsRef()[Setting::allow_experimental_statistics])
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY, "Create table with statistics is now disabled. Turn on allow_experimental_statistics");
            column.statistics = ColumnStatisticsDescription::fromColumnDeclaration(col_decl, column.type);
        }

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        if (col_decl.settings)
        {
            column.settings = col_decl.settings->as<ASTSetQuery &>().changes;
            MergeTreeColumnSettings::validate(column.settings);
        }

        res.add(std::move(column));
    }

    if (mode <= LoadingStrictnessLevel::SECONDARY_CREATE && !is_restore_from_backup && context_->getSettingsRef()[Setting::flatten_nested])
        res.flattenNested();


    if (res.getAllPhysical().empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Cannot CREATE table without physical columns");

    return res;
}


ConstraintsDescription InterpreterCreateQuery::getConstraintsDescription(const ASTExpressionList * constraints)
{
    ASTs constraints_data;
    if (constraints)
        for (const auto & constraint : constraints->children)
            constraints_data.push_back(constraint->clone());

    return ConstraintsDescription{constraints_data};
}


InterpreterCreateQuery::TableProperties InterpreterCreateQuery::getTablePropertiesAndNormalizeCreateQuery(
    ASTCreateQuery & create, LoadingStrictnessLevel mode) const
{
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    /// We have to check access rights again (in case engine was changed).
    if (create.storage && create.storage->engine)
        getContext()->checkAccess(AccessType::TABLE_ENGINE, create.storage->engine->name);

    /// If this is a TimeSeries table then we need to normalize list of columns (add missing columns and reorder), and also set inner table engines.
    if (create.is_time_series_table && (mode < LoadingStrictnessLevel::ATTACH))
        StorageTimeSeries::normalizeTableDefinition(create, getContext());

    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (create.columns_list)
    {
        if (create.as_table_function && (create.columns_list->indices || create.columns_list->constraints))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Indexes and constraints are not supported for table functions");

        /// Dictionaries have dictionary_attributes_list instead of columns_list
        assert(!create.is_dictionary);

        if (create.columns_list->columns)
        {
            properties.columns = getColumnsDescription(*create.columns_list->columns, getContext(), mode, is_restore_from_backup);
        }

        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
            {
                IndexDescription index_desc = IndexDescription::getIndexFromAST(index->clone(), properties.columns, getContext());
                if (properties.indices.has(index_desc.name))
                    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Duplicated index name {} is not allowed. Please use different index names.", backQuoteIfNeed(index_desc.name));

                const auto & settings = getContext()->getSettingsRef();
                if (index_desc.type == FULL_TEXT_INDEX_NAME && !settings[Setting::allow_experimental_full_text_index])
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental full-text index feature is disabled. Turn on setting 'allow_experimental_full_text_index'");
                /// ----
                /// Temporary check during a transition period. Please remove at the end of 2024.
                if (index_desc.type == INVERTED_INDEX_NAME && !settings[Setting::allow_experimental_inverted_index])
                    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Please use index type 'full_text' instead of 'inverted'");
                /// ----
                if (index_desc.type == "vector_similarity" && !settings[Setting::allow_experimental_vector_similarity_index])
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental vector similarity index is disabled. Turn on setting 'allow_experimental_vector_similarity_index'");

                properties.indices.push_back(index_desc);
            }

        if (create.columns_list->projections)
            for (const auto & projection_ast : create.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, properties.columns, getContext());
                properties.projections.add(std::move(projection));
            }

        properties.constraints = getConstraintsDescription(create.columns_list->constraints);
    }
    else if (!create.as_table.empty())
    {
        String as_database_name = getContext()->resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, getContext());

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr();
        properties.columns = as_storage_metadata->getColumns();

        if (!create.comment && !as_storage_metadata->comment.empty())
            create.set(create.comment, std::make_shared<ASTLiteral>(as_storage_metadata->comment));

        /// Secondary indices and projections make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
        {
            properties.indices = as_storage_metadata->getSecondaryIndices();
            properties.projections = as_storage_metadata->getProjections().clone();
        }
        else
        {
            /// Only MergeTree support TTL
            properties.columns.resetColumnTTLs();
        }

        properties.constraints = as_storage_metadata->getConstraints();
    }
    else if (create.select)
    {
        if (create.isParameterizedView())
            return properties;

        Block as_select_sample;

        if (getContext()->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            as_select_sample = InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(), getContext());
        }
        else
        {
            as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), getContext());
        }

        properties.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
        properties.columns_inferred_from_select_query = true;
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function_ast = create.as_table_function->ptr();
        auto table_function = TableFunctionFactory::instance().get(table_function_ast, getContext());
        properties.columns = table_function->getActualTableStructure(getContext(), /*is_insert_query*/ true);
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
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    ASTPtr new_columns = formatColumns(properties.columns);
    ASTPtr new_indices = formatIndices(properties.indices);
    ASTPtr new_constraints = formatConstraints(properties.constraints);
    ASTPtr new_projections = formatProjections(properties.projections);

    create.columns_list->setOrReplace(create.columns_list->columns, new_columns);
    create.columns_list->setOrReplace(create.columns_list->indices, new_indices);
    create.columns_list->setOrReplace(create.columns_list->constraints, new_constraints);
    create.columns_list->setOrReplace(create.columns_list->projections, new_projections);

    validateTableStructure(create, properties);

    assert(as_database_saved.empty() && as_table_saved.empty());
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

void validateVirtualColumns(const IStorage & storage)
{
    auto virtual_columns = storage.getVirtualsPtr();
    for (const auto & storage_column : storage.getInMemoryMetadataPtr()->getColumns())
    {
        if (virtual_columns->tryGet(storage_column.name, VirtualsKind::Persistent))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Cannot create table with column '{}' for {} engines because it is reserved for persistent virtual column",
                storage_column.name, storage.getName());
        }
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
        if (StoragePtr to_table = DatabaseCatalog::instance().tryGetTable(
                create.getTargetTableID(ViewTarget::To), getContext()))
        {
            all_output_columns = to_table->getInMemoryMetadataPtr()->getSampleBlock().getNamesAndTypesList();
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
        if (database && database->getEngineName() != "Atomic")
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Refreshable materialized views (except with APPEND) only support Atomic database engine, but database {} has engine {}", create.getDatabase(), database->getEngineName());

        std::string message;
        if (!supportsAtomicRename(&message))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Can't create refreshable materialized view because exchanging files is not supported by the OS ({})", message);
    }

    Block input_block;

    if (check_columns)
    {
        try
        {
            if (getContext()->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                input_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(), getContext());
            }
            else
            {
                input_block = InterpreterSelectWithUnionQuery(create.select->clone(),
                    getContext(),
                    SelectQueryOptions().analyze()).getSampleBlock();
            }
        }
        catch (Exception &)
        {
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
        for (const auto & input_column : input_block)
        {
            auto it = output_types.find(input_column.name);
            if (it != output_types.end())
            {
                input_columns.push_back(input_column.cloneEmpty());
                output_columns.push_back(ColumnWithTypeAndName(it->second->createColumn(), it->second, input_column.name));
            }
            else if (create.refresh_strategy)
            {
                /// Unrecognized columns produced by SELECT query are allowed by regular materialized
                /// views, but not by refreshable ones. This is in part because it was easier to
                /// implement, in part because refreshable views have less concern about ALTERing target
                /// tables.
                ///
                /// The motivating scenario for allowing this in regular MV is ALTERing the table+query.
                /// Suppose the user removes a column from target table, then a minute later
                /// correspondingly updates the view's query to not produce that column.
                /// If MV didn't allow unrecognized columns then during that minute all INSERTs into the
                /// source table would fail - unacceptable.
                /// For refreshable views, during that minute refreshes will fail - acceptable.
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "SELECT query outputs column with name '{}', which is not found in the target table. Use 'AS' to assign alias that matches a column name.", input_column.name);
            }
        }

        if (input_columns.empty())
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "None of the columns produced by the SELECT query are present in the target table. Use 'AS' to assign aliases that match column names.");

        ActionsDAG::makeConvertingActions(
            input_columns,
            output_columns,
            ActionsDAG::MatchColumnsMode::Position
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

        auto engine_ast = std::make_shared<ASTFunction>();
        engine_ast->name = SettingFieldDefaultTableEngine(engine).toString();
        engine_ast->no_empty_args = true;
        storage.set(storage.engine, engine_ast);
    }

    void setNullTableEngine(ASTStorage & storage)
    {
        storage.forEachPointerToChild([](void ** ptr) mutable
        {
            *ptr = nullptr;
        });

        auto engine_ast = std::make_shared<ASTFunction>();
        engine_ast->name = "Null";
        engine_ast->no_empty_args = true;
        storage.set(storage.engine, engine_ast);
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
                auto storage_ast = std::make_shared<ASTStorage>();
                create.set(create.storage, storage_ast);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage should not be created yet, it's a bug.");
            create.as_table_function = nullptr;
            setNullTableEngine(*create.storage);
        }
        return;
    }

    if (create.is_dictionary || create.is_ordinary_view || create.is_live_view || create.is_window_view)
        return;

    if (create.temporary)
    {
        /// Some part of storage definition is specified, but ENGINE is not: just set the one from default_temporary_table_engine setting.

        if (!create.cluster.empty())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Temporary tables cannot be created with ON CLUSTER clause");

        if (!create.storage)
        {
            auto storage_ast = std::make_shared<ASTStorage>();
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

        if (auto to_engine = create.getTargetInnerEngine(ViewTarget::To))
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

    if (create.storage)
    {
        /// This table already has a storage definition.
        if (!create.storage->engine)
        {
            /// Some part of storage definition (such as PARTITION BY) is specified, but ENGINE is not: just set default one.
            setDefaultTableEngine(*create.storage, getContext()->getSettingsRef()[Setting::default_table_engine].value);
        }
        /// For external tables with restore_replace_external_engine_to_null setting we replace external engines to
        /// Null table engine.
        else if (getContext()->getSettingsRef()[Setting::restore_replace_external_engines_to_null])
        {
            if (StorageFactory::instance().getStorageFeatures(create.storage->engine->name).source_access_type != AccessType::NONE)
            {
                setNullTableEngine(*create.storage);
            }
        }
        return;
    }

    /// We'll try to extract a storage definition from clause `AS`:
    ///     CREATE TABLE table_name AS other_table_name
    std::shared_ptr<ASTStorage> storage_def;
    if (!create.as_table.empty())
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

        if (as_create.is_live_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Live View", qualified_name);

        if (as_create.is_window_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Window View", qualified_name);

        if (as_create.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Dictionary", qualified_name);

        if (as_create.is_materialized_view)
        {
            storage_def = as_create.getTargetInnerEngine(ViewTarget::To);
        }
        else if (as_create.as_table_function)
        {
            create.set(create.as_table_function, as_create.as_table_function->ptr());
            return;
        }
        else if (as_create.storage)
        {
            storage_def = typeid_cast<std::shared_ptr<ASTStorage>>(as_create.storage->ptr());
            create.is_time_series_table = as_create.is_time_series_table;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set engine, it's a bug.");
        }
    }

    if (!storage_def)
    {
        /// Set ENGINE by default.
        storage_def = std::make_shared<ASTStorage>();
        setDefaultTableEngine(*storage_def, getContext()->getSettingsRef()[Setting::default_table_engine].value);
    }

    /// Use the found table engine to modify the create query.
    if (create.is_materialized_view)
        create.setTargetInnerEngine(ViewTarget::To, storage_def);
    else
        create.set(create.storage, storage_def);
}

void InterpreterCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";
    bool is_replicated_database_internal = database->getEngineName() == "Replicated" && getContext()->getClientInfo().is_replicated_database_internal;
    bool from_path = create.attach_from_path.has_value();
    bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;

    if (database->getEngineName() == "Replicated" && create.uuid != UUIDHelpers::Nil && !is_replicated_database_internal && !is_on_cluster && !create.attach)
    {
        if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "It's not allowed to explicitly specify UUIDs for tables in Replicated databases, "
                                                       "see database_replicated_allow_explicit_uuid");
        }
        else if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 1)
        {
            LOG_WARNING(&Poco::Logger::get("InterpreterCreateQuery"), "It's not recommended to explicitly specify UUIDs for tables in Replicated databases");
        }
        else if (getContext()->getSettingsRef()[Setting::database_replicated_allow_explicit_uuid] == 2)
        {
            UUID old_uuid = create.uuid;
            create.uuid = UUIDHelpers::Nil;
            create.generateRandomUUIDs();
            LOG_WARNING(&Poco::Logger::get("InterpreterCreateQuery"), "Replaced a user-provided UUID ({}) with a random one ({}) "
                                                                   "to make sure it's unique", old_uuid, create.uuid);
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
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table);
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
    DatabaseCatalog::instance().addDependencies(qualified_name, ref_dependencies, loading_dependencies);
}

void checkTableCanBeAddedWithNoCyclicDependencies(const ASTCreateQuery & create, const ASTPtr & query_ptr, const ContextPtr & context)
{
    QualifiedTableName qualified_name{create.getDatabase(), create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr, context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr);
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(qualified_name, ref_dependencies, loading_dependencies);
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
    /// Temporary tables are created out of databases.
    if (create.temporary && create.database)
        throw Exception(ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE,
                        "Temporary tables cannot be inside a database. "
                        "You should not specify a database for a temporary table.");

    String current_database = getContext()->getCurrentDatabase();
    auto database_name = create.database ? create.getDatabase() : current_database;

    bool is_secondary_query = getContext()->getZooKeeperMetadataTransaction() && !getContext()->getZooKeeperMetadataTransaction()->isInitialQuery();
    auto mode = getLoadingStrictnessLevel(create.attach, /*force_attach*/ false, /*has_force_restore_data_flag*/ false, is_secondary_query || is_restore_from_backup);

    if (!create.sql_security && create.supportSQLSecurity() && !getContext()->getServerSettings().ignore_empty_sql_security_in_create_view_query)
        create.sql_security = std::make_shared<ASTSQLSecurity>();

    if (create.sql_security)
        processSQLSecurityOption(getContext(), create.sql_security->as<ASTSQLSecurity &>(), create.is_materialized_view, /* skip_check_permissions= */ mode >= LoadingStrictnessLevel::SECONDARY_CREATE);

    DDLGuardPtr ddl_guard;

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns_list)
    {
        // In case of an ON CLUSTER query, the database may not be present on the initiator node
        auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (database && database->shouldReplicateQuery(getContext(), query_ptr))
        {
            auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.getTable());
            create.setDatabase(database_name);
            guard->releaseTableLock();
            return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), QueryFlags{ .internal = internal, .distributed_backup_restore = is_restore_from_backup });
        }

        if (!create.cluster.empty())
            return executeQueryOnCluster(create);

        if (!database)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(database_name));

        /// For short syntax of ATTACH query we have to lock table name here, before reading metadata
        /// and hold it until table is attached
        if (likely(need_ddl_guard))
            ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.getTable());

        bool if_not_exists = create.if_not_exists;

        // Table SQL definition is available even if the table is detached (even permanently)
        auto query = database->getCreateTableQuery(create.getTable(), getContext());
        FunctionNameNormalizer::visit(query.get());
        auto create_query = query->as<ASTCreateQuery &>();

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

    if (create.attach_from_path)
    {
        chassert(!ddl_guard);
        fs::path user_files = fs::path(getContext()->getUserFilesPath()).lexically_normal();
        fs::path root_path = fs::path(getContext()->getPath()).lexically_normal();

        if (getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            fs::path data_path = fs::path(*create.attach_from_path).lexically_normal();
            if (data_path.is_relative())
                data_path = (user_files / data_path).lexically_normal();
            if (!startsWith(data_path, user_files))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));

            /// Data path must be relative to root_path
            create.attach_from_path = fs::relative(data_path, root_path) / "";
        }
        else
        {
            fs::path data_path = (root_path / *create.attach_from_path).lexically_normal();
            if (!startsWith(data_path, user_files))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));
        }
    }
    else if (create.attach && !create.attach_short_syntax && getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        auto log = getLogger("InterpreterCreateQuery");
        LOG_WARNING(log, "ATTACH TABLE query with full table definition is not recommended: "
                         "use either ATTACH TABLE {}; to attach existing table "
                         "or CREATE TABLE {} <table definition>; to create new table "
                         "or ATTACH TABLE {} FROM '/path/to/data/' <table definition>; to create new table and attach data.",
                         create.getTable(), create.getTable(), create.getTable());
    }

    if (!create.temporary && !create.database)
        create.setDatabase(current_database);

    if (create.targets)
        create.targets->setCurrentDatabase(current_database);

    if (create.select && create.isView())
    {
        // Expand CTE before filling default database
        ApplyWithSubqueryVisitor::visit(*create.select);
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.select);
    }

    if (create.refresh_strategy)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_refreshable_materialized_view])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Refreshable materialized views are experimental. Enable allow_experimental_refreshable_materialized_view to use");

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
        UserDefinedSQLFunctionVisitor::visit(query_ptr);

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = getTablePropertiesAndNormalizeCreateQuery(create, mode);

    DatabasePtr database;
    bool need_add_to_database = !create.temporary;
    // In case of an ON CLUSTER query, the database may not be present on the initiator node
    if (need_add_to_database)
        database = DatabaseCatalog::instance().tryGetDatabase(database_name);

    /// Check type compatible for materialized dest table and select columns
    if (create.select && create.is_materialized_view && mode <= LoadingStrictnessLevel::CREATE)
        validateMaterializedViewColumnsAndEngine(create, properties, database);

    bool allow_heavy_populate = getContext()->getSettingsRef()[Setting::database_replicated_allow_heavy_create] && create.is_populate;
    if (!allow_heavy_populate && database && database->getEngineName() == "Replicated" && (create.select || create.is_populate))
    {
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

        const bool allow_create_select_for_replicated = (create.isView() && !create.is_populate) || create.is_create_empty || !is_storage_replicated;
        if (!allow_create_select_for_replicated)
        {
            /// POPULATE can be enabled with setting, provide hint in error message
            if (create.is_populate)
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "CREATE with POPULATE is not supported with Replicated databases. Consider using separate CREATE and INSERT queries. "
                    "Alternatively, you can enable 'database_replicated_allow_heavy_create' setting to allow this operation, use with caution");

            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "CREATE AS SELECT is not supported with Replicated databases. Consider using separate CREATE and INSERT queries.");
        }
    }

    if (database && database->shouldReplicateQuery(getContext(), query_ptr))
    {
        chassert(!ddl_guard);
        auto guard = DatabaseCatalog::instance().getDDLGuard(create.getDatabase(), create.getTable());
        assertOrSetUUID(create, database);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), QueryFlags{ .internal = internal, .distributed_backup_restore = is_restore_from_backup });
    }

    if (!create.cluster.empty())
    {
        chassert(!ddl_guard);
        return executeQueryOnCluster(create);
    }

    if (need_add_to_database && !database)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(database_name));

    if (create.replace_table)
    {
        chassert(!ddl_guard);
        return doCreateOrReplaceTable(create, properties, mode);
    }

    /// Actually creates table
    bool created = doCreateTable(create, properties, ddl_guard, mode);
    ddl_guard.reset();

    if (!created)   /// Table already exists
        return {};

    /// If table has dependencies - add them to the graph
    addTableDependencies(create, query_ptr, getContext());
    return fillTableIfNeeded(create);
}

bool InterpreterCreateQuery::doCreateTable(ASTCreateQuery & create,
                                           const InterpreterCreateQuery::TableProperties & properties,
                                           DDLGuardPtr & ddl_guard, LoadingStrictnessLevel mode)
{
    if (create.temporary)
    {
        if (create.if_not_exists && getContext()->tryResolveStorageID({"", create.getTable()}, Context::ResolveExternal))
            return false;

        DatabasePtr database = DatabaseCatalog::instance().getDatabase(DatabaseCatalog::TEMPORARY_DATABASE);

        String temporary_table_name = create.getTable();
        auto creator = [&](const StorageID & table_id)
        {
            return StorageFactory::instance().get(create,
                database->getTableDataPath(table_id.getTableName()),
                getContext(),
                getContext()->getGlobalContext(),
                properties.columns,
                properties.constraints,
                mode);
        };
        auto temporary_table = TemporaryTableHolder(getContext(), creator, query_ptr);

        getContext()->getSessionContext()->addExternalTable(temporary_table_name, std::move(temporary_table));
        return true;
    }

    if (!ddl_guard && likely(need_ddl_guard))
        ddl_guard = DatabaseCatalog::instance().getDDLGuard(create.getDatabase(), create.getTable());

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
        else if (create.replace_view)
        {
            /// when executing CREATE OR REPLACE VIEW, drop current existing view
            auto drop_ast = std::make_shared<ASTDropQuery>();
            drop_ast->setDatabase(create.getDatabase());
            drop_ast->setTable(create.getTable());
            drop_ast->no_ddl_lock = true;

            auto drop_context = Context::createCopy(context);
            InterpreterDropQuery interpreter(drop_ast, drop_context);
            interpreter.execute();
        }
        else
        {
            if (database->getTable(create.getTable(), getContext())->isDictionary())
                throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS,
                                "Dictionary {}.{} already exists", backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable()));
            else
                throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                                "Table {}.{} already exists", backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable()));
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
    }

    data_path = database->getTableDataPath(create);
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

    bool from_path = create.attach_from_path.has_value();
    String actual_data_path = data_path;
    if (from_path)
    {
        if (data_path.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ATTACH ... FROM ... query is not supported for {} database engine", database->getEngineName());
        /// We will try to create Storage instance with provided data path
        data_path = *create.attach_from_path;
        create.attach_from_path = std::nullopt;
    }

    if (create.attach)
    {
        /// If table was detached it's not possible to attach it back while some threads are using
        /// old instance of the storage. For example, AsynchronousMetrics may cause ATTACH to fail,
        /// so we allow waiting here. If database_atomic_wait_for_drop_and_detach_synchronously is disabled
        /// and old storage instance still exists it will throw exception.
        if (getContext()->getSettingsRef()[Setting::database_atomic_wait_for_drop_and_detach_synchronously])
            database->waitDetachedTableNotInUse(create.uuid);
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

    StoragePtr res;
    /// NOTE: CREATE query may be rewritten by Storage creator or table function
    if (create.as_table_function)
    {
        auto table_function_ast = create.as_table_function->ptr();
        auto table_function = TableFunctionFactory::instance().get(table_function_ast, getContext());
        /// In case of CREATE AS table_function() query we should use global context
        /// in storage creation because there will be no query context on server startup
        /// and because storage lifetime is bigger than query context lifetime.
        res = table_function->execute(table_function_ast, getContext(), create.getTable(), properties.columns, /*use_global_context=*/true);
        res->renameInMemory({create.getDatabase(), create.getTable(), create.uuid});
    }
    else
    {
        res = StorageFactory::instance().get(create,
            data_path,
            getContext(),
            getContext()->getGlobalContext(),
            properties.columns,
            properties.constraints,
            mode);

        /// If schema wes inferred while storage creation, add columns description to create query.
        addColumnsDescriptionToCreateQueryIfNecessary(query_ptr->as<ASTCreateQuery &>(), res);
    }

    validateVirtualColumns(*res);

    if (!res->supportsDynamicSubcolumnsDeprecated() && hasDynamicSubcolumns(res->getInMemoryMetadataPtr()->getColumns()) && mode <= LoadingStrictnessLevel::CREATE)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Cannot create table with column of type Object, "
            "because storage {} doesn't support dynamic subcolumns",
            res->getName());
    }

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
        std::bernoulli_distribution fault(probability);
        if (fault(thread_local_rng))
        {
            /// We emulate the case when the exception was thrown in StorageReplicatedMergeTree constructor
            if (!create.attach)
                replicated_storage->dropIfEmpty();

            throw Coordination::Exception(Coordination::Error::ZCONNECTIONLOSS, "Fault injected (during table creation)");
        }
    }

    UInt64 table_num_limit = getContext()->getGlobalContext()->getServerSettings().max_table_num_to_throw;
    if (table_num_limit > 0 && !internal)
    {
        UInt64 table_count = CurrentMetrics::get(CurrentMetrics::AttachedTable);
        if (table_count >= table_num_limit)
            throw Exception(ErrorCodes::TOO_MANY_TABLES,
                            "Too many tables. "
                            "The limit (server configuration parameter `max_table_num_to_throw`) is set to {}, the current number of tables is {}",
                            table_num_limit, table_count);
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


BlockIO InterpreterCreateQuery::doCreateOrReplaceTable(ASTCreateQuery & create,
                                                       const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode)
{
    /// Replicated database requires separate contexts for each DDL query
    ContextPtr current_context = getContext();
    if (auto txn = current_context->getZooKeeperMetadataTransaction())
        txn->setIsCreateOrReplaceQuery();
    ContextMutablePtr create_context = Context::createCopy(current_context);
    create_context->setQueryContext(std::const_pointer_cast<Context>(current_context));

    /// Before actually creating/replacing the table, check if it will lead to cyclic dependencies.
    checkTableCanBeAddedWithNoCyclicDependencies(create, query_ptr, create_context);

    auto make_drop_context = [&]() -> ContextMutablePtr
    {
        ContextMutablePtr drop_context = Context::createCopy(current_context);
        drop_context->setQueryContext(std::const_pointer_cast<Context>(current_context));
        return drop_context;
    };

    auto ast_drop = std::make_shared<ASTDropQuery>();
    String table_to_replace_name = create.getTable();

    {
        auto database = DatabaseCatalog::instance().getDatabase(create.getDatabase());
        if (database->getUUID() == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} query is supported only for Atomic databases",
                            create.create_or_replace ? "CREATE OR REPLACE TABLE" : "REPLACE TABLE");


        UInt64 name_hash = sipHash64(create.getDatabase() + create.getTable());
        UInt16 random_suffix = thread_local_rng();
        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            /// Avoid different table name on database replicas
            random_suffix = sipHash64(txn->getTaskZooKeeperPath());
        }
        create.setTable(fmt::format("_tmp_replace_{}_{}",
                            getHexUIntLowercase(name_hash),
                            getHexUIntLowercase(random_suffix)));

        ast_drop->setTable(create.getTable());
        ast_drop->is_dictionary = create.is_dictionary;
        ast_drop->setDatabase(create.getDatabase());
        ast_drop->kind = ASTDropQuery::Drop;
    }

    bool created = false;
    bool renamed = false;
    try
    {
        /// Create temporary table (random name will be generated)
        DDLGuardPtr ddl_guard;
        [[maybe_unused]] bool done = InterpreterCreateQuery(query_ptr, create_context).doCreateTable(create, properties, ddl_guard, mode);
        ddl_guard.reset();
        assert(done);
        created = true;

        /// If table has dependencies - add them to the graph
        addTableDependencies(create, query_ptr, getContext());

        /// Try fill temporary table
        BlockIO fill_io = fillTableIfNeeded(create);
        executeTrivialBlockIO(fill_io, getContext());

        /// Replace target table with created one
        ASTRenameQuery::Element elem
        {
            ASTRenameQuery::Table
            {
                create.getDatabase().empty() ? nullptr : std::make_shared<ASTIdentifier>(create.getDatabase()),
                std::make_shared<ASTIdentifier>(create.getTable())
            },
            ASTRenameQuery::Table
            {
                create.getDatabase().empty() ? nullptr : std::make_shared<ASTIdentifier>(create.getDatabase()),
                std::make_shared<ASTIdentifier>(table_to_replace_name)
            }
        };

        auto ast_rename = std::make_shared<ASTRenameQuery>(ASTRenameQuery::Elements{std::move(elem)});
        ast_rename->dictionary = create.is_dictionary;
        if (create.create_or_replace)
        {
            /// CREATE OR REPLACE TABLE
            /// Will execute ordinary RENAME instead of EXCHANGE if the target table does not exist
            ast_rename->rename_if_cannot_exchange = true;
            ast_rename->exchange = false;
        }
        else
        {
            /// REPLACE TABLE
            /// Will execute EXCHANGE query and fail if the target table does not exist
            ast_rename->exchange = true;
        }

        InterpreterRenameQuery interpreter_rename{ast_rename, current_context};
        interpreter_rename.execute();
        renamed = true;

        if (!interpreter_rename.renamedInsteadOfExchange())
        {
            /// Target table was replaced with new one, drop old table
            auto drop_context = make_drop_context();
            InterpreterDropQuery(ast_drop, drop_context).execute();
        }

        create.setTable(table_to_replace_name);

        return {};
    }
    catch (...)
    {
        /// Drop temporary table if it was successfully created, but was not renamed to target name
        if (created && !renamed)
        {
            auto drop_context = make_drop_context();
            InterpreterDropQuery(ast_drop, drop_context).execute();
        }
        throw;
    }
}

BlockIO InterpreterCreateQuery::fillTableIfNeeded(const ASTCreateQuery & create)
{
    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.select && !create.attach && !create.is_create_empty
        && !create.is_ordinary_view && !create.is_live_view
        && (!(create.is_materialized_view || create.is_window_view) || create.is_populate))
    {
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = {create.getDatabase(), create.getTable(), create.uuid};
        if (create.is_window_view)
        {
            auto table = DatabaseCatalog::instance().getTable(insert->table_id, getContext());
            insert->select = typeid_cast<StorageWindowView *>(table.get())->getSourceTableSelectQuery();
        }
        else
            insert->select = create.select->clone();

        return InterpreterInsertQuery(
                   insert,
                   getContext(),
                   getContext()->getSettingsRef()[Setting::insert_allow_materialized_columns],
                   /* no_squash */ false,
                   /* no_destination */ false,
                   /* async_isnert */ false)
            .execute();
    }

    return {};
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
        auto on_cluster_version = local_context->getSettingsRef()[Setting::distributed_ddl_entry_format_version];
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
        auto on_cluster_version = getContext()->getSettingsRef()[Setting::distributed_ddl_entry_format_version];
        if (is_create_database || on_cluster_version < DDLLogEntry::NORMALIZE_CREATE_ON_INITIATOR_VERSION)
            return executeQueryOnCluster(create);
    }

    getContext()->checkAccess(getRequiredAccess());

    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (is_create_database)
        return createDatabase(create);
    else
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
        assert(!create.temporary);
        if (create.replace_view)
            required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, create.getDatabase(), create.getTable());
        else
            required_access.emplace_back(AccessType::CREATE_VIEW, create.getDatabase(), create.getTable());
    }
    else
    {
        if (create.temporary)
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

    auto ast_storage = std::make_shared<ASTStorage>();
    unsigned max_parser_depth_v = static_cast<unsigned>(getContext()->getSettingsRef()[Setting::max_parser_depth]);
    unsigned max_parser_backtracks_v = static_cast<unsigned>(getContext()->getSettingsRef()[Setting::max_parser_backtracks]);
    auto query_from_storage = DB::getCreateQueryFromStorage(storage, ast_storage, false, max_parser_depth_v, max_parser_backtracks_v, true);
    auto & create_query_from_storage = query_from_storage->as<ASTCreateQuery &>();

    if (!create.columns_list)
    {
        ASTPtr columns_list = std::make_shared<ASTColumns>(*create_query_from_storage.columns_list);
        create.set(create.columns_list, columns_list);
    }
    else
    {
        ASTPtr columns = std::make_shared<ASTExpressionList>(*create_query_from_storage.columns_list->columns);
        create.columns_list->set(create.columns_list->columns, columns);
    }
}

void InterpreterCreateQuery::processSQLSecurityOption(ContextPtr context_, ASTSQLSecurity & sql_security, bool is_materialized_view, bool skip_check_permissions)
{
    /// If no SQL security is specified, apply default from default_*_view_sql_security setting.
    if (!sql_security.type)
    {
        SQLSecurityType default_security;

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
                sql_security.definer = std::make_shared<ASTUserNameWithHost>(default_definer);
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
            sql_security.definer = std::make_shared<ASTUserNameWithHost>(current_user_name);
    }

    /// Checks the permissions for the specified definer user.
    if (sql_security.definer && !sql_security.is_definer_current_user && !skip_check_permissions)
    {
        const auto definer_name = sql_security.definer->toString();

        /// Validate that the user exists.
        context_->getAccessControl().getID<User>(definer_name);
        if (definer_name != current_user_name)
            context_->checkAccess(AccessType::SET_DEFINER, definer_name);
    }

    if (sql_security.type == SQLSecurityType::NONE && !skip_check_permissions)
        context_->checkAccess(AccessType::ALLOW_SQL_SECURITY_NONE);
}

void registerInterpreterCreateQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateQuery", create_fn);
}

}
