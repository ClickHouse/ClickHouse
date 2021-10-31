#include <memory>

#include <filesystem>

#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include <Common/randomSeed.h>
#include <Common/renameat2.h>
#include <Common/hex.h>

#include <Core/Defines.h>
#include <Core/Settings.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Access/Common/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/TablesLoader.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <base/logger_useful.h>


namespace DB
{

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
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int PATH_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
}

namespace fs = std::filesystem;

InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    String database_name = create.database;

    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    /// Database can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard
    if (DatabaseCatalog::instance().isDatabaseExist(database_name))
    {
        if (create.if_not_exists)
            return {};
        else
            throw Exception("Database " + database_name + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
    }

    /// Will write file with database metadata, if needed.
    String database_name_escaped = escapeForFileName(database_name);
    fs::path metadata_path = fs::canonical(getContext()->getPath());
    fs::path metadata_file_tmp_path = metadata_path / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = metadata_path / "metadata" / (database_name_escaped + ".sql");

    if (!create.storage && create.attach)
    {
        if (!fs::exists(metadata_file_path))
            throw Exception("Database engine must be specified for ATTACH DATABASE query", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        /// Short syntax: try read database definition from file
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, getContext(), metadata_file_path);
        create = ast->as<ASTCreateQuery &>();
        if (!create.table.empty() || !create.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Metadata file {} contains incorrect CREATE DATABASE query", metadata_file_path.string());
        create.attach = true;
        create.attach_short_syntax = true;
        create.database = database_name;
    }
    else if (!create.storage)
    {
        /// For new-style databases engine is explicitly specified in .sql
        /// When attaching old-style database during server startup, we must always use Ordinary engine
        if (create.attach)
            throw Exception("Database engine must be specified for ATTACH DATABASE query", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        bool old_style_database = getContext()->getSettingsRef().default_database_engine.value == DefaultDatabaseEngine::Ordinary;
        auto engine = std::make_shared<ASTFunction>();
        auto storage = std::make_shared<ASTStorage>();
        engine->name = old_style_database ? "Ordinary" : "Atomic";
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

    if (create.storage->engine->name == "Atomic"
        || create.storage->engine->name == "Replicated"
        || create.storage->engine->name == "MaterializedPostgreSQL")
    {
        if (create.attach && create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "UUID must be specified for ATTACH. "
                            "If you want to attach existing database, use just ATTACH DATABASE {};", create.database);
        else if (create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();

        metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);

        if (!create.attach && fs::exists(metadata_path))
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists", metadata_path.string());
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
            const auto & default_engine = getContext()->getSettingsRef().default_database_engine.value;
            if (create.uuid == UUIDHelpers::Nil && default_engine == DefaultDatabaseEngine::Atomic)
                create.uuid = UUIDHelpers::generateV4();    /// Will enable Atomic engine for nested database
        }
        else if (attach_from_user && create.uuid == UUIDHelpers::Nil)
        {
            /// Ambiguity is possible: should we attach nested database as Ordinary
            /// or throw "UUID must be specified" for Atomic? So we suggest short syntax for Ordinary.
            throw Exception("Use short attach syntax ('ATTACH DATABASE name;' without engine) to attach existing database "
                            "or specify UUID to attach new database with Atomic engine", ErrorCodes::INCORRECT_QUERY);
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
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster)
            throw Exception("Ordinary database engine does not support UUID", ErrorCodes::INCORRECT_QUERY);

        /// Ignore UUID if it's ON CLUSTER query
        create.uuid = UUIDHelpers::Nil;
        metadata_path = metadata_path / "metadata" / database_name_escaped;
    }

    if ((create.storage->engine->name == "MaterializeMySQL" || create.storage->engine->name == "MaterializedMySQL")
        && !getContext()->getSettingsRef().allow_experimental_database_materialized_mysql
        && !internal)
    {
        throw Exception("MaterializedMySQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialized_mysql to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    if (create.storage->engine->name == "Replicated"
        && !getContext()->getSettingsRef().allow_experimental_database_replicated
        && !internal)
    {
        throw Exception("Replicated is an experimental database engine. "
                        "Enable allow_experimental_database_replicated to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    if (create.storage->engine->name == "MaterializedPostgreSQL"
        && !getContext()->getSettingsRef().allow_experimental_database_materialized_postgresql
        && !internal)
    {
        throw Exception("MaterializedPostgreSQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialized_postgresql to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    DatabasePtr database = DatabaseFactory::get(create, metadata_path / "", getContext());

    if (create.uuid != UUIDHelpers::Nil)
        create.database = TABLE_WITH_UUID_NAME_PLACEHOLDER;

    bool need_write_metadata = !create.attach || !fs::exists(metadata_file_path);

    if (need_write_metadata)
    {
        create.attach = true;
        create.if_not_exists = false;

        WriteBufferFromOwnString statement_buf;
        formatAST(create, statement_buf, false);
        writeChar('\n', statement_buf);
        String statement = statement_buf.str();

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (getContext()->getSettingsRef().fsync_metadata)
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

        if (need_write_metadata)
        {
            /// Prevents from overwriting metadata of detached database
            renameNoReplace(metadata_file_tmp_path, metadata_file_path);
            renamed = true;
        }

        if (!load_database_without_tables)
        {
            /// We use global context here, because storages lifetime is bigger than query context lifetime
            TablesLoader loader{getContext()->getGlobalContext(), {{database_name, database}}, has_force_restore_data_flag, create.attach && force_attach}; //-V560
            loader.loadTables();
            loader.startupTables();
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
        column_declaration->type = parseQuery(type_parser, pos, end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
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
        column_declaration->type = parseQuery(type_parser, type_pos, type_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        column_declaration->default_specifier = "ALIAS";

        const auto & alias = alias_column.expression;
        const char * alias_pos = alias.data();
        const char * alias_end = alias_pos + alias.size();
        ParserExpression expression_parser;
        column_declaration->default_expression = parseQuery(expression_parser, alias_pos, alias_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

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
        column_declaration->type = parseQuery(type_parser, type_name_pos, type_name_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (column.default_desc.expression)
        {
            column_declaration->default_specifier = toString(column.default_desc.kind);
            column_declaration->default_expression = column.default_desc.expression->clone();
        }

        if (!column.comment.empty())
        {
            column_declaration->comment = std::make_shared<ASTLiteral>(Field(column.comment));
        }

        if (column.codec)
            column_declaration->codec = column.codec;

        if (column.ttl)
            column_declaration->ttl = column.ttl;

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

    for (const auto & constraint : constraints.constraints)
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
    const ASTExpressionList & columns_ast, ContextPtr context_, bool attach)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;
    bool make_columns_nullable = !attach && context_->getSettingsRef().data_type_default_nullable;

    for (const auto & ast : columns_ast.children)
    {
        const auto & col_decl = ast->as<ASTColumnDeclaration &>();

        DataTypePtr column_type = nullptr;

        if (col_decl.type)
        {
            column_type = DataTypeFactory::instance().get(col_decl.type);

            if (col_decl.null_modifier)
            {
                if (column_type->isNullable())
                    throw Exception("Can't use [NOT] NULL modifier with Nullable type", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
                if (*col_decl.null_modifier)
                    column_type = makeNullable(column_type);
            }
            else if (make_columns_nullable)
            {
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
                    setAlias(
                        col_decl.default_expression->clone(),
                        tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }
    }

    Block defaults_sample_block;
    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!default_expr_list->children.empty())
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_list, column_names_and_types, context_);

    bool sanity_check_compression_codecs = !attach && !context_->getSettingsRef().allow_suspicious_codecs;
    bool allow_experimental_codecs = attach || context_->getSettingsRef().allow_experimental_codecs;

    ColumnsDescription res;
    auto name_type_it = column_names_and_types.begin();
    for (auto ast_it = columns_ast.children.begin(); ast_it != columns_ast.children.end(); ++ast_it, ++name_type_it)
    {
        ColumnDescription column;

        auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();

        column.name = col_decl.name;

        if (col_decl.default_expression)
        {
            ASTPtr default_expr = col_decl.default_expression->clone();
            if (col_decl.type)
                column.type = name_type_it->type;
            else
                column.type = defaults_sample_block.getByName(column.name).type;

            column.default_desc.kind = columnDefaultKindFromString(col_decl.default_specifier);
            column.default_desc.expression = default_expr;
        }
        else if (col_decl.type)
            column.type = name_type_it->type;
        else
            throw Exception();

        if (col_decl.comment)
            column.comment = col_decl.comment->as<ASTLiteral &>().value.get<String>();

        if (col_decl.codec)
        {
            if (col_decl.default_specifier == "ALIAS")
                throw Exception{"Cannot specify codec for column type ALIAS", ErrorCodes::BAD_ARGUMENTS};
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                col_decl.codec, column.type, sanity_check_compression_codecs, allow_experimental_codecs);
        }

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        res.add(std::move(column));
    }

    if (context_->getSettingsRef().flatten_nested)
        res.flattenNested();

    if (res.getAllPhysical().empty())
        throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

    return res;
}


ConstraintsDescription InterpreterCreateQuery::getConstraintsDescription(const ASTExpressionList * constraints)
{
    ConstraintsDescription res;
    if (constraints)
        for (const auto & constraint : constraints->children)
            res.constraints.push_back(std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint->clone()));
    return res;
}


InterpreterCreateQuery::TableProperties InterpreterCreateQuery::getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create) const
{
    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (create.columns_list)
    {
        if (create.as_table_function && (create.columns_list->indices || create.columns_list->constraints))
            throw Exception("Indexes and constraints are not supported for table functions", ErrorCodes::INCORRECT_QUERY);

        if (create.columns_list->columns)
        {
            properties.columns = getColumnsDescription(*create.columns_list->columns, getContext(), create.attach);
        }

        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
                properties.indices.push_back(
                    IndexDescription::getIndexFromAST(index->clone(), properties.columns, getContext()));

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
        as_storage_lock = as_storage->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr();
        properties.columns = as_storage_metadata->getColumns();

        /// Secondary indices and projections make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
        {
            properties.indices = as_storage_metadata->getSecondaryIndices();
            properties.projections = as_storage_metadata->getProjections().clone();
        }

        properties.constraints = as_storage_metadata->getConstraints();
    }
    else if (create.select)
    {
        Block as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), getContext());
        properties.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function = TableFunctionFactory::instance().get(create.as_table_function, getContext());
        properties.columns = table_function->getActualTableStructure(getContext());
        assert(!properties.columns.empty());
    }
    else if (create.is_dictionary)
    {
        return {};
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

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
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    assert(as_database_saved.empty() && as_table_saved.empty());
    std::swap(create.as_database, as_database_saved);
    std::swap(create.as_table, as_table_saved);

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
            throw Exception("Column " + backQuoteIfNeed(column.name) + " already exists", ErrorCodes::DUPLICATE_COLUMN);
    }

    const auto & settings = getContext()->getSettingsRef();

    /// Check low cardinality types in creating table if it was not allowed in setting
    if (!create.attach && !settings.allow_suspicious_low_cardinality_types && !create.is_materialized_view)
    {
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
        {
            if (const auto * current_type_ptr = typeid_cast<const DataTypeLowCardinality *>(name_and_type_pair.type.get()))
            {
                if (!isStringOrFixedString(*removeNullable(current_type_ptr->getDictionaryType())))
                    throw Exception("Creating columns of type " + current_type_ptr->getName() + " is prohibited by default "
                                    "due to expected negative impact on performance. "
                                    "It can be enabled with the \"allow_suspicious_low_cardinality_types\" setting.",
                                    ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY);
            }
        }
    }

    if (!create.attach && !settings.allow_experimental_geo_types)
    {
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
        {
            const auto & type = name_and_type_pair.type->getName();
            if (type == "MultiPolygon" || type == "Polygon" || type == "Ring" || type == "Point")
            {
                String message = "Cannot create table with column '" + name_and_type_pair.name + "' which type is '"
                                 + type + "' because experimental geo types are not allowed. "
                                 + "Set setting allow_experimental_geo_types = 1 in order to allow it.";
                throw Exception(message, ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }
}

void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.as_table_function)
        return;

    if (create.storage || create.is_dictionary || create.isView())
    {
        if (create.temporary && create.storage && create.storage->engine && create.storage->engine->name != "Memory")
            throw Exception(
                "Temporary tables can only be created with ENGINE = Memory, not " + create.storage->engine->name,
                ErrorCodes::INCORRECT_QUERY);

        return;
    }

    if (create.temporary)
    {
        auto engine_ast = std::make_shared<ASTFunction>();
        engine_ast->name = "Memory";
        engine_ast->no_empty_args = true;
        auto storage_ast = std::make_shared<ASTStorage>();
        storage_ast->set(storage_ast->engine, engine_ast);
        create.set(create.storage, storage_ast);
    }
    else if (!create.as_table.empty())
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = getContext()->resolveDatabase(create.as_database);
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name)->getCreateTableQuery(as_table_name, getContext());
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        const String qualified_name = backQuoteIfNeed(as_database_name) + "." + backQuoteIfNeed(as_table_name);

        if (as_create.is_ordinary_view)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a View",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.is_live_view)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a Live View",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.is_dictionary)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a Dictionary",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.storage)
            create.set(create.storage, as_create.storage->ptr());
        else if (as_create.as_table_function)
            create.as_table_function = as_create.as_table_function->clone();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set engine, it's a bug.");
    }
}

static void generateUUIDForTable(ASTCreateQuery & create)
{
    if (create.uuid == UUIDHelpers::Nil)
        create.uuid = UUIDHelpers::generateV4();

    /// If destination table (to_table_id) is not specified for materialized view,
    /// then MV will create inner table. We should generate UUID of inner table here,
    /// so it will be the same on all hosts if query in ON CLUSTER or database engine is Replicated.
    bool need_uuid_for_inner_table = !create.attach && create.is_materialized_view && !create.to_table_id;
    if (need_uuid_for_inner_table && create.to_inner_uuid == UUIDHelpers::Nil)
        create.to_inner_uuid = UUIDHelpers::generateV4();
}

void InterpreterCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";

    if (database->getEngineName() == "Replicated" && getContext()->getClientInfo().is_replicated_database_internal
        && !internal)
    {
        if (create.uuid == UUIDHelpers::Nil)
            throw Exception("Table UUID is not specified in DDL log", ErrorCodes::LOGICAL_ERROR);
    }

    bool from_path = create.attach_from_path.has_value();

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

        generateUUIDForTable(create);
    }
    else
    {
        bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        bool has_uuid = create.uuid != UUIDHelpers::Nil || create.to_inner_uuid != UUIDHelpers::Nil;
        if (has_uuid && !is_on_cluster)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} UUID specified, but engine of database {} is not Atomic", kind, create.database);

        /// Ignore UUID if it's ON CLUSTER query
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;
    }
}


BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    /// Temporary tables are created out of databases.
    if (create.temporary && !create.database.empty())
        throw Exception("Temporary tables cannot be inside a database. You should not specify a database for a temporary table.",
            ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE);

    String current_database = getContext()->getCurrentDatabase();
    auto database_name = create.database.empty() ? current_database : create.database;

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns_list)
    {
        auto database = DatabaseCatalog::instance().getDatabase(database_name);

        if (database->getEngineName() == "Replicated")
        {
            auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.table);

            if (auto* ptr = typeid_cast<DatabaseReplicated *>(database.get());
                ptr && !getContext()->getClientInfo().is_replicated_database_internal)
            {
                create.database = database_name;
                guard->releaseTableLock();
                return ptr->tryEnqueueReplicatedDDL(query_ptr, getContext());
            }
        }

        bool if_not_exists = create.if_not_exists;

        // Table SQL definition is available even if the table is detached (even permanently)
        auto query = database->getCreateTableQuery(create.table, getContext());
        auto create_query = query->as<ASTCreateQuery &>();

        if (!create.is_dictionary && create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH TABLE {}.{}, it is a Dictionary",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.table));

        if (create.is_dictionary && !create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH DICTIONARY {}.{}, it is a Table",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.table));

        create = create_query; // Copy the saved create query, but use ATTACH instead of CREATE

        create.attach = true;
        create.attach_short_syntax = true;
        create.if_not_exists = if_not_exists;
    }

    /// TODO throw exception if !create.attach_short_syntax && !create.attach_from_path && !internal

    if (create.attach_from_path)
    {
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
        auto * log = &Poco::Logger::get("InterpreterCreateQuery");
        LOG_WARNING(log, "ATTACH TABLE query with full table definition is not recommended: "
                         "use either ATTACH TABLE {}; to attach existing table "
                         "or CREATE TABLE {} <table definition>; to create new table "
                         "or ATTACH TABLE {} FROM '/path/to/data/' <table definition>; to create new table and attach data.",
                         create.table, create.table, create.table);
    }

    if (!create.temporary && create.database.empty())
        create.database = current_database;
    if (create.to_table_id && create.to_table_id.database_name.empty())
        create.to_table_id.database_name = current_database;

    if (create.select && create.isView())
    {
        // Expand CTE before filling default database
        ApplyWithSubqueryVisitor().visit(*create.select);
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.select);
    }

    if (create.columns_list)
    {
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.columns_list);
    }

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = getTablePropertiesAndNormalizeCreateQuery(create);

    DatabasePtr database;
    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
        database = DatabaseCatalog::instance().getDatabase(database_name);

    if (need_add_to_database && database->getEngineName() == "Replicated")
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(create.database, create.table);

        if (auto * ptr = typeid_cast<DatabaseReplicated *>(database.get());
            ptr && !getContext()->getClientInfo().is_replicated_database_internal)
        {
            assertOrSetUUID(create, database);
            guard->releaseTableLock();
            return ptr->tryEnqueueReplicatedDDL(query_ptr, getContext());
        }
    }

    if (create.replace_table)
        return doCreateOrReplaceTable(create, properties);

    /// Actually creates table
    bool created = doCreateTable(create, properties);

    if (!created)   /// Table already exists
        return {};

    return fillTableIfNeeded(create);
}

bool InterpreterCreateQuery::doCreateTable(ASTCreateQuery & create,
                                           const InterpreterCreateQuery::TableProperties & properties)
{
    std::unique_ptr<DDLGuard> guard;

    String data_path;
    DatabasePtr database;

    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
    {
        /** If the request specifies IF NOT EXISTS, we allow concurrent CREATE queries (which do nothing).
          * If table doesn't exist, one thread is creating table, while others wait in DDLGuard.
          */
        guard = DatabaseCatalog::instance().getDDLGuard(create.database, create.table);

        database = DatabaseCatalog::instance().getDatabase(create.database);
        assertOrSetUUID(create, database);

        String storage_name = create.is_dictionary ? "Dictionary" : "Table";
        auto storage_already_exists_error_code = create.is_dictionary ? ErrorCodes::DICTIONARY_ALREADY_EXISTS : ErrorCodes::TABLE_ALREADY_EXISTS;

        /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
        if (database->isTableExist(create.table, getContext()))
        {
            /// TODO Check structure of table
            if (create.if_not_exists)
                return false;
            else if (create.replace_view)
            {
                /// when executing CREATE OR REPLACE VIEW, drop current existing view
                auto drop_ast = std::make_shared<ASTDropQuery>();
                drop_ast->database = create.database;
                drop_ast->table = create.table;
                drop_ast->no_ddl_lock = true;

                auto drop_context = Context::createCopy(context);
                InterpreterDropQuery interpreter(drop_ast, drop_context);
                interpreter.execute();
            }
            else
                throw Exception(storage_already_exists_error_code,
                    "{} {}.{} already exists", storage_name, backQuoteIfNeed(create.database), backQuoteIfNeed(create.table));
        }

        data_path = database->getTableDataPath(create);

        if (!create.attach && !data_path.empty() && fs::exists(fs::path{getContext()->getPath()} / data_path))
            throw Exception(storage_already_exists_error_code,
                "Directory for {} data {} already exists", Poco::toLower(storage_name), String(data_path));
    }
    else
    {
        if (create.if_not_exists && getContext()->tryResolveStorageID({"", create.table}, Context::ResolveExternal))
            return false;

        String temporary_table_name = create.table;
        auto temporary_table = TemporaryTableHolder(getContext(), properties.columns, properties.constraints, query_ptr);
        getContext()->getSessionContext()->addExternalTable(temporary_table_name, std::move(temporary_table));
        return true;
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
        bool throw_if_table_in_use = getContext()->getSettingsRef().database_atomic_wait_for_drop_and_detach_synchronously;
        if (throw_if_table_in_use)
            database->checkDetachedTableNotInUse(create.uuid);
        else
            database->waitDetachedTableNotInUse(create.uuid);
    }

    StoragePtr res;
    /// NOTE: CREATE query may be rewritten by Storage creator or table function
    if (create.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        auto table_func = factory.get(create.as_table_function, getContext());
        res = table_func->execute(create.as_table_function, getContext(), create.table, properties.columns);
        res->renameInMemory({create.database, create.table, create.uuid});
    }
    else
    {
        res = StorageFactory::instance().get(create,
            data_path,
            getContext(),
            getContext()->getGlobalContext(),
            properties.columns,
            properties.constraints,
            false);
    }

    if (from_path && !res->storesDataOnDisk())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "ATTACH ... FROM ... query is not supported for {} table engine, "
                        "because such tables do not store any data on disk. Use CREATE instead.", res->getName());

    database->createTable(getContext(), create.table, res, query_ptr);

    /// Move table data to the proper place. Wo do not move data earlier to avoid situations
    /// when data directory moved, but table has not been created due to some error.
    if (from_path)
        res->rename(actual_data_path, {create.database, create.table, create.uuid});

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
                                                       const InterpreterCreateQuery::TableProperties & properties)
{
    /// Replicated database requires separate contexts for each DDL query
    ContextPtr current_context = getContext();
    ContextMutablePtr create_context = Context::createCopy(current_context);
    create_context->setQueryContext(std::const_pointer_cast<Context>(current_context));

    auto make_drop_context = [&](bool on_error) -> ContextMutablePtr
    {
        ContextMutablePtr drop_context = Context::createCopy(current_context);
        drop_context->makeQueryContext();
        if (on_error)
            return drop_context;

        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            /// Execute drop as separate query, because [CREATE OR] REPLACE query can be considered as
            /// successfully executed after RENAME/EXCHANGE query.
            drop_context->resetZooKeeperMetadataTransaction();
            auto drop_txn = std::make_shared<ZooKeeperMetadataTransaction>(txn->getZooKeeper(), txn->getDatabaseZooKeeperPath(),
                                                                           txn->isInitialQuery(), txn->getTaskZooKeeperPath());
            drop_context->initZooKeeperMetadataTransaction(drop_txn);
        }
        return drop_context;
    };

    auto ast_drop = std::make_shared<ASTDropQuery>();
    String table_to_replace_name = create.table;

    {
        auto database = DatabaseCatalog::instance().getDatabase(create.database);
        if (database->getUUID() == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} query is supported only for Atomic databases",
                            create.create_or_replace ? "CREATE OR REPLACE TABLE" : "REPLACE TABLE");


        UInt64 name_hash = sipHash64(create.database + create.table);
        UInt16 random_suffix = thread_local_rng();
        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            /// Avoid different table name on database replicas
            random_suffix = sipHash64(txn->getTaskZooKeeperPath());
        }
        create.table = fmt::format("_tmp_replace_{}_{}",
                                   getHexUIntLowercase(name_hash),
                                   getHexUIntLowercase(random_suffix));

        ast_drop->table = create.table;
        ast_drop->is_dictionary = create.is_dictionary;
        ast_drop->database = create.database;
        ast_drop->kind = ASTDropQuery::Drop;
    }

    bool created = false;
    bool renamed = false;
    try
    {
        /// Create temporary table (random name will be generated)
        [[maybe_unused]] bool done = InterpreterCreateQuery(query_ptr, create_context).doCreateTable(create, properties);
        assert(done);
        created = true;

        /// Try fill temporary table
        BlockIO fill_io = fillTableIfNeeded(create);
        executeTrivialBlockIO(fill_io, getContext());

        /// Replace target table with created one
        auto ast_rename = std::make_shared<ASTRenameQuery>();
        ASTRenameQuery::Element elem
        {
            ASTRenameQuery::Table{create.database, create.table},
            ASTRenameQuery::Table{create.database, table_to_replace_name}
        };

        ast_rename->elements.push_back(std::move(elem));
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
            auto drop_context = make_drop_context(false);
            InterpreterDropQuery(ast_drop, drop_context).execute();
        }

        create.table = table_to_replace_name;

        return {};
    }
    catch (...)
    {
        /// Drop temporary table if it was successfully created, but was not renamed to target name
        if (created && !renamed)
        {
            auto drop_context = make_drop_context(true);
            InterpreterDropQuery(ast_drop, drop_context).execute();
        }
        throw;
    }
}

BlockIO InterpreterCreateQuery::fillTableIfNeeded(const ASTCreateQuery & create)
{
    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.select && !create.attach
        && !create.is_ordinary_view && !create.is_live_view && (!create.is_materialized_view || create.is_populate))
    {
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = {create.database, create.table, create.uuid};
        insert->select = create.select->clone();

        if (create.temporary && !getContext()->getSessionContext()->hasQueryContext())
            getContext()->getSessionContext()->makeQueryContext();

        return InterpreterInsertQuery(insert,
            create.temporary ? getContext()->getSessionContext() : getContext(),
            getContext()->getSettingsRef().insert_allow_materialized_columns).execute();
    }

    return {};
}

void InterpreterCreateQuery::prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr local_context, const String & cluster_name)
{
    if (create.attach)
        return;

    /// For CREATE query generate UUID on initiator, so it will be the same on all hosts.
    /// It will be ignored if database does not support UUIDs.
    generateUUIDForTable(create);

    /// For cross-replication cluster we cannot use UUID in replica path.
    String cluster_name_expanded = local_context->getMacros()->expand(cluster_name);
    ClusterPtr cluster = local_context->getCluster(cluster_name_expanded);

    if (cluster->maybeCrossReplication())
    {
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
            String zk_path = create.storage->engine->arguments->children[0]->as<ASTLiteral>()->value.get<String>();
            Macros::MacroExpansionInfo info;
            info.table_id.uuid = create.uuid;
            info.ignore_unknown = true;
            local_context->getMacros()->expand(zk_path, info);
            if (!info.expanded_uuid)
                return;
        }

        throw Exception("Seems like cluster is configured for cross-replication, "
                        "but zookeeper_path for ReplicatedMergeTree is not specified or contains {uuid} macro. "
                        "It's not supported for cross replication, because tables must have different UUIDs. "
                        "Please specify unique zookeeper_path explicitly.", ErrorCodes::INCORRECT_QUERY);
    }
}

BlockIO InterpreterCreateQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create = query_ptr->as<ASTCreateQuery &>();
    if (!create.cluster.empty())
    {
        prepareOnClusterQuery(create, getContext(), create.cluster);
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccess());
    }

    getContext()->checkAccess(getRequiredAccess());

    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
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

    if (create.table.empty())
    {
        required_access.emplace_back(AccessType::CREATE_DATABASE, create.database);
    }
    else if (create.is_dictionary)
    {
        required_access.emplace_back(AccessType::CREATE_DICTIONARY, create.database, create.table);
    }
    else if (create.isView())
    {
        assert(!create.temporary);
        if (create.replace_view)
            required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, create.database, create.table);
        else
            required_access.emplace_back(AccessType::CREATE_VIEW, create.database, create.table);
    }
    else
    {
        if (create.temporary)
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
        else
        {
            if (create.replace_table)
                required_access.emplace_back(AccessType::DROP_TABLE, create.database, create.table);
            required_access.emplace_back(AccessType::CREATE_TABLE, create.database, create.table);
        }
    }

    if (create.to_table_id)
        required_access.emplace_back(AccessType::SELECT | AccessType::INSERT, create.to_table_id.database_name, create.to_table_id.table_name);

    if (create.storage && create.storage->engine)
    {
        auto source_access_type = StorageFactory::instance().getSourceAccessType(create.storage->engine->name);
        if (source_access_type != AccessType::NONE)
            required_access.emplace_back(source_access_type);
    }

    return required_access;
}

void InterpreterCreateQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Create";
    if (!as_table_saved.empty())
    {
        String database = backQuoteIfNeed(as_database_saved.empty() ? getContext()->getCurrentDatabase() : as_database_saved);
        elem.query_databases.insert(database);
        elem.query_tables.insert(database + "." + backQuoteIfNeed(as_table_saved));
    }
}

}
