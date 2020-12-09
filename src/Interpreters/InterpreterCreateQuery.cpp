#include <memory>

#include <filesystem>

#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include <Common/randomSeed.h>

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
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Access/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>

#include <Dictionaries/getDictionaryConfigurationFromAST.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/addTypeConversionToAST.h>

#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_DATABASE_FOR_TEMPORARY_TABLE;
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
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
    fs::path metadata_path = fs::canonical(context.getPath());
    fs::path metadata_file_tmp_path = metadata_path / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = metadata_path / "metadata" / (database_name_escaped + ".sql");

    if (!create.storage && create.attach)
    {
        if (!fs::exists(metadata_file_path))
            throw Exception("Database engine must be specified for ATTACH DATABASE query", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        /// Short syntax: try read database definition from file
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, context, metadata_file_path);
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
        bool old_style_database = context.getSettingsRef().default_database_engine.value == DefaultDatabaseEngine::Ordinary;
        auto engine = std::make_shared<ASTFunction>();
        auto storage = std::make_shared<ASTStorage>();
        engine->name = old_style_database ? "Ordinary" : "Atomic";
        engine->no_empty_args = true;
        storage->set(storage->engine, engine);
        create.set(create.storage, storage);
    }
    else if ((create.columns_list && create.columns_list->indices && !create.columns_list->indices->children.empty()))
    {
        /// Currently, there are no database engines, that support any arguments.
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", serializeAST(*create.storage));
    }

    if (create.storage->engine->name == "Atomic")
    {
        if (create.attach && create.uuid == UUIDHelpers::Nil)
            throw Exception("UUID must be specified for ATTACH", ErrorCodes::INCORRECT_QUERY);
        else if (create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();

        metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);

        if (!create.attach && fs::exists(metadata_path))
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists", metadata_path.string());
    }
    else
    {
        bool is_on_cluster = context.getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster)
            throw Exception("Ordinary database engine does not support UUID", ErrorCodes::INCORRECT_QUERY);

        /// Ignore UUID if it's ON CLUSTER query
        create.uuid = UUIDHelpers::Nil;
        metadata_path = metadata_path / "metadata" / database_name_escaped;
    }

    if (create.storage->engine->name == "MaterializeMySQL" && !context.getSettingsRef().allow_experimental_database_materialize_mysql && !internal)
    {
        throw Exception("MaterializeMySQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialize_mysql to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    DatabasePtr database = DatabaseFactory::get(create, metadata_path / "", context);

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
        if (context.getSettingsRef().fsync_metadata)
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
            fs::rename(metadata_file_tmp_path, metadata_file_path);
            renamed = true;
        }

        database->loadStoredObjects(context, has_force_restore_data_flag, create.attach && force_attach);
    }
    catch (...)
    {
        if (renamed)
        {
            [[maybe_unused]] bool removed = fs::remove(metadata_file_path);
            assert(removed);
        }
        if (added)
            DatabaseCatalog::instance().detachDatabase(database_name, false, false);

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

ColumnsDescription InterpreterCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns_ast, const Context & context, bool sanity_check_compression_codecs)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;

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
            else if (context.getSettingsRef().data_type_default_nullable)
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
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_list, column_names_and_types, context);

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
                col_decl.codec, column.type, sanity_check_compression_codecs);
        }

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        res.add(std::move(column));
    }

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


InterpreterCreateQuery::TableProperties InterpreterCreateQuery::setProperties(ASTCreateQuery & create) const
{
    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (create.columns_list)
    {
        if (create.as_table_function && (create.columns_list->indices || create.columns_list->constraints))
            throw Exception("Indexes and constraints are not supported for table functions", ErrorCodes::INCORRECT_QUERY);

        if (create.columns_list->columns)
        {
            bool sanity_check_compression_codecs = !create.attach && !context.getSettingsRef().allow_suspicious_codecs;
            properties.columns = getColumnsDescription(*create.columns_list->columns, context, sanity_check_compression_codecs);
        }

        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
                properties.indices.push_back(
                    IndexDescription::getIndexFromAST(index->clone(), properties.columns, context));

        properties.constraints = getConstraintsDescription(create.columns_list->constraints);
    }
    else if (!create.as_table.empty())
    {
        String as_database_name = context.resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, context);

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr();
        properties.columns = as_storage_metadata->getColumns();

        /// Secondary indices make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
            properties.indices = as_storage_metadata->getSecondaryIndices();

        properties.constraints = as_storage_metadata->getConstraints();
    }
    else if (create.select)
    {
        Block as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), context);
        properties.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function = TableFunctionFactory::instance().get(create.as_table_function, context);
        properties.columns = table_function->getActualTableStructure(context);
        assert(!properties.columns.empty());
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);


    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    if (!create.columns_list)
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    ASTPtr new_columns = formatColumns(properties.columns);
    ASTPtr new_indices = formatIndices(properties.indices);
    ASTPtr new_constraints = formatConstraints(properties.constraints);

    create.columns_list->setOrReplace(create.columns_list->columns, new_columns);
    create.columns_list->setOrReplace(create.columns_list->indices, new_indices);
    create.columns_list->setOrReplace(create.columns_list->constraints, new_constraints);

    validateTableStructure(create, properties);
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);
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

    const auto & settings = context.getSettingsRef();

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

    if (!create.attach && !settings.allow_experimental_bigint_types)
    {
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
        {
            WhichDataType which(*name_and_type_pair.type);
            if (which.IsBigIntOrDeimal())
            {
                const auto & type_name = name_and_type_pair.type->getName();
                String message = "Cannot create table with column '" + name_and_type_pair.name + "' which type is '"
                                 + type_name + "' because experimental bigint types are not allowed. "
                                 + "Set 'allow_experimental_bigint_types' setting to enable.";
                throw Exception(message, ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }
}

void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.as_table_function)
        return;

    if (create.storage || create.is_view || create.is_materialized_view || create.is_live_view || create.is_dictionary)
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

        String as_database_name = context.resolveDatabase(create.as_database);
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name)->getCreateTableQuery(as_table_name, context);
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        const String qualified_name = backQuoteIfNeed(as_database_name) + "." + backQuoteIfNeed(as_table_name);

        if (as_create.is_view)
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

void InterpreterCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";

    if (database->getEngineName() == "Atomic")
    {
        if (create.attach && create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "UUID must be specified in ATTACH {} query for Atomic database engine",
                            kind_upper);
        if (!create.attach && create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();
    }
    else
    {
        bool is_on_cluster = context.getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} UUID specified, but engine of database {} is not Atomic", kind, create.database);

        /// Ignore UUID if it's ON CLUSTER query
        create.uuid = UUIDHelpers::Nil;
    }
}


BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    /// Temporary tables are created out of databases.
    if (create.temporary && !create.database.empty())
        throw Exception("Temporary tables cannot be inside a database. You should not specify a database for a temporary table.",
            ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE);

    String current_database = context.getCurrentDatabase();

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns_list)
    {
        auto database_name = create.database.empty() ? current_database : create.database;
        auto database = DatabaseCatalog::instance().getDatabase(database_name);
        bool if_not_exists = create.if_not_exists;

        // Table SQL definition is available even if the table is detached
        auto query = database->getCreateTableQuery(create.table, context);
        create = query->as<ASTCreateQuery &>(); // Copy the saved create query, but use ATTACH instead of CREATE
        if (create.is_dictionary)
            throw Exception(
                "Cannot ATTACH TABLE " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(create.table) + ", it is a Dictionary",
                ErrorCodes::INCORRECT_QUERY);
        create.attach = true;
        create.attach_short_syntax = true;
        create.if_not_exists = if_not_exists;
    }
    /// TODO maybe assert table structure if create.attach_short_syntax is false?

    if (!create.temporary && create.database.empty())
        create.database = current_database;
    if (create.to_table_id && create.to_table_id.database_name.empty())
        create.to_table_id.database_name = current_database;

    if (create.select && (create.is_view || create.is_materialized_view || create.is_live_view))
    {
        AddDefaultDatabaseVisitor visitor(current_database);
        visitor.visit(*create.select);
    }

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = setProperties(create);

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

    const String table_name = create.table;
    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
    {
        /** If the request specifies IF NOT EXISTS, we allow concurrent CREATE queries (which do nothing).
          * If table doesn't exist, one thread is creating table, while others wait in DDLGuard.
          */
        guard = DatabaseCatalog::instance().getDDLGuard(create.database, table_name);

        database = DatabaseCatalog::instance().getDatabase(create.database);
        assertOrSetUUID(create, database);

        /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
        if (database->isTableExist(table_name, context))
        {
            /// TODO Check structure of table
            if (create.if_not_exists)
                return false;
            else if (create.replace_view)
            {
                /// when executing CREATE OR REPLACE VIEW, drop current existing view
                auto drop_ast = std::make_shared<ASTDropQuery>();
                drop_ast->database = create.database;
                drop_ast->table = table_name;
                drop_ast->no_ddl_lock = true;

                InterpreterDropQuery interpreter(drop_ast, context);
                interpreter.execute();
            }
            else
                throw Exception("Table " + create.database + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
        }

        data_path = database->getTableDataPath(create);
        if (!create.attach && !data_path.empty() && fs::exists(fs::path{context.getPath()} / data_path))
            throw Exception("Directory for table data " + data_path + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
    }
    else
    {
        if (create.if_not_exists && context.tryResolveStorageID({"", table_name}, Context::ResolveExternal))
            return false;

        auto temporary_table = TemporaryTableHolder(context, properties.columns, properties.constraints, query_ptr);
        context.getSessionContext().addExternalTable(table_name, std::move(temporary_table));
        return true;
    }

    StoragePtr res;
    /// NOTE: CREATE query may be rewritten by Storage creator or table function
    if (create.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        res = factory.get(create.as_table_function, context)->execute(create.as_table_function, context, create.table, properties.columns);
        res->renameInMemory({create.database, create.table, create.uuid});
    }
    else
    {
        res = StorageFactory::instance().get(create,
            database ? database->getTableDataPath(create) : "",
            context,
            context.getGlobalContext(),
            properties.columns,
            properties.constraints,
            false);
    }

    database->createTable(context, table_name, res, query_ptr);

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

BlockIO InterpreterCreateQuery::fillTableIfNeeded(const ASTCreateQuery & create)
{
    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.select && !create.attach
        && !create.is_view && !create.is_live_view && (!create.is_materialized_view || create.is_populate))
    {
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = {create.database, create.table, create.uuid};
        insert->select = create.select->clone();

        if (create.temporary && !context.getSessionContext().hasQueryContext())
            context.getSessionContext().makeQueryContext();

        return InterpreterInsertQuery(insert,
            create.temporary ? context.getSessionContext() : context,
            context.getSettingsRef().insert_allow_materialized_columns).execute();
    }

    return {};
}

BlockIO InterpreterCreateQuery::createDictionary(ASTCreateQuery & create)
{
    String dictionary_name = create.table;

    create.database = context.resolveDatabase(create.database);
    const String & database_name = create.database;

    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, dictionary_name);
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

    if (database->isDictionaryExist(dictionary_name))
    {
        /// TODO Check structure of dictionary
        if (create.if_not_exists)
            return {};
        else
            throw Exception(
                "Dictionary " + database_name + "." + dictionary_name + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);
    }

    if (create.attach)
    {
        auto query = DatabaseCatalog::instance().getDatabase(database_name)->getCreateDictionaryQuery(dictionary_name);
        create = query->as<ASTCreateQuery &>();
        create.attach = true;
    }

    assertOrSetUUID(create, database);

    if (create.attach)
    {
        auto config = getDictionaryConfigurationFromAST(create, context);
        auto modification_time = database->getObjectMetadataModificationTime(dictionary_name);
        database->attachDictionary(dictionary_name, DictionaryAttachInfo{query_ptr, config, modification_time});
    }
    else
        database->createDictionary(context, dictionary_name, query_ptr);

    return {};
}

void InterpreterCreateQuery::prepareOnClusterQuery(ASTCreateQuery & create, const Context & context, const String & cluster_name)
{
    if (create.attach)
        return;

    /// For CREATE query generate UUID on initiator, so it will be the same on all hosts.
    /// It will be ignored if database does not support UUIDs.
    if (create.uuid == UUIDHelpers::Nil)
        create.uuid = UUIDHelpers::generateV4();

    /// For cross-replication cluster we cannot use UUID in replica path.
    String cluster_name_expanded = context.getMacros()->expand(cluster_name);
    ClusterPtr cluster = context.getCluster(cluster_name_expanded);

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
            context.getMacros()->expand(zk_path, info);
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
    auto & create = query_ptr->as<ASTCreateQuery &>();
    if (!create.cluster.empty())
    {
        prepareOnClusterQuery(create, context, create.cluster);
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());
    }

    context.checkAccess(getRequiredAccess());

    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
        return createDatabase(create);
    else if (!create.is_dictionary)
        return createTable(create);
    else
        return createDictionary(create);
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
    else if (create.is_view || create.is_materialized_view || create.is_live_view)
    {
        if (create.temporary)
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
        else
        {
            if (create.replace_view)
                required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, create.database, create.table);
            else
                required_access.emplace_back(AccessType::CREATE_VIEW, create.database, create.table);
        }
    }
    else
    {
        if (create.temporary)
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
        else
            required_access.emplace_back(AccessType::CREATE_TABLE, create.database, create.table);
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

}
