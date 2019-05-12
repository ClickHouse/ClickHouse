#include <memory>

#include <boost/range/join.hpp>

#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageLog.h>

#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/IDatabase.h>

#include <Common/ZooKeeper/ZooKeeper.h>

#include <Compression/CompressionFactory.h>
#include <Interpreters/InterpreterDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int ENGINE_REQUIRED;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
    extern const int READONLY;
    extern const int ILLEGAL_COLUMN;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int QUERY_IS_PROHIBITED;
    extern const int THERE_IS_NO_DEFAULT_VALUE;
    extern const int BAD_DATABASE_FOR_TEMPORARY_TABLE;
}


InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    if (!create.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {create.database});

    String database_name = create.database;

    auto guard = context.getDDLGuard(database_name, "");

    /// Database can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard
    if (context.isDatabaseExist(database_name))
    {
        if (create.if_not_exists)
            return {};
        else
            throw Exception("Database " + database_name + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
    }

    String database_engine_name;
    if (!create.storage)
    {
        database_engine_name = "Ordinary"; /// Default database engine.
        auto engine = std::make_shared<ASTFunction>();
        engine->name = database_engine_name;
        auto storage = std::make_shared<ASTStorage>();
        storage->set(storage->engine, engine);
        create.set(create.storage, storage);
    }
    else
    {
        const ASTStorage & storage = *create.storage;
        const ASTFunction & engine = *storage.engine;
        /// Currently, there are no database engines, that support any arguments.
        if (engine.arguments || engine.parameters || storage.partition_by || storage.primary_key
            || storage.order_by || storage.sample_by || storage.settings ||
            (create.columns_list && create.columns_list->indices && !create.columns_list->indices->children.empty()))
        {
            std::stringstream ostr;
            formatAST(storage, ostr, false, false);
            throw Exception("Unknown database engine: " + ostr.str(), ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        }

        database_engine_name = engine.name;
    }

    String database_name_escaped = escapeForFileName(database_name);

    /// Create directories for tables metadata.
    String path = context.getPath();
    String metadata_path = path + "metadata/" + database_name_escaped + "/";
    Poco::File(metadata_path).createDirectory();

    DatabasePtr database = DatabaseFactory::get(database_engine_name, database_name, metadata_path, context);

    /// Will write file with database metadata, if needed.
    String metadata_file_tmp_path = path + "metadata/" + database_name_escaped + ".sql.tmp";
    String metadata_file_path = path + "metadata/" + database_name_escaped + ".sql";

    bool need_write_metadata = !create.attach;

    if (need_write_metadata)
    {
        create.attach = true;
        create.if_not_exists = false;

        std::ostringstream statement_stream;
        formatAST(create, statement_stream, false);
        statement_stream << '\n';
        String statement = statement_stream.str();

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        context.addDatabase(database_name, database);

        if (need_write_metadata)
            Poco::File(metadata_file_tmp_path).renameTo(metadata_file_path);

        database->loadTables(context, thread_pool, has_force_restore_data_flag);
    }
    catch (...)
    {
        if (need_write_metadata)
            Poco::File(metadata_file_tmp_path).remove();

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

        ParserIdentifierWithOptionalParameters storage_p;
        String type_name = column.type->getName();
        auto pos = type_name.data();
        const auto end = pos + type_name.size();
        column_declaration->type = parseQuery(storage_p, pos, end, "data type", 0);
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

        ParserIdentifierWithOptionalParameters storage_p;
        String type_name = column.type->getName();
        auto type_name_pos = type_name.data();
        const auto type_name_end = type_name_pos + type_name.size();
        column_declaration->type = parseQuery(storage_p, type_name_pos, type_name_end, "data type", 0);

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
        {
            String codec_desc = column.codec->getCodecDesc();
            codec_desc = "CODEC(" + codec_desc + ")";
            auto codec_desc_pos = codec_desc.data();
            const auto codec_desc_end = codec_desc_pos + codec_desc.size();
            ParserIdentifierWithParameters codec_p;
            column_declaration->codec = parseQuery(codec_p, codec_desc_pos, codec_desc_end, "column codec", 0);
        }

        if (column.ttl)
            column_declaration->ttl = column.ttl;

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatIndices(const IndicesDescription & indices)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices.indices)
        res->children.push_back(index->clone());

    return res;
}

ColumnsDescription InterpreterCreateQuery::getColumnsDescription(const ASTExpressionList & columns_ast, const Context & context)
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
            /** for columns with explicitly-specified type create two expressions:
             *    1. default_expression aliased as column name with _tmp suffix
             *    2. conversion of expression (1) to explicitly-specified type alias as column name */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp";
                const auto data_type_ptr = column_names_and_types.back().type.get();

                default_expr_list->children.emplace_back(setAlias(
                    makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                        std::make_shared<ASTLiteral>(data_type_ptr->getName())), final_column_name));
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }
    }

    Block defaults_sample_block;
    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!default_expr_list->children.empty())
    {
        auto syntax_analyzer_result = SyntaxAnalyzer(context).analyze(default_expr_list, column_names_and_types);
        const auto actions = ExpressionAnalyzer(default_expr_list, syntax_analyzer_result, context).getActions(true);
        for (auto action : actions->getActions())
            if (action.type == ExpressionAction::Type::JOIN || action.type == ExpressionAction::Type::ARRAY_JOIN)
                throw Exception("Cannot CREATE table. Unsupported default value that requires ARRAY JOIN or JOIN action", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);

        defaults_sample_block = actions->getSampleBlock();
    }

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
            {
                const auto & deduced_type = defaults_sample_block.getByName(column.name + "_tmp").type;
                column.type = name_type_it->type;

                if (!column.type->equals(*deduced_type))
                    default_expr = makeASTFunction("CAST", default_expr, std::make_shared<ASTLiteral>(column.type->getName()));
            }
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
            column.codec = CompressionCodecFactory::instance().get(col_decl.codec, column.type);

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        res.add(std::move(column));
    }

    res.flattenNested();

    if (res.getAllPhysical().empty())
        throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

    return res;
}


ColumnsDescription InterpreterCreateQuery::setColumns(
    ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const
{
    ColumnsDescription columns;
    IndicesDescription indices;

    if (create.columns_list)
    {
        if (create.columns_list->columns)
            columns = getColumnsDescription(*create.columns_list->columns, context);
        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
                indices.indices.push_back(
                    std::dynamic_pointer_cast<ASTIndexDeclaration>(index->clone()));
    }
    else if (!create.as_table.empty())
    {
        columns = as_storage->getColumns();
        indices = as_storage->getIndices();
    }
    else if (create.select)
    {
        columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    ASTPtr new_columns = formatColumns(columns);
    ASTPtr new_indices = formatIndices(indices);

    if (!create.columns_list)
    {
        auto new_columns_list = std::make_shared<ASTColumns>();
        create.set(create.columns_list, new_columns_list);
    }

    if (create.columns_list->columns)
        create.columns_list->replace(create.columns_list->columns, new_columns);
    else
        create.columns_list->set(create.columns_list->columns, new_columns);

    if (new_indices && create.columns_list->indices)
        create.columns_list->replace(create.columns_list->indices, new_indices);
    else if (new_indices)
        create.columns_list->set(create.columns_list->indices, new_indices);

    /// Check for duplicates
    std::set<String> all_columns;
    for (const auto & column : columns)
    {
        if (!all_columns.emplace(column.name).second)
            throw Exception("Column " + backQuoteIfNeed(column.name) + " already exists", ErrorCodes::DUPLICATE_COLUMN);
    }

    return columns;
}


void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.storage)
    {
        if (create.temporary && create.storage->engine->name != "Memory")
            throw Exception(
                "Temporary tables can only be created with ENGINE = Memory, not " + create.storage->engine->name,
                ErrorCodes::INCORRECT_QUERY);

        return;
    }

    if (create.temporary)
    {
        auto engine_ast = std::make_shared<ASTFunction>();
        engine_ast->name = "Memory";
        auto storage_ast = std::make_shared<ASTStorage>();
        storage_ast->set(storage_ast->engine, engine_ast);
        create.set(create.storage, storage_ast);
    }
    else if (!create.as_table.empty())
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = create.as_database.empty() ? context.getCurrentDatabase() : create.as_database;
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = context.getCreateTableQuery(as_database_name, as_table_name);
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        if (as_create.is_view)
            throw Exception(
                "Cannot CREATE a table AS " + as_database_name + "." + as_table_name + ", it is a View",
                ErrorCodes::INCORRECT_QUERY);

        create.set(create.storage, as_create.storage->ptr());
    }
}


BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    if (!create.cluster.empty())
    {
        NameSet databases{create.database};
        if (!create.to_table.empty())
            databases.emplace(create.to_database);

        return executeDDLQueryOnCluster(query_ptr, context, std::move(databases));
    }

    /// Temporary tables are created out of databases.
    if (create.temporary && !create.database.empty())
        throw Exception("Temporary tables cannot be inside a database. You should not specify a database for a temporary table.",
            ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE);

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    String database_name = create.database.empty() ? current_database : create.database;
    String table_name = create.table;
    String table_name_escaped = escapeForFileName(table_name);

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns_list)
    {
        // Table SQL definition is available even if the table is detached
        auto query = context.getCreateTableQuery(database_name, table_name);
        create = query->as<ASTCreateQuery &>(); // Copy the saved create query, but use ATTACH instead of CREATE
        create.attach = true;
    }

    if (create.to_database.empty())
        create.to_database = current_database;

    if (create.select && (create.is_view || create.is_materialized_view))
    {
        AddDefaultDatabaseVisitor visitor(current_database);
        visitor.visit(*create.select);
    }

    Block as_select_sample;
    if (create.select && (!create.attach || !create.columns_list))
        as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), context);

    String as_database_name = create.as_database.empty() ? current_database : create.as_database;
    String as_table_name = create.as_table;

    StoragePtr as_storage;
    TableStructureReadLockHolder as_storage_lock;
    if (!as_table_name.empty())
    {
        as_storage = context.getTable(as_database_name, as_table_name);
        as_storage_lock = as_storage->lockStructureForShare(false, context.getCurrentQueryId());
    }

    /// Set and retrieve list of columns.
    ColumnsDescription columns = setColumns(create, as_select_sample, as_storage);

    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    StoragePtr res;

    {
        std::unique_ptr<DDLGuard> guard;

        String data_path;
        DatabasePtr database;

        if (!create.temporary)
        {
            database = context.getDatabase(database_name);
            data_path = database->getDataPath();

            /** If the request specifies IF NOT EXISTS, we allow concurrent CREATE queries (which do nothing).
              * If table doesnt exist, one thread is creating table, while others wait in DDLGuard.
              */
            guard = context.getDDLGuard(database_name, table_name);

            /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
            if (database->isTableExist(context, table_name))
            {
                if (create.if_not_exists)
                    return {};
                else if (create.replace_view)
                {
                    /// when executing CREATE OR REPLACE VIEW, drop current existing view
                    auto drop_ast = std::make_shared<ASTDropQuery>();
                    drop_ast->database = database_name;
                    drop_ast->table = table_name;
                    drop_ast->no_ddl_lock = true;

                    InterpreterDropQuery interpreter(drop_ast, context);
                    interpreter.execute();
                }
                else
                    throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
            }
        }
        else if (context.tryGetExternalTable(table_name) && create.if_not_exists)
             return {};

        res = StorageFactory::instance().get(create,
            data_path,
            table_name,
            database_name,
            context,
            context.getGlobalContext(),
            columns,
            create.attach,
            false);

        if (create.temporary)
            context.getSessionContext().addExternalTable(table_name, res, query_ptr);
        else
            database->createTable(context, table_name, res, query_ptr);

        /// We must call "startup" and "shutdown" while holding DDLGuard.
        /// Because otherwise method "shutdown" (from InterpreterDropQuery) can be called before startup
        /// (in case when table was created and instantly dropped before started up)
        ///
        /// Method "startup" may create background tasks and method "shutdown" will wait for them.
        /// But if "shutdown" is called before "startup", it will exit early, because there are no background tasks to wait.
        /// Then background task is created by "startup" method. And when destructor of a table object is called, background task is still active,
        /// and the task will use references to freed data.

        res->startup();
    }

    /// If the query is a CREATE SELECT, insert the data into the table.
    if (create.select && !create.attach
        && !create.is_view && (!create.is_materialized_view || create.is_populate))
    {
        auto insert = std::make_shared<ASTInsertQuery>();

        if (!create.temporary)
            insert->database = database_name;

        insert->table = table_name;
        insert->select = create.select->clone();

        if (create.temporary && !context.getSessionContext().hasQueryContext())
            context.getSessionContext().setQueryContext(context.getSessionContext());

        return InterpreterInsertQuery(insert,
            create.temporary ? context.getSessionContext() : context,
            context.getSettingsRef().insert_allow_materialized_columns).execute();
    }

    return {};
}


BlockIO InterpreterCreateQuery::execute()
{
    auto & create = query_ptr->as<ASTCreateQuery &>();
    checkAccess(create);
    ASTQueryWithOutput::resetOutputASTIfExist(create);

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
    {
        return createDatabase(create);
    }
    else
        return createTable(create);
}


void InterpreterCreateQuery::checkAccess(const ASTCreateQuery & create)
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return;

    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;
    auto allow_ddl = settings.allow_ddl;

    if (!readonly && allow_ddl)
        return;

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
    {
        if (readonly)
            throw Exception("Cannot create database in readonly mode", ErrorCodes::READONLY);

        throw Exception("Cannot create database. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
    }

    if (create.temporary && readonly >= 2)
        return;

    if (readonly)
        throw Exception("Cannot create table in readonly mode", ErrorCodes::READONLY);

    throw Exception("Cannot create table. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
}
}
