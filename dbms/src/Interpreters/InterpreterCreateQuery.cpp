#include <memory>

#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <Common/escapeForFileName.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageLog.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/DDLWorker.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeFactory.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/IDatabase.h>

#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int ENGINE_REQUIRED;
    extern const int TABLE_METADATA_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
}


InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    if (!create.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context);

    String database_name = create.database;

    if (create.if_not_exists && context.isDatabaseExist(database_name))
        return {};

    String database_engine_name;
    if (!create.storage)
    {
        database_engine_name = "Ordinary";    /// Default database engine.
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
        if (engine.arguments || engine.parameters
            || storage.partition_by || storage.order_by || storage.sample_by || storage.settings)
        {
            std::stringstream ostr;
            formatAST(storage, ostr, 0, false, false);
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
        formatAST(create, statement_stream, 0, false);
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


using ColumnsAndDefaults = std::pair<NamesAndTypesList, ColumnDefaults>;

/// AST to the list of columns with types. Columns of Nested type are expanded into a list of real columns.
static ColumnsAndDefaults parseColumns(const ASTExpressionList & column_list_ast, const Context & context)
{
    /// list of table columns in correct order
    NamesAndTypesList columns{};
    ColumnDefaults defaults{};

    /// Columns requiring type-deduction or default_expression type-check
    std::vector<std::pair<NameAndTypePair *, ASTColumnDeclaration *>> defaulted_columns{};

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    default_expr_list->children.reserve(column_list_ast.children.size());

    for (const auto & ast : column_list_ast.children)
    {
        auto & col_decl = typeid_cast<ASTColumnDeclaration &>(*ast);

        if (col_decl.type)
        {
            columns.emplace_back(col_decl.name, DataTypeFactory::instance().get(col_decl.type));
        }
        else
            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            columns.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());

        /// add column to postprocessing if there is a default_expression specified
        if (col_decl.default_expression)
        {
            defaulted_columns.emplace_back(&columns.back(), &col_decl);

            /** for columns with explicitly-specified type create two expressions:
             *    1. default_expression aliased as column name with _tmp suffix
             *    2. conversion of expression (1) to explicitly-specified type alias as column name */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp";
                const auto data_type_ptr = columns.back().type.get();

                default_expr_list->children.emplace_back(setAlias(
                    makeASTFunction("CAST", std::make_shared<ASTIdentifier>(StringRange(), tmp_column_name),
                        std::make_shared<ASTLiteral>(StringRange(), Field(data_type_ptr->getName()))), final_column_name));
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }
    }

    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!defaulted_columns.empty())
    {
        const auto actions = ExpressionAnalyzer{default_expr_list, context, {}, columns}.getActions(true);
        const auto block = actions->getSampleBlock();

        for (auto & column : defaulted_columns)
        {
            const auto name_and_type_ptr = column.first;
            const auto col_decl_ptr = column.second;

            const auto & column_name = col_decl_ptr->name;
            const auto has_explicit_type = nullptr != col_decl_ptr->type;
            auto & explicit_type = name_and_type_ptr->type;

            /// if column declaration contains explicit type, name_and_type_ptr->type is not null
            if (has_explicit_type)
            {
                const auto & tmp_column = block.getByName(column_name + "_tmp");
                const auto & deduced_type = tmp_column.type;

                /// type mismatch between explicitly specified and deduced type, add conversion for non-array types
                if (!explicit_type->equals(*deduced_type))
                {
                    col_decl_ptr->default_expression = makeASTFunction("CAST", col_decl_ptr->default_expression,
                        std::make_shared<ASTLiteral>(StringRange(), explicit_type->getName()));

                    col_decl_ptr->children.clear();
                    col_decl_ptr->children.push_back(col_decl_ptr->type);
                    col_decl_ptr->children.push_back(col_decl_ptr->default_expression);
                }
            }
            else
                /// no explicit type, name_and_type_ptr->type is null, set to deduced type
                explicit_type = block.getByName(column_name).type;

            defaults.emplace(column_name, ColumnDefault{
                columnDefaultTypeFromString(col_decl_ptr->default_specifier),
                col_decl_ptr->default_expression
            });
        }
    }

    return { *DataTypeNested::expandNestedColumns(columns), defaults };
}


static NamesAndTypesList removeAndReturnColumns(
    ColumnsAndDefaults & columns_and_defaults, const ColumnDefaultType type)
{
    auto & columns = columns_and_defaults.first;
    auto & defaults = columns_and_defaults.second;

    NamesAndTypesList removed{};

    for (auto it = std::begin(columns); it != std::end(columns);)
    {
        const auto jt = defaults.find(it->name);
        if (jt != std::end(defaults) && jt->second.type == type)
        {
            removed.push_back(*it);
            it = columns.erase(it);
        }
        else
            ++it;
    }

    return removed;
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column.name;

        StringPtr type_name = std::make_shared<String>(column.type->getName());
        auto pos = type_name->data();
        const auto end = pos + type_name->size();

        ParserIdentifierWithOptionalParameters storage_p;
        column_declaration->type = parseQuery(storage_p, pos, end, "data type");
        column_declaration->type->query_string = type_name;
        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(NamesAndTypesList columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults)
{
    columns.insert(std::end(columns), std::begin(materialized_columns), std::end(materialized_columns));
    columns.insert(std::end(columns), std::begin(alias_columns), std::end(alias_columns));

    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;

        StringPtr type_name = std::make_shared<String>(column.type->getName());
        auto pos = type_name->data();
        const auto end = pos + type_name->size();

        ParserIdentifierWithOptionalParameters storage_p;
        column_declaration->type = parseQuery(storage_p, pos, end, "data type");
        column_declaration->type->query_string = type_name;

        const auto it = column_defaults.find(column.name);
        if (it != std::end(column_defaults))
        {
            column_declaration->default_specifier = toString(it->second.type);
            column_declaration->default_expression = it->second.expression->clone();
        }

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}


InterpreterCreateQuery::ColumnsInfo InterpreterCreateQuery::getColumnsInfo(
    const ASTExpressionList & columns, const Context & context)
{
    ColumnsInfo res;

    auto && columns_and_defaults = parseColumns(columns, context);
    res.materialized_columns = removeAndReturnColumns(columns_and_defaults, ColumnDefaultType::Materialized);
    res.alias_columns = removeAndReturnColumns(columns_and_defaults, ColumnDefaultType::Alias);
    res.columns = std::make_shared<NamesAndTypesList>(std::move(columns_and_defaults.first));
    res.column_defaults = std::move(columns_and_defaults.second);

    if (res.columns->size() + res.materialized_columns.size() == 0)
        throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

    return res;
}


InterpreterCreateQuery::ColumnsInfo InterpreterCreateQuery::setColumns(
    ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const
{
    ColumnsInfo res;

    if (create.columns)
    {
        res = getColumnsInfo(*create.columns, context);
    }
    else if (!create.as_table.empty())
    {
        res.columns = std::make_shared<NamesAndTypesList>(as_storage->getColumnsListNonMaterialized());
        res.materialized_columns = as_storage->materialized_columns;
        res.alias_columns = as_storage->alias_columns;
        res.column_defaults = as_storage->column_defaults;
    }
    else if (create.select)
    {
        res.columns = std::make_shared<NamesAndTypesList>();
        for (size_t i = 0; i < as_select_sample.columns(); ++i)
            res.columns->push_back(NameAndTypePair(as_select_sample.safeGetByPosition(i).name, as_select_sample.safeGetByPosition(i).type));
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    ASTPtr new_columns = formatColumns(*res.columns, res.materialized_columns, res.alias_columns, res.column_defaults);
    if (create.columns)
        create.replace(create.columns, new_columns);
    else
        create.set(create.columns, new_columns);

    /// Check for duplicates
    std::set<String> all_columns;
    auto check_column_already_exists = [&all_columns](const NameAndTypePair & column_name_and_type)
    {
        if (!all_columns.emplace(column_name_and_type.name).second)
            throw Exception("Column " + backQuoteIfNeed(column_name_and_type.name) + " already exists", ErrorCodes::DUPLICATE_COLUMN);
    };

    for (const auto & elem : *res.columns)
        check_column_already_exists(elem);
    for (const auto & elem : res.materialized_columns)
        check_column_already_exists(elem);
    for (const auto & elem : res.alias_columns)
        check_column_already_exists(elem);

    return res;
}


void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.storage)
    {
        if (create.is_temporary && create.storage->engine->name != "Memory")
            throw Exception(
                "Temporary tables can only be created with ENGINE = Memory, not " + create.storage->engine->name,
                ErrorCodes::INCORRECT_QUERY);

        return;
    }

    if (create.is_temporary)
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

        ASTPtr as_create_ptr = context.getCreateQuery(as_database_name, as_table_name);
        const auto & as_create = typeid_cast<const ASTCreateQuery &>(*as_create_ptr);

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
        return executeDDLQueryOnCluster(query_ptr, context);

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    String database_name = create.database.empty() ? current_database : create.database;
    String table_name = create.table;
    String table_name_escaped = escapeForFileName(table_name);

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns)
    {
        // Table SQL definition is available even if the table is detached
        auto query = context.getCreateQuery(database_name, table_name);
        auto & as_create = typeid_cast<const ASTCreateQuery &>(*query);
        create = as_create; // Copy the saved create query, but use ATTACH instead of CREATE
        create.attach = true;
    }

    if (create.to_database.empty())
        create.to_database = current_database;

    std::unique_ptr<InterpreterSelectQuery> interpreter_select;
    Block as_select_sample;
    /// For `view` type tables, you may need `sample_block` to get the columns.
    if (create.select && (!create.attach || (!create.columns && (create.is_view || create.is_materialized_view))))
    {
        create.select->setDatabaseIfNeeded(current_database);
        interpreter_select = std::make_unique<InterpreterSelectQuery>(create.select->ptr(), context);
        as_select_sample = interpreter_select->getSampleBlock();
    }

    String as_database_name = create.as_database.empty() ? current_database : create.as_database;
    String as_table_name = create.as_table;

    StoragePtr as_storage;
    TableStructureReadLockPtr as_storage_lock;
    if (!as_table_name.empty())
    {
        as_storage = context.getTable(as_database_name, as_table_name);
        as_storage_lock = as_storage->lockStructure(false, __PRETTY_FUNCTION__);
    }

    /// Set and retrieve list of columns.
    ColumnsInfo columns = setColumns(create, as_select_sample, as_storage);

    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    StoragePtr res;

    {
        std::unique_ptr<DDLGuard> guard;

        String data_path;
        DatabasePtr database;

        if (!create.is_temporary)
        {
            database = context.getDatabase(database_name);
            data_path = database->getDataPath(context);

            /** If the table already exists, and the request specifies IF NOT EXISTS,
              *  then we allow concurrent CREATE queries (which do nothing).
              * Otherwise, concurrent queries for creating a table, if the table does not exist,
              *  can throw an exception, even if IF NOT EXISTS is specified.
              */
            guard = context.getDDLGuardIfTableDoesntExist(database_name, table_name,
                "Table " + database_name + "." + table_name + " is creating or attaching right now");

            if (!guard)
            {
                if (create.if_not_exists)
                    return {};
                else
                    throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
            }
        }

        res = StorageFactory::instance().get(
            create, data_path, table_name, database_name, context, context.getGlobalContext(),
            columns.columns, columns.materialized_columns, columns.alias_columns, columns.column_defaults,
            create.attach, false);

        if (create.is_temporary)
            context.getSessionContext().addExternalTable(table_name, res);
        else
            database->createTable(context, table_name, res, query_ptr);
    }

    res->startup();

    /// If the CREATE SELECT query is, insert the data into the table
    if (create.select && !create.is_view && (!create.is_materialized_view || create.is_populate))
    {
        auto table_lock = res->lockStructure(true, __PRETTY_FUNCTION__);

        /// Also see InterpreterInsertQuery.
        BlockOutputStreamPtr out =
            std::make_shared<ProhibitColumnsBlockOutputStream>(
                std::make_shared<AddingDefaultBlockOutputStream>(
                    std::make_shared<MaterializingBlockOutputStream>(
                        std::make_shared<PushingToViewsBlockOutputStream>(
                            create.database, create.table,
                            create.is_temporary ? context.getSessionContext() : context,
                            query_ptr)),
                    /// @note shouldn't these two contexts be session contexts in case of temporary table?
                    columns.columns, columns.column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults)),
                columns.materialized_columns);

        BlockIO io;
        io.in_sample = as_select_sample;
        io.in = std::make_shared<NullAndDoCopyBlockInputStream>(interpreter_select->execute().in, out);

        return io;
    }

    return {};
}


BlockIO InterpreterCreateQuery::execute()
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_ptr);

    /// CREATE|ATTACH DATABASE
    if (!create.database.empty() && create.table.empty())
    {
        return createDatabase(create);
    }
    else
        return createTable(create);
}


}
