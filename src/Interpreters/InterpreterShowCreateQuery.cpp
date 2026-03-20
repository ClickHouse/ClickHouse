#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool show_table_uuid_in_table_create_query_if_not_nil;
}

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Recursively strip database namespace from all ASTTableIdentifier nodes in the AST.
/// This ensures SHOW CREATE VIEW displays logical names in the SELECT clause,
/// even for tables where AddDefaultDatabaseVisitor filled in the physical database name.
void stripDatabaseNamespaceFromAST(IAST * ast, const ContextPtr & context)
{
    if (!ast)
        return;

    if (auto * table_id = ast->as<ASTTableIdentifier>())
    {
        if (table_id->compound())
        {
            String db = table_id->getDatabaseName();
            String stripped = context->stripDatabaseNamespace(db);
            if (stripped != db)
                table_id->resetTable(stripped, table_id->shortName());
        }
        return;
    }

    for (auto & child : ast->children)
        stripDatabaseNamespaceFromAST(child.get(), context);
}

}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


QueryPipeline InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateViewQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        auto resolve_table_type = show_query->isTemporary() ? Context::ResolveExternal : Context::ResolveOrdinary;
        auto table_id = getContext()->resolveStorageID(*show_query, resolve_table_type);

        bool is_dictionary = static_cast<bool>(query_ptr->as<ASTShowCreateDictionaryQuery>());

        if (is_dictionary)
            getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, table_id);
        else
            getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);

        create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, getContext());

        auto & ast_create_query = create_query->as<ASTCreateQuery &>();
        if (query_ptr->as<ASTShowCreateViewQuery>())
        {
            if (!ast_create_query.isView())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a VIEW",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
        else if (is_dictionary)
        {
            if (!ast_create_query.is_dictionary)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a DICTIONARY",
                    backQuote(ast_create_query.getDatabase()), backQuote(ast_create_query.getTable()));
        }
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->isTemporary())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary databases are not possible.");
        show_query->setDatabase(getContext()->resolveDatabase(show_query->getDatabase()));
        getContext()->checkAccess(AccessType::SHOW_DATABASES, show_query->getDatabase());
        create_query = DatabaseCatalog::instance().getDatabase(show_query->getDatabase())->getCreateDatabaseQuery();
    }

    if (!create_query)
        throw Exception(ErrorCodes::THERE_IS_NO_QUERY,
                        "Unable to show the create query of {}. Maybe it was created by the system.",
                        show_query->getTable());

    if (!getContext()->getSettingsRef()[Setting::show_table_uuid_in_table_create_query_if_not_nil])
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        create.uuid = UUIDHelpers::Nil;
        if (create.targets)
            create.targets->resetInnerUUIDs();
    }

    /// Strip database namespace prefix so the user sees logical names.
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        String db = create.getDatabase();
        if (!db.empty())
            create.setDatabase(getContext()->stripDatabaseNamespace(db));

        /// Also strip namespace from view target databases (e.g., the TO-table of a materialized view).
        if (create.targets)
        {
            for (auto & target : create.targets->targets)
            {
                if (!target.table_id.database_name.empty())
                    target.table_id.database_name = getContext()->stripDatabaseNamespace(target.table_id.database_name);
            }
        }

        /// Strip namespace from database references inside the SELECT clause.
        /// This covers both explicitly-qualified tables and those filled by
        /// AddDefaultDatabaseVisitor (which may store physical names in older metadata).
        if (create.select)
            stripDatabaseNamespaceFromAST(create.select, getContext());
    }

    MutableColumnPtr column = ColumnString::create();
    column->insert(format(
    {
        .ctx = getContext(),
        .query = *create_query,
        .one_line = false
    }));

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}})));
}

void registerInterpreterShowCreateQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowCreateQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterShowCreateQuery", create_fn);
}
}
