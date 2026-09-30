#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

/// Canonical target of the EXISTS query, so the access check and the existence probe see one object.
StorageID resolveExistsTarget(const ASTQueryWithTableAndOutput & query, ContextPtr context)
{
    StorageID storage_id{context->resolveDatabase(query.getDatabase()), query.getTable()};
    /// An implicit current database is already canonical and must not fold.
    storage_id.database_name_quote = query.getDatabase().empty()
        ? IdentifierPartQuote::DoubleQuoted : identifierPartQuoteFromAST(query.database);
    storage_id.table_name_quote = identifierPartQuoteFromAST(query.table);
    return DatabaseCatalog::instance().resolveStorageIDNames(std::move(storage_id), context);
}

}

BlockIO InterpreterExistsQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterExistsQuery::getSampleBlock()
{
    return Block{{
        ColumnUInt8::create(),
        std::make_shared<DataTypeUInt8>(),
        "result" }};
}


QueryPipeline InterpreterExistsQuery::executeImpl()
{
    ASTQueryWithTableAndOutput * exists_query = nullptr;
    bool result = false;

    if ((exists_query = query_ptr->as<ASTExistsTableQuery>()))
    {
        if (exists_query->isTemporary())
        {
            result = static_cast<bool>(getContext()->tryResolveStorageID(
                {"", exists_query->getTable()}, Context::ResolveExternal));
        }
        else
        {
            const StorageID storage_id = resolveExistsTarget(*exists_query, getContext());
            /// A dictionary created by a DDL query is also registered among tables, so a plain `EXISTS <name>`
            /// query can refer to a dictionary. For such a dictionary `SHOW DICTIONARIES` is sufficient, which
            /// matches the behaviour of `EXISTS DICTIONARY <name>` and what the documentation promises.
            const auto access = getContext()->getAccess();
            bool allowed_as_dictionary = !access->isGranted(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name)
                && access->isGranted(AccessType::SHOW_DICTIONARIES, storage_id.database_name, storage_id.table_name)
                && DatabaseCatalog::instance().isDictionaryExist(storage_id);
            if (allowed_as_dictionary)
            {
                /// The privilege decision was made by observing a dictionary via `isDictionaryExist`.
                /// Report existence from that same observation instead of a second `isTableExist` lookup:
                /// otherwise a concurrent drop of the dictionary and creation of a regular table under the
                /// same name could let a user with only `SHOW DICTIONARIES` see the regular table without
                /// the `SHOW TABLES` privilege, widening visibility for regular tables.
                result = true;
            }
            else
            {
                getContext()->checkAccess(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name);
                result = DatabaseCatalog::instance().isTableExist(storage_id, getContext());
            }
        }
    }
    else if ((exists_query = query_ptr->as<ASTExistsViewQuery>()))
    {
        if (exists_query->isTemporary())
        {
            auto storage_id = getContext()->tryResolveStorageID(
                {"", exists_query->getTable()}, Context::ResolveExternal);
            if (storage_id)
            {
                auto table = DatabaseCatalog::instance().tryGetTable(storage_id, getContext());
                result = table && table->isView();
            }
            else
            {
                result = false;
            }
        }
        else
        {
            const StorageID storage_id = resolveExistsTarget(*exists_query, getContext());
            getContext()->checkAccess(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name);
            auto table = DatabaseCatalog::instance().tryGetTable(storage_id, getContext());
            result = table && table->isView();
        }
    }
    else if ((exists_query = query_ptr->as<ASTExistsDatabaseQuery>()))
    {
        String database = DatabaseCatalog::instance().resolveDatabaseNameSpelling(
            getContext()->resolveDatabase(exists_query->getDatabase()), identifierPartQuoteFromAST(exists_query->database), getContext());
        getContext()->checkAccess(AccessType::SHOW_DATABASES, database);
        result = DatabaseCatalog::instance().isDatabaseExist(database);
    }
    else if ((exists_query = query_ptr->as<ASTExistsDictionaryQuery>()))
    {
        if (exists_query->isTemporary())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary dictionaries are not possible.");
        const StorageID storage_id = resolveExistsTarget(*exists_query, getContext());
        getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, storage_id.database_name, storage_id.table_name);
        result = DatabaseCatalog::instance().isDictionaryExist(storage_id);
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        ColumnUInt8::create(1, result),
        std::make_shared<DataTypeUInt8>(),
        "result" }})));
}

void registerInterpreterExistsQuery(InterpreterFactory & factory);
void registerInterpreterExistsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterExistsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterExistsQuery", create_fn);
}
}
