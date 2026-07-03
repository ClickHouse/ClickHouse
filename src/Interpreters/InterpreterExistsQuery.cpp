#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
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
            String database = getContext()->resolveDatabase(exists_query->getDatabase());
            const auto & table = exists_query->getTable();
            /// A dictionary created by a DDL query is also registered among tables, so a plain `EXISTS <name>`
            /// query can refer to a dictionary. For such a dictionary `SHOW DICTIONARIES` is sufficient, which
            /// matches the behaviour of `EXISTS DICTIONARY <name>` and what the documentation promises.
            const auto access = getContext()->getAccess();
            bool allowed_as_dictionary = !access->isGranted(AccessType::SHOW_TABLES, database, table)
                && access->isGranted(AccessType::SHOW_DICTIONARIES, database, table)
                && DatabaseCatalog::instance().isDictionaryExist({database, table});
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
                getContext()->checkAccess(AccessType::SHOW_TABLES, database, table);
                result = DatabaseCatalog::instance().isTableExist({database, table}, getContext());
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
            String database = getContext()->resolveDatabase(exists_query->getDatabase());
            getContext()->checkAccess(AccessType::SHOW_TABLES, database, exists_query->getTable());
            auto table = DatabaseCatalog::instance().tryGetTable({database, exists_query->getTable()}, getContext());
            result = table && table->isView();
        }
    }
    else if ((exists_query = query_ptr->as<ASTExistsDatabaseQuery>()))
    {
        String database = getContext()->resolveDatabase(exists_query->getDatabase());
        getContext()->checkAccess(AccessType::SHOW_DATABASES, database);
        result = DatabaseCatalog::instance().isDatabaseExist(database);
    }
    else if ((exists_query = query_ptr->as<ASTExistsDictionaryQuery>()))
    {
        if (exists_query->isTemporary())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary dictionaries are not possible.");
        String database = getContext()->resolveDatabase(exists_query->getDatabase());
        getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, database, exists_query->getTable());
        result = DatabaseCatalog::instance().isDictionaryExist({database, exists_query->getTable()});
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
