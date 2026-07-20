#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Databases/DatabaseOverlay.h>
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
            const StorageID dictionary_id{database, table};
            bool allowed_as_dictionary = !access->isGranted(AccessType::SHOW_TABLES, database, table)
                && access->isGranted(AccessType::SHOW_DICTIONARIES, database, table)
                && DatabaseCatalog::instance().isDictionaryExist(dictionary_id);
            if (allowed_as_dictionary)
            {
                /// The privilege decision was made by observing a dictionary via `isDictionaryExist`.
                /// Report existence from that same observation instead of a second `isTableExist` lookup:
                /// otherwise a concurrent drop of the dictionary and creation of a regular table under the
                /// same name could let a user with only `SHOW DICTIONARIES` see the regular table without
                /// the `SHOW TABLES` privilege, widening visibility for regular tables.
                result = true;

                /// Same rule as for tables: through a read-only `Overlay` facade a dictionary is visible
                /// only when `SHOW_DICTIONARIES` is also granted on the underlying source dictionary, so
                /// the facade cannot widen visibility. Report "does not exist" rather than throwing — the
                /// source-side check can only run when the dictionary exists, so a denial would itself leak
                /// existence. The source id is resolved from metadata only, without loading the source
                /// table: a load could throw the source's own error before the grant is proven, turning
                /// the facade into an oracle for hidden broken sources.
                if (const auto * facade
                    = DatabaseOverlay::asReadonlyFacade(DatabaseCatalog::instance().tryGetDatabase(database).get()))
                {
                    auto source_id = facade->resolveSourceTableIdNoLoad(table, getContext());
                    result = source_id
                        && access->isGranted(AccessType::SHOW_DICTIONARIES, source_id->database_name, source_id->table_name);
                }
            }
            else
            {
                getContext()->checkAccess(AccessType::SHOW_TABLES, database, table);
                result = DatabaseCatalog::instance().isTableExist({database, table}, getContext());

                /// Through a read-only `Overlay` facade a table is reported as existing only when
                /// `SHOW_TABLES` is also granted on the underlying source table: the facade must
                /// not widen visibility. Report "does not exist" instead of throwing — a denial
                /// here would itself leak existence, because the source-side check can only run
                /// when the table exists. The source id is resolved from metadata only, without
                /// loading the source table: a load could throw the source's own error before the
                /// grant is proven, turning the facade into an oracle for hidden broken sources.
                if (result)
                {
                    if (const auto * facade
                        = DatabaseOverlay::asReadonlyFacade(DatabaseCatalog::instance().tryGetDatabase(database).get()))
                    {
                        auto source_id = facade->resolveSourceTableIdNoLoad(table, getContext());
                        result = source_id
                            && access->isGranted(AccessType::SHOW_TABLES, source_id->database_name, source_id->table_name);
                    }
                }
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

            /// Same rule as for `EXISTS TABLE`: through a read-only `Overlay` facade a view is
            /// visible only when `SHOW_TABLES` is also granted on the underlying source. The
            /// grant is checked from a metadata-only resolution *before* the lookup that loads
            /// the source table: the load could throw the source's own error before the grant is
            /// proven, turning the facade into an oracle for hidden broken sources.
            bool source_visible = true;
            if (const auto * facade
                = DatabaseOverlay::asReadonlyFacade(DatabaseCatalog::instance().tryGetDatabase(database).get()))
            {
                auto source_id = facade->resolveSourceTableIdNoLoad(exists_query->getTable(), getContext());
                source_visible = source_id
                    && getContext()->getAccess()->isGranted(AccessType::SHOW_TABLES, source_id->database_name, source_id->table_name);
            }

            if (source_visible)
            {
                auto table = DatabaseCatalog::instance().tryGetTable({database, exists_query->getTable()}, getContext());
                result = table && table->isView();

                /// Re-verify against the loaded storage: the name could have started resolving to a
                /// different source between the metadata-only check above and the lookup.
                if (result)
                    if (auto source_id = DatabaseOverlay::getSourceTableIdForReadonlyFacade(StorageID{database, exists_query->getTable()}, table))
                        result = getContext()->getAccess()->isGranted(AccessType::SHOW_TABLES, source_id->database_name, source_id->table_name);
            }
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
        const auto & dictionary = exists_query->getTable();
        getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, database, dictionary);

        /// Same rule as for `EXISTS TABLE`: through a read-only `Overlay` facade a dictionary is
        /// visible only when `SHOW_DICTIONARIES` is also granted on the underlying source dictionary.
        /// The grant is checked from a metadata-only resolution *before* the lookup that loads the
        /// source table: the load could throw the source's own error before the grant is proven,
        /// turning the facade into an oracle for hidden broken sources.
        bool source_visible = true;
        if (const auto * facade
            = DatabaseOverlay::asReadonlyFacade(DatabaseCatalog::instance().tryGetDatabase(database).get()))
        {
            auto source_id = facade->resolveSourceTableIdNoLoad(dictionary, getContext());
            source_visible = source_id
                && getContext()->getAccess()->isGranted(AccessType::SHOW_DICTIONARIES, source_id->database_name, source_id->table_name);
        }

        if (source_visible)
        {
            auto storage = DatabaseCatalog::instance().tryGetTable({database, dictionary}, getContext());
            result = storage && storage->isDictionary();

            /// Re-verify against the loaded storage: the name could have started resolving to a
            /// different source between the metadata-only check above and the lookup.
            if (result)
                if (auto source_id = DatabaseOverlay::getSourceTableIdForReadonlyFacade(StorageID{database, dictionary}, storage))
                    result = getContext()->getAccess()->isGranted(AccessType::SHOW_DICTIONARIES, source_id->database_name, source_id->table_name);
        }
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
