#include <Interpreters/SystemLogStorage.h>

#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <Interpreters/BackupLog.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Common/SystemLogBase.h>
#include <Common/ActionLock.h>
#include <Common/logger_useful.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

namespace
{

class StorageWithComment : public IAST
{
public:
    ASTPtr storage;
    ASTPtr comment;

    String getID(char) const override { return "Storage with comment definition"; }

    ASTPtr clone() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method clone is not supported");
    }

    void formatImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported");
    }
};

class ParserStorageWithComment : public IParserBase
{
protected:
    const char * getName() const override { return "storage definition with comment"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
        ASTPtr storage;

        if (!storage_p.parse(pos, storage, expected))
            return false;

        ParserKeyword s_comment("COMMENT");
        ParserStringLiteral string_literal_parser;
        ASTPtr comment;

        if (s_comment.ignore(pos, expected))
            string_literal_parser.parse(pos, comment, expected);

        auto storage_with_comment = std::make_shared<StorageWithComment>();
        storage_with_comment->storage = std::move(storage);
        storage_with_comment->comment = std::move(comment);

        node = storage_with_comment;
        return true;
    }
};

}

template <typename LogElement>
SystemLogStorage<LogElement>::SystemLogStorage(ContextPtr context_, const String & database, const String & table, const String & engine, bool prepare)
    : WithContext(context_), log(&Poco::Logger::get("SystemLogStorage (" + database + "." + table + ")"))
    , table_id(database, table), storage_def(engine), create_query(serializeAST(*getCreateTableQuery()))
{
    if (prepare)
        prepareTable();
}

template <typename LogElement>
void SystemLogStorage<LogElement>::prepareTable()
{
    String description = table_id.getNameForLogs();

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (table)
    {
        if (old_create_query.empty())
        {
            old_create_query = serializeAST(*getCreateTableQueryClean(table_id, getContext()));
            if (old_create_query.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty CREATE QUERY for {}", backQuoteIfNeed(table_id.table_name));
        }

        if (old_create_query != create_query)
        {
            /// Rename the existing table.
            int suffix = 0;
            while (DatabaseCatalog::instance().isTableExist(
                {table_id.database_name, table_id.table_name + "_" + toString(suffix)}, getContext()))
                ++suffix;

            auto rename = std::make_shared<ASTRenameQuery>();
            ASTRenameQuery::Element elem
            {
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name)
                },
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name + "_" + toString(suffix))
                }
            };

            LOG_DEBUG(
                log,
                "Existing table {} for system log has obsolete or different structure. Renaming it to {}.\nOld: {}\nNew: {}\n.",
                description,
                backQuoteIfNeed(elem.to.getTable()),
                old_create_query,
                create_query);

            rename->elements.emplace_back(std::move(elem));

            ActionLock merges_lock;
            if (DatabaseCatalog::instance().getDatabase(table_id.database_name)->getUUID() == UUIDHelpers::Nil)
                merges_lock = table->getActionLock(ActionLocks::PartsMerge);

            auto query_context = Context::createCopy(context);
            /// As this operation is performed automatically we don't want it to fail because of user dependencies on log tables
            query_context->setSetting("check_table_dependencies", Field{false});
            query_context->setSetting("check_referential_table_dependencies", Field{false});
            query_context->makeQueryContext();
            InterpreterRenameQuery(rename, query_context).execute();

            /// The required table will be created.
            table = nullptr;
        }
        else if (!is_prepared)
            LOG_DEBUG(log, "Will use existing table {} for {}", description, LogElement::name());
    }

    if (!table)
    {
        /// Create the table.
        LOG_DEBUG(log, "Creating new table {} for {}", description, LogElement::name());

        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();

        auto create_query_ast = getCreateTableQuery();
        InterpreterCreateQuery interpreter(create_query_ast, query_context);
        interpreter.setInternal(true);
        interpreter.execute();

        table = DatabaseCatalog::instance().getTable(table_id, getContext());

        old_create_query.clear();
    }

    is_prepared = true;
}

template <typename LogElement>
void SystemLogStorage<LogElement>::add(const LogElement & element)
{
    add(LogElements{element});
}

template <typename LogElement>
void SystemLogStorage<LogElement>::add(const LogElements & elements)
{
    try
    {
        /// We check for existence of the table and create it as needed at every
        /// flush. This is done to allow user to drop the table at any moment
        /// (new empty table will be created automatically). BTW, flush method
        /// is called from single thread.
        prepareTable();

        ColumnsWithTypeAndName log_element_columns;
        auto log_element_names_and_types = LogElement::getNamesAndTypes();

        for (const auto & name_and_type : log_element_names_and_types)
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        Block block(std::move(log_element_columns));

        MutableColumns columns = block.mutateColumns();
        for (const auto & elem : elements)
            elem.appendToBlock(columns);

        block.setColumns(std::move(columns));

        /// We write to table indirectly, using InterpreterInsertQuery.
        /// This is needed to support DEFAULT-columns in table.

        auto insert = std::make_unique<ASTInsertQuery>();
        insert->table_id = table_id;
        ASTPtr query_ptr(insert.release());

        // we need query context to do inserts to target table with MV containing subqueries or joins
        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();
        /// We always want to deliver the data to the original table regardless of the MVs
        query_context->setSetting("materialized_views_ignore_errors", true);

        InterpreterInsertQuery interpreter(query_ptr, query_context);
        BlockIO io = interpreter.execute();

        PushingPipelineExecutor executor(io.pipeline);

        executor.start();
        executor.push(block);
        executor.finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template <typename LogElement>
void SystemLogStorage<LogElement>::flushAndShutdown()
{
    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (table)
        table->flushAndShutdown();
}

template <typename LogElement>
ASTPtr SystemLogStorage<LogElement>::getCreateTableQuery()
{
    auto create = std::make_shared<ASTCreateQuery>();

    create->setDatabase(table_id.database_name);
    create->setTable(table_id.table_name);

    auto new_columns_list = std::make_shared<ASTColumns>();

    if (const char * custom_column_list = LogElement::getCustomColumnList())
    {
        ParserColumnDeclarationList parser;
        const Settings & settings = getContext()->getSettingsRef();

        ASTPtr columns_list_raw = parseQuery(parser, custom_column_list, "columns declaration list", settings.max_query_size, settings.max_parser_depth);
        new_columns_list->set(new_columns_list->columns, columns_list_raw);
    }
    else
    {
        auto ordinary_columns = LogElement::getNamesAndTypes();
        auto alias_columns = LogElement::getNamesAndAliases();

        new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(ordinary_columns, alias_columns));
    }

    create->set(create->columns_list, new_columns_list);

    ParserStorageWithComment storage_parser;

    ASTPtr storage_with_comment_ast = parseQuery(
        storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
        "Storage to create table for " + LogElement::name(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    StorageWithComment & storage_with_comment = storage_with_comment_ast->as<StorageWithComment &>();

    create->set(create->storage, storage_with_comment.storage);
    create->set(create->comment, storage_with_comment.comment);

    /// Write additional (default) settings for MergeTree engine to make it make it possible to compare ASTs
    /// and recreate tables on settings changes.
    const auto & engine = create->storage->engine->as<ASTFunction &>();
    if (endsWith(engine.name, "MergeTree"))
    {
        auto storage_settings = std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
        storage_settings->loadFromQuery(*create->storage, getContext());
    }

    return create;
}

/// returns CREATE TABLE query, but with removed UUID
/// That way it can be used to compare with the SystemLog::getCreateTableQuery()
ASTPtr getCreateTableQueryClean(const StorageID & table_id, ContextPtr context)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    ASTPtr old_ast = database->getCreateTableQuery(table_id.table_name, context);
    auto & old_create_query_ast = old_ast->as<ASTCreateQuery &>();
    /// Reset UUID
    old_create_query_ast.uuid = UUIDHelpers::Nil;
    return old_ast;
}

void validateEngineDefinition(const String & engine, const String & description)
{
    ParserStorageWithComment storage_parser;
    parseQuery(storage_parser, engine.data(), engine.data() + engine.size(), description, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
}

#define INSTANTIATE_SYSTEM_LOG_STORAGE(ELEMENT) template class SystemLogStorage<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_STORAGE)

INSTANTIATE_SYSTEM_LOG_STORAGE(BackupInfoElement)
}
