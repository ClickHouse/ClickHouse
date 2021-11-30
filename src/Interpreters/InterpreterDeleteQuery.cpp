#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>

#include <fmt/format.h>

#include <Core/iostream_debug_helpers.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
using namespace DB;
auto getDeletedRows(const ASTDeleteQuery & delete_query, ContextPtr context)
{
//    auto query = std::make_shared<ASTSelectQuery>();
    WriteBufferFromOwnString buf;
    delete_query.predicate->format(IAST::FormatSettings(buf, false));
    buf.finalize();

    const auto & database = delete_query.database.empty() ? context->getCurrentDatabase() : delete_query.database;

    auto query = fmt::format("SELECT _part, _part_uuid, _row_id FROM {}.{} WHERE {}",
            database,
            delete_query.table,
            buf.str());
    DUMP(query);

    ParserQuery parser_query(query.data() + query.size());
    const auto & settings = context->getSettingsRef();
    ASTPtr query_ast = parseQuery(parser_query, query, settings.max_query_size, settings.max_parser_depth);

    SelectQueryOptions query_options(QueryProcessingStage::FetchColumns);
    query_options.is_internal = true;
    query_options.modify_inplace = true;

    auto select_result = InterpreterSelectWithUnionQuery(
            query_ast, context,
            query_options
//            Names {"_part", "_part_uuid", "_row_id"}
    ).execute();

    PullingPipelineExecutor executor(select_result.pipeline);
    Block block;
    while (executor.pull(block))
    {
        DUMP("!!!!!!!!!!!! ", block);
    }
}

}

namespace DB
{
InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_) { }

BlockIO InterpreterDeleteQuery::execute()
{
    const ASTDeleteQuery & query = query_ptr->as<ASTDeleteQuery &>();
    const StorageID table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);

    getContext()->checkAccess(AccessType::DELETE, table_id);

    StoragePtr table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());
    StorageMergeTree * const table = typeid_cast<StorageMergeTree *>(table_ptr.get());

    if (table == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only MergeTree tables are supported");

    getDeletedRows(query, getContext());
////    auto deleted_rows = getDeletedRows(query, getContext());
////    deleted_rows.pipeline.pulling()

//    // TODO
//    auto alter_lock = table->lockForAlter(
//        getContext()->getCurrentQueryId(),
//        getContext()->getSettingsRef().lock_acquire_timeout);

//    auto metadata_snapshot = table->getInMemoryMetadataPtr();

//    const MutationCommands commands = {{ .type = MutationCommand::DELETE, .predicate = query.predicate }};

//    // TODO
//    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
//    /// mutation expression, etc.
//    //AddDefaultDatabaseVisitor visitor(table_id.getDatabaseName());
//    //ASTPtr command_list_ptr = alter.command_list->ptr();
//    //visitor.visit(command_list_ptr);

//    table->checkMutationIsPossible(commands, getContext()->getSettingsRef());

//    MutationsInterpreter(table_ptr, metadata_snapshot, commands, getContext(), false).validate();

//    table->mutate(commands, getContext(), MutationType::Lightweight);

    return {};
}

}
