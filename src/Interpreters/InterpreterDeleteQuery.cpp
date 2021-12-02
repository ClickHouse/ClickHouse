#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Storages/MergeTree/MergeTreeDataPartDeletedMask.h>

#include <fmt/format.h>
#include <type_traits>

#include <Core/iostream_debug_helpers.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
using namespace DB;

template <typename ColumnType>
typename ColumnType::Ptr slice(const ColumnType & source_col, size_t begin, size_t end)
{
    auto result = ColumnType::create();
    if constexpr (std::is_base_of_v<ColumnVectorHelper, ColumnType>)
    {
        const auto & source_data = source_col.getData();
        auto & dest_data = result->getData();
        dest_data.insert(source_data.begin() + begin, source_data.begin() + end);
    }
    else
    {
        result->insertManyFrom(source_col, begin, end - begin);
    }
    return typename ColumnType::Ptr(std::move(result));
}

void updateDeletedMask(const ColumnUInt8 & mask_column, const String & part_name, size_t begin, size_t end, StorageMergeTree * table)
{
    DUMP("!!!! ", table->getStorageID(), mask_column, begin, end);

    const auto part_size = end - begin;
    if (part_size && !part_name.empty())
    {
        auto deleted_rows = slice(mask_column, begin, end);
        DUMP("!!!\t\tSetting new deleted mask to part: ", part_name, *deleted_rows);
        table->updateDeletedRowsMask(part_name, deleted_rows);
    }
}

auto getDeletedRows(const ASTDeleteQuery & delete_query, ContextPtr context, StorageMergeTree * table)
{
    // Fetch a mask of all deleted rows,
    // split that mask into part-sized columns, and assign mask to part
    // flush the part info.

    // TODO (nemkov): validate that delete_query.predicate is determenistic.

    WriteBufferFromOwnString buf;
    delete_query.predicate->format(IAST::FormatSettings(buf, false));
    buf.finalize();

    const auto & database = delete_query.database.empty() ? context->getCurrentDatabase() : delete_query.database;

    auto query = fmt::format("SELECT _part, or(({}), _is_deleted) FROM {}.{}",
            buf.str(),
            database,
            delete_query.table);
    DUMP(query);

    ParserQuery parser_query(query.data() + query.size());
    const auto & settings = context->getSettingsRef();
    ASTPtr query_ast = parseQuery(parser_query, query, settings.max_query_size, settings.max_parser_depth);

    SelectQueryOptions query_options(QueryProcessingStage::Complete);
    query_options.is_internal = true;
    query_options.modify_inplace = true;

    auto select_result = InterpreterSelectWithUnionQuery(
            query_ast, context,
            query_options).execute();

    PullingPipelineExecutor executor(select_result.pipeline);
    Block block;

    while (executor.pull(block))
    {
        // Now extract mask specifit to a each part,
        // we assume that blocks are part-sized and no block crosses part boundary.
        // Basically find part that has name different from the prev_part_name,
        // slice mask column from start_pos to end_pos and set it to apropriate data_part
        // as MergeTreeDataPartDeletedMask instance.
        DUMP("!!!!!!!!!!!! ", block);

        const auto & cols = block.getColumns();
        if (cols.size() < 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expecting 2 at least columns");

        const auto & part_name_col = *cols[0];
        const auto * mask_col = typeid_cast<const ColumnUInt8 *>(cols[1].get());

        size_t part_start_row = 0;
        String current_part_name;
        size_t current_row = 0;

        for (; current_row < part_name_col.size(); ++current_row)
        {
            const auto part_name = part_name_col.getDataAt(current_row);
            if (part_name != current_part_name)
            {
                updateDeletedMask(*mask_col, current_part_name, part_start_row, current_row, table);

                part_start_row = current_row;
                current_part_name = part_name.toString();
            }
        }
        if (!current_part_name.empty() && current_row)
            updateDeletedMask(*mask_col, current_part_name, part_start_row, current_row, table);
    }
    return select_result;
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

    return getDeletedRows(query, getContext(), table);
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

//    return {};
}

}
