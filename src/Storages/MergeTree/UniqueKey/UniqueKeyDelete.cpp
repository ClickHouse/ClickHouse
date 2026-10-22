#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyTxnCommit.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageMergeTree.h>

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <map>
#include <set>


namespace ProfileEvents
{
    extern const Event UniqueKeyDeleteRows;
}


namespace DB
{

namespace FailPoints
{
    extern const char unique_key_delete_pause_before_commit[];
}


namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

/// A merge that retires a target between a partition's scan and its commit aborts the commit;
/// each retry rescans the partition at a new snapshot, which sees the merged part.
constexpr size_t MAX_ATTEMPTS_PER_PARTITION = 5;

/// The partitions of `IN PARTITION`, or nullopt without one
std::optional<std::set<String>> requestedPartitions(const MergeTreeData & storage, const ASTDeleteQuery & query, const ContextPtr & context)
{
    if (query.partitions)
    {
        std::set<String> partition_ids;
        for (const auto & partition : query.partitions->children)
            partition_ids.insert(storage.getPartitionIDFromQuery(partition, context));
        return partition_ids;
    }

    if (query.partition)
        return std::set<String>{storage.getPartitionIDFromQuery(query.partition, context)};

    return std::nullopt;
}

/// The partitions the predicate can touch, in partition id order
std::vector<String> partitionsToVisit(const MergeTreeData & storage, const ASTDeleteQuery & query, const ContextPtr & context)
{
    auto partition_ids = storage.getPartitionIdsPrunedByPredicate(
        query.predicate, context, /*command_runs_in_background=*/false, nullptr);
    if (!partition_ids)
    {
        auto all = storage.getAllPartitionIds();
        partition_ids.emplace(all.begin(), all.end());
    }

    if (auto requested = requestedPartitions(storage, query, context))
        std::erase_if(*partition_ids, [&](const String & partition_id) { return !requested->contains(partition_id); });

    return {partition_ids->begin(), partition_ids->end()};
}

/// The predicate reads only the deleted table: a subquery, or a table on the right of `IN`, brings in SQL with its
/// own settings, whose limits could end it early and leave a `NOT IN` set incomplete.
/// A column with an expression brings it in: an `ALIAS` column on every read, a `DEFAULT` or `MATERIALIZED` one in a
/// part written before it was added. So the expressions of the columns it names are checked too.
void throwIfPredicateReadsTables(const IAST & ast, const ColumnsDescription & columns, NameSet & expanded_columns)
{
    if (ast.as<ASTSubquery>())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "DELETE on a UNIQUE KEY table does not support subqueries");

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        /// Every dotted run of the parts, for a column qualified with its table or one whose subcolumn is named.
        const auto & parts = identifier->name_parts;
        for (size_t begin = 0; begin < parts.size(); ++begin)
        {
            String name;
            for (size_t end = begin; end < parts.size(); ++end)
            {
                name += (end == begin ? "" : ".") + parts[end];
                if (const auto * column = columns.tryGet(name);
                    column && column->default_desc.expression && expanded_columns.insert(column->name).second)
                    throwIfPredicateReadsTables(*column->default_desc.expression, columns, expanded_columns);
            }
        }
        return;
    }

    if (const auto * function = ast.as<ASTFunction>();
        function && functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
    {
        const auto & rhs = function->arguments->children[1];
        if ((rhs->as<ASTIdentifier>() && !columns.has(rhs->as<ASTIdentifier>()->name())) || rhs->as<ASTTableIdentifier>())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "DELETE on a UNIQUE KEY table does not support a table on the right of `{}`", function->name);
    }

    for (const auto & child : ast.children)
        throwIfPredicateReadsTables(*child, columns, expanded_columns);
}

/// `READ_COLUMN _part, _part_offset` plus an `UPDATE _row_exists = 0 IN PARTITION ID ... WHERE <predicate>`
/// that is never applied: it only carries the predicate and the partition to `MutationsInterpreter`.
/// An UPDATE because a DELETE stage reads every column; the UPDATE reads only the predicate's.
MutationCommands scanCommands(
    const ASTDeleteQuery & query, const StorageInMemoryMetadata & metadata, const String & partition_id, const ContextPtr & context)
{
    /// `READ_COLUMN` and the `IN PARTITION ID` filter on `_partition_id` resolve their column by name,
    /// where a table column of the same name wins.
    for (const auto * name : {"_part", "_part_offset", "_partition_id"})
        if (metadata.getColumns().has(name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "DELETE on a UNIQUE KEY table reads the virtual column `{}`, which a table column of the same name shadows", name);

    MutationCommands commands;
    for (const auto * name : {"_part", "_part_offset"})
    {
        const auto column = metadata.virtuals.get(name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::Reader);
        commands.push_back({.type = MutationCommand::READ_COLUMN, .column_name = column.name, .data_type = column.type});
    }

    auto partition = make_intrusive<ASTPartition>();
    partition->setPartitionID(make_intrusive<ASTLiteral>(partition_id));

    auto assignment = make_intrusive<ASTAssignment>();
    assignment->column_name = RowExistsColumn::name;
    assignment->children.push_back(make_intrusive<ASTLiteral>(Field(UInt8(0))));
    auto assignments = make_intrusive<ASTExpressionList>();
    assignments->children.push_back(std::move(assignment));

    auto alter = make_intrusive<ASTAlterCommand>();
    alter->type = ASTAlterCommand::UPDATE;
    alter->update_assignments = alter->children.emplace_back(std::move(assignments)).get();
    alter->partition = alter->children.emplace_back(std::move(partition)).get();
    alter->predicate = alter->children.emplace_back(query.predicate->clone()).get();

    const auto & settings = context->getSettingsRef();
    commands.push_back(*MutationCommand::parse(
        *alter,
        /*parse_alter_commands=*/false,
        /*with_pure_metadata_commands=*/false,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]));
    return commands;
}

/// Adds a block's matches without a `Field` per row: `_part` through its dictionary index
void addMatches(const Block & block, std::map<String, DeleteBitmap> & rows_by_part_name)
{
    const auto part_column = block.getByName("_part").column->convertToFullColumnIfConst();
    const auto & parts = assert_cast<const ColumnLowCardinality &>(*part_column);
    const auto & offsets = assert_cast<const ColumnUInt64 &>(*block.getByName("_part_offset").column).getData();

    std::vector<DeleteBitmap *> rows_by_index(parts.getDictionary().size(), nullptr);
    for (size_t row = 0; row < block.rows(); ++row)
    {
        auto & rows = rows_by_index[parts.getIndexAt(row)];
        if (!rows)
            rows = &rows_by_part_name[String(parts.getDataAt(row))];
        rows->add(offsets[row]);
    }
}

struct DeleteStatement
{
    StorageMergeTree & storage;
    const ASTDeleteQuery & query;
    const ContextPtr query_context;
    /// The one the predicate was checked against.
    const StorageMetadataPtr metadata_snapshot;
    const LoggerPtr log;

    /// The rows of one partition the predicate matches at `txn`'s snapshot. Scanned like a lightweight
    /// update: the storage is read directly, so only `ALTER DELETE` is checked.
    DeleteRowsByPart findRowsInPartition(const MergeTreeTransactionPtr & txn, const String & partition_id) const
    {
        auto scan_context = Context::createCopy(query_context);
        scan_context->makeQueryContext();
        scan_context->setCurrentTransaction(txn);

        /// The DELETE ignores the query's read limits; in `throw` mode the scan's index analysis would check them.
        for (const auto * name : {"max_rows_to_read", "max_bytes_to_read", "max_rows_to_read_leaf", "max_bytes_to_read_leaf"})
            scan_context->setSetting(name, Field(UInt64(0)));

        MutationsInterpreter::Settings settings(/*can_execute_=*/true);
        settings.return_mutated_rows = true;
        settings.max_threads = query_context->getSettingsRef()[Setting::max_threads];

        MutationsInterpreter interpreter(
            storage.shared_from_this(),
            metadata_snapshot,
            scanCommands(query, *metadata_snapshot, partition_id, scan_context),
            scan_context,
            settings);

        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter.execute());
        pipeline.setProcessListElement(query_context->getProcessListElement());

        std::map<String, DeleteBitmap> rows_by_part_name;
        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
            if (block.rows())
                addMatches(block, rows_by_part_name);

        /// Otherwise a killed or timed-out scan looks like a DELETE that found fewer rows.
        /// `checkTimeLimit` throws on a kill and on a `throw`-mode timeout; a `break`-mode one truncated the scan.
        if (const auto status = pipeline.getProcessListElement(); status && !status->checkTimeLimit())
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "UNIQUE KEY DELETE: the scan of partition {} ran out of time before it finished", partition_id);

        DeleteRowsByPart rows_by_part;
        for (auto & [part_name, rows] : rows_by_part_name)
            rows_by_part.push_back({.part_name = part_name, .rows = std::make_shared<DeleteBitmap>(std::move(rows))});
        return rows_by_part;
    }

    /// Scan and commit one partition under one transaction, retrying on a conflict.
    /// Each partition has its own snapshot, so a DELETE is atomic per partition, not statement-wide as a mutation.
    /// Returns the newly-dead rows committed.
    size_t deleteInPartition(const String & partition_id) const
    {
        for (size_t attempt = 1;; ++attempt)
        {
            auto uk_txn = beginUniqueKeyTransaction(query_context, "DELETE");
            const auto rows_by_part = findRowsInPartition(uk_txn.getTransaction(), partition_id);
            if (rows_by_part.empty())
                return 0;

            /// The window a merge can retire the scanned parts in, otherwise too narrow to hit.
            FailPointInjection::pauseFailPoint(FailPoints::unique_key_delete_pause_before_commit);

            try
            {
                return UniqueKeyTxnCommit::deleteRows(
                    storage, {.transaction = uk_txn, .partition_id = partition_id, .rows_by_part = rows_by_part});
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::ABORTED)
                    throw;

                if (attempt == MAX_ATTEMPTS_PER_PARTITION)
                {
                    e.addMessage("UNIQUE KEY DELETE gave up on partition {} after {} attempts", partition_id, attempt);
                    throw;
                }

                LOG_DEBUG(log, "UNIQUE KEY DELETE (partition {}): attempt {} conflicted, rescanning: {}",
                    partition_id, attempt, e.message());
            }
        }
    }
};

}

void StorageMergeTree::deleteByUniqueKey(const ASTPtr & query_ptr, ContextPtr query_context)
{
    assertNotReadonly();

    if (!hasUniqueKey())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "deleteByUniqueKey called on table {} without UNIQUE KEY", getStorageID().getNameForLogs());

    /// Refused before pruning, so a DELETE with no partition to visit is refused too.
    throwIfInsideTransaction(query_context->getCurrentTransaction(), "DELETE");

    const auto & query = query_ptr->as<ASTDeleteQuery &>();
    const auto metadata_snapshot = getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    NameSet expanded_columns;
    throwIfPredicateReadsTables(*query.predicate, metadata_snapshot->getColumns(), expanded_columns);

    const DeleteStatement statement{
        .storage = *this,
        .query = query,
        .query_context = query_context,
        .metadata_snapshot = metadata_snapshot,
        .log = getLogger(getLogName())};

    const auto partition_ids = partitionsToVisit(*this, query, query_context);

    size_t total_committed_rows = 0;
    for (const auto & partition_id : partition_ids)
        total_committed_rows += statement.deleteInPartition(partition_id);

    ProfileEvents::increment(ProfileEvents::UniqueKeyDeleteRows, total_committed_rows);
}

}
