#include <Storages/MergeTree/UniqueKey/UniqueKeyDeleteRowFinder.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/StorageMergeTree.h>

#include <Common/quoteString.h>

#include <fmt/core.h>

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

}

namespace DB::UniqueKeyTxn
{

UniqueKeyDeleteRowFinder::Result UniqueKeyDeleteRowFinder::group(
    const std::vector<PartRowEntry> & pairs,
    MergeTreeDataFormatVersion format_version)
{
    /// Bucket rows by part name first; this keeps Roaring construction off
    /// the per-pair hot loop and lets us sort the per-part vector once.
    std::unordered_map<PartName, std::vector<UInt32>> rows_by_part_name;
    for (const auto & e : pairs)
        rows_by_part_name[e.part_name].push_back(e.row_number);

    for (auto & [_, rows] : rows_by_part_name)
        std::sort(rows.begin(), rows.end());

    Result result;
    result.stats.total_matched_rows = pairs.size();
    for (auto & [part_name, rows] : rows_by_part_name)
    {
        auto info_opt = MergeTreePartInfo::tryParsePartName(part_name, format_version);
        if (!info_opt)
            continue;   /// pure-seam leniency; find() fail-closes first

        auto rb = std::make_shared<DeleteBitmap>();
        for (auto r : rows)
            rb->add(r);

        UniqueKeyDeleteRowsForPart entry;
        entry.part_name = part_name;
        entry.rows = std::move(rb);
        result.by_partition[info_opt->getPartitionId()].push_back(std::move(entry));
    }

    for (auto & [_, parts] : result.by_partition)
    {
        std::sort(parts.begin(), parts.end(),
            [](const auto & a, const auto & b) { return a.part_name < b.part_name; });
        result.stats.parts_with_hits += parts.size();
    }

    return result;
}

UniqueKeyDeleteRowFinder::Result UniqueKeyDeleteRowFinder::find(
    StorageMergeTree & storage,
    const ASTPtr & query_ptr,
    ContextPtr query_context)
{
    const auto & delete_query = query_ptr->as<ASTDeleteQuery &>();

    /// TODO(unique-key): translate the partition AST to a `_partition_id` filter
    /// (see MutationsInterpreter.cpp ~line 320) and AND it into the WHERE, then drop this guard.
    if (delete_query.partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DELETE ... IN PARTITION is not yet supported for UNIQUE KEY tables; "
            "use a partition-key predicate in the WHERE clause instead");

    const auto & storage_id = storage.getStorageID();

    /// Build a self-contained SELECT that pulls `(_part, _part_offset)` for
    /// rows satisfying the user's predicate. Using a SELECT is the simplest
    /// way to reuse MergeTree's read-path predicate plumbing (primary-key
    /// skipping, PREWHERE, skip indexes, bitmap row-level filtering so
    /// already-dead rows aren't re-added).
    String select_query = fmt::format(
        "SELECT _part, _part_offset FROM {}.{} WHERE {}",
        backQuoteIfNeed(storage_id.database_name),
        backQuoteIfNeed(storage_id.table_name),
        delete_query.predicate->formatWithSecretsOneLine());

    /// Internal cloned context so we don't pollute the caller's query id / logs.
    auto select_context = Context::createCopy(query_context);
    select_context->makeQueryContext();
    select_context->setCurrentQueryId("");

    /// Force the internal scan onto the local single-replica read path. It returns
    /// (_part, _part_offset) pairs that are converted straight into local delete
    /// bitmaps, so a part name / offset collected from another replica or a
    /// distributed fragment would target the wrong rows. Mirror MutationsInterpreter,
    /// which disables parallel replicas on its synthetic mutation read.
    /// (`enable_parallel_replicas` is the alias of
    /// `allow_experimental_parallel_reading_from_replicas`, so this also neutralizes
    /// `parallel_replicas_for_non_replicated_merge_tree`.)
    select_context->setSetting("enable_parallel_replicas", Field(0));
    select_context->setSetting("make_distributed_plan", false);

    /// Neutralize result/read size limits on the internal scan. This is the
    /// DELETE's row-finder machinery, not a user-bounded result, so a user-set
    /// max_result_rows / max_rows_to_read (under overflow_mode='break') must not
    /// truncate it to a prefix and silently under-delete. executeQuery(String)
    /// gives no SelectQueryOptions hook, so we zero only the size counts on the
    /// cloned context (0 means unlimited, which makes the overflow mode
    /// irrelevant); speed/time limits are intentionally left intact.
    select_context->setSetting("max_result_rows", Field(UInt64(0)));
    select_context->setSetting("max_result_bytes", Field(UInt64(0)));
    select_context->setSetting("max_rows_to_read", Field(UInt64(0)));
    select_context->setSetting("max_bytes_to_read", Field(UInt64(0)));
    select_context->setSetting("max_rows_to_read_leaf", Field(UInt64(0)));
    select_context->setSetting("max_bytes_to_read_leaf", Field(UInt64(0)));

    /// Drive the SELECT and collect `(part_name, row_number)` pairs; group
    /// them via the pure-function seam below.
    std::vector<PartRowEntry> pairs;

    auto io = executeQuery(select_query, std::move(select_context), QueryFlags{ .internal = true }).second;
    io.executeWithCallbacks([&]()
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (block.empty())
                continue;

            const auto & part_col = block.getByName("_part").column;
            const auto & offset_col = block.getByName("_part_offset").column;
            const size_t rows = block.rows();
            pairs.reserve(pairs.size() + rows);
            for (size_t i = 0; i < rows; ++i)
            {
                /// `_part` is LowCardinality(String) in the virtual-column
                /// descriptor; `Field` extraction handles both LC and plain
                /// String.
                Field part_field;
                part_col->get(i, part_field);
                PartName part_name = part_field.safeGet<String>();

                Field offset_field;
                offset_col->get(i, offset_field);
                const UInt64 row_number = offset_field.safeGet<UInt64>();
                if (row_number > std::numeric_limits<UInt32>::max())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "_part_offset {} exceeds UInt32 range on UNIQUE KEY DELETE", row_number);

                pairs.push_back({std::move(part_name), static_cast<UInt32>(row_number)});
            }
        }
    });

    /// `_part` is produced by the read pipeline for a live part, so every
    /// name must parse. A parse failure is corrupt state, not a tolerable
    /// edge case: fail closed rather than silently under-deleting that
    /// part's rows.
    for (const auto & e : pairs)
        if (!MergeTreePartInfo::tryParsePartName(e.part_name, storage.format_version))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "UNIQUE KEY DELETE row finder: unparseable part name '{}' from read pipeline",
                e.part_name);

    return group(pairs, storage.format_version);
}

}
