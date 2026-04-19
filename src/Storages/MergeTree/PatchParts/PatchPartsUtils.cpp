#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/IndicesDescription.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/Context.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PatchParts getPatchesForPart(const MergeTreePartInfo & source_part, const DataPartPtr & patch_part)
{
    if (!patch_part->info.isPatch())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch part, got: {}", patch_part->name);

    return patch_part->getSourcePartsSet().getPatchParts(source_part, patch_part);
}

static String getColumnsHash(Names column_names)
{
    std::sort(column_names.begin(), column_names.end());

    SipHash hash;
    for (const auto & name : column_names)
        hash.update(name);

    return getSipHash128AsHexString(hash);
}

static void addCodecsForPatchSystemColumns(ColumnsDescription & columns_desc)
{
    /// Apply for these columns the same codecs as for the virtual columns in the original parts.
    /// `_part_offset` only exists in v1 patches; v2 patches drop it, so the modify is guarded.
    columns_desc.modify(BlockNumberColumn::name, [&](auto & column_desc)
    {
        column_desc.codec = BlockNumberColumn::codec;
    });

    columns_desc.modify(BlockOffsetColumn::name, [&](auto & column_desc)
    {
        column_desc.codec = BlockOffsetColumn::codec;
    });

    if (columns_desc.has("_part_offset"))
    {
        columns_desc.modify("_part_offset", [&](auto & column_desc)
        {
            column_desc.codec = BlockOffsetColumn::codec;
        });
    }
}

StorageMetadataPtr getPatchPartMetadata(Block sample_block, ContextPtr local_context)
{
    ColumnsDescription columns_desc(sample_block.getNamesAndTypesList());
    return getPatchPartMetadata(std::move(columns_desc), local_context);
}

StorageMetadataPtr getPatchPartMetadata(ColumnsDescription patch_part_desc, ContextPtr local_context)
{
    StorageInMemoryMetadata part_metadata;

    /// Ensure patch part system columns are present.
    /// They may be missing when creating empty coverage parts
    /// (e.g. DROP PART for a patch part), because createEmptyPart
    /// only includes data columns from table metadata.
    for (const auto & col : getPatchPartSystemColumns())
        if (!patch_part_desc.has(col.name))
            patch_part_desc.add(ColumnDescription(col.name, col.type));

    /// Use hash of column names to put patch parts with different structure to different partitions.
    auto part_identifier = make_intrusive<ASTIdentifier>("_part");
    auto columns_hash = getColumnsHash(patch_part_desc.getNamesOfPhysical());
    auto hash_literal = make_intrusive<ASTLiteral>(std::move(columns_hash));

    auto partition_by_expression = makeASTFunction("__patchPartitionID", part_identifier, hash_literal);
    part_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_expression, patch_part_desc, {}, local_context);

    const auto & key_columns = getPatchPartKeyColumns();
    auto order_by_expression = makeASTOperator("tuple");

    for (const auto & [key_column_name, _] : key_columns)
        order_by_expression->arguments->children.push_back(make_intrusive<ASTIdentifier>(key_column_name));

    addCodecsForPatchSystemColumns(patch_part_desc);

    IndicesDescription secondary_indices;
    constexpr bool escape_index_filenames = true; /// It doesn't matter, the hardcoded names don't contain non ascii characters
    secondary_indices.push_back(createImplicitMinMaxIndexDescription(BlockNumberColumn::name, patch_part_desc, escape_index_filenames, local_context));
    secondary_indices.push_back(createImplicitMinMaxIndexDescription(BlockOffsetColumn::name, patch_part_desc, escape_index_filenames, local_context));

    part_metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_expression, patch_part_desc, {}, local_context);
    part_metadata.primary_key = KeyDescription::getKeyFromAST(order_by_expression, patch_part_desc, {}, local_context);
    part_metadata.primary_key.definition_ast = nullptr;
    part_metadata.setSecondaryIndices(std::move(secondary_indices));
    part_metadata.setColumns(std::move(patch_part_desc));

    return std::make_shared<StorageInMemoryMetadata>(std::move(part_metadata));
}

/// Unique marker mixed into the v2 partition-id hash. Guarantees that v2 patches never share a
/// partition with v1 patches (whose hash input is just the column names) even when the rest of
/// the schema coincides. Changing the marker is equivalent to an on-disk format break.
static constexpr auto PATCH_FORMAT_V2_HASH_MARKER = "__patch_format_v2__";

StorageMetadataPtr getPatchPartMetadataV2(
    ColumnsDescription patch_part_desc,
    const KeyDescription & main_sorting_key,
    UInt64 sort_key_prefix_size,
    ContextPtr local_context)
{
    StorageInMemoryMetadata part_metadata;

    /// Keep `_part` on disk — it's the first argument of the partition expression
    /// `__patchPartitionID(_part, hash(...))` that routes rows to the right
    /// `patch-<hash>-<original_partition_id>` partition, and dropping it from the stored
    /// columns breaks the sink's header match against the mutation pipeline (which always
    /// emits `_part`). In practice `_part` is `LowCardinality(String)` with at most a handful
    /// of distinct values — a few KB on disk per patch.
    ///
    /// Drop `_part_offset` — it was v1's sort-key tie-breaker and is purely dead weight on
    /// disk for v2 (the apply path keys on sort-key columns + `_block_number`/`_block_offset`,
    /// never on `_part_offset`). The mutation pipeline still emits the column but the writer
    /// filters down to the metadata's stored columns (`writeTempPartImpl` filters via
    /// `metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames())`), so the
    /// extra block column is silently dropped before anything touches disk.
    auto part_column_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    if (!patch_part_desc.has("_part"))
        patch_part_desc.add(ColumnDescription("_part", part_column_type));
    if (patch_part_desc.has("_part_offset"))
        patch_part_desc.remove("_part_offset");

    /// Ensure v2 identity + version columns are present (they may be missing when constructing
    /// an empty coverage part via createEmptyPart).
    auto ensure_column = [&](const String & name, const DataTypePtr & type)
    {
        if (!patch_part_desc.has(name))
            patch_part_desc.add(ColumnDescription(name, type));
    };
    ensure_column(BlockNumberColumn::name, BlockNumberColumn::type);
    ensure_column(BlockOffsetColumn::name, BlockOffsetColumn::type);
    ensure_column(PartDataVersionColumn::name, PartDataVersionColumn::type);

    /// Pull the sort-key expression list from the target table's KeyDescription and take only
    /// the first `sort_key_prefix_size` children — that's exactly the shape the patch was
    /// written with (see `SourcePartsSetForPatch::getSortKeyPrefixSize`). Slicing decouples the
    /// patch's on-disk layout from later additions to the target table's sort key; it also lets
    /// `ORDER BY tuple()` (or any shorter prefix) produce a sort key with only the two identity
    /// columns, matching the design's degenerate-sort-key case.
    const auto * sort_key_expr_list = main_sorting_key.expression_list_ast
        ? main_sorting_key.expression_list_ast->as<ASTExpressionList>()
        : nullptr;
    const size_t main_sort_key_children = sort_key_expr_list ? sort_key_expr_list->children.size() : 0;

    if (sort_key_prefix_size > main_sort_key_children)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Patch's persisted sort-key prefix size ({}) exceeds the main table's current sort-key children count ({})",
            sort_key_prefix_size, main_sort_key_children);

    /// Partition id: `__patchPartitionID(_part, hash(...))`. Hash input uses the *stored*
    /// column names (including `_part`), a serialized form of the target table's sort-key AST,
    /// and a v2 marker. Serializing the AST (not just the final column names) ensures that two
    /// tables with sort keys that collide in their *result* names but differ in expression
    /// structure still land in different partitions. The v2 marker guarantees that v1 and v2
    /// patches never land in the same partition even if they happen to cover identical columns.
    /// After `ALTER MODIFY ORDER BY`, the new AST text changes and pre-ALTER patches end up in a
    /// different partition than post-ALTER patches — they never co-merge. We hash the full
    /// main-table AST rather than the sliced prefix so the hash is stable against incidental
    /// reconstruction differences (fresh `ASTExpressionList` vs the parsed one) — any real
    /// change to the sort-key prefix already shows up in the main AST text.
    auto part_identifier = make_intrusive<ASTIdentifier>("_part");

    Names hash_input = patch_part_desc.getNamesOfPhysical();
    hash_input.emplace_back(sort_key_expr_list ? sort_key_expr_list->formatWithSecretsOneLine() : String{});
    hash_input.emplace_back(PATCH_FORMAT_V2_HASH_MARKER);
    auto columns_hash = getColumnsHash(std::move(hash_input));
    auto hash_literal = make_intrusive<ASTLiteral>(std::move(columns_hash));

    auto partition_by_expression = makeASTFunction("__patchPartitionID", part_identifier, hash_literal);
    part_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_expression, patch_part_desc, {}, local_context);

    /// Sort key: `(<sort_key_expr_children[0..sort_key_prefix_size]>..., _block_number,
    /// _block_offset)`. Primary key equals sort key — the widened sparse index subsumes the v1
    /// per-granule minmax index on `_block_number` and `_block_offset`, so no secondary indices
    /// are needed here. Building via `KeyDescription::getKeyFromAST` gives us `expression` (an
    /// ExpressionActions that materializes the result columns from physical source columns — the
    /// same way FINAL does for base parts), `column_names` (the result column names), and
    /// `reverse_flags` (we override the reverse flags from the per-shard DESC info since our AST
    /// children are plain expressions, not ASTStorageOrderByElements).
    auto order_by_expression = makeASTOperator("tuple");
    if (sort_key_expr_list)
    {
        for (size_t i = 0; i < sort_key_prefix_size; ++i)
            order_by_expression->arguments->children.push_back(sort_key_expr_list->children[i]->clone());
    }

    order_by_expression->arguments->children.push_back(make_intrusive<ASTIdentifier>(BlockNumberColumn::name));
    order_by_expression->arguments->children.push_back(make_intrusive<ASTIdentifier>(BlockOffsetColumn::name));

    addCodecsForPatchSystemColumns(patch_part_desc);
    part_metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_expression, patch_part_desc, {}, local_context);

    /// Honour DESC flags from the main table for the semantic sort-key prefix. The two trailing
    /// identity columns (`_block_number`, `_block_offset`) are always ASC.
    part_metadata.sorting_key.reverse_flags.assign(sort_key_prefix_size + 2, false);
    for (size_t i = 0; i < main_sorting_key.reverse_flags.size() && i < sort_key_prefix_size; ++i)
        part_metadata.sorting_key.reverse_flags[i] = main_sorting_key.reverse_flags[i];

    part_metadata.primary_key = part_metadata.sorting_key;
    part_metadata.primary_key.definition_ast = nullptr;

    part_metadata.setColumns(std::move(patch_part_desc));
    return std::make_shared<StorageInMemoryMetadata>(std::move(part_metadata));
}

StorageMetadataPtr getPatchPartMetadataV2(
    Block sample_block,
    const KeyDescription & main_sorting_key,
    UInt64 sort_key_prefix_size,
    ContextPtr local_context)
{
    ColumnsDescription columns_desc(sample_block.getNamesAndTypesList());
    return getPatchPartMetadataV2(std::move(columns_desc), main_sorting_key, sort_key_prefix_size, local_context);
}

const NamesAndTypesList & getPatchPartKeyColumns()
{
    static const NamesAndTypesList key_columns
    {
        {"_part", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_part_offset", std::make_shared<DataTypeUInt64>()},
    };

    return key_columns;
}

const NamesAndTypesList & getPatchPartSystemColumns()
{
    static const NamesAndTypesList other_columns
    {
        {BlockNumberColumn::name, BlockNumberColumn::type},
        {BlockOffsetColumn::name, BlockOffsetColumn::type},
        {PartDataVersionColumn::name, PartDataVersionColumn::type},
    };

    static const NamesAndTypesList all_columns = []
    {
        auto columns = getPatchPartKeyColumns();
        columns.insert(columns.end(), other_columns.begin(), other_columns.end());
        return columns;
    }();

    return all_columns;
}

bool isPatchPartSystemColumn(const String & column_name)
{
    static const NameSet system_columns_set = getPatchPartSystemColumns().getNameSet();
    return system_columns_set.contains(column_name);
}

std::pair<UInt64, UInt64> getPartNameRange(const ColumnLowCardinality & part_name_column, const String & part_name)
{
    auto indices = collections::range(0, part_name_column.size());

    const auto [begin, end] = std::ranges::equal_range(
        indices,
        std::string_view{part_name},
        std::less{},
        [&](const auto idx) { return part_name_column.getDataAt(idx); });

    return {begin - indices.begin(), end - indices.begin()};
}

std::pair<UInt64, UInt64> getPartNameOffsetRange(
    const ColumnLowCardinality & part_name_column,
    const PaddedPODArray<UInt64> & part_offset_data,
    const String & part_name,
    UInt64 part_offset_begin,
    UInt64 part_offset_end)
{
    using NameWithIdx = std::pair<std::string_view, UInt64>;

    auto compare = [&part_name_column, &part_offset_data](size_t index, const NameWithIdx & name_with_idx) -> int
    {
        const auto & [name, result_idx] = name_with_idx;

        auto data = part_name_column.getDataAt(index);
        int res = memcmp(data.data(), name.data(), std::min(data.size(), name.size()));

        if (res != 0)
            return res;

        if (data.size() < name.size())
            return -1;

        if (data.size() > name.size())
            return 1;

        UInt64 patch_idx = part_offset_data[index];
        return patch_idx > result_idx ? 1 : (patch_idx < result_idx ? -1 : 0);
    };

    auto indices = collections::range(0, part_name_column.size());

    const size_t begin = std::lower_bound(
        indices.begin(), indices.end(),
        NameWithIdx{part_name, part_offset_begin},
        [&compare](size_t lhs, const NameWithIdx & rhs) { return compare(lhs, rhs) < 0; }) - indices.begin();

    const size_t end = std::upper_bound(
        indices.begin(), indices.end(),
        NameWithIdx{part_name, part_offset_end},
        [&compare](const NameWithIdx & lhs, size_t rhs) { return compare(rhs, lhs) > 0; }) - indices.begin();

    return {begin, end};
}

Names getVirtualsRequiredForPatch(const PatchPartInfoForReader & patch)
{
    Names columns;
    switch (patch.mode)
    {
        case PatchMode::Merge:
            columns = {"_part", "_part_offset"};
            break;
        case PatchMode::Join:
            columns = {BlockNumberColumn::name, BlockOffsetColumn::name};
            break;
        case PatchMode::MergeOnKey:
            /// v2 reads the **physical source columns** of the patch's sort-key expression plus
            /// the two identity columns. For a plain sort key the source set equals the result
            /// set; for an expression sort key (e.g. `ORDER BY cityHash64(id)`) the source set
            /// is just `{id}` and the result column `cityHash64(id)` is materialized by
            /// `sort_key.expression` at apply time. Read straight off `patch.sort_key`, which
            /// was populated at `PatchPartInfo` construction from the patch's own rebuilt
            /// metadata sliced to the persisted prefix length (see `makePatchSortKey`).
            columns = patch.sort_key.source_column_names;
            columns.emplace_back(BlockNumberColumn::name);
            columns.emplace_back(BlockOffsetColumn::name);
            break;
    }

    columns.push_back(PartDataVersionColumn::name);
    return columns;
}

bool isPatchPartitionId(const String & partition_id)
{
    return partition_id.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX) && partition_id.size() > MergeTreePartInfo::PATCH_PART_PREFIX_SIZE;
}

static void assertValidPartitionIdOfPatch(const String & partition_id)
{
    if (!isPatchPartitionId(partition_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition id {} of patch part is invalid", partition_id);
}

bool isPatchForPartition(const MergeTreePartInfo & info, const String & partition_id)
{
    if (!info.isPatch())
        return false;

    assertValidPartitionIdOfPatch(info.getPartitionId());
    static constexpr size_t prefix_size = MergeTreePartInfo::PATCH_PART_PREFIX_SIZE;
    std::string_view original_partition_id{info.getPartitionId().data() + prefix_size, info.getPartitionId().size() - prefix_size};
    return original_partition_id == partition_id;
}

String getOriginalPartitionIdOfPatch(const String & partition_id)
{
    assertValidPartitionIdOfPatch(partition_id);
    return partition_id.substr(MergeTreePartInfo::PATCH_PART_PREFIX_SIZE);
}

String getPartitionIdForPatch(const MergeTreePartition & partition)
{
    if (partition.value.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected one string value in partition key for patch part, got: {}", partition.value.size());

    const auto & value = partition.value[0];
    if (partition.value[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected one string value in partition key for patch part, got: {}", value.getType());

    return value.safeGet<String>();
}

static bool patchHasHigherDataVersion(const String & part_name, Int64 min_patch_version, Int64 max_patch_version, Int64 max_data_version)
{
    if (max_patch_version > max_data_version)
    {
        if (min_patch_version <= max_data_version)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Found patch part {} (min data version: {}, max data version: {}) that intersects mutation with version {}",
                part_name, min_patch_version, max_patch_version, max_data_version);
        }
        return true;
    }

    return false;
}

bool patchHasHigherDataVersion(const IMergeTreeDataPart & patch, Int64 max_data_version)
{
    Int64 min_patch_version = patch.getSourcePartsSet().getMinDataVersion();
    Int64 max_patch_version = patch.getSourcePartsSet().getMaxDataVersion();

    return patchHasHigherDataVersion(patch.name, min_patch_version, max_patch_version, max_data_version);
}

bool patchHasHigherDataVersion(const IMergeTreeDataPartInfoForReader & patch, Int64 max_data_version)
{
    Int64 min_patch_version = patch.getMinDataVersion();
    Int64 max_patch_version = patch.getMaxDataVersion();

    return patchHasHigherDataVersion(patch.getPartName(), min_patch_version, max_patch_version, max_data_version);
}

PartsRange getPatchesToApplyOnMerge(const std::vector<MergeTreePartInfo> & patch_parts, const PartsRange & range, Int64 next_mutation_version)
{
    if (range.empty())
        return {};

    /// Set to infinity value for convenience.
    if (next_mutation_version == 0)
        next_mutation_version = std::numeric_limits<Int64>::max();

    Int64 min_source_data_version = std::numeric_limits<Int64>::max();
    for (const auto & part : range)
        min_source_data_version = std::min(min_source_data_version, part.info.getDataVersion());

    /// There is no room for increasing mutation version.
    if (min_source_data_version + 1 == next_mutation_version)
        return {};

    PartsRange result;
    for (const auto & patch : patch_parts)
    {
        auto max_patch_version = patch.getDataVersion();
        if (max_patch_version > next_mutation_version)
            continue;

        /// Patch may intersect with min version in part.
        /// So we cannot call 'patchHasHigherDataVersion'.
        if (max_patch_version <= min_source_data_version)
            continue;

        result.push_back(PartProperties
        {
            .name = patch.getPartNameV1(),
            .info = patch,
        });
    }

    return result;
}

std::optional<Int64> getMinUpdateBlockNumber(const CommittingBlocksSet & committing_blocks)
{
    for (const auto & block : committing_blocks)
    {
        if (block.op == CommittingBlock::Op::Update || block.op == CommittingBlock::Op::Unknown)
            return block.number;
    }
    return std::nullopt;
}

PatchesByPartition getPatchPartsByPartition(const DataPartsVector & patch_parts)
{
    PatchesByPartition res;
    for (const auto & patch : patch_parts)
    {
        auto partition_id = patch->info.getOriginalPartitionId();
        res[partition_id].push_back(patch);
    }
    return res;
}

PatchesByPartition getPatchPartsByPartition(const DataPartsVector & patch_parts, const PartitionIdToMaxBlockPtr & partitions)
{
    if (!partitions)
        return getPatchPartsByPartition(patch_parts);

    PatchesByPartition res;
    for (const auto & patch : patch_parts)
    {
        auto partition_id = patch->info.getOriginalPartitionId();
        auto it = partitions->find(partition_id);

        if (it != partitions->end() && !patchHasHigherDataVersion(*patch, it->second))
            res[partition_id].push_back(patch);
    }
    return res;
}

PatchInfosByPartition getPatchPartsByPartition(const std::vector<MergeTreePartInfo> & patch_parts, Int64 max_data_version)
{
    PatchInfosByPartition res;
    for (const auto & patch : patch_parts)
    {
        if (patch.getDataVersion() < max_data_version)
        {
            auto partition_id = patch.getOriginalPartitionId();
            res[partition_id].push_back(patch);
        }
    }
    return res;
}

PatchInfosByPartition getPatchPartsByPartition(const std::vector<MergeTreePartInfo> & patch_parts, const CommittingBlocks & committing_blocks)
{
    PatchInfosByPartition res;
    for (const auto & info : patch_parts)
    {
        auto partition_id = info.getOriginalPartitionId();
        auto data_version = info.getDataVersion();
        auto it = committing_blocks.find(partition_id);

        if (it == committing_blocks.end() || it->second.empty() || data_version < it->second.begin()->number)
            res[partition_id].push_back(info);
    }
    return res;
}

}
