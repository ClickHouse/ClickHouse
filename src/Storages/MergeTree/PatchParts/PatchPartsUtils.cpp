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
    columns_desc.modify(BlockNumberColumn::name, [&](auto & column_desc)
    {
        column_desc.codec = BlockNumberColumn::codec;
    });

    columns_desc.modify(BlockOffsetColumn::name, [&](auto & column_desc)
    {
        column_desc.codec = BlockOffsetColumn::codec;
    });

    columns_desc.modify("_part_offset", [&](auto & column_desc)
    {
        column_desc.codec = BlockOffsetColumn::codec;
    });
}

StorageMetadataPtr getPatchPartMetadata(Block sample_block, ContextPtr local_context)
{
    ColumnsDescription columns_desc(sample_block.getNamesAndTypesList());
    return getPatchPartMetadata(std::move(columns_desc), local_context);
}

StorageMetadataPtr getPatchPartMetadata(ColumnsDescription patch_part_desc, ContextPtr local_context)
{
    StorageInMemoryMetadata part_metadata;

    /// Use hash of column names to put patch parts with different structure to different partitions.
    auto part_identifier = std::make_shared<ASTIdentifier>("_part");
    auto columns_hash = getColumnsHash(patch_part_desc.getNamesOfPhysical());
    auto hash_literal = std::make_shared<ASTLiteral>(std::move(columns_hash));

    auto partition_by_expression = makeASTFunction("__patchPartitionID", part_identifier, hash_literal);
    part_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_expression, patch_part_desc, local_context);

    const auto & key_columns = getPatchPartKeyColumns();
    auto order_by_expression = makeASTOperator("tuple");

    for (const auto & [key_column_name, _] : key_columns)
        order_by_expression->arguments->children.push_back(std::make_shared<ASTIdentifier>(key_column_name));

    addCodecsForPatchSystemColumns(patch_part_desc);

    IndicesDescription secondary_indices;
    secondary_indices.push_back(createImplicitMinMaxIndexDescription(BlockNumberColumn::name, patch_part_desc, local_context));
    secondary_indices.push_back(createImplicitMinMaxIndexDescription(BlockOffsetColumn::name, patch_part_desc, local_context));

    part_metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_by_expression, patch_part_desc, local_context, {});
    part_metadata.primary_key = KeyDescription::getKeyFromAST(order_by_expression, patch_part_desc, local_context);
    part_metadata.primary_key.definition_ast = nullptr;
    part_metadata.setSecondaryIndices(std::move(secondary_indices));
    part_metadata.setColumns(std::move(patch_part_desc));

    return std::make_shared<StorageInMemoryMetadata>(std::move(part_metadata));
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
        StringRef{part_name},
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
        int res = memcmp(data.data, name.data(), std::min(data.size, name.size()));

        if (res != 0)
            return res;

        if (data.size < name.size())
            return -1;

        if (data.size > name.size())
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
