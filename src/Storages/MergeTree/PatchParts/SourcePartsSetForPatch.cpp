#include <Storages/MergeTree/PatchParts/SourcePartsSetForPatch.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_DATA_PART;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Build the shared semantic sorting key prefix `KeyDescription` from the target table's metadata.
/// Slices the original ORDER BY expression list by the prefix size.
std::shared_ptr<const KeyDescription> buildSortingKeyPrefixDescription(const StorageMetadataPtr & metadata_snapshot, UInt64 prefix_size)
{
    const auto & main_sorting_key = metadata_snapshot->getSortingKey();

    /// Get original expressions to preserve DESC entries for reverse flags.
    const auto original_expr_list_ast = main_sorting_key.getOriginalExpressionList();
    const auto * original_expr_list = original_expr_list_ast ? original_expr_list_ast->as<ASTExpressionList>() : nullptr;
    const size_t main_children = original_expr_list ? original_expr_list->children.size() : 0;

    if (prefix_size > main_children)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Patch's persisted sort-key prefix size ({}) exceeds the main table's current sort-key children count ({})",
            prefix_size, main_children);
    }

    auto order_by_expression = makeASTFunction("tuple");
    order_by_expression->arguments = make_intrusive<ASTExpressionList>();

    for (size_t i = 0; i < prefix_size; ++i)
        order_by_expression->arguments->children.push_back(original_expr_list->children[i]->clone());

    return std::make_shared<const KeyDescription>(KeyDescription::getKeyFromAST(
        order_by_expression,
        metadata_snapshot->getColumns(),
        metadata_snapshot->virtuals,
        Context::getGlobalContextInstance()));
}

}

SourcePartsSetForPatch::SourcePartsSetForPatch(
    const StorageMetadataPtr & metadata_snapshot,
    UInt8 format_version_,
    std::optional<UInt64> sorting_key_prefix_size_)
    : format_version(format_version_)
    , sorting_key_prefix_size(sorting_key_prefix_size_.value_or(0))
{
    /// Only v2 carries a semantic-prefix sort-key. v1 uses the fixed `(_part, _part_offset)`
    /// layout handled by `PatchMode::Merge` / `PatchMode::Join` with no `KeyDescription`.
    if (format_version == V2_FORMAT_VERSION && sorting_key_prefix_size_.has_value())
        sorting_key_prefix_description = buildSortingKeyPrefixDescription(metadata_snapshot, *sorting_key_prefix_size_);
}

void SourcePartsSetForPatch::addSourcePart(const String & name, UInt64 data_version)
{
    if (min_max_versions_by_part.contains(name))
        throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Source part {} already exists", name);

    if (empty())
    {
        min_data_version = data_version;
        max_data_version = data_version;
    }
    else
    {
        min_data_version = std::min(min_data_version, data_version);
        max_data_version = std::max(max_data_version, data_version);
    }

    source_parts_by_version[data_version].add(name);
    min_max_versions_by_part[name] = {data_version, data_version};
}

void SourcePartsSetForPatch::buildSourcePartsSet()
{
    min_data_version = 0;
    max_data_version = 0;
    source_parts_by_version.clear();

    bool is_first = true;
    for (const auto & [part_name, min_max] : min_max_versions_by_part)
    {
        source_parts_by_version[min_max.second].add(part_name);

        if (std::exchange(is_first, false))
        {
            min_data_version = min_max.first;
            max_data_version = min_max.second;
        }
        else
        {
            min_data_version = std::min(min_data_version, min_max.first);
            max_data_version = std::max(max_data_version, min_max.second);
        }
    }
}

PatchParts SourcePartsSetForPatch::getPatchParts(const MergeTreePartInfo & original_part, const DataPartPtr & patch_part) const
{
    UInt64 data_version = original_part.getDataVersion();
    auto it = source_parts_by_version.upper_bound(data_version);

    if (it == source_parts_by_version.end())
        return {};

    PatchParts patch_parts;
    auto part_name = original_part.getPartNameV1();

    NameSet names_for_join;
    bool has_merge = false;

    if (format_version == V2_FORMAT_VERSION)
    {
        bool use_patch = false;

        for (; it != source_parts_by_version.end(); ++it)
        {
            auto covered_parts = it->second.getPartsCoveredBy(original_part);

            if (!covered_parts.empty())
            {
                use_patch = true;
                break;
            }
        }

        if (use_patch)
        {
            patch_parts.push_back(PatchPartInfo
            {
                .mode = PatchMode::MergeOnKey,
                .part = patch_part,
                .source_parts = {}, // source part name are not needed for MergeOnKey
                .source_data_version = original_part.getDataVersion(),
                .perform_alter_conversions = true,
                .sorting_key = sorting_key_prefix_description,
            });
        }

        return patch_parts;
    }

    for (; it != source_parts_by_version.end(); ++it)
    {
        auto covered_parts = it->second.getPartsCoveredBy(original_part);

        if (covered_parts.size() == 1 && covered_parts.front() == part_name)
            has_merge = true;
        else
            std::move(covered_parts.begin(), covered_parts.end(), std::inserter(names_for_join, names_for_join.end()));
    }

    if (has_merge)
    {
        patch_parts.push_back(PatchPartInfo
        {
            .mode = PatchMode::Merge,
            .part = patch_part,
            .source_parts = {part_name},
            .source_data_version = original_part.getDataVersion(),
            .perform_alter_conversions = true,
            .sorting_key = nullptr,
        });
    }

    if (!names_for_join.empty())
    {
        patch_parts.push_back(PatchPartInfo
        {
            .mode = PatchMode::Join,
            .part = patch_part,
            .source_parts = Names(names_for_join.begin(), names_for_join.end()),
            .source_data_version = original_part.getDataVersion(),
            .perform_alter_conversions = true,
            .sorting_key = nullptr,
        });
    }

    return patch_parts;
}

SourcePartsSetForPatch SourcePartsSetForPatch::build(
    const Block & block,
    UInt64 data_version,
    const StorageMetadataPtr & metadata_snapshot,
    std::optional<UInt64> sorting_key_prefix_size_)
{
    const auto & column_part_name = block.getByName("_part").column;
    const auto & part_name_lc = assert_cast<const ColumnLowCardinality &>(*column_part_name);
    const auto & part_name_dict = part_name_lc.getDictionary().getNestedColumn();
    const auto & part_name_str = assert_cast<const ColumnString &>(*part_name_dict);

    /// Sink callers pass `v2_sorting_key_prefix_size` as the prefix_size
    /// only when writing v2 patches; absence of the optional is the v1 signal.
    const UInt8 format_version = sorting_key_prefix_size_.has_value() ? V2_FORMAT_VERSION : V1_FORMAT_VERSION;
    SourcePartsSetForPatch parts_set(metadata_snapshot, format_version, sorting_key_prefix_size_);

    for (size_t i = 0; i < part_name_str.size(); ++i)
    {
        auto part_name = part_name_str.getDataAt(i);

        /// LowCardinality dictionary always has default value.
        if (!part_name.empty())
            parts_set.addSourcePart(std::string{part_name}, data_version);
    }

    return parts_set;
}

SourcePartsSetForPatch SourcePartsSetForPatch::merge(const DataPartsVector & source_parts)
{
    SourcePartsSetForPatch merged_set;

    bool format_version_set = false;
    for (const auto & part : source_parts)
    {
        const auto & set = part->getSourcePartsSet();

        /// v1 and v2 patches live in different partitions (their partition-id hash differs), so
        /// inputs to a patch-on-patch merge always share the same format version. For v2 the
        /// sort-key prefix length is derived from the sort-key AST text, which is itself hashed
        /// into the partition id — so two v2 patches in the same partition also have equal
        /// prefix lengths *and* the same `sorting_key_prefix_description` (pointer equality is
        /// not guaranteed, but the `KeyDescription` contents are). We copy the shared_ptr from
        /// the first part so the merged set reuses the same cached `KeyDescription` without
        /// re-building from metadata.
        if (!format_version_set)
        {
            merged_set.format_version = set.format_version;
            merged_set.sorting_key_prefix_size = set.sorting_key_prefix_size;
            merged_set.sorting_key_prefix_description = set.sorting_key_prefix_description;
            format_version_set = true;
        }
        else
        {
            chassert(merged_set.format_version == set.format_version);
            chassert(merged_set.sorting_key_prefix_size == set.sorting_key_prefix_size);
        }

        for (const auto & [part_name, min_max] : set.min_max_versions_by_part)
        {
            auto [it, inserted] = merged_set.min_max_versions_by_part.emplace(part_name, min_max);

            if (!inserted)
            {
                auto & merged_min_max = it->second;
                merged_min_max.first = std::min(merged_min_max.first, min_max.first);
                merged_min_max.second = std::max(merged_min_max.second, min_max.second);
            }
        }
    }

    merged_set.buildSourcePartsSet();
    return merged_set;
}

void SourcePartsSetForPatch::writeBinary(WriteBuffer & out) const
{
    writeBinaryLittleEndian(format_version, out);

    /// v2 adds the semantic sort-key prefix length right after the version byte, so readers can
    /// recover it without a round-trip through the target table's in-memory metadata.
    if (format_version == V2_FORMAT_VERSION)
        writeBinaryLittleEndian(sorting_key_prefix_size, out);

    writeBinaryLittleEndian(min_max_versions_by_part.size(), out);

    for (const auto & [part_name, min_max] : min_max_versions_by_part)
    {
        writeStringBinary(part_name, out);
        writeBinaryLittleEndian(min_max.first, out);
        writeBinaryLittleEndian(min_max.second, out);
    }
}

void SourcePartsSetForPatch::readBinary(ReadBuffer & in, const StorageMetadataPtr & metadata_snapshot)
{
    UInt8 version;
    readBinaryLittleEndian(version, in);

    if (version > MAX_SUPPORTED_FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version of SourcePartsSetForPatch: {}", std::to_string(version));

    format_version = version;

    if (format_version == V2_FORMAT_VERSION)
    {
        readBinaryLittleEndian(sorting_key_prefix_size, in);
        sorting_key_prefix_description = buildSortingKeyPrefixDescription(metadata_snapshot, sorting_key_prefix_size);
    }

    UInt64 num_parts;
    readBinaryLittleEndian(num_parts, in);

    for (size_t i = 0; i < num_parts; ++i)
    {
        String part_name;
        readStringBinary(part_name, in);

        auto & min_max = min_max_versions_by_part[part_name];
        readBinaryLittleEndian(min_max.first, in);
        readBinaryLittleEndian(min_max.second, in);
    }

    buildSourcePartsSet();
}

SourcePartsSetForPatch buildSourceSetForPatch(
    Block & block,
    UInt64 data_version,
    const StorageMetadataPtr & metadata_snapshot,
    std::optional<UInt64> sorting_key_prefix_size)
{
    /// Need to update data version column because it contains data version
    /// of source part, but we store the data version of updated data in patch part.
    auto & data_version_column = block.getByName(PartDataVersionColumn::name).column;
    data_version_column = PartDataVersionColumn::type->createColumnConst(block.rows(), data_version)->convertToFullColumnIfConst();
    return SourcePartsSetForPatch::build(block, data_version, metadata_snapshot, sorting_key_prefix_size);
}

}
