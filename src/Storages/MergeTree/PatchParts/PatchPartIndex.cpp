#include <Storages/MergeTree/PatchParts/PatchPartIndex.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_DATA_PART;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

PatchPartIndex::PatchPartIndex(MergeTreePatchPartsVersion format_version_, String sorting_key_desc_)
    : format_version(format_version_)
    , sorting_key_desc(std::move(sorting_key_desc_))
{
    if (format_version == MergeTreePatchPartsVersion::V1 && !sorting_key_desc.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting key description must be empty for v1 patch part. Got: {}", sorting_key_desc);
}

PatchBlockOffsets PatchPartIndex::getBlockOffsets(const String & part_name) const
{
    auto it = block_offsets_by_part.find(part_name);
    return it == block_offsets_by_part.end() ? PatchBlockOffsets::Stored : it->second;
}

void PatchPartIndex::addSourcePart(const String & name, UInt64 data_version, PatchBlockOffsets block_offsets)
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

    if (block_offsets != PatchBlockOffsets::Stored)
        block_offsets_by_part[name] = block_offsets;
}

void PatchPartIndex::checkBlockOffsetsOfSourceParts(const MergeTreePartInfo & original_part, PatchBlockOffsets block_offsets, const String & patch_part_name) const
{
    const auto part_name = original_part.getPartNameV1();

    for (auto it = source_parts_by_version.upper_bound(original_part.getDataVersion()); it != source_parts_by_version.end(); ++it)
    {
        for (const auto & source_part_name : it->second.getPartsCoveredBy(original_part))
        {
            /// A v1 patch is matched with the rows of its source part itself by `_part_offset` (`Merge` mode).
            if (format_version == MergeTreePatchPartsVersion::V1 && source_part_name == part_name)
                continue;

            if (getBlockOffsets(source_part_name) == block_offsets)
                continue;

            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Patch part {} cannot be applied to part {}. "
                "The part stores _block_offset without _block_number, because it was merged by an older version "
                "with enable_block_offset_column = 1 and enable_block_number_column = 0. "
                "Its _block_offset is read {} the setting use_block_offset_only_with_block_number, "
                "and the patch for source part {} was written {} it, so the patch would change other rows. "
                "To read the data without patches, use the setting apply_patch_parts = 0. "
                "To discard the patch, use ALTER TABLE ... DROP PART",
                patch_part_name, part_name,
                block_offsets == PatchBlockOffsets::RowPositions ? "with" : "without",
                source_part_name,
                getBlockOffsets(source_part_name) == PatchBlockOffsets::Mixed ? "both with and without" : (block_offsets == PatchBlockOffsets::RowPositions ? "without" : "with"));
        }
    }
}

void PatchPartIndex::buildSourcePartsByVersion()
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

PatchParts PatchPartIndex::getPatchParts(
    const MergeTreePartInfo & original_part,
    const DataPartPtr & patch_part,
    std::shared_ptr<const KeyDescription> effective_sorting_key,
    NameSet stored_sorting_key_columns) const
{
    UInt64 data_version = original_part.getDataVersion();
    auto it = source_parts_by_version.upper_bound(data_version);

    if (it == source_parts_by_version.end())
        return {};

    PatchParts patch_parts;
    auto part_name = original_part.getPartNameV1();

    NameSet names_for_join;
    bool has_merge = false;

    if (format_version == MergeTreePatchPartsVersion::V2)
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
                .source_parts = {}, // source part names are not needed for MergeOnKey
                .source_data_version = original_part.getDataVersion(),
                .perform_alter_conversions = true,
                .sorting_key = std::move(effective_sorting_key),
                .stored_sorting_key_columns = std::move(stored_sorting_key_columns),
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
            .stored_sorting_key_columns = {},
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
            .stored_sorting_key_columns = {},
        });
    }

    return patch_parts;
}

PatchPartIndex PatchPartIndex::build(
    const Block & block,
    UInt64 data_version,
    MergeTreePatchPartsVersion format_version,
    String sorting_key_,
    PatchBlockOffsets block_offsets)
{
    const auto & column_part_name = block.getByName("_part").column;
    const auto & part_name_lc = assert_cast<const ColumnLowCardinality &>(*column_part_name);
    const auto & part_name_dict = part_name_lc.getDictionary().getNestedColumn();
    const auto & part_name_str = assert_cast<const ColumnString &>(*part_name_dict);

    PatchPartIndex parts_set(format_version, std::move(sorting_key_));

    for (size_t i = 0; i < part_name_str.size(); ++i)
    {
        auto part_name = part_name_str.getDataAt(i);

        /// LowCardinality dictionary always has default value.
        if (!part_name.empty())
            parts_set.addSourcePart(std::string{part_name}, data_version, block_offsets);
    }

    return parts_set;
}

PatchPartIndex PatchPartIndex::merge(const DataPartsVector & source_parts)
{
    if (source_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot patch part indexes from empty vector of source parts");

    const auto & first_set = source_parts.front()->getPatchPartIndex();
    PatchPartIndex merged_set(first_set.format_version, first_set.sorting_key_desc);

    for (const auto & part : source_parts)
    {
        const auto & set = part->getPatchPartIndex();

        if (set.format_version != merged_set.format_version)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Format version mismatch in patch part index. Got: {}, expected: {}", static_cast<UInt64>(set.format_version), static_cast<UInt64>(merged_set.format_version));

        if (set.sorting_key_desc != merged_set.sorting_key_desc)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting key description mismatch in patch part index. Got: {}, expected: {}", set.sorting_key_desc, merged_set.sorting_key_desc);

        for (const auto & [part_name, min_max] : set.min_max_versions_by_part)
        {
            auto [it, inserted] = merged_set.min_max_versions_by_part.emplace(part_name, min_max);
            const auto block_offsets = set.getBlockOffsets(part_name);

            if (inserted)
            {
                if (block_offsets != PatchBlockOffsets::Stored)
                    merged_set.block_offsets_by_part[part_name] = block_offsets;
            }
            else
            {
                auto & merged_min_max = it->second;
                merged_min_max.first = std::min(merged_min_max.first, min_max.first);
                merged_min_max.second = std::max(merged_min_max.second, min_max.second);

                if (block_offsets != merged_set.getBlockOffsets(part_name))
                    merged_set.block_offsets_by_part[part_name] = PatchBlockOffsets::Mixed;
            }
        }
    }

    merged_set.buildSourcePartsByVersion();
    return merged_set;
}

void PatchPartIndex::writeBinary(WriteBuffer & out) const
{
    /// Otherwise the index is written as by older versions, so that they can read it.
    const bool with_block_offsets = !block_offsets_by_part.empty();
    if (with_block_offsets)
        writeBinaryLittleEndian(SERIALIZATION_VERSION_WITH_BLOCK_OFFSETS, out);

    writeBinaryLittleEndian(static_cast<UInt8>(format_version), out);

    if (format_version == MergeTreePatchPartsVersion::V2)
    {
        writeStringBinary(sorting_key_desc, out);
    }

    writeBinaryLittleEndian(min_max_versions_by_part.size(), out);

    for (const auto & [part_name, min_max] : min_max_versions_by_part)
    {
        writeStringBinary(part_name, out);
        writeBinaryLittleEndian(min_max.first, out);
        writeBinaryLittleEndian(min_max.second, out);

        if (with_block_offsets)
            writeBinaryLittleEndian(static_cast<UInt8>(getBlockOffsets(part_name)), out);
    }
}

PatchPartIndex PatchPartIndex::readBinary(ReadBuffer & in)
{
    UInt8 read_version = 0;
    String read_sorting_key_desc;
    readBinaryLittleEndian(read_version, in);

    const bool with_block_offsets = read_version == SERIALIZATION_VERSION_WITH_BLOCK_OFFSETS;
    if (with_block_offsets)
        readBinaryLittleEndian(read_version, in);

    if (read_version > MAX_SUPPORTED_FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version of PatchPartIndex: {}", std::to_string(read_version));

    auto format_version = static_cast<MergeTreePatchPartsVersion>(read_version);
    if (format_version == MergeTreePatchPartsVersion::V2)
        readStringBinary(read_sorting_key_desc, in);

    PatchPartIndex res(format_version, std::move(read_sorting_key_desc));
    UInt64 num_parts = 0;
    readBinaryLittleEndian(num_parts, in);

    for (size_t i = 0; i < num_parts; ++i)
    {
        String part_name;
        readStringBinary(part_name, in);

        auto & min_max = res.min_max_versions_by_part[part_name];
        readBinaryLittleEndian(min_max.first, in);
        readBinaryLittleEndian(min_max.second, in);

        if (with_block_offsets)
        {
            UInt8 block_offsets = 0;
            readBinaryLittleEndian(block_offsets, in);

            if (block_offsets > static_cast<UInt8>(PatchBlockOffsets::Mixed))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid block offsets of source part {} in PatchPartIndex: {}", part_name, std::to_string(block_offsets));

            if (block_offsets != static_cast<UInt8>(PatchBlockOffsets::Stored))
                res.block_offsets_by_part[part_name] = static_cast<PatchBlockOffsets>(block_offsets);
        }
    }

    res.buildSourcePartsByVersion();

    /// Consumes exactly the bytes of the index and nothing after them: the index is also embedded in
    /// larger streams, so whether anything may follow it is for the caller to decide (see
    /// `IMergeTreeDataPart::loadPatchPartIndex` for the file that holds nothing else).
    return res;
}

static ASTPtr getTableSortingKeyExpressionFromPatch(const KeyDescription & patch_sorting_key)
{
    const auto patch_expr_list = patch_sorting_key.getOriginalExpressionList();

    if (!patch_expr_list || patch_expr_list->children.size() < 2)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid patch sorting key expression list: {}",
            patch_expr_list ? patch_expr_list->formatForErrorMessage() : "null");
    }

    auto table_expr_list = make_intrusive<ASTExpressionList>();
    table_expr_list->children.reserve(patch_expr_list->children.size() - 2);

    for (size_t i = 0; i < patch_expr_list->children.size() - 2; ++i)
        table_expr_list->children.push_back(patch_expr_list->children[i]->clone());

    return table_expr_list;
}

PatchPartIndex buildPatchPartIndex(
    Block & block,
    UInt64 data_version,
    const PatchPartMetadata & patch_metadata)
{
    /// Need to update data version column because it contains data version
    /// of source part, but we store the data version of updated data in patch part.
    auto & data_version_column = block.getByName(PartDataVersionColumn::name).column;
    data_version_column = PartDataVersionColumn::type->createColumnConst(block.rows(), data_version)->convertToFullColumnIfConst();

    String sorting_key_desc;

    if (patch_metadata.version == MergeTreePatchPartsVersion::V2)
    {
        auto sorting_key_expr_list = getTableSortingKeyExpressionFromPatch(patch_metadata.metadata->getSortingKey());
        sorting_key_desc = sorting_key_expr_list->formatWithSecretsOneLine();
    }

    return PatchPartIndex::build(block, data_version, patch_metadata.version, std::move(sorting_key_desc), patch_metadata.block_offsets);
}

}
