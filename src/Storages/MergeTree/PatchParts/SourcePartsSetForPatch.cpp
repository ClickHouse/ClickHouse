#include <Storages/MergeTree/PatchParts/SourcePartsSetForPatch.h>
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
}

SourcePartsSetForPatch::SourcePartsSetForPatch(UInt8 format_version_, String sorting_key_desc_)
    : format_version(format_version_)
    , sorting_key_desc(std::move(sorting_key_desc_))
{
    if (format_version == V1_FORMAT_VERSION && !sorting_key_desc.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting key description must be empty for v1 patch part. Got: {}", sorting_key_desc);
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

PatchParts SourcePartsSetForPatch::getPatchParts(
    const MergeTreePartInfo & original_part,
    const DataPartPtr & patch_part,
    std::shared_ptr<const KeyDescription> effective_sorting_key) const
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
                .source_parts = {}, // source part names are not needed for MergeOnKey
                .source_data_version = original_part.getDataVersion(),
                .perform_alter_conversions = true,
                .sorting_key = std::move(effective_sorting_key),
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
    UInt8 format_version,
    String sorting_key_)
{
    const auto & column_part_name = block.getByName("_part").column;
    const auto & part_name_lc = assert_cast<const ColumnLowCardinality &>(*column_part_name);
    const auto & part_name_dict = part_name_lc.getDictionary().getNestedColumn();
    const auto & part_name_str = assert_cast<const ColumnString &>(*part_name_dict);

    SourcePartsSetForPatch parts_set(format_version, std::move(sorting_key_));

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
    if (source_parts.empty())
        return merged_set;

    const auto & first_set = source_parts.front()->getSourcePartsSet();
    merged_set.format_version = first_set.format_version;
    merged_set.sorting_key_desc = first_set.sorting_key_desc;

    for (const auto & part : source_parts)
    {
        const auto & set = part->getSourcePartsSet();

        if (set.format_version != merged_set.format_version)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Format version mismatch in source parts set. Got: {}, expected: {}", static_cast<UInt64>(set.format_version), static_cast<UInt64>(merged_set.format_version));

        if (set.sorting_key_desc != merged_set.sorting_key_desc)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting key description mismatch in source parts set. Got: {}, expected: {}", set.sorting_key_desc, merged_set.sorting_key_desc);

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

    if (format_version == V2_FORMAT_VERSION)
    {
        writeStringBinary(sorting_key_desc, out);
    }

    writeBinaryLittleEndian(min_max_versions_by_part.size(), out);

    for (const auto & [part_name, min_max] : min_max_versions_by_part)
    {
        writeStringBinary(part_name, out);
        writeBinaryLittleEndian(min_max.first, out);
        writeBinaryLittleEndian(min_max.second, out);
    }
}

void SourcePartsSetForPatch::readBinary(ReadBuffer & in)
{
    UInt8 version = 0;
    readBinaryLittleEndian(version, in);

    if (version > MAX_SUPPORTED_FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version of SourcePartsSetForPatch: {}", std::to_string(version));

    format_version = version;

    if (format_version == V2_FORMAT_VERSION)
        readStringBinary(sorting_key_desc, in);

    UInt64 num_parts = 0;
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
    const PatchPartMetadata & patch_metadata)
{
    /// Need to update data version column because it contains data version
    /// of source part, but we store the data version of updated data in patch part.
    auto & data_version_column = block.getByName(PartDataVersionColumn::name).column;
    data_version_column = PartDataVersionColumn::type->createColumnConst(block.rows(), data_version)->convertToFullColumnIfConst();

    UInt8 format_version = SourcePartsSetForPatch::V1_FORMAT_VERSION;
    String sorting_key_desc;

    if (patch_metadata.version == MergeTreePatchPartsVersion::V2)
    {
        format_version = SourcePartsSetForPatch::V2_FORMAT_VERSION;
        auto sorting_key_expr_list = getTableSortingKeyExpressionFromPatch(patch_metadata.metadata->getSortingKey());
        sorting_key_desc = sorting_key_expr_list->formatWithSecretsOneLine();
    }

    return SourcePartsSetForPatch::build(block, data_version, format_version, std::move(sorting_key_desc));
}

}
