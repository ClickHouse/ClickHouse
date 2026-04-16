#include <Storages/MergeTree/PatchParts/SourcePartsSetForPatch.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_DATA_PART;
    extern const int INCORRECT_DATA;
}

void SourcePartsSetForPatch::setSortKey(String expr_list_sql, std::vector<UInt8> reverse_flags)
{
    sort_key_expr_list_sql = std::move(expr_list_sql);
    sort_key_reverse_flags = std::move(reverse_flags);
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

    for (; it != source_parts_by_version.end(); ++it)
    {
        auto covered_parts = it->second.getPartsCoveredBy(original_part);

        if (covered_parts.size() == 1 && covered_parts.front() == part_name)
            has_merge = true;
        else
            std::move(covered_parts.begin(), covered_parts.end(), std::inserter(names_for_join, names_for_join.end()));
    }

    /// v2 patches apply with a single `MergeOnKey` pass that handles both the same-lineage case
    /// (`has_merge`) and the cross-lineage case (`names_for_join`) uniformly. Emit one `PatchPartInfo`
    /// with the union of all covered source parts.
    /// Source-column names + ExpressionActions for the patch's sort-key are populated later, in
    /// `MergeTreeData::getAlterConversionsForPart`, from the patch part's own metadata (itself built
    /// from the persisted sort-key AST).
    if (format_version == V2_FORMAT_VERSION)
    {
        Names all_source_parts(names_for_join.begin(), names_for_join.end());
        if (has_merge)
            all_source_parts.push_back(part_name);

        if (!all_source_parts.empty())
        {
            patch_parts.push_back(PatchPartInfo
            {
                .mode = PatchMode::MergeOnKey,
                .part = patch_part,
                .source_parts = std::move(all_source_parts),
                .source_data_version = original_part.getDataVersion(),
                .perform_alter_conversions = true,
                .sort_key_source_column_names = {},
                .sort_key_result_column_names = {},
                .sort_key_reverse_flags = sort_key_reverse_flags,
                .sort_key_expression = nullptr,
            });
        }
        return patch_parts;
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
            .sort_key_source_column_names = {},
            .sort_key_result_column_names = {},
            .sort_key_reverse_flags = {},
            .sort_key_expression = nullptr,
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
            .sort_key_source_column_names = {},
            .sort_key_result_column_names = {},
            .sort_key_reverse_flags = {},
            .sort_key_expression = nullptr,
        });
    }

    return patch_parts;
}

SourcePartsSetForPatch SourcePartsSetForPatch::build(const Block & block, UInt64 data_version)
{
    const auto & column_part_name = block.getByName("_part").column;
    const auto & part_name_lc = assert_cast<const ColumnLowCardinality &>(*column_part_name);
    const auto & part_name_dict = part_name_lc.getDictionary().getNestedColumn();
    const auto & part_name_str = assert_cast<const ColumnString &>(*part_name_dict);

    SourcePartsSetForPatch parts_set;
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
        /// inputs to a patch-on-patch merge always share the same format version — and, for v2,
        /// the same sort-key AST and directions.
        if (!format_version_set)
        {
            merged_set.format_version = set.format_version;
            merged_set.sort_key_expr_list_sql = set.sort_key_expr_list_sql;
            merged_set.sort_key_reverse_flags = set.sort_key_reverse_flags;
            format_version_set = true;
        }
        else
        {
            chassert(merged_set.format_version == set.format_version);
            chassert(merged_set.sort_key_expr_list_sql == set.sort_key_expr_list_sql);
            chassert(merged_set.sort_key_reverse_flags == set.sort_key_reverse_flags);
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
    writeBinaryLittleEndian(min_max_versions_by_part.size(), out);

    for (const auto & [part_name, min_max] : min_max_versions_by_part)
    {
        writeStringBinary(part_name, out);
        writeBinaryLittleEndian(min_max.first, out);
        writeBinaryLittleEndian(min_max.second, out);
    }

    if (format_version >= V2_FORMAT_VERSION)
    {
        /// Persist the sort-key expression-list AST (as SQL) + parallel reverse flags captured at
        /// write time so readers can rebuild the v2 patch's KeyDescription — *including* the
        /// ExpressionActions for expression sort keys — even after the main table has been ALTERed.
        writeStringBinary(sort_key_expr_list_sql, out);
        writeBinaryLittleEndian(sort_key_reverse_flags.size(), out);
        for (auto flag : sort_key_reverse_flags)
            writeBinaryLittleEndian(flag, out);
    }
}

void SourcePartsSetForPatch::readBinary(ReadBuffer & in)
{
    UInt8 version;
    readBinaryLittleEndian(version, in);

    if (version > MAX_SUPPORTED_FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version of SourcePartsSetForPatch: {}", std::to_string(version));

    format_version = version;

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

    if (format_version >= V2_FORMAT_VERSION)
    {
        readStringBinary(sort_key_expr_list_sql, in);

        UInt64 num_flags = 0;
        readBinaryLittleEndian(num_flags, in);
        sort_key_reverse_flags.resize(num_flags);
        for (auto & flag : sort_key_reverse_flags)
            readBinaryLittleEndian(flag, in);
    }

    buildSourcePartsSet();
}

SourcePartsSetForPatch buildSourceSetForPatch(Block & block, UInt64 data_version)
{
    /// Need to update data version column because it contains data version
    /// of source part, but we store the data version of updated data in patch part.
    auto & data_version_column = block.getByName(PartDataVersionColumn::name).column;
    data_version_column = PartDataVersionColumn::type->createColumnConst(block.rows(), data_version)->convertToFullColumnIfConst();
    return SourcePartsSetForPatch::build(block, data_version);
}

}
