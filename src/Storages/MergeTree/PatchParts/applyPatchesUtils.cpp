#include <Storages/MergeTree/PatchParts/applyPatchesUtils.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnsForPatches getColumnsForPatches(
    const Block & header,
    const Columns & columns,
    const MergeTreePatchReaders & patch_readers)
{
    ColumnsForPatches res;
    auto block = header.cloneWithColumns(columns);
    using enum ColumnForPatch::Order;

    for (const auto & patch_reader : patch_readers)
    {
        const auto & patch = patch_reader->getPatchPart();
        const auto & patch_columns = patch.part->getColumnsDescription();
        const auto & alter_conversions = patch.part->getAlterConversions();
        auto & columns_for_patch = res.emplace_back();

        for (const auto & column : block)
        {
            if (isPatchPartSystemColumn(column.name))
                continue;

            String column_name_in_patch = column.name;
            if (alter_conversions && alter_conversions->isColumnRenamed(column.name))
                column_name_in_patch = alter_conversions->getColumnOldName(column.name);

            if (!patch_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, column_name_in_patch))
                continue;

            /// Add requested column name, not the column name in patch, for correct applying patches.
            /// This column name is translated to the column name in patch in MergeTree reader.
            /// If columns is not created at this stage, it will be filled with defaults at the latest stage of reading.
            if (!column.column)
            {
                columns_for_patch.emplace_back(column.name, AfterEvaluatingDefaults);
            }
            else if (patch.perform_alter_conversions)
            {
                columns_for_patch.emplace_back(column.name, AfterConversions);
            }
            else
            {
                columns_for_patch.emplace_back(column.name, BeforeConversions);
            }
        }
    }

    return res;
}

void addPatchVirtualColumns(Block & to, const Block & from)
{
    const auto & system_columns = getPatchPartSystemColumns();
    for (const auto & column : system_columns)
    {
        /// All system columns must be read on previous steps.
        if (!to.has(column.name) && from.has(column.name))
            to.insert(from.getByName(column.name));
    }
}

void applyPatchesToColumns(
    const MergeTreePatchReaders & patch_readers,
    const std::vector<std::deque<PatchReadResultPtr>> & patches_results,
    const Block & result_header,
    Columns & result_columns,
    Block & versions_block,
    std::optional<UInt64> min_version,
    std::optional<UInt64> max_version,
    const ColumnsForPatches & columns_for_patches,
    const std::set<ColumnForPatch::Order> & suitable_orders,
    const Block & additional_columns)
{
    if (patch_readers.empty() || result_columns.empty())
        return;

    auto result_block = result_header.cloneWithColumns(result_columns);
    addPatchVirtualColumns(result_block, additional_columns);

    /// Combine patches with the same structure.
    std::unordered_map<Names, PatchesToApply, NamesHash> patches_to_apply;
    UInt64 source_data_version = patch_readers.front()->getPatchPart().source_data_version;

    for (size_t i = 0; i < patch_readers.size(); ++i)
    {
        const auto & patch = patch_readers[i]->getPatchPart();
        const auto & patch_results = patches_results[i];

        if (static_cast<UInt64>(patch.source_data_version) != source_data_version)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Patches for one reader must have the same source data version. Got: {}, expected: {}",
                patch.source_data_version, source_data_version);
        }

        if (min_version.has_value() && !patchHasHigherDataVersion(*patch.part, *min_version))
            continue;

        if (max_version.has_value() && patchHasHigherDataVersion(*patch.part, *max_version))
            continue;

        Names updated_columns;
        for (const auto & columns_for_patch : columns_for_patches[i])
        {
            if (suitable_orders.contains(columns_for_patch.order))
                updated_columns.push_back(columns_for_patch.column_name);
        }

        if (updated_columns.empty())
            continue;

        std::sort(updated_columns.begin(), updated_columns.end());

        for (const auto & patch_result : patch_results)
        {
            /// TODO: build indices once and filter them in MergeTreeRangeReader.
            auto patches = patch_readers[i]->applyPatch(result_block, *patch_result);

            for (auto & patch_to_apply : patches)
            {
                if (!patch_to_apply->empty())
                    patches_to_apply[updated_columns].push_back(std::move(patch_to_apply));
            }
        }
    }

    if (min_version.has_value())
        source_data_version = std::max(source_data_version, *min_version);

    for (const auto & [updated_columns, patches] : patches_to_apply)
        applyPatchesToBlock(result_block, versions_block, patches, updated_columns, source_data_version);

    result_columns = result_block.getColumns();
    result_columns.resize(result_header.columns());
}

}
