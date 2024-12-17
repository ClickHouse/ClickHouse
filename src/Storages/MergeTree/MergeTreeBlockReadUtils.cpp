#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <DataTypes/NestedUtils.h>
#include <Core/NamesAndTypes.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Columns/ColumnConst.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{

/// Columns absent in part may depend on other absent columns so we are
/// searching all required physical columns recursively. Return true if found at
/// least one existing (physical) column in part.
bool injectRequiredColumnsRecursively(
    const String & column_name,
    const StorageSnapshotPtr & storage_snapshot,
    const AlterConversionsPtr & alter_conversions,
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const GetColumnsOptions & options,
    Names & columns,
    NameSet & required_columns,
    NameSet & injected_columns)
{
    /// This is needed to prevent stack overflow in case of cyclic defaults or
    /// huge AST which for some reason was not validated on parsing/interpreter
    /// stages.
    checkStackSize();

    auto column_in_storage = storage_snapshot->tryGetColumn(options, column_name);
    if (column_in_storage)
    {
        auto column_name_in_part = column_in_storage->getNameInStorage();
        if (alter_conversions && alter_conversions->isColumnRenamed(column_name_in_part))
            column_name_in_part = alter_conversions->getColumnOldName(column_name_in_part);

        auto column_in_part = data_part_info_for_reader.getColumns().tryGetByName(column_name_in_part);

        if (column_in_part
            && (!column_in_storage->isSubcolumn()
                || column_in_part->type->tryGetSubcolumnType(column_in_storage->getSubcolumnName())))
        {
            /// ensure each column is added only once
            if (!required_columns.contains(column_name))
            {
                columns.emplace_back(column_name);
                required_columns.emplace(column_name);
                injected_columns.emplace(column_name);
            }
            return true;
        }
    }

    /// Column doesn't have default value and don't exist in part
    /// don't need to add to required set.
    const auto column_default = storage_snapshot->metadata->getColumns().getDefault(column_name);
    if (!column_default)
        return false;

    /// collect identifiers required for evaluation
    IdentifierNameSet identifiers;
    column_default->expression->collectIdentifierNames(identifiers);

    bool result = false;
    for (const auto & identifier : identifiers)
        result |= injectRequiredColumnsRecursively(
            identifier, storage_snapshot, alter_conversions, data_part_info_for_reader,
            options, columns, required_columns, injected_columns);

    return result;
}

}

NameSet injectRequiredColumns(
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const StorageSnapshotPtr & storage_snapshot,
    bool with_subcolumns,
    Names & columns)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    bool have_at_least_one_physical_column = false;
    AlterConversionsPtr alter_conversions;
    if (!data_part_info_for_reader.isProjectionPart())
        alter_conversions = data_part_info_for_reader.getAlterConversions();

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withExtendedObjects()
        .withVirtuals()
        .withSubcolumns(with_subcolumns);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        /// We are going to fetch physical columns and system columns first
        if (!storage_snapshot->tryGetColumn(options, columns[i]))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column or subcolumn {} in table", columns[i]);

        have_at_least_one_physical_column |= injectRequiredColumnsRecursively(
            columns[i], storage_snapshot, alter_conversions,
            data_part_info_for_reader, options, columns, required_columns, injected_columns);
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (!have_at_least_one_physical_column)
    {
        auto available_columns = storage_snapshot->metadata->getColumns().get(options);
        const auto minimum_size_column_name = data_part_info_for_reader.getColumnNameWithMinimumCompressedSize(available_columns);
        columns.push_back(minimum_size_column_name);
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}

MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const DataPartPtr & data_part_, const Names & columns, const Block & sample_block)
    : data_part(data_part_)
{
    number_of_rows_in_part = data_part->rows_count;
    /// Initialize with sample block until update won't called.
    initialize(sample_block, {}, columns);
}

void MergeTreeBlockSizePredictor::initialize(const Block & sample_block, const Columns & columns, const Names & names, bool from_update)
{
    fixed_columns_bytes_per_row = 0;
    dynamic_columns_infos.clear();

    std::unordered_set<String> names_set;
    if (!from_update)
        names_set.insert(names.begin(), names.end());

    size_t num_columns = sample_block.columns();
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column_with_type_and_name = sample_block.getByPosition(pos);
        const auto & column_name = column_with_type_and_name.name;
        const auto & column_data = from_update ? columns[pos] : column_with_type_and_name.column;

        if (!from_update && !names_set.contains(column_name))
            continue;

        /// At least PREWHERE filter column might be const.
        if (typeid_cast<const ColumnConst *>(column_data.get()))
            continue;

        if (column_data->valuesHaveFixedSize())
        {
            size_t size_of_value = column_data->sizeOfValueIfFixed();
            fixed_columns_bytes_per_row += column_data->sizeOfValueIfFixed();
            max_size_per_row_fixed = std::max<double>(max_size_per_row_fixed, size_of_value);
        }
        else
        {
            ColumnInfo info;
            info.name = column_name;
            /// If column isn't fixed and doesn't have checksum, than take first
            ColumnSize column_size = data_part->getColumnSize(column_name);

            info.bytes_per_row_global = column_size.data_uncompressed
                ? column_size.data_uncompressed / number_of_rows_in_part
                : column_data->byteSize() / std::max<size_t>(1, column_data->size());

            dynamic_columns_infos.emplace_back(info);
        }
    }

    bytes_per_row_global = fixed_columns_bytes_per_row;
    for (auto & info : dynamic_columns_infos)
    {
        info.bytes_per_row = info.bytes_per_row_global;
        bytes_per_row_global += info.bytes_per_row_global;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
    bytes_per_row_current = bytes_per_row_global;
}

void MergeTreeBlockSizePredictor::startBlock()
{
    block_size_bytes = 0;
    block_size_rows = 0;
    for (auto & info : dynamic_columns_infos)
        info.size_bytes = 0;
}

/// TODO: add last_read_row_in_part parameter to take into account gaps between adjacent ranges
void MergeTreeBlockSizePredictor::update(const Block & sample_block, const Columns & columns, size_t num_rows, double decay)
{
    if (columns.size() != sample_block.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent number of columns passed to MergeTreeBlockSizePredictor. "
                        "Have {} in sample block and {} columns in list",
                        toString(sample_block.columns()), toString(columns.size()));

    if (!is_initialized_in_update)
    {
        /// Reinitialize with read block to update estimation for DEFAULT and MATERIALIZED columns without data.
        initialize(sample_block, columns, {}, true);
        is_initialized_in_update = true;
    }

    if (num_rows < block_size_rows)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Updated block has less rows ({}) than previous one ({})",
                        num_rows, block_size_rows);
    }

    size_t diff_rows = num_rows - block_size_rows;
    block_size_bytes = num_rows * fixed_columns_bytes_per_row;
    bytes_per_row_current = fixed_columns_bytes_per_row;
    block_size_rows = num_rows;

    /// Make recursive updates for each read row: v_{i+1} = (1 - decay) v_{i} + decay v_{target}
    /// Use sum of geometric sequence formula to update multiple rows: v{n} = (1 - decay)^n v_{0} + (1 - (1 - decay)^n) v_{target}
    /// NOTE: DEFAULT and MATERIALIZED columns without data has inaccurate estimation of v_{target}
    double alpha = std::pow(1. - decay, diff_rows);

    max_size_per_row_dynamic = 0;
    for (auto & info : dynamic_columns_infos)
    {
        size_t new_size = columns[sample_block.getPositionByName(info.name)]->byteSize();
        size_t diff_size = new_size - info.size_bytes;

        double local_bytes_per_row = static_cast<double>(diff_size) / diff_rows;
        info.bytes_per_row = alpha * info.bytes_per_row + (1. - alpha) * local_bytes_per_row;

        info.size_bytes = new_size;
        block_size_bytes += new_size;
        bytes_per_row_current += info.bytes_per_row;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
}


MergeTreeReadTaskColumns getReadTaskColumns(
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & required_columns,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    bool with_subcolumns)
{
    Names column_to_read_after_prewhere = required_columns;

    /// Inject columns required for defaults evaluation
    injectRequiredColumns(
        data_part_info_for_reader, storage_snapshot, with_subcolumns, column_to_read_after_prewhere);

    MergeTreeReadTaskColumns result;
    auto options = GetColumnsOptions(GetColumnsOptions::All)
        .withExtendedObjects()
        .withVirtuals()
        .withSubcolumns(with_subcolumns);

    NameSet columns_from_previous_steps;
    auto add_step = [&](const PrewhereExprStep & step)
    {
        Names step_column_names;

        /// Virtual columns that are filled by RangeReader
        /// must be read in the first step before any filtering.
        if (columns_from_previous_steps.empty())
        {
            for (const auto & required_column : required_columns)
                if (MergeTreeRangeReader::virtuals_to_fill.contains(required_column))
                    step_column_names.push_back(required_column);
        }

        /// Computation results from previous steps might be used in the current step as well. In such a case these
        /// computed columns will be present in the current step inputs. They don't need to be read from the disk so
        /// exclude them from the list of columns to read. This filtering must be done before injecting required
        /// columns to avoid adding unnecessary columns or failing to find required columns that are computation
        /// results from previous steps.
        /// Example: step1: sin(a)>b, step2: sin(a)>c
        for (const auto & name : step.actions->getActionsDAG().getRequiredColumnsNames())
            if (!columns_from_previous_steps.contains(name))
                step_column_names.push_back(name);

        if (!step_column_names.empty())
            injectRequiredColumns(
                data_part_info_for_reader, storage_snapshot,
                with_subcolumns, step_column_names);

        /// More columns could have been added, filter them as well by the list of columns from previous steps.
        Names columns_to_read_in_step;
        for (const auto & name : step_column_names)
        {
            if (columns_from_previous_steps.contains(name))
                continue;

            columns_to_read_in_step.push_back(name);
            columns_from_previous_steps.insert(name);
        }

        /// Add results of the step to the list of already "known" columns so that we don't read or compute them again.
        for (const auto & name : step.actions->getActionsDAG().getNames())
            columns_from_previous_steps.insert(name);

        result.pre_columns.push_back(storage_snapshot->getColumnsByNames(options, columns_to_read_in_step));
    };

    if (prewhere_info)
    {
        auto prewhere_actions = MergeTreeSelectProcessor::getPrewhereActions(
            prewhere_info,
            actions_settings,
            reader_settings.enable_multiple_prewhere_read_steps);

        for (const auto & step : prewhere_actions.steps)
            add_step(*step);
    }

    /// Remove columns read in prewehere from the list of columns to read
    Names post_column_names;
    for (const auto & name : column_to_read_after_prewhere)
        if (!columns_from_previous_steps.contains(name))
            post_column_names.push_back(name);

    column_to_read_after_prewhere = std::move(post_column_names);

    /// Rest of the requested columns
    result.columns = storage_snapshot->getColumnsByNames(options, column_to_read_after_prewhere);
    return result;
}

}
