#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataTypes/NestedUtils.h>
#include <Core/NamesAndTypes.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
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
    const MergeTreeData::AlterConversions & alter_conversions,
    const MergeTreeData::DataPartPtr & part,
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
        if (alter_conversions.isColumnRenamed(column_name_in_part))
            column_name_in_part = alter_conversions.getColumnOldName(column_name_in_part);

        auto column_in_part = part->getColumns().tryGetByName(column_name_in_part);

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
    auto metadata_snapshot = storage_snapshot->getMetadataForQuery();
    const auto column_default = metadata_snapshot->getColumns().getDefault(column_name);
    if (!column_default)
        return false;

    /// collect identifiers required for evaluation
    IdentifierNameSet identifiers;
    column_default->expression->collectIdentifierNames(identifiers);

    bool result = false;
    for (const auto & identifier : identifiers)
        result |= injectRequiredColumnsRecursively(
            identifier, storage_snapshot, alter_conversions, part,
            options, columns, required_columns, injected_columns);

    return result;
}

}

NameSet injectRequiredColumns(
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeData::DataPartPtr & part,
    bool with_subcolumns,
    Names & columns)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    bool have_at_least_one_physical_column = false;
    MergeTreeData::AlterConversions alter_conversions;
    if (!part->isProjectionPart())
        alter_conversions = storage.getAlterConversionsForPart(part);

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withExtendedObjects()
        .withSystemColumns();
    if (with_subcolumns)
        options.withSubcolumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        /// We are going to fetch only physical columns and system columns
        if (!storage_snapshot->tryGetColumn(options, columns[i]))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no physical column or subcolumn {} in table", columns[i]);

        have_at_least_one_physical_column |= injectRequiredColumnsRecursively(
            columns[i], storage_snapshot, alter_conversions,
            part, options, columns, required_columns, injected_columns);
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (!have_at_least_one_physical_column)
    {
        const auto minimum_size_column_name = part->getColumnNameWithMinimumCompressedSize(with_subcolumns);
        columns.push_back(minimum_size_column_name);
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}


MergeTreeReadTask::MergeTreeReadTask(
    const MergeTreeData::DataPartPtr & data_part_, const MarkRanges & mark_ranges_, size_t part_index_in_query_,
    const Names & ordered_names_, const NameSet & column_name_set_, const MergeTreeReadTaskColumns & task_columns_,
    bool remove_prewhere_column_,
    MergeTreeBlockSizePredictorPtr && size_predictor_)
    : data_part{data_part_}, mark_ranges{mark_ranges_}, part_index_in_query{part_index_in_query_},
    ordered_names{ordered_names_}, column_name_set{column_name_set_}, task_columns{task_columns_},
    remove_prewhere_column{remove_prewhere_column_}, size_predictor{std::move(size_predictor_)}
{
}


MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const MergeTreeData::DataPartPtr & data_part_, const Names & columns, const Block & sample_block)
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
        const String & column_name = column_with_type_and_name.name;
        const ColumnPtr & column_data = from_update ? columns[pos]
                                                    : column_with_type_and_name.column;

        if (!from_update && !names_set.contains(column_name))
            continue;

        /// At least PREWHERE filter column might be const.
        if (typeid_cast<const ColumnConst *>(column_data.get()))
            continue;

        if (column_data->valuesHaveFixedSize())
        {
            size_t size_of_value = column_data->sizeOfValueIfFixed();
            fixed_columns_bytes_per_row += column_data->sizeOfValueIfFixed();
            max_size_per_row_fixed = std::max<size_t>(max_size_per_row_fixed, size_of_value);
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
        throw Exception("Inconsistent number of columns passed to MergeTreeBlockSizePredictor. "
                        "Have " + toString(sample_block.columns()) + " in sample block "
                        "and " + toString(columns.size()) + " columns in list", ErrorCodes::LOGICAL_ERROR);

    if (!is_initialized_in_update)
    {
        /// Reinitialize with read block to update estimation for DEFAULT and MATERIALIZED columns without data.
        initialize(sample_block, columns, {}, true);
        is_initialized_in_update = true;
    }

    if (num_rows < block_size_rows)
    {
        throw Exception("Updated block has less rows (" + toString(num_rows) + ") than previous one (" + toString(block_size_rows) + ")",
                        ErrorCodes::LOGICAL_ERROR);
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
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeData::DataPartPtr & data_part,
    const Names & required_columns,
    const Names & system_columns,
    const PrewhereInfoPtr & prewhere_info,
    bool with_subcolumns)
{
    Names column_names = required_columns;
    Names pre_column_names;

    /// Read system columns such as lightweight delete mask "_row_exists" if it is persisted in the part
    for (const auto & name : system_columns)
    {
        if (data_part->getColumns().contains(name))
            column_names.push_back(name);
    }

    /// inject columns required for defaults evaluation
    injectRequiredColumns(
        storage, storage_snapshot, data_part, with_subcolumns, column_names);

    MergeTreeReadTaskColumns result;
    auto options = GetColumnsOptions(GetColumnsOptions::All)
        .withExtendedObjects()
        .withSystemColumns();
    if (with_subcolumns)
        options.withSubcolumns();

    if (prewhere_info)
    {
        NameSet pre_name_set;

        /// Add column reading steps:
        /// 1. Columns for row level filter
        if (prewhere_info->row_level_filter)
        {
            Names row_filter_column_names =  prewhere_info->row_level_filter->getRequiredColumnsNames();
            result.pre_columns.push_back(storage_snapshot->getColumnsByNames(options, row_filter_column_names));
            pre_name_set.insert(row_filter_column_names.begin(), row_filter_column_names.end());
        }

        /// 2. Columns for prewhere
        Names all_pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

        const auto injected_pre_columns = injectRequiredColumns(
            storage, storage_snapshot, data_part, with_subcolumns, all_pre_column_names);

        for (const auto & name : all_pre_column_names)
        {
            if (pre_name_set.contains(name))
                continue;
            pre_column_names.push_back(name);
            pre_name_set.insert(name);
        }

        Names post_column_names;
        for (const auto & name : column_names)
            if (!pre_name_set.contains(name))
                post_column_names.push_back(name);

        column_names = post_column_names;
    }

    result.pre_columns.push_back(storage_snapshot->getColumnsByNames(options, pre_column_names));

    /// 3. Rest of the requested columns
    result.columns = storage_snapshot->getColumnsByNames(options, column_names);
    return result;
}


std::string MergeTreeReadTaskColumns::dump() const
{
    WriteBufferFromOwnString s;
    for (size_t i = 0; i < pre_columns.size(); ++i)
    {
        s << "STEP " << i << ": " << pre_columns[i].toString() << "\n";
    }
    s << "COLUMNS: " << columns.toString() << "\n";
    return s.str();
}

}
