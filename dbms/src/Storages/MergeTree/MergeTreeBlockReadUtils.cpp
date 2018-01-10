#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnConst.h>
#include <unordered_set>


namespace DB
{

NameSet injectRequiredColumns(const MergeTreeData & storage, const MergeTreeData::DataPartPtr & part, Names & columns)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    auto all_column_files_missing = true;

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column_name = columns[i];

        /// column has files and hence does not require evaluation
        if (part->hasColumnFiles(column_name))
        {
            all_column_files_missing = false;
            continue;
        }

        const auto default_it = storage.column_defaults.find(column_name);
        /// columns has no explicit default expression
        if (default_it == std::end(storage.column_defaults))
            continue;

        /// collect identifiers required for evaluation
        IdentifierNameSet identifiers;
        default_it->second.expression->collectIdentifierNames(identifiers);

        for (const auto & identifier : identifiers)
        {
            if (storage.hasColumn(identifier))
            {
                /// ensure each column is added only once
                if (required_columns.count(identifier) == 0)
                {
                    columns.emplace_back(identifier);
                    required_columns.emplace(identifier);
                    injected_columns.emplace(identifier);
                }
            }
        }
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (all_column_files_missing)
    {
        const auto minimum_size_column_name = part->getColumnNameWithMinumumCompressedSize();
        columns.push_back(minimum_size_column_name);
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}


MergeTreeReadTask::MergeTreeReadTask(
    const MergeTreeData::DataPartPtr & data_part, const MarkRanges & mark_ranges, const size_t part_index_in_query,
    const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
    const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder,
    MergeTreeBlockSizePredictorPtr && size_predictor)
: data_part{data_part}, mark_ranges{mark_ranges}, part_index_in_query{part_index_in_query},
ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
remove_prewhere_column{remove_prewhere_column}, should_reorder{should_reorder}, size_predictor{std::move(size_predictor)}
{}

MergeTreeReadTask::~MergeTreeReadTask() = default;


MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const MergeTreeData::DataPartPtr & data_part_, const Names & columns, const Block & sample_block)
    : data_part(data_part_)
{
    number_of_rows_in_part = data_part->rows_count;
    /// Initialize with sample block untill update won't called.
    initialize(sample_block, columns);
}

void MergeTreeBlockSizePredictor::initialize(const Block & sample_block, const Names & columns, bool from_update)
{
    fixed_columns_bytes_per_row = 0;
    dynamic_columns_infos.clear();

    std::unordered_set<String> names_set;
    if (!from_update)
        names_set.insert(columns.begin(), columns.end());

    for (const auto & column_with_type_and_name : sample_block)
    {
        const String & column_name = column_with_type_and_name.name;
        const ColumnPtr & column_data = column_with_type_and_name.column;

        const auto column_checksum = data_part->tryGetBinChecksum(column_name);

        if (!from_update && !names_set.count(column_name))
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
            info.bytes_per_row_global = column_checksum
                ? column_checksum->uncompressed_size / number_of_rows_in_part
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
void MergeTreeBlockSizePredictor::update(const Block & block, double decay)
{
    if (!is_initialized_in_update)
    {
        /// Reinitialize with read block to update estimation for DEFAULT and MATERIALIZED columns without data.
        initialize(block, {}, true);
        is_initialized_in_update = true;
    }
    size_t new_rows = block.rows();
    if (new_rows < block_size_rows)
    {
        throw Exception("Updated block has less rows (" + toString(new_rows) + ") than previous one (" + toString(block_size_rows) + ")",
                        ErrorCodes::LOGICAL_ERROR);
    }

    size_t diff_rows = new_rows - block_size_rows;
    block_size_bytes = new_rows * fixed_columns_bytes_per_row;
    bytes_per_row_current = fixed_columns_bytes_per_row;
    block_size_rows = new_rows;

    /// Make recursive updates for each read row: v_{i+1} = (1 - decay) v_{i} + decay v_{target}
    /// Use sum of gemetric sequence formula to update multiple rows: v{n} = (1 - decay)^n v_{0} + (1 - (1 - decay)^n) v_{target}
    /// NOTE: DEFAULT and MATERIALIZED columns without data has inaccurate estimation of v_{target}
    double alpha = std::pow(1. - decay, diff_rows);

    max_size_per_row_dynamic = 0;
    for (auto & info : dynamic_columns_infos)
    {
        size_t new_size = block.getByName(info.name).column->byteSize();
        size_t diff_size = new_size - info.size_bytes;

        double local_bytes_per_row = static_cast<double>(diff_size) / diff_rows;
        info.bytes_per_row = alpha * info.bytes_per_row + (1. - alpha) * local_bytes_per_row;

        info.size_bytes = new_size;
        block_size_bytes += new_size;
        bytes_per_row_current += info.bytes_per_row;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
}

}
