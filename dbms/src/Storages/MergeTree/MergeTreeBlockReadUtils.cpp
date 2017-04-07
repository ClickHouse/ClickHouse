#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB {


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
    const MergeTreeData::DataPartPtr & data_part, const MarkRanges & mark_ranges, const std::size_t part_index_in_query,
    const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
    const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder,
    const MergeTreeBlockSizePredictorPtr & size_predictor)
: data_part{data_part}, mark_ranges{mark_ranges}, part_index_in_query{part_index_in_query},
ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
remove_prewhere_column{remove_prewhere_column}, should_reorder{should_reorder}, size_predictor{size_predictor}
{}

MergeTreeReadTask::~MergeTreeReadTask() = default;


MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns,
    const NamesAndTypesList & pre_columns,
    size_t index_granularity_)
: data_part(data_part_), index_granularity(index_granularity_)
{
    auto add_column = [&] (const NameAndTypePair & column)
    {
        ColumnPtr column_data = column.type->createColumn();
        const auto column_checksum = data_part->tryGetBinChecksum(column.name);

        /// There are no data files, column will be const
        if (!column_checksum)
            return;

        if (column_data->isFixed())
        {
            fixed_columns_bytes_per_row += column_data->sizeOfField();
        }
        else
        {
            ColumnInfo info;
            info.name = column.name;
            info.bytes_per_row_global = column_checksum->uncompressed_size;

            dynamic_columns_infos.emplace_back(info);
        }
    };

    for (const NameAndTypePair & column : pre_columns)
        add_column(column);

    for (const NameAndTypePair & column : columns)
        add_column(column);

    size_t rows_approx = data_part->tryGetExactSizeRows().first;

    bytes_per_row_global = fixed_columns_bytes_per_row;
    for (auto & info : dynamic_columns_infos)
    {
        info.bytes_per_row_global /= rows_approx;
        info.bytes_per_row = info.bytes_per_row_global;
        bytes_per_row_global += info.bytes_per_row_global;
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


void MergeTreeBlockSizePredictor::update(const Block & block, size_t read_marks)
{
    size_t dif_rows = read_marks * index_granularity;
    block_size_rows += dif_rows;
    block_size_bytes = block_size_rows * fixed_columns_bytes_per_row;
    bytes_per_row_current = fixed_columns_bytes_per_row;

    /// Make recursive updates for each read mark
    double alpha = std::pow(1. - decay, read_marks);

    for (auto & info : dynamic_columns_infos)
    {
        size_t new_size = block.getByName(info.name).column->byteSize();
        size_t dif_size = new_size - info.size_bytes;

        double local_bytes_per_row = static_cast<double>(dif_size) / dif_rows;
        info.bytes_per_row = alpha * info.bytes_per_row + (1. - alpha) * local_bytes_per_row;

        info.size_bytes = new_size;
        block_size_bytes += new_size;
        bytes_per_row_current += info.bytes_per_row;
    }
}

}
