#include <Storages/MergeTree/MergeTreeLazilyReader.h>

#include <base/defines.h>
#include <Columns/ColumnLazy.h>
#include <Common/Logger.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSparse.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_uncompressed_cache;
}

void matchDataPartToRowOffsets(
    const ColumnUInt64 * row_num_column,
    const ColumnUInt64 * part_num_column,
    PartIndexToRowOffsets & part_to_row_offsets,
    IColumn::Permutation & permutation)
{
    const size_t rows_size = row_num_column->size();

    for (size_t row_idx = 0; row_idx < rows_size; ++row_idx)
    {
        size_t row_offset = row_num_column->getUInt(row_idx);
        size_t part_index = part_num_column->getUInt(row_idx);

        part_to_row_offsets[part_index].emplace_back(row_offset, row_idx);
    }

    size_t curr_idx = 0;
    permutation.resize(rows_size);

    for (auto & part_and_row_offsets : part_to_row_offsets)
    {
        auto & row_offsets_with_idx = part_and_row_offsets.second;

        std::sort(
            row_offsets_with_idx.begin(),
            row_offsets_with_idx.end(),
            [](const RowOffsetWithIdx & left, const RowOffsetWithIdx & right) { return left.row_offset < right.row_offset; });

        for (const auto & row_offset_with_idx : row_offsets_with_idx)
        {
            permutation[row_offset_with_idx.row_idx] = curr_idx++;
        }
    }
}

static void addDummyColumnWithRowCount(Block & block, Columns & res_columns, size_t num_rows)
{
    bool has_columns = false;
    for (const auto & column : res_columns)
    {
        if (column)
        {
            has_columns = true;
            break;
        }
    }

    if (has_columns)
        return;

    ColumnWithTypeAndName dummy_column;
    dummy_column.column = DataTypeUInt8().createColumnConst(num_rows, Field(1));
    dummy_column.type = std::make_shared<DataTypeUInt8>();
    dummy_column.name = "....dummy...." + toString(UUIDHelpers::generateV4());
    block.insert(dummy_column);
}

MergeTreeLazilyReader::MergeTreeLazilyReader(
    SharedHeader header_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const LazilyReadInfoPtr & lazily_read_info_,
    const ContextPtr & context_,
    const AliasToName & alias_index_)
    : storage(storage_)
    , data_part_infos(lazily_read_info_->data_part_infos)
    , storage_snapshot(storage_snapshot_)
    , use_uncompressed_cache(context_->getSettingsRef()[Setting::use_uncompressed_cache])
{
    NameSet columns_name_set;

    for (const auto & column_name : lazily_read_info_->lazily_read_columns)
        columns_name_set.insert(column_name.name);

    for (const auto & column : *header_)
    {
        const auto it = alias_index_.find(column.name);
        if (it == alias_index_.end())
            continue; // TODO: check if it's what we want

        const auto & requested_column_name = it->second;
        if (columns_name_set.contains(requested_column_name))
        {
            requested_column_names.emplace_back(requested_column_name);
            lazy_columns.emplace_back(header_->getByName(column.name));
        }
    }
}

void MergeTreeLazilyReader::readLazyColumns(
    const MergeTreeReaderSettings & reader_settings,
    const PartIndexToRowOffsets & part_to_row_offsets,
    MutableColumns & lazily_read_columns)
{
    const auto columns_size = lazily_read_columns.size();

    for (const auto & it : part_to_row_offsets)
    {
        auto part_index = it.first;
        const auto & row_offsets = it.second;

        auto it_part_info = data_part_infos->find(part_index);
        if (it_part_info == data_part_infos->end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part index '{}' not found in data parts info", part_index);

        const auto & data_part_info = it_part_info->second;
        const auto & index_granularity = data_part_info->getIndexGranularity();

        MarkRanges mark_ranges;

        for (const auto & row_offset_with_idx : row_offsets)
        {
            auto row_offset = row_offset_with_idx.row_offset;
            MarkRange mark_range = index_granularity.getMarkRangeForRowOffset(row_offset);
            mark_ranges.push_back(mark_range);
        }

        Names tmp_requested_column_names(requested_column_names.begin(), requested_column_names.end());
        injectRequiredColumns(
            *data_part_info,
            storage_snapshot,
            storage.supportsSubcolumns(),
            tmp_requested_column_names);

        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
            .withSubcolumns(storage.supportsSubcolumns());

        NamesAndTypesList columns_for_reader = storage_snapshot->getColumnsByNames(options, tmp_requested_column_names);

        MergeTreeReaderPtr reader = createMergeTreeReader(
            data_part_info,
            columns_for_reader,
            storage_snapshot,
            storage.getSettings(),
            mark_ranges,
            /*virtual_fields=*/ {},
            use_uncompressed_cache ? storage.getContext()->getUncompressedCache().get() : nullptr,
            storage.getContext()->getMarkCache().get(),
            /*deserialization_prefixes_cache=*/ nullptr,
            reader_settings,
            ValueSizeMap{},
            ReadBufferFromFileBase::ProfileCallback{});

        size_t idx = 0;
        auto * mark_range_iter = mark_ranges.begin();
        auto row_offset = row_offsets[idx].row_offset;
        size_t next_offset = row_offset - index_granularity.getMarkStartingRow(mark_range_iter->begin);
        size_t skipped_rows = next_offset;
        bool continue_reading = false;

        while (mark_range_iter != mark_ranges.end())
        {
            Columns columns_to_read;
            columns_to_read.resize(columns_for_reader.size());

            /// Read a row of data from wide or compact MergeTree, skipping the first skipped_rows rows.
            auto read_rows = reader->readRows(mark_range_iter->begin, mark_range_iter->end, continue_reading,
                                                      1, skipped_rows, columns_to_read);

            /// Handle cases where columns are missing in MergeTree.
            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns_to_read, should_evaluate_missing_defaults, read_rows);

            if (should_evaluate_missing_defaults)
            {
                Block block;
                addDummyColumnWithRowCount(block, columns_to_read, read_rows);
                reader->evaluateMissingDefaults(block, columns_to_read);
            }

            reader->performRequiredConversions(columns_to_read);

            for (auto & col : columns_to_read)
                col = removeSpecialRepresentations(col->convertToFullColumnIfConst());

            for (size_t i = 0; i < columns_size; ++i)
                lazily_read_columns[i]->insertFrom((*columns_to_read[i]), 0);

            auto prev_mark = mark_range_iter->begin;
            auto prev_offset = next_offset;

            while (true)
            {
                ++mark_range_iter;
                ++idx;

                if (mark_range_iter == mark_ranges.end())
                    break;

                row_offset = row_offsets[idx].row_offset;
                next_offset = row_offset - index_granularity.getMarkStartingRow(mark_range_iter->begin);
                skipped_rows = next_offset;

                if (mark_range_iter->begin == prev_mark)
                {
                    if (next_offset - prev_offset < read_rows)
                    {
                        /// The next row of data was already read during the previous read.
                        for (size_t i = 0; i < columns_size; ++i)
                            lazily_read_columns[i]->insertFrom((*columns_to_read[i]), next_offset - prev_offset);
                    }
                    else
                    {
                        /// If the next row of data is within the same granule, calculate the number of rows to skip for the next read.
                        chassert(next_offset > prev_offset);
                        skipped_rows = next_offset - prev_offset - 1;
                        continue_reading = true;
                        break;
                    }
                }
                else
                {
                    /// If the next row of data is not within the same granule, reposition to the next granule.
                    continue_reading = false;
                    break;
                }
            }

        }
    }
}

void MergeTreeLazilyReader::transformLazyColumns(
    const ColumnLazy & column_lazy,
    ColumnsWithTypeAndName & res_columns)
{
    const size_t columns_size = lazy_columns.size();
    const auto & columns = column_lazy.getColumns();

    chassert(res_columns.empty());
    chassert(columns.size() == 2);
    const auto * row_num_column = typeid_cast<const ColumnUInt64 *>(columns[0].get());
    const auto * part_num_column = typeid_cast<const ColumnUInt64 *>(columns[1].get());

    chassert(row_num_column->size() == part_num_column->size());
    const size_t rows_size = row_num_column->size();

    ReadSettings read_settings;
    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = true,
    };

    MutableColumns lazily_read_columns;
    lazily_read_columns.resize(columns_size);

    for (size_t i = 0; i < lazy_columns.size(); ++i)
    {
        const auto & column_with_type_and_name = lazy_columns[i];
        lazily_read_columns[i] = column_with_type_and_name.type->createColumn();
        lazily_read_columns[i]->reserve(rows_size);
        res_columns.emplace_back(column_with_type_and_name.type, column_with_type_and_name.name);
    }

    PartIndexToRowOffsets part_to_row_offsets;
    IColumn::Permutation permutation;

    /// Reorder the rows to organize rows within the same data part sequentially, making full use of sequential reads.
    matchDataPartToRowOffsets(row_num_column, part_num_column, part_to_row_offsets, permutation);

    /// Actually read the lazily materialized column data from MergeTree.
    readLazyColumns(reader_settings, part_to_row_offsets, lazily_read_columns);

    /// Restore the original order of the rows.
    for (size_t i = 0; i < lazily_read_columns.size(); ++i)
        res_columns[i].column = lazily_read_columns[i]->permute(permutation, 0);
}

}
