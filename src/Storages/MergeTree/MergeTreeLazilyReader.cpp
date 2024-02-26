#include <Storages/MergeTree/MergeTreeLazilyReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Columns/ColumnLazy.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include "base/defines.h"

namespace DB
{

MergeTreeLazilyReader::MergeTreeLazilyReader(
    const Block & header_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const LazilyReadInfoPtr & lazily_read_info_,
    const ContextPtr & context_,
    const AliasToNamePtr & alias_index_)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , use_uncompressed_cache(context_->getSettings().use_uncompressed_cache)
    , lazily_read_info(lazily_read_info_)
{
    NameSet columns_name_set(lazily_read_info_->lazily_read_columns_names.begin(),
                             lazily_read_info_->lazily_read_columns_names.end());
    for (const auto & it : header_)
    {
        const auto & requested_column_name = (*alias_index_)[it.name];
        if (columns_name_set.contains(requested_column_name))
        {
            requested_column_names.emplace_back(requested_column_name);
            lazy_columns.emplace_back(header_.getByName(it.name));
        }
    }
}

void MergeTreeLazilyReader::transformLazyColumns(
    const ColumnLazy & column_lazy,
    ColumnsWithTypeAndName & res_columns)
{
    const size_t columns_size = lazy_columns.size();
    const auto & columns = column_lazy.getColumns();
    const size_t origin_size = res_columns.size();

    chassert(columns.size() == 2);
    const auto * row_num_column = typeid_cast<const ColumnUInt64 *>(columns[0].get());
    const auto * part_num_column = typeid_cast<const ColumnUInt64 *>(columns[1].get());
    const size_t rows_size = part_num_column->size();

    ReadSettings read_settings;
    read_settings.direct_io_threshold = 1;
    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
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

    for (size_t row_idx = 0; row_idx < rows_size; ++row_idx)
    {
        size_t row_offset = row_num_column->getUInt(row_idx);
        size_t part_index = part_num_column->getUInt(row_idx);
        MergeTreeData::DataPartPtr data_part = (*lazily_read_info->data_parts_info)[part_index].data_part;
        AlterConversionsPtr alter_conversions = (*lazily_read_info->data_parts_info)[part_index].alter_conversions;
        MarkRange mark_range = data_part->index_granularity.getMarkRangeForRowOffset(row_offset);
        MarkRanges mark_ranges{mark_range};

        Names tmp_requested_column_names(requested_column_names.begin(), requested_column_names.end());
        injectRequiredColumns(
            LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions),
            storage_snapshot,
            storage.supportsSubcolumns(),
            tmp_requested_column_names);

        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
            .withExtendedObjects()
            .withSystemColumns();
        if (storage.supportsSubcolumns())
            options.withSubcolumns();
        NamesAndTypesList columns_for_reader = storage_snapshot->getColumnsByNames(options, tmp_requested_column_names);

        MergeTreeReaderPtr reader = data_part->getReader(
            columns_for_reader, storage_snapshot, mark_ranges,
            use_uncompressed_cache ? storage.getContext()->getUncompressedCache().get() : nullptr,
            storage.getContext()->getMarkCache().get(), alter_conversions,
            reader_settings, {}, {});

        Columns columns_to_read;
        columns_to_read.resize(columns_for_reader.size());
        size_t current_offset = row_offset - data_part->index_granularity.getMarkStartingRow(mark_range.begin);

        reader->readRows(
            mark_range.begin, mark_range.end, /* continue_reading */false,
            1, current_offset, columns_to_read);

        bool should_evaluate_missing_defaults = false;
        reader->fillMissingColumns(columns_to_read, should_evaluate_missing_defaults, current_offset + 1, data_part->info.min_block);

        if (should_evaluate_missing_defaults)
            reader->evaluateMissingDefaults({}, columns_to_read);

        reader->performRequiredConversions(columns_to_read);

        for (size_t i = 0; i < columns_size; ++i)
            lazily_read_columns[i]->insert((*columns_to_read[i])[0]);
    }

    for (size_t i = origin_size; i < lazily_read_columns.size(); ++i)
        res_columns[i].column = std::move(lazily_read_columns[i]);
}

SerializationPtr MergeTreeLazilyReader::getSerialization()
{
    DataTypes types;
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUInt64>());

    return DataTypeTuple(types).getDefaultSerialization();
}

}
