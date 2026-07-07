#include <Storages/MergeTree/MergeTreeReaderBloomSlicedIndex.h>

#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <algorithm>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

MergeTreeReaderBloomSlicedIndex::MergeTreeReaderBloomSlicedIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_,
    NamesAndTypesList columns_,
    MergeTreeIndexGranulePtr index_granule_)
    : IMergeTreeReader(
          main_reader_->data_part_info_for_read,
          columns_,
          {},
          main_reader_->storage_snapshot,
          main_reader_->storage_settings,
          nullptr,
          nullptr,
          main_reader_->all_mark_ranges,
          main_reader_->settings)
    , index(std::move(index_))
    , granule(std::dynamic_pointer_cast<const MergeTreeIndexGranuleBloomSliced>(index_granule_))
{
    for (const auto & column : columns_)
    {
        if (!isBloomSlicedVirtualColumn(column.name) || !WhichDataType(column.type).isUInt8())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} with type {} should not be filled by bloom_sliced index reader",
                column.name,
                column.type->getName());
        }
    }
}

size_t MergeTreeReaderBloomSlicedIndex::readRows(
    size_t from_mark,
    size_t /*current_task_last_mark*/,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();

    size_t from_row = 0;
    if (continue_reading)
    {
        from_row = current_row + rows_offset;
    }
    else
    {
        from_row = index_granularity.getMarkStartingRow(from_mark) + rows_offset;
        current_mark = from_mark;
    }

    const size_t total_rows = data_part_info_for_read->getRowCount();
    if (from_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - from_row);
    else
        max_rows_to_read = 0;

    if (!initialized && max_rows_to_read > 0)
    {
        initializeBitmaps();
        initialized = true;
    }

    for (size_t i = 0; i < res_columns.size(); ++i)
    {
        if (!res_columns[i])
            res_columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);

        auto mutable_column = IColumn::mutate(std::move(res_columns[i]));
        auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const size_t old_size = data.size();
        data.resize(old_size + max_rows_to_read);

        if (max_rows_to_read == 0 || cached_bitmap_kinds.empty())
        {
            res_columns[i] = std::move(mutable_column);
            continue;
        }

        if (cached_bitmap_kinds[i] == CachedBitmapKind::AllTrue)
        {
            std::fill(data.begin() + old_size, data.end(), 1);
        }
        else if (cached_bitmap_kinds[i] == CachedBitmapKind::AllFalse)
        {
            std::fill(data.begin() + old_size, data.end(), 0);
        }
        else
        {
            const auto & bitmap = cached_bitmaps[i];
            const size_t row_end = from_row + max_rows_to_read;
            const UInt64 matching_rows = roaring::api::roaring_bitmap_range_cardinality(&bitmap.roaring, from_row, row_end);

            if (matching_rows == 0)
            {
                std::fill(data.begin() + old_size, data.end(), 0);
            }
            else if (matching_rows == max_rows_to_read)
            {
                std::fill(data.begin() + old_size, data.end(), 1);
            }
            else
            {
                std::fill(data.begin() + old_size, data.end(), 0);

                roaring::api::roaring_uint32_iterator_t iterator;
                roaring::api::roaring_iterator_init(&bitmap.roaring, &iterator);
                if (roaring::api::roaring_uint32_iterator_move_equalorlarger(&iterator, static_cast<UInt32>(from_row)))
                {
                    while (iterator.has_value && iterator.current_value < row_end)
                    {
                        data[old_size + iterator.current_value - from_row] = 1;
                        roaring::api::roaring_uint32_iterator_advance(&iterator);
                    }
                }
            }
        }

        res_columns[i] = std::move(mutable_column);
    }

    current_row = from_row + max_rows_to_read;
    while (current_mark < index_granularity.getMarksCountWithoutFinal() && index_granularity.getMarkStartingRow(current_mark) < current_row)
        ++current_mark;

    return max_rows_to_read;
}

void MergeTreeReaderBloomSlicedIndex::initializeBitmaps()
{
    cached_bitmap_kinds.assign(columns_to_read.size(), CachedBitmapKind::AllTrue);
    cached_bitmaps.assign(columns_to_read.size(), roaring::Roaring{});

    if (!granule)
        return;

    const auto & condition_bloom_sliced = assert_cast<const MergeTreeIndexConditionBloomSliced &>(*index.condition_template->generateUnsubstituted());
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        auto predicate = condition_bloom_sliced.getTokenPredicateForVirtualColumn(columns_to_read[i].name);
        auto bitmap = granule->bitmapForPredicate(predicate);

        if (bitmap.isEmpty())
        {
            cached_bitmap_kinds[i] = CachedBitmapKind::AllFalse;
        }
        else if (bitmap.cardinality() == granule->row_count)
        {
            cached_bitmap_kinds[i] = CachedBitmapKind::AllTrue;
        }
        else
        {
            cached_bitmap_kinds[i] = CachedBitmapKind::Bitmap;
            cached_bitmaps[i] = std::move(bitmap);
        }
    }
}

MergeTreeReaderPtr createMergeTreeReaderBloomSlicedIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read,
    MergeTreeIndexGranulePtr index_granule)
{
    return std::make_unique<MergeTreeReaderBloomSlicedIndex>(main_reader, index, columns_to_read, std::move(index_granule));
}

}
