#pragma once

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexBloomSliced.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <roaring/roaring.hh>

#include <memory>
#include <vector>

namespace DB
{

class MergeTreeReaderBloomSlicedIndex : public IMergeTreeReader
{
public:
    MergeTreeReaderBloomSlicedIndex(
        const IMergeTreeReader * main_reader_,
        MergeTreeIndexWithCondition index_,
        NamesAndTypesList columns_,
        MergeTreeIndexGranulePtr index_granule_);

    bool canReadIncompleteGranules() const override { return false; }

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t rows_offset,
        Columns & res_columns) override;

private:
    enum class CachedBitmapKind : UInt8
    {
        AllFalse,
        AllTrue,
        Bitmap,
    };

    void initializeBitmaps();

    MergeTreeIndexWithCondition index;
    std::shared_ptr<const MergeTreeIndexGranuleBloomSliced> granule;
    std::vector<CachedBitmapKind> cached_bitmap_kinds;
    std::vector<roaring::Roaring> cached_bitmaps;
    bool initialized = false;
    size_t current_row = 0;
    size_t current_mark = 0;
};

MergeTreeReaderPtr createMergeTreeReaderBloomSlicedIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read,
    MergeTreeIndexGranulePtr index_granule);

}
