#include "WatermarkTransform.h"
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/WindowView/StorageWindowView.h>


namespace DB
{

WatermarkTransform::WatermarkTransform(
    const Block & header_,
    StorageWindowView & storage_,
    const String & window_column_name_,
    UInt32 lateness_upper_bound_)
    : ISimpleTransform(header_, header_, false)
    , block_header(header_)
    , storage(storage_)
    , window_column_name(window_column_name_)
    , lateness_upper_bound(lateness_upper_bound_)
{
}

WatermarkTransform::~WatermarkTransform()
{
    if (max_watermark)
        storage.updateMaxWatermark(max_watermark);
    if (lateness_upper_bound)
        storage.addFireSignal(late_signals);
}

void WatermarkTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    auto column_window_idx = block_header.getPositionByName(window_column_name);
    const auto & window_column = columns[column_window_idx];
    const ColumnUInt32::Container & window_end_data = static_cast<const ColumnUInt32 &>(*window_column).getData();
    for (const auto & ts : window_end_data)
    {
        max_watermark = std::max(ts, max_watermark);
        if (lateness_upper_bound && ts <= lateness_upper_bound)
            late_signals.insert(ts);
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
