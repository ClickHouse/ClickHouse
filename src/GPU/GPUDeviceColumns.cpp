#include <GPU/GPUDeviceColumns.h>

#if USE_GPU

#include <GPU/GPUColumns.h>

#include <Core/Block.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

GPUDeviceColumns::GPUDeviceColumns(
    std::vector<DeviceColumnView> views_, size_t rows_, std::vector<std::shared_ptr<const void>> owners_, cuda::stream_ref stream)
    : GPUDeviceColumns(std::move(views_), rows_, std::move(owners_), SharedEvent(createEvent()))
{
    checkCuda(cudaEventRecord(ready.get(), stream.get()), "Cannot mark a point in a stream");
}

GPUDeviceColumns::GPUDeviceColumns(
    std::vector<DeviceColumnView> views_, size_t rows_, std::vector<std::shared_ptr<const void>> owners_, SharedEvent ready_)
    : column_views(std::move(views_))
    , num_rows(rows_)
    , memory_owners(std::move(owners_))
    , ready(std::move(ready_))
{
    for (const auto & view : column_views)
    {
        if (view.rows() != num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A device column of {} rows in a chunk of {}", view.rows(), num_rows);
    }
}

std::shared_ptr<const GPUDeviceColumns> GPUDeviceColumns::select(const std::vector<size_t> & positions) const
{
    std::vector<DeviceColumnView> selected;
    selected.reserve(positions.size());
    for (const size_t position : positions)
    {
        if (position >= column_views.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Device column {} of a chunk of {}", position, column_views.size());
        selected.push_back(column_views[position]);
    }
    return std::shared_ptr<const GPUDeviceColumns>(new GPUDeviceColumns(std::move(selected), num_rows, memory_owners, ready));
}

std::shared_ptr<const GPUDeviceColumns> GPUDeviceColumns::slice(size_t offset, size_t rows) const
{
    if (offset + rows > num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows {} to {} of device columns of {} rows", offset, offset + rows, num_rows);

    std::vector<DeviceColumnView> sliced;
    sliced.reserve(column_views.size());
    for (const auto & view : column_views)
    {
        if (view.kind != GPUColumnKind::Fixed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A slice of a device column of values of varying width");
        sliced.push_back(DeviceFixedColumn{
            .element_type = view.fixed.element_type,
            .data = view.fixed.data + offset * sizeOf(view.fixed.element_type),
            .rows = rows});
    }
    return std::shared_ptr<const GPUDeviceColumns>(new GPUDeviceColumns(std::move(sliced), rows, memory_owners, ready));
}

void GPUDeviceColumns::waitIn(cuda::stream_ref stream) const
{
    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for device columns");
}

Columns GPUDeviceColumns::materialize(const Block & header) const
{
    if (header.columns() != column_views.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} device columns of a chunk materialized for a header of {}", column_views.size(), header.columns());

    /// On a stream of the thread that pulls the chunk, so that the threads copy theirs at once.
    thread_local StreamPtr thread_stream = createStream();
    const cuda::stream_ref stream{thread_stream.get()};
    waitIn(stream);

    MutableColumns columns;
    std::vector<IColumn *> to;
    columns.reserve(header.columns());
    to.reserve(header.columns());
    for (const auto & column : header)
    {
        columns.push_back(column.type->createColumn());
        to.push_back(columns.back().get());
    }

    copyDeviceToHost(column_views, to, stream);

    Columns result;
    result.reserve(columns.size());
    for (auto & column : columns)
        result.push_back(std::move(column));
    return result;
}

std::shared_ptr<const GPUDeviceColumns> gpuColumnsOf(const Chunk & chunk)
{
    return std::dynamic_pointer_cast<const GPUDeviceColumns>(deviceColumnsOf(chunk));
}

}

#endif
