#include <GPU/GPUColumns.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <cstring>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

std::optional<GPUColumnType> columnTypeOf(const IDataType & type)
{
    if (isString(type))
        return GPUColumnType{GPUColumnKind::Variable, GPUElementType::UInt8};

    if (const auto element_type = elementTypeOf(type))
        return GPUColumnType{GPUColumnKind::Fixed, *element_type};

    return std::nullopt;
}

GPUColumnType columnTypeOrThrow(const IDataType & type)
{
    if (const auto column_type = columnTypeOf(type))
        return *column_type;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} for the device, which has no layout for it", type.getName());
}

ColumnLayout ColumnLayout::of(GPUColumnType type)
{
    switch (type.kind)
    {
        case GPUColumnKind::Fixed:
            return {.type = type, .buffers = {{.element_size = sizeOf(type.element_type), .offsets_into = std::nullopt}}};
        case GPUColumnKind::Variable:
            return {
                .type = type,
                .buffers = {{.element_size = sizeof(UInt64), .offsets_into = 1}, {.element_size = 1, .offsets_into = std::nullopt}},
            };
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind {}", static_cast<int>(type.kind));
}

std::vector<std::string_view> ColumnLayout::buffersOf(const IColumn & column) const
{
    switch (type.kind)
    {
        case GPUColumnKind::Fixed:
            return {rawValuesOf(column, column.size(), sizeOf(type.element_type))};
        case GPUColumnKind::Variable:
        {
            const auto * strings = typeid_cast<const ColumnString *>(&column);
            if (!strings)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} where the device takes strings", column.getName());

            const auto & offsets = strings->getOffsets();
            const auto & chars = strings->getChars();
            return {
                {reinterpret_cast<const char *>(offsets.data()), offsets.size() * sizeof(UInt64)},
                {reinterpret_cast<const char *>(chars.data()), chars.size()},
            };
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind {}", static_cast<int>(type.kind));
}

DeviceColumnView ColumnLayout::viewOf(std::span<char * const> data, size_t rows, std::span<const size_t> elements) const
{
    if (data.size() != buffers.size() || elements.size() != buffers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A view of {} buffers of a column made of {}", data.size(), buffers.size());

    switch (type.kind)
    {
        case GPUColumnKind::Fixed:
            return {.element_type = type.element_type, .data = data[0], .rows = rows};
        case GPUColumnKind::Variable:
            return {
                .element_type = type.element_type,
                .data = data[1],
                .rows = rows,
                .kind = GPUColumnKind::Variable,
                .offsets = reinterpret_cast<const uint64_t *>(data[0]),
                .bytes = elements[1],
            };
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind {}", static_cast<int>(type.kind));
}

std::vector<std::pair<const char *, size_t>> ColumnLayout::buffersOf(const DeviceColumnView & view) const
{
    if (view.type() != type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A view of another type of column than its layout's");

    switch (type.kind)
    {
        case GPUColumnKind::Fixed:
            return {{view.data, view.rows * sizeOf(type.element_type)}};
        case GPUColumnKind::Variable:
            return {{reinterpret_cast<const char *>(view.offsets), (view.rows + 1) * sizeof(UInt64)}, {view.data, view.bytes}};
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind {}", static_cast<int>(type.kind));
}

void ColumnLayout::fill(IColumn & column, size_t rows, std::span<const std::string_view> from) const
{
    if (from.size() != buffers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} buffers for a column made of {}", from.size(), buffers.size());

    switch (type.kind)
    {
        case GPUColumnKind::Fixed:
        {
            const HostColumnView values = resizeForElementType(column, rows, type.element_type);
            if (from[0].size() != rows * sizeOf(type.element_type))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of {} values of {} bytes", from[0].size(), rows, sizeOf(type.element_type));
            memcpy(values.data, from[0].data(), from[0].size());
            return;
        }
        case GPUColumnKind::Variable:
        {
            auto * strings = typeid_cast<ColumnString *>(&column);
            if (!strings || !strings->empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot put strings from the device into a column of {}", column.getName());

            if (from[0].size() != (rows + 1) * sizeof(UInt64))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of offsets for {} strings", from[0].size(), rows);

            const auto * offsets = reinterpret_cast<const UInt64 *>(from[0].data());
            if (offsets[0] != 0 || offsets[rows] != from[1].size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Strings from the device run from {} to {} over {} bytes", offsets[0], offsets[rows], from[1].size());

            auto & chars = strings->getChars();
            chars.resize(from[1].size());
            memcpy(chars.data(), from[1].data(), from[1].size());

            auto & column_offsets = strings->getOffsets();
            column_offsets.resize(rows);
            memcpy(column_offsets.data(), offsets + 1, rows * sizeof(UInt64));
            return;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU column kind {}", static_cast<int>(type.kind));
}

void ColumnDownload::add(const DeviceColumnView & view, IColumn & column)
{
    Pending & added = pending.emplace_back(Pending{.layout = ColumnLayout::of(view.type()), .column = &column, .rows = view.rows, .buffers = {}});

    const auto device_buffers = added.layout.buffersOf(view);
    added.buffers.resize(device_buffers.size());
    for (size_t i = 0; i < device_buffers.size(); ++i)
        added.buffers[i].appendFromDevice(device_buffers[i].first, device_buffers[i].second, stream);
}

void ColumnDownload::finish()
{
    checkCuda(cudaStreamSynchronize(stream), "Cannot wait for columns to be copied back from the device");

    for (Pending & column : pending)
    {
        std::vector<std::string_view> buffers;
        buffers.reserve(column.buffers.size());
        for (const PinnedBuffer & buffer : column.buffers)
            buffers.push_back(buffer.bytes());
        column.layout.fill(*column.column, column.rows, buffers);
    }

    pending.clear();
}

}

#endif
