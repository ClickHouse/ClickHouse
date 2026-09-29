#include <GPU/GPUColumns.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <cstring>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

std::optional<GPUElementType> columnTypeOf(const IDataType & type)
{
    if (isString(type))
        return GPUElementType::String;

    return elementTypeOf(type);
}

GPUElementType columnTypeOrThrow(const IDataType & type)
{
    if (const auto column_type = columnTypeOf(type))
        return *column_type;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} for the device, which does not take it", type.getName());
}

namespace
{

void checkNoNulls(const DeviceColumnView & view)
{
    if (view.null_mask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A column from the device with a null mask, which nothing takes yet");
}

/// Puts `rows` values of `element_type` into an empty `column` from `values`.
void fillFixed(IColumn & column, GPUElementType element_type, size_t rows, std::string_view values)
{
    if (values.size() != rows * sizeOf(element_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of {} values of {} bytes", values.size(), rows, sizeOf(element_type));

    const HostColumnView to = resizeForElementType(column, rows, element_type);
    memcpy(to.data, values.data(), values.size());
}

/// Puts `rows` values of varying width into an empty `ColumnString` from their bytes and their
/// `rows + 1` offsets as the device keeps them, from 0 on.
void fillVariable(IColumn & column, size_t rows, std::string_view offsets, std::string_view chars)
{
    auto * strings = typeid_cast<ColumnString *>(&column);
    if (!strings || !strings->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot put strings from the device into a column of {}", column.getName());

    if (offsets.size() != (rows + 1) * sizeof(UInt64))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of offsets for {} strings", offsets.size(), rows);

    const auto * from = reinterpret_cast<const UInt64 *>(offsets.data());
    if (from[0] != 0 || from[rows] != chars.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Strings from the device run from {} to {} over {} bytes", from[0], from[rows], chars.size());

    auto & column_chars = strings->getChars();
    column_chars.resize(chars.size());
    memcpy(column_chars.data(), chars.data(), chars.size());

    /// A `ColumnString` keeps the ends of its rows, without the leading 0.
    auto & column_offsets = strings->getOffsets();
    column_offsets.resize(rows);
    memcpy(column_offsets.data(), from + 1, rows * sizeof(UInt64));
}

}

const DeviceFixedColumn & fixedOrThrow(const DeviceColumnView & view)
{
    checkNoNulls(view);
    if (view.kind != GPUColumnKind::Fixed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of kind {} where one of fixed-width values is expected", view.kind);
    return view.fixed;
}

void copyDeviceToHost(std::span<const DeviceColumnView> from, std::span<IColumn * const> to, rmm::cuda_stream_view stream)
{
    if (from.size() != to.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} columns from the device into {} columns", from.size(), to.size());

    /// Where each column lands in host memory: the values of a `Fixed` column, or the offsets and
    /// the bytes of a `Variable` one.
    struct Landed
    {
        PinnedBuffer first;
        PinnedBuffer chars;
    };
    std::vector<Landed> landed(from.size());

    for (size_t i = 0; i < from.size(); ++i)
    {
        const DeviceColumnView & view = from[i];
        checkNoNulls(view);
        switch (view.kind)
        {
            case GPUColumnKind::Fixed:
                landed[i].first.appendFromDevice(view.fixed.data, view.fixed.rows * sizeOf(view.fixed.element_type), stream);
                break;
            case GPUColumnKind::Variable:
                if (view.variable.rows == 0)
                    break;
                landed[i].first.appendFromDevice(
                    reinterpret_cast<const char *>(view.variable.offsets), (view.variable.rows + 1) * sizeof(UInt64), stream);
                landed[i].chars.appendFromDevice(view.variable.chars, view.variable.chars_bytes, stream);
                break;
        }
    }

    checkCuda(cudaStreamSynchronize(stream), "Cannot wait for columns to be copied back from the device");

    for (size_t i = 0; i < from.size(); ++i)
    {
        const DeviceColumnView & view = from[i];
        switch (view.kind)
        {
            case GPUColumnKind::Fixed:
                fillFixed(*to[i], view.fixed.element_type, view.fixed.rows, landed[i].first.bytes());
                break;
            case GPUColumnKind::Variable:
                if (view.variable.rows != 0)
                    fillVariable(*to[i], view.variable.rows, landed[i].first.bytes(), landed[i].chars.bytes());
                break;
        }
    }
}

}

#endif
