#include "base/types.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Formats/PngSerializer.h>
#include <Formats/PngWriter.h>
#include <Common/Exception.h>

namespace DB
{


namespace ErrorCodes
{
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS;
extern const int CANNOT_CONVERT_TYPE;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/* Extracts a boolean value specifically for the binary pixel format
 * Handles Nullable, Const wrappers (TODO), ensures the underlying type is Bool */
inline bool extractBool(const IColumn & col, size_t row_num)
{
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        if (null_col->isNullAt(row_num))
            return false;

        const auto & nested = null_col->getNestedColumn();
        return nested.getBool(row_num);
    }

    return col.getBool(row_num);
}

inline UInt16 extractPixelComponentImpl(const IColumn & data_col, size_t row_num, int bit_depth)
{
    auto type_id = data_col.getDataType();
    auto max_val_u16 = (bit_depth == 16) ? 65535 : 255;
    Float64 max_val_float = static_cast<Float64>(max_val_u16);
    switch (type_id)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64: {
            UInt64 val = data_col.getUInt(row_num);
            return static_cast<UInt16>(std::min(val, static_cast<UInt64>(max_val_u16)));
        }

        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64: {
            Int64 val = data_col.getInt(row_num);
            return static_cast<UInt16>(std::clamp(val, INT64_C(0), static_cast<Int64>(max_val_u16)));
        }

        case TypeIndex::Float32:
        case TypeIndex::Float64: {
            Float64 val = data_col.getFloat64(row_num);
            /* IMPORTANT: Input floats are expected to be directly in the target range
             * For 8 bit is [0, 255], or [0, 65535] for 16
             * Values outside this range will be clamped. No automatic scaling */
            val = std::round(std::clamp(val, 0.0, max_val_float));
            return static_cast<UInt16>(val);
        }

        default:
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert to UInt8 for PNG output pixel component. Unsupported type");
    }
}

/* Handles wrapper columns like Nullable, Const, and LowCardinality (TODO),
* then delegates the actual data conversion */
inline UInt16 extractPixelComponent(const IColumn & col, size_t row_num, int bit_depth)
{
    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(&col)) [[unlikely]]
    {
        if (nullable_col->isNullAt(row_num))
            return 0; ///< Assume default color for NULL values is black

        return extractPixelComponent(nullable_col->getNestedColumn(), row_num, bit_depth);
    }

    /// TODO
    if ([[maybe_unused]] const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col)) [[unlikely]]
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LowCardinality column support is not implemented yet for PNG format");
    }


    if ([[maybe_unused]] const auto * const_col = typeid_cast<const ColumnConst *>(&col)) [[unlikely]]
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Const column support is not implemented yet for PNG format");
    }

    return extractPixelComponentImpl(col, row_num, bit_depth);
}

}

class PngSerializer::SerializerImpl
{
public:
    SerializerImpl(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
        : writer(writer_)
        , max_width(width_)
        , max_height(height_)
        , row_count(0)
        , bit_depth(bit_depth_)
        , channels(channels_)
    {
        if (bit_depth != 8 && bit_depth != 16)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PNG format currently only supports 8 or 16 bit depth, got {}", bit_depth);

        bytes_per_component = (bit_depth == 16) ? 2 : 1;
        pixels.resize(channels * bytes_per_component * max_width * max_height);
    }

    void commonSetColumns(const ColumnPtr * columns, size_t num_columns, size_t expected)
    {
        if (num_columns != expected)
        {
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Expected {} columns, got {}", expected, num_columns);
        }

        src_columns.assign(columns, columns + num_columns);
    }

    void commonFinalizeWrite(size_t width, size_t height)
    {
        try
        {
            const size_t final_byte_size = channels * bytes_per_component * width * height;
            writer.startImage(width, height);
            writer.writeRows(reinterpret_cast<const unsigned char *>(pixels.data()), final_byte_size);
            writer.finalize();
        }
        catch (...)
        {
            clear();
            throw;
        }
    }

    void clear()
    {
        pixels.clear();
        row_count = 0;
    }

    void commonReset() { clear(); }

    void commonAppendPixelRow(const UInt16 * components, size_t count)
    {
        if (count != channels)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Provided component count ({}) does not match expected channel count ({})", count, channels);

        if (row_count >= max_height * max_width)
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "Exceeded maximum image resolution: {}x{}", max_width, max_height);

        const size_t write_pos = row_count * channels * bytes_per_component;

        std::byte * ptr = reinterpret_cast<std::byte *>(pixels.data()) + write_pos;

        if (bit_depth == 16)
        {
            std::memcpy(ptr, components, channels * sizeof(UInt16));
        }
        else if (bit_depth == 8)
        {
            for (size_t c = 0; c < channels; ++c)
                ptr[c] = static_cast<std::byte>(static_cast<UInt8>(components[c]));
        }

        ++row_count;
    }


    PngWriter & writer;

    size_t max_width;
    size_t max_height;
    size_t row_count;

    int bit_depth;
    size_t bytes_per_component;
    size_t channels;

    std::vector<std::byte> pixels;
    std::vector<ColumnPtr> src_columns;
};

PngSerializer::PngSerializer(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
    : impl(std::make_unique<SerializerImpl>(width_, height_, writer_, bit_depth_, channels_))
{
}

PngSerializer::~PngSerializer() = default;

void PngSerializer::finalizeWrite(size_t width, size_t height)
{
    impl->commonFinalizeWrite(width, height);
}

void PngSerializer::reset()
{
    impl->commonReset();
}

size_t PngSerializer::getRowCount() const
{
    return impl->row_count;
}

class PngSerializerBinary : public PngSerializer
{
public:
    PngSerializerBinary(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
        : PngSerializer(width_, height_, writer_, bit_depth_, channels_)
    {
        if (impl->bit_depth != 8)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary serialization for png images currently only supports 8-bit depth");
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 1); }

    void writeRow(size_t row_num) override
    {
        bool val = extractBool(*impl->src_columns[0], row_num);
        std::array<UInt16, 1> comps = {static_cast<UInt16>(val ? 255 : 0)};

        impl->commonAppendPixelRow(comps.data(), components_per_pixel);
    }

private:
    size_t components_per_pixel = 1;
};

class PngSerializerGrayscale : public PngSerializer
{
public:
    PngSerializerGrayscale(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
        : PngSerializer(width_, height_, writer_, bit_depth_, channels_)
    {
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 1); }

    void writeRow(size_t row_num) override
    {
        std::array<UInt16, 1> comps = {extractPixelComponent(*impl->src_columns[0], row_num, impl->bit_depth)};

        impl->commonAppendPixelRow(comps.data(), components_per_pixel);
    }

private:
    size_t components_per_pixel = 1;
};

class PngSerializerRGB : public PngSerializer
{
public:
    PngSerializerRGB(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
        : PngSerializer(width_, height_, writer_, bit_depth_, channels_)
    {
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 3); }

    void writeRow(size_t row_num) override
    {
        std::array<UInt16, 3> comps
            = {extractPixelComponent(*impl->src_columns[0], row_num, impl->bit_depth),
               extractPixelComponent(*impl->src_columns[1], row_num, impl->bit_depth),
               extractPixelComponent(*impl->src_columns[2], row_num, impl->bit_depth)};

        impl->commonAppendPixelRow(comps.data(), components_per_pixel);
    }

private:
    size_t components_per_pixel = 3;
};

class PngSerializerRGBA : public PngSerializer
{
public:
    PngSerializerRGBA(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels_)
        : PngSerializer(width_, height_, writer_, bit_depth_, channels_)
    {
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 4); }

    void writeRow(size_t row_num) override
    {
        std::array<UInt16, 4> comps
            = {extractPixelComponent(*impl->src_columns[0], row_num, impl->bit_depth),
               extractPixelComponent(*impl->src_columns[1], row_num, impl->bit_depth),
               extractPixelComponent(*impl->src_columns[2], row_num, impl->bit_depth),
               extractPixelComponent(*impl->src_columns[3], row_num, impl->bit_depth)};

        impl->commonAppendPixelRow(comps.data(), components_per_pixel);
    }

private:
    size_t components_per_pixel = 4;
};

std::unique_ptr<PngSerializer> PngSerializer::create(
    const DataTypes & data_types, size_t width, size_t height, PngPixelFormat pixel_format, PngWriter & writer, int bit_depth)
{
    size_t required_columns;
    size_t channels;
    switch (pixel_format)
    {
        case PngPixelFormat::BINARY:
        case PngPixelFormat::GRAYSCALE:
            required_columns = 1;
            channels = 1;
            break;
        case PngPixelFormat::RGB:
            required_columns = 3;
            channels = 3;
            break;
        case PngPixelFormat::RGBA:
            required_columns = 4;
            channels = 4;
            break;
    }

    if (data_types.size() != required_columns)
    {
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE,
            "Serializer expects {} columns for pixel format {}, but got {}. "
            "The default pixel format is 'RGB'. To resolve this, explicitly set the "
            "'output_png_image_pixel_format' setting in your query",
            required_columns,
            required_columns,
            data_types.size());
    }

    std::unique_ptr<PngSerializer> serializer;

    switch (pixel_format)
    {
        case PngPixelFormat::BINARY:
            serializer = std::make_unique<PngSerializerBinary>(width, height, writer, bit_depth, channels);
            break;
        case PngPixelFormat::GRAYSCALE:
            serializer = std::make_unique<PngSerializerGrayscale>(width, height, writer, bit_depth, channels);
            break;
        case PngPixelFormat::RGB:
            serializer = std::make_unique<PngSerializerRGB>(width, height, writer, bit_depth, channels);
            break;
        case PngPixelFormat::RGBA:
            serializer = std::make_unique<PngSerializerRGBA>(width, height, writer, bit_depth, channels);
            break;
    }

    return serializer;
}

}
