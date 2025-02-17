#include "base/types.h"

#include <Columns/ColumnNullable.h>
#include <Formats/PngSerializer.h>
#include <Formats/PngWriter.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>

namespace DB
{

namespace
{

/// TODO: LowCardinality
inline bool extractBool(const IColumn & col, size_t row_num)
{
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        if (null_col->isNullAt(row_num))
            return false;

        const auto & nested = null_col->getNestedColumn();
        return nested.getBool(row_num);
    }
    else
    {
        return col.getBool(row_num);
    }
}

/// TODO: LowCardinality
inline UInt8 extractUInt8(const IColumn & col, size_t row_num)
{
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        if (null_col->isNullAt(row_num))
            return 0;

        const auto & nested = null_col->getNestedColumn();
        auto val = nested.getUInt(row_num);
        if (val > 255) [[unlikely]]
            val = 255;
        return static_cast<UInt8>(val);
    }
    else
    {
        auto val = col.getUInt(row_num);
        if (val > 255) [[unlikely]]
            val = 255;
        return static_cast<UInt8>(val);
    }
}

}

struct RGBA
{
    UInt8 r;
    UInt8 g;
    UInt8 b;
    UInt8 a;
};

namespace Colors
{
    static constexpr auto WHITE = RGBA{255, 255, 255, 255};
    static constexpr auto BLACK = RGBA{0, 0, 0, 255};
}

class PngSerializer::SerializerImpl
{
public:
    SerializerImpl(size_t width_, size_t height_, PngWriter & writer_)
        : max_width(width_), max_height(height_), row_count(0), writer(writer_)
    {
        pixels.reserve(4 * max_width * max_height);
    }

    void commonSetColumns(const ColumnPtr * columns, size_t num_columns, size_t expected)
    {
        if (num_columns != expected)
        {
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "Expected {} columns, got {}", expected, num_columns);
        }

        src_columns.assign(columns, columns + num_columns);
    }

    void commonWriteRow(const RGBA & pixel)
    {
        if (row_count >= max_height * max_width)
        {
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "Exceeded maximum image resolution: {}x{}", max_width, max_height);
        }

        pixels.push_back(pixel.r);
        pixels.push_back(pixel.g);
        pixels.push_back(pixel.b);
        pixels.push_back(pixel.a);

        ++row_count;
    }

    void commonFinalizeWrite(size_t width, size_t height)
    {
        pixels.resize(4 * width * height);
        writer.startImage(width, height);
        writer.writeEntireImage(
        reinterpret_cast<const unsigned char *>(pixels.data())
        );
        writer.finishImage();
    }

    void commonReset()
    {
        pixels.clear();
        row_count = 0;
    }

    size_t max_width;
    size_t max_height;
    size_t row_count;
    PngWriter & writer;
    std::vector<ColumnPtr> src_columns;
    PODArray<UInt8> pixels;
};

PngSerializer::PngSerializer(size_t width_, size_t height_, PngWriter & writer_)
    : impl(std::make_unique<SerializerImpl>(width_, height_, writer_))
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

size_t & PngSerializer::getRowCount()
{
    return impl->row_count;
}

class PngSerializerBinary : public PngSerializer
{
public:
    PngSerializerBinary(size_t width_, size_t height_, PngWriter & writer_) : PngSerializer(width_, height_, writer_) { }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 1); }

    void writeRow(size_t row_num) override
    {
        bool val = extractBool(*impl->src_columns[0], row_num);
        impl->commonWriteRow(val ? Colors::WHITE : Colors::BLACK);
    }
};

class PngSerializerGrayscale : public PngSerializer
{
public:
    PngSerializerGrayscale(size_t width_, size_t height_, PngWriter & writer_) : PngSerializer(width_, height_, writer_) { }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 1); }

    /// Transform to shade of gray
    void writeRow(size_t row_num) override
    {
        const UInt8 val = extractUInt8(*impl->src_columns[0], row_num);
        impl->commonWriteRow({val, val, val, 255});
    }
};

class PngSerializerRGB : public PngSerializer
{
public:
    PngSerializerRGB(size_t width_, size_t height_, PngWriter & writer_) : PngSerializer(width_, height_, writer_) { }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 3); }

    void writeRow(size_t row_num) override
    {
        const UInt8 r = extractUInt8(*impl->src_columns[0], row_num);
        const UInt8 g = extractUInt8(*impl->src_columns[1], row_num);
        const UInt8 b = extractUInt8(*impl->src_columns[2], row_num);
        impl->commonWriteRow({r, g, b, 255});
    }
};

class PngSerializerRGBA : public PngSerializer
{
public:
    PngSerializerRGBA(size_t width_, size_t height_, PngWriter & writer_) : PngSerializer(width_, height_, writer_) { }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override { impl->commonSetColumns(columns, num_columns, 4); }

    void writeRow(size_t row_num) override
    {
        const UInt8 r = extractUInt8(*impl->src_columns[0], row_num);
        const UInt8 g = extractUInt8(*impl->src_columns[1], row_num);
        const UInt8 b = extractUInt8(*impl->src_columns[2], row_num);
        const UInt8 a = extractUInt8(*impl->src_columns[3], row_num);
        impl->commonWriteRow({r, g, b, a});
    }
};

std::unique_ptr<PngSerializer> PngSerializer::create(
    [[maybe_unused]] const Strings & column_names,
    const DataTypes & data_types,
    size_t width,
    size_t height,
    PngPixelFormat pixel_format,
    PngWriter & writer)
{
    size_t required_columns = 0;
    switch (pixel_format)
    {
        case PngPixelFormat::BINARY:
        case PngPixelFormat::GRAYSCALE:
            required_columns = 1;
            break;
        case PngPixelFormat::RGB:
            required_columns = 3;
            break;
        case PngPixelFormat::RGBA:
            required_columns = 4;
            break;
    }

    if (data_types.size() != required_columns)
    {
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE,
            "Serializer expects {} columns for pixel format {}, but got {}",
            required_columns,
            required_columns,
            data_types.size());
    }

    std::unique_ptr<PngSerializer> serializer;

    switch (pixel_format)
    {
        case PngPixelFormat::BINARY:
            serializer = std::make_unique<PngSerializerBinary>(
                width, 
                height,
                writer
            );
            break;
        case PngPixelFormat::GRAYSCALE:
            serializer = std::make_unique<PngSerializerGrayscale>(
                width, 
                height, 
                writer
            );
            break;
        case PngPixelFormat::RGB:
            serializer = std::make_unique<PngSerializerRGB>(
                width, 
                height,
                writer
            );
            break;
        case PngPixelFormat::RGBA:
            serializer = std::make_unique<PngSerializerRGBA>(
                width, 
                height, 
                writer
            );
            break;
    }

    return serializer;
}

}
