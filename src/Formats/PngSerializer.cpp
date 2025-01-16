#include "base/types.h"


#include <Formats/PngSerializer.h>
#include <Formats/PngWriter.h>
#include <Common/PODArray.h>
#include "Common/Exception.h"
#include <Columns/ColumnNullable.h>

namespace DB
{

/// TODO: LowCardinality types research
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

/// TODO: LowCardinality types research
inline UInt8 extractUInt8(const IColumn & col, size_t row_num)
{
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        if (null_col->isNullAt(row_num))
            return 0;

        const auto & nested = null_col->getNestedColumn();
        auto val = nested.getUInt(row_num);
        if (val > 255) [[unlikely]] val = 255;
        return static_cast<UInt8>(val);
    }
    else
    {
        auto val = col.getUInt(row_num);
        if (val > 255) [[unlikely]] val = 255;
        return static_cast<UInt8>(val);
    }
}

class PngSerializerBinary : public PngSerializer
{
public:
    PngSerializerBinary(size_t width_, size_t height_, PngWriter & writer_)
        : width(width_), height(height_), writer(writer_)
    {
        pixels.resize(width * height * 4, 0);
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != 1)
            /// TODO: rename to `ILLEGAL_COLUMNS_AMOUNT
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                "PngSerializerBinary needs exactly 1 column. Actually {} were given",
                num_columns);

        src_columns.clear();
        src_columns.reserve(1);
        src_columns.push_back(columns[0]);
    }

    void writeRow(size_t row_num) override
    {
        if (row_count >= width * height)
            throw Exception(ErrorCodes::TOO_MANY_ROWS,
                "Too many rows in PngSerializerBinary. We have {} capacity, row count is {}",
                (width * height), row_count);

        auto px = extractPixel(row_num);
        size_t off_t = row_count * 4;
        pixels[off_t] = px.r;
        pixels[off_t + 1] = px.g;
        pixels[off_t + 2] = px.b;
        pixels[off_t + 3] = px.a; 
        ++row_count;
    }

    void finalizeWrite() override
    {
        writer.startImage(width, height);
        writer.writeEntireImage(reinterpret_cast<const unsigned char *>(pixels.data()));
        writer.finishImage();
    }

    void reset() override
    {
        std::fill(pixels.begin(), pixels.end(), 0);
        row_count = 0;
    }

private:

    RGBA extractPixel(size_t row_num)
    {
        bool val = extractBool(*src_columns[0], row_num);
        return val ? Colors::WHITE : Colors::BLACK;
    }

private:
    size_t width;
    size_t height;
    PngWriter & writer;

    std::vector<ColumnPtr> src_columns;
    size_t row_count = 0;
    PODArray<UInt8> pixels; 
};

class PngSerializerGrayscale: public PngSerializer
{
public:
    PngSerializerGrayscale(size_t width_, size_t height_, PngWriter & writer_)
        : width(width_), height(height_), writer(writer_)
    {
        pixels.resize(width * height * 4, 0);
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != 1)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                "PngSerializerGrayscale needs exactly 1 column. Actually {} were given",
                num_columns);

        src_columns.clear();
        src_columns.reserve(1);
        src_columns.push_back(columns[0]);
    }

    void writeRow(size_t row_num) override
    {
        if (row_count >= width * height)
            throw Exception(ErrorCodes::TOO_MANY_ROWS,
                "Too many rows in PngSerializerGrayscale. capacity {}, row count {}",
                (width * height), row_count);

        auto px = extractPixel(row_num);
        size_t off_t = row_count * 4;
        pixels[off_t] = px.r;
        pixels[off_t + 1] = px.g;
        pixels[off_t + 2] = px.b;
        pixels[off_t + 3] = px.a; 
        ++row_count;
    }

    void finalizeWrite() override
    {
        writer.startImage(width, height);
        writer.writeEntireImage(reinterpret_cast<const unsigned char *>(pixels.data()));
        writer.finishImage();
    }

    void reset() override
    {
        std::fill(pixels.begin(), pixels.end(), 0);
        row_count = 0;
    }

private:

    RGBA extractPixel(size_t row_num) const
    {
        auto g = extractUInt8(*src_columns[0], row_num);
        return RGBA{g, g, g, 255};
    }

private:
    size_t width;
    size_t height;
    PngWriter & writer;

    std::vector<ColumnPtr> src_columns;
    size_t row_count = 0;
    PODArray<UInt8> pixels; 
};


class PngSerializerRGB: public PngSerializer
{
public:
    PngSerializerRGB(size_t width_, size_t height_, PngWriter & writer_)
        : width(width_), height(height_), writer(writer_)
    {
        pixels.resize(width * height * 4, 0);
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != 3)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                "PngSerializerRGB needs exactly 3 columns. Actually {} were given",
                num_columns);

        src_columns.clear();
        src_columns.reserve(3);
        src_columns.assign(columns, columns + 3);
    }

    void writeRow(size_t row_num) override
    {
        if (row_count >= width * height)
            throw Exception(ErrorCodes::TOO_MANY_ROWS,
                "Too many rows in PngSerializerRGB. capacity {}, row count {}",
                (width * height), row_count);

        auto px = extractPixel(row_num);
        size_t off_t = row_count * 4;
        pixels[off_t] = px.r;
        pixels[off_t + 1] = px.g;
        pixels[off_t + 2] = px.b;
        pixels[off_t + 3] = px.a; 
        ++row_count;
    }

    void finalizeWrite() override
    {
        writer.startImage(width, height);
        writer.writeEntireImage(reinterpret_cast<const unsigned char *>(pixels.data()));
        writer.finishImage();
    }

    void reset() override
    {
        std::fill(pixels.begin(), pixels.end(), 0);
        row_count = 0;
    }

private:

    RGBA extractPixel(size_t row_num) const
    {
        auto r = extractUInt8(*src_columns[0], row_num);
        auto g = extractUInt8(*src_columns[1], row_num);
        auto b = extractUInt8(*src_columns[2], row_num);
        return RGBA{r, g, b, UInt8{255}};
    }

private:
    size_t width;
    size_t height;
    PngWriter & writer;

    std::vector<ColumnPtr> src_columns;
    size_t row_count = 0;
    PODArray<UInt8> pixels; 
};

class PngSerializerRGBA: public PngSerializer
{
public:
    PngSerializerRGBA(size_t width_, size_t height_, PngWriter & writer_)
        : width(width_), height(height_), writer(writer_)
    {
        pixels.resize(width * height * 4, 0);
    }

    void setColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != 4)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                "PngSerializerRGBA needs exactly 4 columns. Actually {} were given",
                num_columns);

        src_columns.clear();
        src_columns.reserve(4);
        src_columns.assign(columns, columns+4);
    }

    void writeRow(size_t row_num) override
    {
        if (row_count>=width*height)
            throw Exception(ErrorCodes::TOO_MANY_ROWS,
                "Too many rows in PngSerializerRGBA, capacity {}, row count {}",
                (width*height), row_count);

        auto px = extractPixel(row_num);
        size_t off_t = row_count * 4;
        pixels[off_t] = px.r;
        pixels[off_t + 1] = px.g;
        pixels[off_t + 2] = px.b;
        pixels[off_t + 3] = px.a; 
        ++row_count;
    }

    void finalizeWrite() override
    {
        writer.startImage(width, height);
        writer.writeEntireImage(reinterpret_cast<const unsigned char *>(pixels.data()));
        writer.finishImage();
    }

    void reset() override
    {
        std::fill(pixels.begin(), pixels.end(), 0);
        row_count = 0;
    }

private:

    RGBA extractPixel(size_t row_num) const
    {
        auto r = extractUInt8(*src_columns[0], row_num);
        auto g = extractUInt8(*src_columns[1], row_num);
        auto b = extractUInt8(*src_columns[2], row_num);
        auto a = extractUInt8(*src_columns[3], row_num); 
        return RGBA{r, g, b, a};
    }

private:
    size_t width;
    size_t height;
    PngWriter & writer;

    std::vector<ColumnPtr> src_columns;
    size_t row_count = 0;
    PODArray<UInt8> pixels; 
};


std::unique_ptr<PngSerializer> PngSerializer::create(
    [[maybe_unused]] const Strings & column_names,
    const DataTypes & data_types,
    size_t width,
    size_t height,
    FormatSettings::PixelMode pixel_mode,
    PngWriter & writer)
{
    size_t required_columns = 0;
    switch (pixel_mode)
    {
        case FormatSettings::PixelMode::BINARY:
        case FormatSettings::PixelMode::GRAYSCALE:
            required_columns = 1;
            break;
        case FormatSettings::PixelMode::RGB:
            required_columns = 3;
            break;
        case FormatSettings::PixelMode::RGBA:
            required_columns = 4;
            break;
    }

    if (data_types.size() != required_columns)
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, 
            "PngSerializer expects {} columns for pixel mode {}, but got {}", 
            required_columns, static_cast<int>(pixel_mode), data_types.size()
        );
    }

    std::unique_ptr<PngSerializer> serializer;

    switch (pixel_mode)
    {
        case FormatSettings::PixelMode::BINARY:
            serializer = std::make_unique<PngSerializerBinary>(width, height, writer);
            break;
        case FormatSettings::PixelMode::GRAYSCALE:
            serializer = std::make_unique<PngSerializerGrayscale>(width, height, writer);
            break;
        case FormatSettings::PixelMode::RGB:
            serializer = std::make_unique<PngSerializerRGB>(width, height, writer);
            break;
        case FormatSettings::PixelMode::RGBA:
            serializer = std::make_unique<PngSerializerRGBA>(width, height, writer);
            break;
    }

    return serializer;
}


}
