#include "PngOutputFormat.h"
#include <lodepng.h>
#include <Core/Field.h>
#include <Formats/FormatFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_ERROR_CODE;
    extern const int BAD_TYPE_OF_FIELD;
}

PngOutputFormat::PngOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_) : IOutputFormat(header_, out_)
{
    draw_lines = settings_.png.output_format_png_draw_lines;
    file_name = settings_.png.output_format_png_file_name;
    result_width = width = settings_.png.output_format_png_width;
    result_height = height = settings_.png.output_format_png_height;
    data.resize(3 * width * height, 255);
}

Float64 getDoubleValueOfField(const Field & f) {
    switch (f.getType())
    {
        case Field::Types::UInt64:  return static_cast<Float64>(f.get<UInt64>());
        case Field::Types::Int64:   return static_cast<Float64>(f.get<Int64>());
        case Field::Types::UInt128:  return static_cast<Float64>(f.get<UInt128>());
        case Field::Types::Int128:   return static_cast<Float64>(f.get<Int128>());
        case Field::Types::UInt256:  return static_cast<Float64>(f.get<UInt256>());
        case Field::Types::Int256:   return static_cast<Float64>(f.get<Int256>());
        case Field::Types::Float64: return static_cast<Float64>(f.get<Float64>());
        default: break;
    }
    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Incorrect Field type for conversion to Float64");
}

int iPartOfNumber(Float64 x)
{
    return static_cast<int>(x);
}

Float64 fPartOfNumber(Float64 x)
{
    if (x > 0)
    {
        return x - iPartOfNumber(x);
    }
    return x - (iPartOfNumber(x) + 1);
}

Float64 rfPartOfNumber(Float64 x)
{
    return 1 - fPartOfNumber(x);
}

void PngOutputFormat::drawPoint(size_t x, size_t y, const Colour & c = Colour())
{
    data[(height - 1 - y) * width * 3 + x * 3] = c.r;
    data[(height - 1 - y) * width * 3 + x * 3 + 1] = c.g;
    data[(height - 1 - y) * width * 3 + x * 3 + 2] = c.b;
}

/// Connect points with Xiaolin Wu's line algorithm
/// https://en.wikipedia.org/wiki/Xiaolin_Wu%27s_line_algorithm
void PngOutputFormat::connectPoints(int x1, int y1, int x2, int y2)
{
    bool steep = std::abs(y2 - y1) > std::abs(x2 - x1);

    if (steep)
    {
        std::swap(x1, y1);
        std::swap(x2, y2);
    }
    if (x1 > x2)
    {
        std::swap(x1, x2);
        std::swap(y1, y2);
    }

    Float64 dx = x2 - x1;
    Float64 dy = y2 - y1;
    Float64 gradient = dy / dx;
    if (dx == 0.0)
        gradient = 1;

    int xpxl1 = x1;
    int xpxl2 = x2;
    Float64 intersectY = y1;

    if (steep)
    {
        int x;
        for (x = xpxl1; x <= xpxl2; x++)
        {
            uint8_t c = static_cast<uint8_t>(rfPartOfNumber(intersectY) * 255);
            drawPoint(iPartOfNumber(intersectY), x, Colour{c, c, c});
            c = static_cast<uint8_t>(fPartOfNumber(intersectY) * 255);
            drawPoint(iPartOfNumber(intersectY) - 1, x, Colour{c, c, c});
            intersectY += gradient;
        }
    }
    else
    {
        int x;
        for (x = xpxl1; x <= xpxl2; x++)
        {
            uint8_t c = static_cast<uint8_t>(rfPartOfNumber(intersectY) * 255);
            drawPoint(x, iPartOfNumber(intersectY), Colour{c, c, c});
            c = static_cast<uint8_t>(fPartOfNumber(intersectY) * 255);
            drawPoint(x, iPartOfNumber(intersectY) - 1, Colour{c, c, c});
            intersectY += gradient;
        }
    }
}

/// Draw column between y and zero_height (coordinates of height) and 
/// between x and x + column_width (coordinates of width)   
void PngOutputFormat::drawColumn(size_t x, size_t y, size_t column_width, size_t zero_height, const Colour & c = Colour())
{
    size_t lower_bound = zero_height;
    size_t upper_bound = zero_height;
    if (y > zero_height)
    {
        upper_bound = y;
    }
    else
    {
        lower_bound = y;
    }
    for (size_t i = lower_bound; i <= upper_bound; ++i)
    {
        for (size_t j = 0; j < column_width; ++j)
        {
            uint8_t r = c.r / 2;
            uint8_t g = c.g / 2;
            uint8_t b = c.b / 2;
            drawPoint(x + j, i, Colour{r, g, b});
        }
    }

    size_t top_len = std::min(height * 1 / 100, upper_bound - lower_bound);

    if (y >= zero_height) {
        lower_bound = upper_bound - top_len;
    } else {
        upper_bound = lower_bound + top_len;
    }

    for (size_t i = lower_bound; i <= upper_bound; ++i)
    {
        for (size_t j = 0; j < column_width; ++j)
        {
            drawPoint(x + j, i, c);
        }
    }
}

/// Resize current pixel data of image to image with sizes w * h
/// with bilinear interpolation - change one pixel to 
/// distanceweighted average of the four nearest pixel 
void PngOutputFormat::resampleData(size_t w, size_t h)
{
    std::vector<uint8_t> resampled_data(w * h * 3, 255);
    Float64 x_ratio = static_cast<Float64>(width - 1) / static_cast<Float64>(w - 1);
    Float64 y_ratio = static_cast<Float64>(height - 1) / static_cast<Float64>(h - 1);
    for (size_t i = 0; i < h; ++i)
    {
        for (size_t j = 0; j < w; ++j)
        {
            size_t x_l = static_cast<size_t>(x_ratio * j);
            size_t y_l = static_cast<size_t>(y_ratio * i);
            size_t x_h = static_cast<size_t>(x_ratio * j + 0.5);
            size_t y_h = static_cast<size_t>(y_ratio * i + 0.5);

            Float64 x_weight = (x_ratio * j) - x_l;
            Float64 y_weight = (y_ratio * i) - y_l;

            uint8_t r = static_cast<uint8_t>(
                data[y_l * width * 3 + 3 * x_l] * (1 - x_weight) * (1 - y_weight)
                + data[y_l * width * 3 + 3 * x_h] * x_weight * (1 - y_weight) 
                + data[y_h * width * 3 + 3 * x_l] * (1 - x_weight) * y_weight
                + data[y_h * width * 3 + 3 * x_h] * x_weight * y_weight);
            uint8_t g = static_cast<uint8_t>(
                data[y_l * width * 3 + 3 * x_l + 1] * (1 - x_weight) * (1 - y_weight)
                + data[y_l * width * 3 + 3 * x_h + 1] * x_weight * (1 - y_weight)
                + data[y_h * width * 3 + 3 * x_l + 1] * (1 - x_weight) * y_weight
                + data[y_h * width * 3 + 3 * x_h + 1] * x_weight * y_weight);
            uint8_t b = static_cast<uint8_t>(
                data[y_l * width * 3 + 3 * x_l + 2] * (1 - x_weight) * (1 - y_weight)
                + data[y_l * width * 3 + 3 * x_h + 2] * x_weight * (1 - y_weight)
                + data[y_h * width * 3 + 3 * x_l + 2] * (1 - x_weight) * y_weight
                + data[y_h * width * 3 + 3 * x_h + 2] * x_weight * y_weight);
            resampled_data[i * w * 3 + j * 3] = r;
            resampled_data[i * w * 3 + j * 3 + 1] = g;
            resampled_data[i * w * 3 + j * 3 + 2] = b;
        }
    }
    data = resampled_data;
    width = w;
    height = h;
}

void PngOutputFormat::getExtremesOfChunks(Field & min_field, Field & max_field, size_t column)
{
    chunks[0].getColumns()[column]->getExtremes(min_field, max_field);
    for (size_t i = 1; i < chunks.size(); ++i)
    {
        Field min_tmp, max_tmp;
        chunks[i].getColumns()[column]->getExtremes(min_tmp, max_tmp);
        if (min_tmp < min_field)
        {
            min_field = min_tmp;
        }
        if (max_tmp > max_field)
        {
            max_field = max_tmp;
        }
    }
}

void PngOutputFormat::writePngToFile()
{
    auto error = lodepng::encode(file_name, data, width, height, LCT_RGB, 8);
    if (error)
    {
        throw Exception(ErrorCodes::NO_SUCH_ERROR_CODE, "Lodepng library error: " + std::string(lodepng_error_text(error)));
    }
}

/// Draw bar chart for one dimension input table
/// 1. If values_size of input table is less than result_width of image, 
/// when reduce current width, draw columns and resample data of image to size
/// result_width * result_height
/// 2. If values_size of input table is bigger that result_width of image, 
/// when we split values of table into consecutive equal parts and correlate
/// each part to column with 1-pixel width. Colour's brightness of column's pixel depends on proportion 
/// of values that are less than value matching to height of pixel.
void PngOutputFormat::drawOneDimension()
{
    if (values_size == 0)
    {
        return;
    }

    Field min_field, max_field;
    getExtremesOfChunks(min_field, max_field, 0);
    Float64 max_value = std::max(getDoubleValueOfField(max_field), 0.);
    Float64 min_value = std::min(getDoubleValueOfField(min_field), 0.);
    if (max_value == min_value)
    {
        max_value = 1;
    }
    size_t zero_height = 0;
    zero_height = static_cast<size_t>((height - 1) * (-min_value) / (max_value - min_value));
    if (values_size <= width)
    {
        if (values_size > width / 10) {
            width = values_size;
        } else {
            width = (width / values_size) * values_size;
        }
        data.resize(height * width * 3, 255);
        size_t w = 0;
        size_t column_width = width / values_size;
        size_t space_width = 8 * column_width / 10;
        for (const auto & chunk : chunks)
        {
            for (size_t i = 0; i < chunk.getColumns()[0]->size(); ++i)
            {
                Float64 value = chunk.getColumns()[0]->getFloat64(i);
                size_t column_height = static_cast<size_t>((height - 1) * (value - min_value) / (max_value - min_value));
                drawColumn(w * column_width, column_height, column_width - space_width, zero_height);
                ++w;
            }
        }
        resampleData(result_width, result_height);
    }
    else
    {
        std::vector<size_t> counters(height, 0);
        size_t step = values_size / width;
        size_t extra = values_size % width;
        size_t c = 0;
        size_t w = 0;
        for (const auto & chunk : chunks)
        {
            for (size_t i = 0; i < chunk.getColumns()[0]->size(); ++i)
            {
                Float64 value = chunk.getColumns()[0]->getFloat64(i);
                size_t column_height = static_cast<size_t>((height - 1) * (value - min_value) / (max_value - min_value));
                ++counters[column_height];
                ++c;
                if ((c == step && extra == 0) || (c == step + 1))
                {
                    if (extra != 0)
                    {
                        --extra;
                    }
                    size_t counter = 0;

                    for (size_t h = 0; h < height; ++h)
                    {
                        counter += counters[h];
                        uint8_t r = 180 - static_cast<uint8_t>(180 * counter / c);
                        uint8_t g = 180 - static_cast<uint8_t>(180 * counter / c);
                        uint8_t b = 255;

                        drawPoint(w, h, Colour{r, g, b});
                    }
                    size_t h = height - 1;
                    while (h > 0 && counters[h] == 0)
                    {
                        drawPoint(w, h, Colour{255, 255, 255});
                        --h;
                    }
                    c = 0;
                    ++w;
                    counters.assign(height, 0);
                }
            }
        }
    }
    /// Draw y - axis
    for (size_t i = 0; i < width; ++i)
    {
        drawPoint(i, zero_height, Colour{255, 0, 0});
    }
}

/// Draw two-dimensional space for two dimension input table.
/// Also connect points if draw_lines flag is setted.
void PngOutputFormat::drawTwoDimension()
{
    if (values_size == 0)
    {
        return;
    }
    Field x_min_field, x_max_field, y_min_field, y_max_field;
    getExtremesOfChunks(x_min_field, x_max_field, 0);
    getExtremesOfChunks(y_min_field, y_max_field, 1);
    Float64 x_min = std::min(getDoubleValueOfField(x_min_field), 0.);
    Float64 x_max = std::max(getDoubleValueOfField(x_max_field), 0.);
    Float64 y_min = std::min(getDoubleValueOfField(y_min_field), 0.);
    Float64 y_max = std::max(getDoubleValueOfField(y_max_field), 0.);
    if (x_min == x_max)
    {
        x_max = 1;
    }
    if (y_min == y_max)
    {
        y_max = 1;
    }

    std::vector<std::pair<size_t, size_t>> coordinates;
    if (draw_lines) {
        coordinates.reserve(values_size);
    } 

    size_t x_start = static_cast<size_t>((width - 1) * (-x_min) / (x_max - x_min));
    size_t y_start = static_cast<size_t>((height - 1) * (-y_min) / (y_max - y_min));
    Float64 x_diff = x_max - x_min;
    Float64 y_diff = y_max - y_min;
    for (const auto & chunk : chunks)
    {
        size_t sz = chunk.getColumns()[0]->size();
        for (size_t i = 0; i < sz; ++i)
        {
            Float64 x_value = chunk.getColumns()[0]->getFloat64(i);
            Float64 y_value = chunk.getColumns()[1]->getFloat64(i);
            size_t x = static_cast<size_t>(static_cast<Float64>(width - 1) * (x_value - x_min) / x_diff);
            size_t y = static_cast<size_t>(static_cast<Float64>(height - 1) * (y_value - y_min) / y_diff);
            drawPoint(x, y);
            if (draw_lines) {
                coordinates.push_back({x, y});
            }
        }
    }
    /// Draw y and x - axes
    for (size_t x = 0; x < width; ++x)
    {
        drawPoint(x, y_start, Colour{255, 0, 0});
    }

    for (size_t y = 0; y < height; ++y)
    {
        drawPoint(x_start, y, Colour{255, 0, 0});
    }
    if (draw_lines) {
        std::sort(coordinates.begin(), coordinates.end());
        for (size_t i = 0; i + 1 < coordinates.size(); ++i) {
            connectPoints(coordinates[i].first, coordinates[i].second, coordinates[i + 1].first, coordinates[i + 1].second);
        }
    }
}

void PngOutputFormat::consume(Chunk chunk)
{
    chunks.emplace_back(std::move(chunk));
}

/// PngOutputFormat supports onli one dimension and two dimension
/// numeric table, so we check it in finalizeImpl before
/// drawing graph
void PngOutputFormat::finalizeImpl()
{
    for (const auto & chunk : chunks)
    {
        if (chunk.getColumns().empty() || chunk.getColumns().size() > 2)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect number of columns for Png format");
        }
        for (const auto & column : chunk.getColumns())
        {
            if (!column->isNumeric())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Png format doesn't support tables with non-numeric columns");
            }
        }
        values_size += chunk.getColumns()[0]->size();
    }

    if (chunks[0].getColumns().size() == 1)
    {
        drawOneDimension();
    }
    else
    {
        drawTwoDimension();
    }

    writePngToFile();
}

void registerOutputFormatPng(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Png",
        [](WriteBuffer & buf,
           const Block & sample,
           const RowOutputFormatParams &,
           const FormatSettings & settings) { return std::make_shared<PngOutputFormat>(buf, sample, settings); });
}
}
