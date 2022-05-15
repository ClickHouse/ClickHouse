#include <IO/WriteHelpers.h>
#include "PngOutputFormat.h"
#include <Formats/FormatFactory.h>
#include <Core/Field.h>
#include <lodepng.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_ERROR_CODE;
}

PngOutputFormat::PngOutputFormat(WriteBuffer & out_, const Block & header_)
    : IOutputFormat(header_, out_)
{
    data.resize(3 * width * height, 255);
}

  
//returns integer part of a floating point number
int iPartOfNumber(double x)
{
    return static_cast<int>(x);
}
  
//returns fractional part of a number
double fPartOfNumber(double x)
{
    if (x > 0) {
        return x - iPartOfNumber(x);
    }
    return x - (iPartOfNumber(x)+1);
}
  
//returns 1 - fractional part of number
double rfPartOfNumber(double x)
{
    return 1 - fPartOfNumber(x);
}

void drawPixel(int x, int y, double brightness, std::vector<uint8_t>& data)
{
    if (y < 0 || x < 0 || x >= 800 || y >= 800) {
        return;
    }
    unsigned char c = static_cast<unsigned char>(255*brightness);
    data[y * 800 * 3 + x * 3] = c; 
    data[y * 800 * 3 + x * 3 + 1] = c; 
    data[y * 800 * 3 + x * 3 + 2] = c; 
}

void PngOutputFormat::drawColumn(size_t x, size_t y, size_t len, size_t middle, Colour c = Colour()) {
    if (y >= middle) {
        for (size_t i = middle; i < y; ++i) {
            for (size_t j = 0; j < len; ++j) {
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3] = c.r / 2;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 1] = c.g / 2;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 2] = c.b / 2;
            }
        } 
    } else {
        for (size_t i = y; i < middle; ++i) {
            for (size_t j = 0; j < len; ++j) {
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3] = c.r / 2;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 1] = c.g / 2;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 2] = c.b / 2;
            }
        } 
    }
    size_t bound = height * 1 / 100;
    if (y >= middle) {
        for (size_t i = middle; i < y; ++i) {
            if (i + bound <= y) {
                continue;
            }
            for (size_t j = 0; j < len; ++j) {
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3] = c.r;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 1] = c.g;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 2] = c.b;
            }
        } 
    } else {
        for (size_t i = y; i < middle && i < y + bound; ++i) {
            for (size_t j = 0; j < len; ++j) {
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3] = c.r;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 1] = c.g;
                data[(height - 1 - i) * width * 3 + x * 3 + j * 3 + 2] = c.b;
            }
        } 
    }
}


void PngOutputFormat::drawPoint(size_t x, size_t y, Colour c = Colour()) {
    data[(height - 1 - y) * width * 3  + x * 3] = c.r;
    data[(height - 1 - y) * width * 3  + x * 3 + 1] = c.g;
    data[(height - 1 - y) * width * 3  + x * 3 + 2] = c.b;
}

void PngOutputFormat::connectPoints(int x1, int y1, int x2, int y2) {
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
  
    //compute the slope
    double dx = x2-x1;
    double dy = y2-y1;
    double gradient = dy/dx;
    if (dx == 0.0)
        gradient = 1;
  
    int xpxl1 = x1;
    int xpxl2 = x2;
    double intersectY = y1;
  
    // main loop
    if (steep)
    {
        int x;
        for (x = xpxl1 ; x <=xpxl2 ; x++)
        {
            drawPixel(iPartOfNumber(intersectY), x,
                        rfPartOfNumber(intersectY), data);
            drawPixel(iPartOfNumber(intersectY)-1, x,
                        fPartOfNumber(intersectY), data);
            intersectY += gradient;
        }
    }
    else
    {
        int x;
        for (x = xpxl1 ; x <=xpxl2 ; x++)
        {
            drawPixel(x, iPartOfNumber(intersectY),
                        rfPartOfNumber(intersectY), data);
            drawPixel(x, iPartOfNumber(intersectY)-1,
                          fPartOfNumber(intersectY), data);
            intersectY += gradient;
        }
    }
}

void PngOutputFormat::writePrefix()
{
    std::string str = "Png format in WIP mode\n";
    out.write(str.data(), str.size());
}

void PngOutputFormat::writePngToFile(const std::string & FilePath = "pngOutputClickHouse.png")
{
    auto error = lodepng::encode(FilePath, data, width, height, LCT_RGB, 8);
    if (error) {
        throw Exception(ErrorCodes::NO_SUCH_ERROR_CODE, "Lodepng library error: " + std::string(lodepng_error_text(error)));
    }
}

void PngOutputFormat::consume(Chunk chunk) {
    chunks.emplace_back(std::move(chunk));
}

void PngOutputFormat::resampleData(size_t w = 1920, size_t h = 1080) {
    std::vector<uint8_t> resampleData(w * h * 3, 255);
    double x_ratio = static_cast<double>(width - 1)/static_cast<double>(w-1);
    double y_ratio = static_cast<double>(height-1)/static_cast<double>(h-1);
    for (size_t i = 0; i < h; ++i) {
        for (size_t j = 0; j < w; ++j) {
            size_t x_l =  static_cast<size_t>(x_ratio * j);
            size_t y_l =  static_cast<size_t>(y_ratio * i);
            size_t x_h =  static_cast<size_t>(x_ratio * j + 0.5);
            size_t y_h =  static_cast<size_t>(y_ratio * i + 0.5);
            
            double x_weight = (x_ratio * j)  - x_l;
            double y_weight = (y_ratio * i) - y_l;

            uint8_t r = static_cast<uint8_t>(data[y_l * width * 3 + 3 * x_l] * (1 - x_weight) * (1 - y_weight) +
            data[y_l * width * 3 + 3 * x_h] * x_weight * (1 - y_weight) +
            data[y_h * width * 3 + 3 * x_l] * (1 - x_weight) * y_weight +
            data[y_h * width * 3 + 3 * x_h] * x_weight * y_weight);
            uint8_t g = static_cast<uint8_t>(data[y_l * width * 3 + 3 * x_l + 1] * (1 - x_weight) * (1 - y_weight) +
            data[y_l * width * 3 + 3 * x_h + 1] * x_weight * (1 - y_weight) +
            data[y_h * width * 3 + 3 * x_l + 1] * (1 - x_weight) * y_weight +
            data[y_h * width * 3 + 3 * x_h + 1] * x_weight * y_weight);
            uint8_t b = static_cast<uint8_t>(data[y_l * width * 3 + 3 * x_l + 2] * (1 - x_weight) * (1 - y_weight) +
            data[y_l * width * 3 + 3 * x_h + 2] * x_weight * (1 - y_weight) +
            data[y_h * width * 3 + 3 * x_l + 2] * (1 - x_weight) * y_weight +
            data[y_h * width * 3 + 3 * x_h + 2] * x_weight * y_weight);
            resampleData[i * w * 3 + j * 3] = r;
            resampleData[i * w * 3 + j * 3 + 1] = g;
            resampleData[i * w * 3 + j * 3 + 2] = b;

        }
    }
    data = resampleData;
    width = w;
    height = h;

}

void PngOutputFormat::getExtremesOfChunks(Field & min_field, Field & max_field, size_t column) {
    chunks[0].getColumns()[column]->getExtremes(min_field, max_field);
    for (size_t i = 1; i < chunks.size(); ++i) {
        Field min_tmp, max_tmp;
        chunks[i].getColumns()[column]->getExtremes(min_tmp, max_tmp);
        if (min_tmp < min_field) {
            min_field = min_tmp;
        }
        if (max_tmp > max_field) {
            max_field = max_tmp;
        }
    }
}

double getDoubleValueOfField(const Field & f) {
    switch (f.getType())
    {
        case Field::Types::UInt64:  return static_cast<double>(f.get<UInt64>());
        case Field::Types::Int64:   return static_cast<double>(f.get<Int64>());
        case Field::Types::Float64: return static_cast<double>(f.get<Float64>());
        default: return 0;
    }

}

void PngOutputFormat::drawOneDimension(size_t total_size)
{
    if (total_size == 0) {
        return;
    }

    Field min_field, max_field;
    getExtremesOfChunks(min_field, max_field, 0);
    double max_value = std::max(getDoubleValueOfField(max_field), 0.);
    double min_value = std::min(getDoubleValueOfField(min_field), 0.);
    if (max_value == min_value) {
        max_value = 1;
    }
    size_t middle = 0;
    middle = static_cast<size_t>((height - 1) * (-min_value) / (max_value - min_value));
    if (total_size <= width || total_size <= 4 * width) {
        if (total_size >= width) {
            width = total_size;
        } else if ((width / total_size) * total_size == total_size) {
                width = total_size * 2;
            } else {
                width = (width / total_size) * total_size;
            }
        data.resize(height * width * 3, 255);
        size_t w = 0;
        size_t len = width / total_size;
        //size_t extra = 0;
        size_t diff = 8 * len / 10;
        for (const auto & chunk : chunks) {
            for(size_t i = 0; i < chunk.getColumns()[0]->size(); ++i) {
                size_t column_height = static_cast<size_t>((height - 1) * (getDoubleValueOfField((*chunk.getColumns()[0])[i]) - min_value) / (max_value - min_value));
                // if (extra != width % total_size) {
                //     size_t ost = width % total_size;
                //     if ((ost * w) % total_size != 0) {
                //         ++extra;
                //     }
                    
                // }
                drawColumn(w * len, column_height, len - diff, middle);
                ++w;
            }
        }
        resampleData();
    } else {
        std::vector<size_t> counters(height + 1, 0);
        size_t step = total_size / width;
        size_t extra = total_size % width;
        size_t c = 0;
        size_t w = 0;
        for (const auto & chunk : chunks) {
            for(size_t i = 0; i < chunk.getColumns()[0]->size(); ++i) {
                size_t column_height = static_cast<size_t>((height - 1) * (getDoubleValueOfField((*chunk.getColumns()[0])[i]) - min_value) / (max_value - min_value));
                ++counters[column_height];
                ++c;
                if ((c == step && extra == 0) || (c == step + 1)) {
                    if (extra != 0) {
                        --extra;
                    }
                    size_t counter = 0;
                    
                    for (size_t h = 1; h <= height; ++h) {
                        counter += counters[h];
                        uint8_t r = 180 - static_cast<uint8_t>(180 * counter / c);
                        uint8_t g = 180 - static_cast<uint8_t>(180 * counter / c);
                        uint8_t b = 255;
                        
                        data[(height - h) * width * 3  + w * 3] = r;
                        data[(height - h) * width * 3  + w * 3 + 1] = g;
                        data[(height - h) * width * 3  + w * 3 + 2] = b;
                    }
                    size_t h = height;
                    while (h > 0 && counters[h] == 0) {
                        data[(height - h) * width * 3  + w * 3] = 255;
                        data[(height - h) * width * 3  + w * 3 + 1] = 255;
                        data[(height - h) * width * 3  + w * 3 + 2] = 255;
                        --h;
                    }
                    c = 0;
                    ++w;
                    counters.assign(height + 1, 0);
                }
            }
            
        }
        if (c != 0) {
            size_t counter = 0;
            for (size_t h = 1; h <= height; ++h) {
                counter += counters[h];
                uint8_t r = 180 - static_cast<uint8_t>(180 * counter / c);
                uint8_t g = 180 - static_cast<uint8_t>(180 * counter / c);
                uint8_t b = 255;
                        
                data[(height - h) * width * 3  + w * 3] = r;
                data[(height - h) * width * 3  + w * 3 + 1] = g;
                data[(height - h) * width * 3  + w * 3 + 2] = b;
            }
            size_t h = height;
            while (h > 0 && counters[h] == 0) {
                data[(height - h) * width * 3  + w * 3] = 255;
                data[(height - h) * width * 3  + w * 3 + 1] = 255;
                data[(height - h) * width * 3  + w * 3 + 2] = 255;
                --h;
            }
            c = 0;
            ++w;
            counters.assign(height + 1, 0);
        }
    }
    for (size_t i = 0; i < width; ++i) {
        data[(height - 1 - middle) * width * 3  + i * 3] = 255; 
        data[(height - 1 - middle) * width * 3  + i * 3 + 1] = 0;
        data[(height - 1 - middle) * width * 3  + i * 3 + 2] = 0;
    }

}

void PngOutputFormat::drawTwoDimension(size_t total_size)
{
    if (total_size == 0) {
        return;
    }
    Field x_min_field, x_max_field, y_min_field, y_max_field;
    getExtremesOfChunks(x_min_field, x_max_field, 0);
    getExtremesOfChunks(y_min_field, y_max_field, 1);
    double x_min = std::min(getDoubleValueOfField(x_min_field), 0.);
    double x_max = std::max(getDoubleValueOfField(x_max_field), 0.);
    double y_min = std::min(getDoubleValueOfField(y_min_field), 0.);
    double y_max = std::max(getDoubleValueOfField(y_max_field), 0.);
    if (x_min == x_max) {
        x_max = 1;
    }
    if (y_min == y_max) {
        y_max = 1;
    }

    size_t x_start = static_cast<size_t>((width - 1) * (-x_min) / (x_max - x_min));
    size_t y_start = static_cast<size_t>((height - 1) * (-y_min) / (y_max - y_min));
    for (const auto & chunk : chunks) {
        for(size_t i = 0; i < chunk.getColumns()[0]->size(); ++i) {
            int x = static_cast<int>(static_cast<double>(width - 1) * (getDoubleValueOfField((*chunk.getColumns()[0])[i])-x_min) / (x_max - x_min));
            int y = static_cast<int>(static_cast<double>(height - 1) * (getDoubleValueOfField((*chunk.getColumns()[1])[i])-y_min) / (y_max - y_min));
            drawPoint(x, y);
        }
    }
    for (size_t x = 0; x < width; ++x) {
        drawPoint(x, y_start, Colour{255, 0, 0});
    }

    for (size_t y = 0; y < height; ++y) {
        drawPoint(x_start, y, Colour{255, 0, 0});
    }
}

void PngOutputFormat::flush()
{
}

void PngOutputFormat::finalizeImpl() {
    size_t total_size = 0;
    for (const auto & chunk : chunks) {
        if (chunk.getColumns().empty() || chunk.getColumns().size() > 2) {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect number of columns for Png format");
        }
        for (const auto & column : chunk.getColumns()) {
            if (!column->isNumeric()) {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Png format doesn't support tables with non-numeric columns");
            }
        }
        total_size += chunk.getColumns()[0]->size();
    }

    if (chunks[0].getColumns().size() == 1) {
        drawOneDimension(total_size);
    } else {
        drawTwoDimension(total_size);
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
           const FormatSettings &) { return std::make_shared<PngOutputFormat>(buf, sample); });
}
}
