#pragma once
#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{
class PngOutputFormat final : public IOutputFormat
{
public:
    struct Colour
    {
        uint8_t r = 0;
        uint8_t g = 0;
        uint8_t b = 255;
    };

    PngOutputFormat(WriteBuffer & out_, const Block & header_);
    String getName() const override { return "PngOutputFormat"; }
    void flush() override;

private:
    void drawColumn(size_t x, size_t y, size_t len, size_t middle, Colour c);
    void drawPoint(size_t x, size_t y, Colour c);
    void connectPoints(int x1, int y1, int x2, int y2);

    void resampleData(size_t w, size_t h);

    void writePngToFile();
    void getExtremesOfChunks(Field & min_field, Field & max_field, size_t column);
    void drawOneDimension();
    void drawTwoDimension();
    void writePrefix() override;
    void consume(Chunk) override;
    void finalizeImpl() override;


    size_t width = 1920;
    size_t height = 1080;
    size_t values_size = 0;
    std::string file_name = "pngOutputClickHouse.png";
    std::vector<uint8_t> data;
    std::vector<Chunk> chunks;
};
}
