#pragma once
#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>

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

    PngOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);
    String getName() const override { return "PngOutputFormat"; }

private:
    void drawPoint(size_t x, size_t y, const Colour & c);
    void connectPoints(int x1, int y1, int x2, int y2);
    void drawColumn(size_t x, size_t y, size_t len, size_t middle, const Colour & c);
    void resampleData(size_t w, size_t h);

    void getExtremesOfChunks(Field & min_field, Field & max_field, size_t column);
    void writePngToFile();
    void drawOneDimension();
    void drawTwoDimension();
    void consume(Chunk) override;
    void finalizeImpl() override;

    bool draw_lines;
    size_t width, height, result_width, result_height;
    size_t values_size = 0;
    std::string file_name;
    std::vector<uint8_t> data;
    std::vector<Chunk> chunks;
};
}
