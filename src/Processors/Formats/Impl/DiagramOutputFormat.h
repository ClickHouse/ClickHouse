#pragma once

#include <cstddef>
#include <vector>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{


class DiagramOutputFormat : public IOutputFormat
{
    enum class DiagramType
    {
        SCATTER,
        LINE,
        HISTOGRAM,
        HEAT_MAP,
    };
    struct Color
    {
        Float64 r = 0;
        Float64 g = 0;
        Float64 b = 0;
    };
    struct BraileSymbol
    {
        ssize_t points[4][2] = {{-1, -1}, {-1, -1}, {-1, -1}, {-1, -1}};
        ssize_t getColor() const;
        UInt8 getSymbolNum() const;
    };
    static inline const std::unordered_map<String, DiagramType> diagram_type
        = {{"SCATTER", DiagramType::SCATTER},
           {"HISTOGRAM", DiagramType::HISTOGRAM},
           {"LINE", DiagramType::LINE},
           {"HEAT_MAP", DiagramType::HEAT_MAP}};


    static inline const String color_reset = "\033[0m";
    std::vector<Chunk> chunks;
    size_t height = 30;
    size_t width = 75;

public:
    DiagramOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

protected:
    // Write plot
    void write();

    // Write scatter plot
    void writeScatter();

    // Write histogram
    void writeHistogram();

    // Write heatmap
    void writeHeatMap();

    // Write lineplot
    void writeLineplot();

    void writeSuffix() override;
    void consume(Chunk) override;

    // Generate repeating sequence of string
    String genRepeatString(String s, size_t n);
    String getName() const override { return "DiagramOutputFormat"; }

    // Get serialied value in row_num row of column
    std::pair<size_t, String>
    getSerialzedStr(const IColumn & column, const ISerialization & serialization, size_t row_num, size_t max_size);

    // Get string representatin of number with limited size
    String limitNumSize(Float64 num, size_t limit) const;

    // Generate points of segment
    std::vector<std::pair<size_t, size_t>> drawLine(std::pair<Int64, Int64> p1, std::pair<Int64, Int64> p2);
    const FormatSettings format_settings;
    Serializations serializations;
    bool is_ascii_symbols = false;
};
}
