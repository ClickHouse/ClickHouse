#include <cmath>
#include <cstddef>
#include <limits>
#include <Core/SortDescription.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Formats/Impl/DiagramOutputFormat.h>
#include <Common/UTF8Helpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int UNKNOWN_SETTING;
}

namespace
{

struct DiagramSymbols
{
    const char * left_top_corner = "┌";
    const char * right_top_corner = "┐";
    const char * bold_left_top_corner = "┏";
    const char * bold_right_top_corner = "┓";
    const char * left_bottom_corner = "└";
    const char * right_bottom_corner = "┘";
    const char * bold_left_separator = "┡";
    const char * left_separator = "├";
    const char * bold_right_separator = "┩";
    const char * right_separator = "┤";
    const char * bold_top_separator = "┳";
    const char * bold_middle_separator = "╇";
    const char * middle_separator = "┼";
    const char * bottom_separator = "┴";
    const char * bold_dash = "━";
    const char * dash = "─";
    const char * bold_bar = "┃";
    const char * bar = "│";
    const char * hist_block = "█";
    const char * ellipsis = "…";
    const char * power = "^";
};

const char * braille_symbols[] = {
    "\u2800", "\u2801", "\u2802", "\u2803", "\u2804", "\u2805", "\u2806", "\u2807", "\u2808", "\u2809", "\u280A", "\u280B", "\u280C",
    "\u280D", "\u280E", "\u280F", "\u2810", "\u2811", "\u2812", "\u2813", "\u2814", "\u2815", "\u2816", "\u2817", "\u2818", "\u2819",
    "\u281A", "\u281B", "\u281C", "\u281D", "\u281E", "\u281F", "\u2820", "\u2821", "\u2822", "\u2823", "\u2824", "\u2825", "\u2826",
    "\u2827", "\u2828", "\u2829", "\u282A", "\u282B", "\u282C", "\u282D", "\u282E", "\u282F", "\u2830", "\u2831", "\u2832", "\u2833",
    "\u2834", "\u2835", "\u2836", "\u2837", "\u2838", "\u2839", "\u283A", "\u283B", "\u283C", "\u283D", "\u283E", "\u283F", "\u2840",
    "\u2841", "\u2842", "\u2843", "\u2844", "\u2845", "\u2846", "\u2847", "\u2848", "\u2849", "\u284A", "\u284B", "\u284C", "\u284D",
    "\u284E", "\u284F", "\u2850", "\u2851", "\u2852", "\u2853", "\u2854", "\u2855", "\u2856", "\u2857", "\u2858", "\u2859", "\u285A",
    "\u285B", "\u285C", "\u285D", "\u285E", "\u285F", "\u2860", "\u2861", "\u2862", "\u2863", "\u2864", "\u2865", "\u2866", "\u2867",
    "\u2868", "\u2869", "\u286A", "\u286B", "\u286C", "\u286D", "\u286E", "\u286F", "\u2870", "\u2871", "\u2872", "\u2873", "\u2874",
    "\u2875", "\u2876", "\u2877", "\u2878", "\u2879", "\u287A", "\u287B", "\u287C", "\u287D", "\u287E", "\u287F", "\u2880", "\u2881",
    "\u2882", "\u2883", "\u2884", "\u2885", "\u2886", "\u2887", "\u2888", "\u2889", "\u288A", "\u288B", "\u288C", "\u288D", "\u288E",
    "\u288F", "\u2890", "\u2891", "\u2892", "\u2893", "\u2894", "\u2895", "\u2896", "\u2897", "\u2898", "\u2899", "\u289A", "\u289B",
    "\u289C", "\u289D", "\u289E", "\u289F", "\u28A0", "\u28A1", "\u28A2", "\u28A3", "\u28A4", "\u28A5", "\u28A6", "\u28A7", "\u28A8",
    "\u28A9", "\u28AA", "\u28AB", "\u28AC", "\u28AD", "\u28AE", "\u28AF", "\u28B0", "\u28B1", "\u28B2", "\u28B3", "\u28B4", "\u28B5",
    "\u28B6", "\u28B7", "\u28B8", "\u28B9", "\u28BA", "\u28BB", "\u28BC", "\u28BD", "\u28BE", "\u28BF", "\u28C0", "\u28C1", "\u28C2",
    "\u28C3", "\u28C4", "\u28C5", "\u28C6", "\u28C7", "\u28C8", "\u28C9", "\u28CA", "\u28CB", "\u28CC", "\u28CD", "\u28CE", "\u28CF",
    "\u28D0", "\u28D1", "\u28D2", "\u28D3", "\u28D4", "\u28D5", "\u28D6", "\u28D7", "\u28D8", "\u28D9", "\u28DA", "\u28DB", "\u28DC",
    "\u28DD", "\u28DE", "\u28DF", "\u28E0", "\u28E1", "\u28E2", "\u28E3", "\u28E4", "\u28E5", "\u28E6", "\u28E7", "\u28E8", "\u28E9",
    "\u28EA", "\u28EB", "\u28EC", "\u28ED", "\u28EE", "\u28EF", "\u28F0", "\u28F1", "\u28F2", "\u28F3", "\u28F4", "\u28F5", "\u28F6",
    "\u28F7", "\u28F8", "\u28F9", "\u28FA", "\u28FB", "\u28FC", "\u28FD", "\u28FE", "\u28FF",
};

DiagramSymbols utf_diagram_symbols;

DiagramSymbols ascii_diagram_symbols{"+", "+", "+", "+", "+", "+", "+", "+", "+", "+", "+",
                                     "+", "+", "+", "-", "-", "|", "|", "#", "~", "^"};
}


void DiagramOutputFormat::write()
{
    if (!diagram_type.contains(format_settings.diagram.diagram_type))
        throw Exception(ErrorCodes::UNKNOWN_SETTING, "No such diagram type.");
    switch (diagram_type.at(format_settings.diagram.diagram_type))
    {
        case DiagramType::SCATTER:
            writeScatter();
            break;
        case DiagramType::LINE:
            writeLineplot();
            break;
        case DiagramType::HISTOGRAM:
            writeHistogram();
            break;
        case DiagramType::HEAT_MAP:
            writeHeatMap();
            break;
    }
}

DiagramOutputFormat::DiagramOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}, serializations(header_.getSerializations())
{
    is_ascii_symbols = format_settings_.diagram.is_ascii_symbols;
    height = DiagramConstants::DEFAULT_HEIGHT;
    width = DiagramConstants::DEFAULT_WIEDTH;
}

void DiagramOutputFormat::consume(Chunk chunk)
{
    chunks.push_back(std::move(chunk));
}


void DiagramOutputFormat::writeScatter()
{
    const DiagramSymbols & diagram_symbols = is_ascii_symbols ? ascii_diagram_symbols : utf_diagram_symbols;
    if (format_settings.diagram.limit_height > 0)
        height = format_settings.diagram.limit_height;
    if (format_settings.diagram.limit_width > 0)
        width = format_settings.diagram.limit_width;
    std::vector<std::vector<BraileSymbol>> plot = std::vector<std::vector<BraileSymbol>>(height, std::vector<BraileSymbol>(width));

    // Calculate data limits for scaling
    height *= DiagramConstants::BRAILE_BLOCK_HEIGHT;
    width *= DiagramConstants::BRAILE_BLOCK_WIDTH;
    Float64 max_x = std::numeric_limits<Float64>::min();
    Float64 max_y = std::numeric_limits<Float64>::min();
    Float64 min_x = std::numeric_limits<Float64>::max();
    Float64 min_y = std::numeric_limits<Float64>::max();
    for (const auto & chunk : chunks)
    {
        if (chunk.getNumColumns() != DiagramConstants::EXPEXTED_COLUMN_NUMBER)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Incorrect number of columns");

        const auto x_column = chunk.getColumns()[0];
        const auto y_column = chunk.getColumns()[1];

        if (!x_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of X column isn't numeric.");

        if (!y_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of Y column isn't numeric.");

        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            max_x = std::max(max_x, x_column->getFloat64(i));
            max_y = std::max(max_y, y_column->getFloat64(i));
            min_x = std::min(min_x, x_column->getFloat64(i));
            min_y = std::min(min_y, y_column->getFloat64(i));
        }
    }
    Float64 x_len = std::fabs(max_x - min_x) > std::numeric_limits<Float64>::epsilon() ? max_x - min_x : 1;
    Float64 y_len = std::fabs(max_y - min_y) > std::numeric_limits<Float64>::epsilon() ? max_y - min_y : 1;
    Float64 scale_coef_x = static_cast<Float64>(width - 1) / x_len;
    Float64 scale_coef_y = static_cast<Float64>(height - 1) / y_len;

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        const auto x_column = chunks[i].getColumns()[0];
        const auto y_column = chunks[i].getColumns()[1];
        for (size_t j = 0; j < chunks[i].getNumRows(); ++j)
        {
            Float64 x = x_column->getFloat64(j);
            Float64 y = y_column->getFloat64(j);
            size_t plot_x = static_cast<size_t>(std::round((x - min_x) * scale_coef_x));
            size_t plot_y = static_cast<size_t>(std::round((y - min_y) * scale_coef_y));
            if (plot_x < width && plot_y < height)
            {
                plot_y = height - plot_y - 1;
                size_t braille_block_x = plot_x / DiagramConstants::BRAILE_BLOCK_WIDTH;
                size_t braille_block_y = plot_y / DiagramConstants::BRAILE_BLOCK_HEIGHT;
                size_t x_block = plot_x % DiagramConstants::BRAILE_BLOCK_WIDTH;
                size_t y_block = plot_y % DiagramConstants::BRAILE_BLOCK_HEIGHT;
                plot[braille_block_y][braille_block_x].points[y_block][x_block] = i;
            }
        }
    }

    // Write plot
    if (!format_settings.diagram.title.empty())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + format_settings.diagram.title + "\n", out);

    String x_max_str = limitNumSize(max_x, DiagramConstants::FIELD_SIZE);
    String y_max_str = limitNumSize(max_y, DiagramConstants::FIELD_SIZE);
    String x_min_str = limitNumSize(min_x, DiagramConstants::FIELD_SIZE);
    String y_min_str = limitNumSize(min_y, DiagramConstants::FIELD_SIZE);
    writeString(
        y_max_str + diagram_symbols.left_top_corner + genRepeatString(diagram_symbols.dash, width / DiagramConstants::BRAILE_BLOCK_WIDTH)
            + diagram_symbols.right_top_corner + '\n',
        out);
    for (const auto & row : plot)
    {
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.bar, out);
        for (const auto & el : row)
        {
            ssize_t color = el.getColor();
            String colored_string;
            if (color != -1)
                colored_string = "\033[38;5;" + std::to_string((color + 1) % DiagramConstants::MAX_COLOR_MODULE) + "m"
                    + (is_ascii_symbols ? "." : braille_symbols[el.getSymbolNum()]) + color_reset;
            else
                colored_string = (is_ascii_symbols ? " " : braille_symbols[el.getSymbolNum()]);
            writeString(colored_string, out);
        }
        writeString(String(diagram_symbols.bar) + "\n", out);
    }
    writeString(
        y_min_str + diagram_symbols.left_bottom_corner + genRepeatString(diagram_symbols.dash, width / DiagramConstants::BRAILE_BLOCK_WIDTH)
            + diagram_symbols.right_bottom_corner + '\n',
        out);
    if (width / DiagramConstants::BRAILE_BLOCK_WIDTH < x_min_str.size() + x_max_str.size())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str + ' ' + x_max_str, out);
    else
        writeString(
            String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str
                + String(width / DiagramConstants::BRAILE_BLOCK_WIDTH - x_min_str.size() - x_max_str.size(), ' ') + x_max_str,
            out);
}
String DiagramOutputFormat::genRepeatString(String s, size_t n)
{
    String result;
    for (size_t i = 0; i < n; ++i)
        result += s;
    return result;
}

void DiagramOutputFormat::writeHistogram()
{
    const DiagramSymbols & diagram_symbols = is_ascii_symbols ? ascii_diagram_symbols : utf_diagram_symbols;
    if (format_settings.diagram.limit_height > 0)
        height = format_settings.diagram.limit_height;
    else
        height = std::numeric_limits<size_t>::max();

    if (format_settings.diagram.limit_width > 0)
        width = format_settings.diagram.limit_width;

    // Calculate data limits for scaling
    Float64 max_y = std::numeric_limits<Float64>::min();
    Float64 min_y = std::numeric_limits<Float64>::max();
    for (const auto & chunk : chunks)
    {
        if (chunk.getNumColumns() != DiagramConstants::EXPEXTED_COLUMN_NUMBER)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Incorrect number of columns");

        const auto x_column = chunk.getColumns()[0];
        const auto y_column = chunk.getColumns()[1];
        if (!y_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of Y column isn't numeric.");

        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Float64 y = y_column->getFloat64(i);
            max_y = std::max(max_y, y);
            min_y = std::min(min_y, y);
        }
    }


    Float64 y_len = std::fabs(max_y - min_y) > std::numeric_limits<Float64>::epsilon() ? max_y - min_y : 1;
    Float64 scale_coef_y = std::max(0.0, static_cast<Float64>(width) - static_cast<Float64>(DiagramConstants::FIELD_SIZE + 1)) / y_len;

    // Write plot
    if (!format_settings.diagram.title.empty())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + format_settings.diagram.title + "\n", out);
    writeString(
        String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.left_top_corner + String(width, ' ') + diagram_symbols.right_top_corner,
        out);
    writeCString("\n", out);
    size_t printed_strings = 0;
    for (size_t i = 0; i < chunks.size(); ++i)
    {
        const auto x_column = chunks[i].getColumns()[0];
        const auto y_column = chunks[i].getColumns()[1];
        String color = "\033[38;5;" + std::to_string((i + 1) % DiagramConstants::MAX_COLOR_MODULE) + "m";
        for (size_t j = 0; j < chunks[i].getNumRows(); ++j)
        {
            if (printed_strings >= height)
                break;
            auto [size, serialized_str] = getSerialzedStr(*x_column, *serializations[0], j, DiagramConstants::FIELD_SIZE - 1);
            String x_string = String(DiagramConstants::FIELD_SIZE, ' ');
            x_string = String(DiagramConstants::FIELD_SIZE - 1 - size, ' ') + serialized_str + " ";
            Float64 y = y_column->getFloat64(j);
            size_t hist_block_cnt = static_cast<size_t>(std::round((y - min_y) * scale_coef_y)) + 1;
            String y_str = limitNumSize(y, DiagramConstants::FIELD_SIZE - 1);
            writeString(
                x_string + diagram_symbols.right_separator + color + genRepeatString(diagram_symbols.hist_block, hist_block_cnt)
                    + color_reset + " " + y_str + "\n",
                out);
            ++printed_strings;
            if ((j + 1 != chunks[i].getNumRows() || i + 1 != chunks.size()) && printed_strings + 1 < height)
                writeString(String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.bar + String(width, ' ') + "\n", out);
            else if (printed_strings + 1 >= height)
                break;
        }
        if (printed_strings >= height)
            break;
    }
    writeString(
        String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.left_bottom_corner + String(width, ' ')
            + diagram_symbols.right_bottom_corner,
        out);
    if (printed_strings >= height)
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + "To much data to print all.", out);
}


void DiagramOutputFormat::writeHeatMap()
{
    const DiagramSymbols & diagram_symbols = is_ascii_symbols ? ascii_diagram_symbols : utf_diagram_symbols;

    if (format_settings.diagram.limit_height > 0)
        height = format_settings.diagram.limit_height;

    if (format_settings.diagram.limit_width > 0)
        width = format_settings.diagram.limit_width;

    // Calculate data limits for scaling
    Float64 max_x = std::numeric_limits<Float64>::min();
    Float64 max_y = std::numeric_limits<Float64>::min();
    Float64 min_x = std::numeric_limits<Float64>::max();
    Float64 min_y = std::numeric_limits<Float64>::max();
    for (const auto & chunk : chunks)
    {
        if (chunk.getNumColumns() != DiagramConstants::EXPEXTED_COLUMN_NUMBER)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Incorrect number of columns");

        const auto x_column = chunk.getColumns()[0];
        const auto y_column = chunk.getColumns()[1];

        if (!x_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of X column isn't numeric.");

        if (!y_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of Y column isn't numeric.");
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Float64 x = x_column->getFloat64(i);
            Float64 y = y_column->getFloat64(i);
            max_x = std::max(max_x, x);
            max_y = std::max(max_y, y);
            min_x = std::min(min_x, x);
            min_y = std::min(min_y, y);
        }
    }
    Float64 x_len = std::fabs(max_x - min_x) > std::numeric_limits<Float64>::epsilon() ? max_x - min_x : 1;
    Float64 y_len = std::fabs(max_y - min_y) > std::numeric_limits<Float64>::epsilon() ? max_y - min_y : 1;
    Float64 scale_coef_x = static_cast<Float64>(width - 1) / x_len;
    Float64 scale_coef_y = static_cast<Float64>(height - 1) / y_len;
    size_t heat_min = std::numeric_limits<size_t>::max();
    size_t heat_max = 0;
    std::vector<std::vector<size_t>> heat_map(height, std::vector<size_t>(width, 0));
    for (const auto & chunk : chunks)
    {
        const auto x_column = chunk.getColumns()[0];
        const auto y_column = chunk.getColumns()[1];
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Float64 x = x_column->getFloat64(i);
            Float64 y = y_column->getFloat64(i);
            size_t plot_x = static_cast<size_t>(std::round((x - min_x) * scale_coef_x));
            size_t plot_y = static_cast<size_t>(std::round((y - min_y) * scale_coef_y));
            if (plot_x < width && plot_y < height)
                ++heat_map[plot_y][plot_x];
        }
    }
    for (const auto & row : heat_map)
    {
        for (const auto & el : row)
        {
            heat_max = std::max(heat_max, el);
            heat_min = std::min(heat_min, el);
        }
    }
    Float64 heat_len = std::max(1.0, static_cast<Float64>(heat_max - heat_min));

    // Write plot
    if (!format_settings.diagram.title.empty())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + format_settings.diagram.title + "\n", out);

    String x_max_str = limitNumSize(max_x, DiagramConstants::FIELD_SIZE);
    String y_max_str = limitNumSize(max_y, DiagramConstants::FIELD_SIZE);
    String x_min_str = limitNumSize(min_x, DiagramConstants::FIELD_SIZE);
    String y_min_str = limitNumSize(min_y, DiagramConstants::FIELD_SIZE);
    writeString(
        y_max_str + diagram_symbols.left_top_corner + genRepeatString(diagram_symbols.dash, width) + diagram_symbols.right_top_corner
            + '\n',
        out);
    for (const auto & row : heat_map)
    {
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.bar, out);
        for (const auto & el : row)
        {
            size_t r_chanel = static_cast<size_t>(
                DiagramConstants::GRADIENT_START.r
                + static_cast<Float64>(el - heat_min) / heat_len * (DiagramConstants::GRADIENT_END.r - DiagramConstants::GRADIENT_START.r));
            size_t g_chanel = static_cast<size_t>(
                DiagramConstants::GRADIENT_START.g
                + static_cast<Float64>(el - heat_min) / heat_len * (DiagramConstants::GRADIENT_END.g - DiagramConstants::GRADIENT_START.g));
            size_t b_chanel = static_cast<size_t>(
                DiagramConstants::GRADIENT_START.b
                + static_cast<Float64>(el - heat_min) / heat_len * (DiagramConstants::GRADIENT_END.b - DiagramConstants::GRADIENT_START.b));
            String colored_string = "\033[48;2;" + std::to_string(r_chanel) + ";" + std::to_string(g_chanel) + ";"
                + std::to_string(b_chanel) + "m" + " " + "\033[0m";
            writeString(colored_string, out);
        }
        writeString(String(diagram_symbols.bar) + "\n", out);
    }
    writeString(
        y_min_str + diagram_symbols.left_bottom_corner + genRepeatString(diagram_symbols.dash, width) + diagram_symbols.right_bottom_corner
            + '\n',
        out);
    if (width < x_min_str.size() + x_max_str.size())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str + ' ' + x_max_str, out);
    else
        writeString(
            String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str + String(width - x_min_str.size() - x_max_str.size(), ' ') + x_max_str,
            out);
}
void DiagramOutputFormat::writeLineplot()
{
    const DiagramSymbols & diagram_symbols = is_ascii_symbols ? ascii_diagram_symbols : utf_diagram_symbols;
    if (format_settings.diagram.limit_height > 0)
        height = format_settings.diagram.limit_height;

    if (format_settings.diagram.limit_width > 0)
        width = format_settings.diagram.limit_width;

    std::vector<std::vector<BraileSymbol>> plot = std::vector<std::vector<BraileSymbol>>(height, std::vector<BraileSymbol>(width));

    // Calculate data limits for scaling
    height *= DiagramConstants::BRAILE_BLOCK_HEIGHT;
    width *= DiagramConstants::BRAILE_BLOCK_WIDTH;
    Float64 max_x = std::numeric_limits<Float64>::min();
    Float64 max_y = std::numeric_limits<Float64>::min();
    Float64 min_x = std::numeric_limits<Float64>::max();
    Float64 min_y = std::numeric_limits<Float64>::max();
    for (auto & chunk : chunks)
    {
        if (chunk.getNumColumns() != DiagramConstants::EXPEXTED_COLUMN_NUMBER)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Incorrect number of columns");
        const auto x_column = chunk.getColumns()[0];
        const auto y_column = chunk.getColumns()[1];

        if (!x_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of X column isn't numeric.");

        if (!y_column->isNumeric())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Data type of Y column isn't numeric.");

        auto block = getPort(PortKind::Main).getHeader();
        block.setColumns(chunk.getColumns());
        SortDescription sort_description;
        sort_description.reserve(chunk.getNumColumns());

        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
            sort_description.emplace_back(block.getColumnsWithTypeAndName()[i].name, 1, 1);

        if (!isAlreadySorted(block, sort_description))
            sortBlock(block, sort_description);

        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            max_x = std::max(max_x, x_column->getFloat64(i));
            max_y = std::max(max_y, y_column->getFloat64(i));
            min_x = std::min(min_x, x_column->getFloat64(i));
            min_y = std::min(min_y, y_column->getFloat64(i));
        }
    }
    Float64 x_len = std::fabs(max_x - min_x) > std::numeric_limits<Float64>::epsilon() ? max_x - min_x : 1;
    Float64 y_len = std::fabs(max_y - min_y) > std::numeric_limits<Float64>::epsilon() ? max_y - min_y : 1;
    Float64 scale_coef_x = static_cast<Float64>(width - 1) / x_len;
    Float64 scale_coef_y = static_cast<Float64>(height - 1) / y_len;

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        const auto x_column = chunks[i].getColumns()[0];
        const auto y_column = chunks[i].getColumns()[1];
        for (size_t j = 0; j + 1 < chunks[i].getNumRows(); ++j)
        {
            Float64 x1 = x_column->getFloat64(j);
            Float64 y1 = y_column->getFloat64(j);
            Int64 plot_x1 = static_cast<Int64>(std::round((x1 - min_x) * scale_coef_x));
            Int64 plot_y1 = static_cast<Int64>(std::round((y1 - min_y) * scale_coef_y));
            Float64 x2 = x_column->getFloat64(j + 1);
            Float64 y2 = y_column->getFloat64(j + 1);
            Int64 plot_x2 = static_cast<Int64>(std::round((x2 - min_x) * scale_coef_x));
            Int64 plot_y2 = static_cast<Int64>(std::round((y2 - min_y) * scale_coef_y));
            std::vector<std::pair<size_t, size_t>> line_points = drawLine({plot_x1, plot_y1}, {plot_x2, plot_y2});
            for (auto & [x, y] : line_points)
            {
                if (x < width && y < height)
                {
                    y = height - y - 1;
                    size_t braille_block_x = x / DiagramConstants::BRAILE_BLOCK_WIDTH;
                    size_t braille_block_y = y / DiagramConstants::BRAILE_BLOCK_HEIGHT;
                    size_t x_block = x % DiagramConstants::BRAILE_BLOCK_WIDTH;
                    size_t y_block = y % DiagramConstants::BRAILE_BLOCK_HEIGHT;
                    plot[braille_block_y][braille_block_x].points[y_block][x_block] = i;
                }
            }
        }
    }
    String x_max_str = limitNumSize(max_x, DiagramConstants::FIELD_SIZE);
    String y_max_str = limitNumSize(max_y, DiagramConstants::FIELD_SIZE);
    String x_min_str = limitNumSize(min_x, DiagramConstants::FIELD_SIZE);
    String y_min_str = limitNumSize(min_y, DiagramConstants::FIELD_SIZE);
    if (!format_settings.diagram.title.empty())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + format_settings.diagram.title + "\n", out);
    writeString(
        y_max_str + diagram_symbols.left_top_corner + genRepeatString(diagram_symbols.dash, width / DiagramConstants::BRAILE_BLOCK_WIDTH)
            + diagram_symbols.right_top_corner + '\n',
        out);
    for (const auto & row : plot)
    {
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + diagram_symbols.bar, out);
        for (const auto & el : row)
        {
            ssize_t color = el.getColor();
            String colored_string;
            if (color != -1)
                colored_string = "\033[38;5;" + std::to_string((color + 1) % DiagramConstants::MAX_COLOR_MODULE) + "m"
                    + (is_ascii_symbols ? "." : braille_symbols[el.getSymbolNum()]) + color_reset;
            else
                colored_string = is_ascii_symbols ? " " : braille_symbols[el.getSymbolNum()];
            writeString(colored_string, out);
        }
        writeString(String(diagram_symbols.bar) + "\n", out);
    }
    writeString(
        y_min_str + diagram_symbols.left_bottom_corner + genRepeatString(diagram_symbols.dash, width / DiagramConstants::BRAILE_BLOCK_WIDTH)
            + diagram_symbols.right_bottom_corner + '\n',
        out);
    if (width / DiagramConstants::BRAILE_BLOCK_WIDTH < x_min_str.size() + x_max_str.size())
        writeString(String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str + ' ' + x_max_str, out);
    else
        writeString(
            String(DiagramConstants::FIELD_SIZE, ' ') + x_min_str
                + String(width / DiagramConstants::BRAILE_BLOCK_WIDTH - x_min_str.size() - x_max_str.size(), ' ') + x_max_str,
            out);
}
String DiagramOutputFormat::limitNumSize(Float64 num, size_t limit) const
{
    const int buffer_len = 100;
    const int precision = 2;
    char buffer[buffer_len];
    int chars_written = std::snprintf(buffer, buffer_len, "%.*e", precision, num);
    String str(buffer, chars_written);
    if (chars_written < static_cast<int>(limit))
        str = String(limit - 1 - static_cast<size_t>(chars_written), ' ') + str;
    str += ' ';
    return str;
}

void DiagramOutputFormat::writeSuffix()
{
    write();
}
std::pair<size_t, String>
DiagramOutputFormat::getSerialzedStr(const IColumn & column, const ISerialization & serialization, size_t row_num, size_t max_size)
{
    String serialized_value = " ";
    const DiagramSymbols & diagram_symbols = is_ascii_symbols ? ascii_diagram_symbols : utf_diagram_symbols;
    WriteBufferFromString out_serialize(serialized_value, AppendModeTag());
    serialization.serializeText(column, row_num, out_serialize, format_settings);
    size_t serialized_value_visible_size = UTF8::computeWidth(reinterpret_cast<UInt8 *>(serialized_value.data()), serialized_value.size());
    size_t new_size = serialized_value_visible_size;
    if (serialized_value_visible_size > max_size)
    {
        serialized_value.resize(
            UTF8::computeBytesBeforeWidth(reinterpret_cast<UInt8 *>(serialized_value.data()), serialized_value.size(), 0, max_size - 1));
        serialized_value += diagram_symbols.ellipsis;
        new_size = UTF8::computeWidth(reinterpret_cast<UInt8 *>(serialized_value.data()), serialized_value.size());
    }
    return {new_size, serialized_value};
}
ssize_t DiagramOutputFormat::BraileSymbol::getColor() const
{
    std::unordered_map<ssize_t, size_t> counts;
    ssize_t most_common = -1;
    size_t max_count = 0;
    for (const auto & point_row : points)
    {
        for (const auto & point : point_row)
        {
            if (point != -1)
            {
                counts[point]++;
                if (counts[point] > max_count)
                {
                    max_count = counts[point];
                    most_common = point;
                }
            }
        }
    }
    return most_common;
}

UInt8 DiagramOutputFormat::BraileSymbol::getSymbolNum() const
{
    UInt8 res = 0;
    res |= points[0][0] != -1 ? 1 : 0;
    res |= points[1][0] != -1 ? 1 << 1 : 0;
    res |= points[2][0] != -1 ? 1 << 2 : 0;
    res |= points[0][1] != -1 ? 1 << 2 : 0;
    res |= points[1][1] != -1 ? 1 << 4 : 0;
    res |= points[2][1] != -1 ? 1 << 5 : 0;
    res |= points[3][0] != -1 ? 1 << 6 : 0;
    res |= points[3][1] != -1 ? 1 << 7 : 0;
    return res;
}

std::vector<std::pair<size_t, size_t>> DiagramOutputFormat::drawLine(std::pair<Int64, Int64> p1, std::pair<Int64, Int64> p2)
{
    auto [x1, y1] = p1;
    auto [x2, y2] = p2;
    std::vector<std::pair<size_t, size_t>> result;
    const Int64 delta_x = abs(p2.first - p1.first);
    const Int64 delta_y = abs(p2.second - p1.second);
    const Int64 sign_x = x1 < x2 ? 1 : -1;
    const Int64 sign_y = y1 < y2 ? 1 : -1;
    Int64 error = delta_x - delta_y;
    while (x1 != x2 || y1 != y2)
    {
        result.emplace_back(static_cast<size_t>(x1), static_cast<size_t>(y1));
        Int64 error2 = error * 2;
        if (error2 > -delta_y)
        {
            error -= delta_y;
            x1 += sign_x;
        }
        if (error2 < delta_x)
        {
            error += delta_x;
            y1 += sign_y;
        }
    }
    result.emplace_back(x2, y2);
    return result;
}

void registerOutputFrormatDiagram(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Diagram",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings)
        { return std::make_shared<DiagramOutputFormat>(buf, sample, format_settings); });
}

}
