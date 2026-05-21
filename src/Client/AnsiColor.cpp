#include <Client/AnsiColor.h>

#include <fmt/format.h>
#include <iterator>

namespace DB::AnsiColor
{

std::string ansiSequence(Color c)
{
    if (c == 0)
        return "\033[0m";

    std::string out = "\033[0";
    if (c & BOLD)
        out += ";1";
    if (c & UNDERLINE)
        out += ";4";
    if (c & FG_SET)
    {
        auto idx = static_cast<unsigned>(c & 0xFFu);
        if (idx < 8)
            fmt::format_to(std::back_inserter(out), ";{}", 30 + idx);
        else if (idx < 16)
            fmt::format_to(std::back_inserter(out), ";{}", 90 + (idx - 8));
        else
            fmt::format_to(std::back_inserter(out), ";38;5;{}", idx);
    }
    if (c & BG_SET)
    {
        auto idx = static_cast<unsigned>((c >> BG_SHIFT) & 0xFFu);
        if (idx < 8)
            fmt::format_to(std::back_inserter(out), ";{}", 40 + idx);
        else if (idx < 16)
            fmt::format_to(std::back_inserter(out), ";{}", 100 + (idx - 8));
        else
            fmt::format_to(std::back_inserter(out), ";48;5;{}", idx);
    }
    out += 'm';
    return out;
}

}
