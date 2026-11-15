#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFormatting.h>
#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <array>
#include <limits>
#include <vector>


namespace DB
{

namespace
{

constexpr size_t no_group = std::numeric_limits<size_t>::max();

size_t displayWidth(std::string_view text, size_t prefix = 0)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(text.data()), text.size(), prefix);
}

bool fitsLineLength(std::string_view query, size_t line_length)
{
    while (!query.empty())
    {
        size_t newline = query.find('\n');
        if (displayWidth(query.substr(0, newline)) > line_length)
            return false;
        if (newline == std::string_view::npos)
            break;
        query.remove_prefix(newline + 1);
    }
    return true;
}

struct FlatWidth
{
    /// Tabs advance to multiples of eight columns, so width depends on the starting column modulo eight.
    std::array<size_t, 8> columns{};
    bool multiline = false;

    void append(const FlatWidth & other)
    {
        multiline |= other.multiline;
        if (multiline)
            return;
        for (size_t prefix = 0; prefix < columns.size(); ++prefix)
            columns[prefix] += other.columns[(prefix + columns[prefix]) % columns.size()];
    }

    void append(std::string_view text)
    {
        multiline |= text.contains('\n');
        if (multiline)
            return;

        FlatWidth width;
        width.columns.fill(displayWidth(text));
        if (text.contains('\t'))
            for (size_t prefix = 1; prefix < columns.size(); ++prefix)
                width.columns[prefix] = displayWidth(text, prefix);
        append(width);
    }
};

struct FormattingGroup
{
    size_t parent;
    FlatWidth width;
    std::array<size_t, 8> suffix_columns{};
    bool broken = false;
};

struct FormattingEvent
{
    enum Kind : uint8_t
    {
        Open,
        Close,
        Break,
    };

    Kind kind;
    size_t offset;
    size_t group;
    size_t indent = 0;
    size_t length = 0;
    /// Optional breaks check the following fragment instead of inheriting the group's layout.
    bool optional = false;
};

}

class ASTFormatting
{
public:
    explicit ASTFormatting(WriteBuffer & out_)
        : out(out_)
    {
    }

    WriteBuffer & out;
    std::vector<FormattingGroup> groups;
    /// Events have nondecreasing byte offsets in `out`; groups open and close in stack order.
    std::vector<FormattingEvent> events;
    size_t current_group = no_group;
    size_t open_groups = 0;

    void reserveEvents(size_t extra)
    {
        /// Reserve pending group closures so `ASTFormattingGroup` destruction cannot allocate.
        size_t required = events.size() + open_groups + extra;
        if (required > events.capacity())
            events.reserve(std::max(required, events.capacity() * 2));
    }

    static String
    layout(std::string_view query, size_t line_length, std::vector<FormattingGroup> & groups, const std::vector<FormattingEvent> & events)
    {
        size_t cursor = 0;
        size_t group = no_group;
        for (const auto & event : events)
        {
            chassert(cursor <= event.offset && event.offset <= query.size());
            if (group != no_group)
                groups[group].width.append(query.substr(cursor, event.offset - cursor));
            cursor = event.offset;
            if (event.kind == FormattingEvent::Open)
                group = event.group;
            else if (event.kind == FormattingEvent::Close)
            {
                group = groups[event.group].parent;
                if (group != no_group)
                    groups[group].width.append(groups[event.group].width);
            }
        }

        /// Include text after each group up to the next break or newline, such as closing brackets and commas.
        FlatWidth suffix;
        cursor = query.size();
        for (auto it = events.rbegin(); it != events.rend(); ++it)
        {
            auto text = query.substr(it->offset, cursor - it->offset);
            if (size_t newline = text.find('\n'); newline != std::string_view::npos)
            {
                text = text.substr(0, newline);
                suffix = {};
            }
            FlatWidth preceding;
            preceding.append(text);
            preceding.append(suffix);
            suffix = preceding;
            if (it->kind == FormattingEvent::Close)
                groups[it->group].suffix_columns = suffix.columns;
            else if (it->kind == FormattingEvent::Break)
                suffix = {};
            cursor = it->offset;
        }

        String result;
        result.reserve(query.size());
        size_t column = 0;
        size_t line_start = 0;
        bool line_has_content = false;
        auto append = [&](std::string_view text)
        {
            size_t newline = text.rfind('\n');
            if (newline == std::string_view::npos)
            {
                column += displayWidth(text, column);
                line_has_content |= text.find_first_not_of(" \t") != std::string_view::npos;
            }
            else
            {
                line_start = result.size() + newline + 1;
                text = text.substr(newline + 1);
                column = displayWidth(text);
                line_has_content = text.find_first_not_of(" \t") != std::string_view::npos;
            }
        };

        cursor = 0;
        for (size_t i = 0; i < events.size(); ++i)
        {
            const auto & event = events[i];
            auto text = query.substr(cursor, event.offset - cursor);
            append(text);
            result.append(text);
            cursor = event.offset;

            if (event.kind == FormattingEvent::Open)
            {
                auto & current = groups[event.group];
                size_t width = current.width.columns[column % 8];
                current.broken = current.width.multiline || column > line_length || width > line_length - column
                    || current.suffix_columns[(column + width) % 8] > line_length - column - width;
            }
            else if (event.kind == FormattingEvent::Break)
            {
                bool broken = event.group != no_group && groups[event.group].broken;
                if (event.group == no_group || event.optional)
                {
                    size_t next_break = query.size();
                    for (size_t j = i + 1; j < events.size(); ++j)
                    {
                        if (events[j].kind == FormattingEvent::Break)
                        {
                            next_break = events[j].offset;
                            break;
                        }
                    }
                    auto following = query.substr(cursor, next_break - cursor);
                    following = following.substr(0, following.find('\n'));
                    broken = column > line_length || displayWidth(following, column) > line_length - column;
                }

                if (broken)
                {
                    if (line_has_content)
                    {
                        result += '\n';
                        line_start = result.size();
                    }
                    else
                    {
                        result.resize(line_start);
                    }
                    result.append(event.indent, ' ');
                    column = event.indent;
                    line_has_content = false;
                }
                else
                {
                    text = query.substr(cursor, event.length);
                    append(text);
                    result.append(text);
                }
                cursor += event.length;
            }
        }
        result.append(query.substr(cursor));
        return result;
    }
};

ASTFormattingGroup::ASTFormattingGroup(WriteBuffer & out, IASTFormatState & state)
{
    if (!state.formatting || &state.formatting->out != &out)
        return;

    formatting = state.formatting;
    formatting->reserveEvents(2);
    index = formatting->groups.size();
    formatting->groups.push_back({.parent = formatting->current_group, .width = {}});
    formatting->events.push_back({FormattingEvent::Open, out.count(), index});
    formatting->current_group = index;
    ++formatting->open_groups;
}

ASTFormattingGroup::~ASTFormattingGroup()
{
    finish();
}

void ASTFormattingGroup::finish() noexcept
{
    if (!formatting)
        return;

    chassert(formatting->current_group == index);
    formatting->events.push_back({FormattingEvent::Close, formatting->out.count(), index});
    formatting->current_group = formatting->groups[index].parent;
    --formatting->open_groups;
    formatting = nullptr;
}

static void writeASTFormattingBreakImpl(WriteBuffer & out, IASTFormatState & state, size_t indent, char flat, bool optional)
{
    if (state.formatting && &state.formatting->out == &out)
    {
        auto & formatting = *state.formatting;
        formatting.reserveEvents(1);
        formatting.events.push_back(
            {FormattingEvent::Break, out.count(), formatting.current_group, indent, flat ? size_t{1} : size_t{0}, optional});
    }
    if (flat)
        out.write(flat);
}

void writeASTFormattingBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, char flat)
{
    writeASTFormattingBreakImpl(out, state, indent, flat, false);
}

void writeASTFormattingOptionalBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, char flat)
{
    writeASTFormattingBreakImpl(out, state, indent, flat, true);
}

String formatASTWithLineLength(const IAST & ast, const IAST::FormatSettings & settings, size_t line_length)
{
    {
        WriteBufferFromOwnString out;
        ast.format(out, settings);
        if (!line_length || settings.one_line || fitsLineLength(out.stringView(), line_length))
            return out.str();
    }

    WriteBufferFromOwnString out;
    ASTFormatting formatting(out);
    IAST::FormatState state;
    state.formatting = &formatting;
    ast.format(out, settings, state, {});
    return ASTFormatting::layout(out.stringView(), line_length, formatting.groups, formatting.events);
}

}
