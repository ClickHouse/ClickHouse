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
    /// Tabs use eight starting-column residues; later physical lines retain their widest and final widths.
    std::array<size_t, 8> columns{};
    size_t last_line_width = 0;
    size_t max_after_first_line = 0;
    bool multiline = false;

    void append(const FlatWidth & other)
    {
        if (multiline)
        {
            size_t joined_width = last_line_width + other.columns[last_line_width % columns.size()];
            max_after_first_line = std::max({max_after_first_line, joined_width, other.max_after_first_line});
            last_line_width = other.multiline ? other.last_line_width : joined_width;
        }
        else if (other.multiline)
        {
            last_line_width = other.last_line_width;
            max_after_first_line = other.max_after_first_line;
        }
        if (multiline)
            return;
        for (size_t prefix = 0; prefix < columns.size(); ++prefix)
            columns[prefix] += other.columns[(prefix + columns[prefix]) % columns.size()];
        multiline = other.multiline;
    }

    void append(std::string_view text)
    {
        size_t newline = text.find('\n');
        auto first_line = text.substr(0, newline);
        FlatWidth width;
        width.columns.fill(displayWidth(first_line));
        if (first_line.contains('\t'))
            for (size_t prefix = 1; prefix < columns.size(); ++prefix)
                width.columns[prefix] = displayWidth(first_line, prefix);
        width.multiline = newline != std::string_view::npos;
        while (newline != std::string_view::npos)
        {
            text.remove_prefix(newline + 1);
            newline = text.find('\n');
            width.last_line_width = displayWidth(text.substr(0, newline));
            width.max_after_first_line = std::max(width.max_after_first_line, width.last_line_width);
        }
        append(width);
    }

    bool fits(size_t column, size_t limit) const
    {
        return column <= limit && columns[column % columns.size()] <= limit - column;
    }
};

struct FormattingGroup
{
    size_t parent;
    size_t source_indent;
    ASTFormattingGroup::Mode mode;
    size_t source_line_indent = 0;
    size_t end_offset = 0;
    size_t end_event = 0;
    FlatWidth width{};
    FlatWidth suffix{};
    size_t indent = 0;
    bool broken = false;
};

struct FormattingEvent
{
    enum Kind : uint8_t
    {
        Open,
        Close,
        Break,
        Indent,
    };
    Kind kind;
    size_t offset;
    size_t group;
    size_t indent = 0;
    size_t nest = 0;
    size_t length = 0;
    bool fill = false;
    FlatWidth following{};
};

}

class ASTFormatting
{
public:
    explicit ASTFormatting(WriteBuffer & out_) : out(out_)
    {
    }

    WriteBuffer & out;
    std::vector<FormattingGroup> groups;
    /// Offsets are monotonic; close positions are stored in groups so destruction never allocates.
    std::vector<FormattingEvent> events;
    size_t current_group = no_group;

    String layout(std::string_view query, size_t line_length)
    {
        chassert(current_group == no_group);
        std::vector<FormattingEvent> ordered;
        ordered.reserve(events.size() + groups.size());
        size_t group = no_group;
        for (size_t i = 0; i <= events.size(); ++i)
        {
            while (group != no_group && groups[group].end_event == i)
            {
                ordered.push_back({.kind = FormattingEvent::Close, .offset = groups[group].end_offset, .group = group});
                group = groups[group].parent;
            }
            if (i == events.size())
                break;
            ordered.push_back(events[i]);
            if (events[i].kind == FormattingEvent::Open)
            {
                chassert(groups[events[i].group].parent == group);
                group = events[i].group;
            }
        }
        chassert(group == no_group);

        size_t cursor = 0;
        size_t source_line_indent = 0;
        bool source_line_has_content = false;
        for (const auto & event : ordered)
        {
            chassert(cursor <= event.offset && event.offset <= query.size());
            auto fragment = query.substr(cursor, event.offset - cursor);
            if (group != no_group)
                groups[group].width.append(fragment);
            size_t newline = fragment.rfind('\n');
            if (newline != std::string_view::npos)
            {
                source_line_indent = 0;
                source_line_has_content = false;
                fragment.remove_prefix(newline + 1);
            }
            if (!source_line_has_content)
            {
                size_t content = fragment.find_first_not_of(" \t");
                source_line_indent += displayWidth(fragment.substr(0, content), source_line_indent);
                source_line_has_content = content != std::string_view::npos;
            }
            cursor = event.offset;
            if (event.kind == FormattingEvent::Open)
            {
                group = event.group;
                groups[group].source_line_indent = source_line_indent;
            }
            else if (event.kind == FormattingEvent::Close)
            {
                group = groups[event.group].parent;
                if (group != no_group)
                    groups[group].width.append(groups[event.group].width);
            }
        }

        /// Lookahead ends at the next break in the same scope; nested groups remain whole fitting candidates.
        FlatWidth suffix;
        cursor = query.size();
        for (auto it = ordered.rbegin(); it != ordered.rend(); ++it)
        {
            FlatWidth preceding;
            preceding.append(query.substr(it->offset, cursor - it->offset));
            preceding.append(suffix);
            suffix = preceding;
            if (it->kind == FormattingEvent::Close)
            {
                groups[it->group].suffix = suffix;
            }
            else if (it->kind == FormattingEvent::Open)
            {
                suffix = groups[it->group].width;
                suffix.append(groups[it->group].suffix);
            }
            else if (it->kind == FormattingEvent::Break)
            {
                it->following = suffix;
                suffix = {};
            }
            cursor = it->offset;
        }

        String result;
        result.reserve(query.size());
        size_t column = 0;
        size_t line_start = 0;
        size_t line_indent = 0;
        bool line_has_content = false;
        bool line_indent_assigned = false;
        auto append = [&](std::string_view text)
        {
            size_t newline = text.rfind('\n');
            if (newline != std::string_view::npos)
            {
                line_start = result.size() + newline + 1;
                column = 0;
                line_indent = 0;
                line_has_content = false;
                line_indent_assigned = false;
            }
            auto tail = newline == std::string_view::npos ? text : text.substr(newline + 1);
            if (!line_has_content)
            {
                size_t content = tail.find_first_not_of(" \t");
                line_indent = column + displayWidth(tail.substr(0, content), column);
                line_has_content = content != std::string_view::npos;
            }
            column += displayWidth(tail, column);
            result.append(text);
        };

        cursor = 0;
        for (const auto & event : ordered)
        {
            append(query.substr(cursor, event.offset - cursor));
            cursor = event.offset;
            if (event.kind == FormattingEvent::Open)
            {
                auto & current = groups[event.group];
                current.indent = line_indent;
                FlatWidth width = current.width;
                width.append(current.suffix);
                current.broken = !current.width.multiline && !width.fits(column, line_length);
                if (current.width.multiline && current.mode == ASTFormattingGroup::Mode::ConsistentLines)
                {
                    /// Call arguments break together when the opening line or the tail after a subquery does not fit.
                    size_t tail_column = current.width.last_line_width;
                    size_t widest_column = current.width.max_after_first_line;
                    if (current.indent != current.source_line_indent)
                    {
                        tail_column = current.indent + (tail_column >= current.source_indent ? tail_column - current.source_indent : 0);
                        widest_column = current.indent + (widest_column >= current.source_indent ? widest_column - current.source_indent : 0);
                    }
                    current.broken = !current.width.fits(column, line_length) || widest_column > line_length
                        || !current.suffix.fits(tail_column, line_length);
                }
            }
            else if (event.kind == FormattingEvent::Indent)
            {
                size_t indent = event.indent;
                if (event.group != no_group && !line_has_content)
                {
                    const auto & owner = groups[event.group];
                    if (owner.indent != owner.source_line_indent)
                        indent = owner.indent + (indent >= owner.source_indent ? indent - owner.source_indent : 0);
                }
                append(String(indent, ' '));
                line_indent_assigned = !line_has_content;
                cursor += event.length;
            }
            else if (event.kind == FormattingEvent::Break)
            {
                bool independent = event.fill || event.group == no_group
                    || (groups[event.group].width.multiline && !groups[event.group].broken);
                bool broken = independent ? !event.following.fits(column, line_length) : groups[event.group].broken;
                if (broken)
                {
                    size_t indent = event.group == no_group ? event.indent : groups[event.group].indent;
                    indent += 4 * event.nest;
                    /// A leading break shares an indentation already supplied by the enclosing group.
                    if (!line_has_content && line_indent_assigned && !event.length && event.nest)
                    {
                        indent = line_indent;
                        if (event.group != no_group)
                        {
                            auto & owner = groups[event.group];
                            size_t nest = 4 * event.nest;
                            owner.indent = std::min(owner.indent, indent >= nest ? indent - nest : 0);
                        }
                    }
                    if (line_has_content)
                    {
                        while (result.size() > line_start && (result.back() == ' ' || result.back() == '\t'))
                            result.pop_back();
                        result += '\n';
                        line_start = result.size();
                    }
                    else
                        result.resize(line_start);
                    result.append(indent, ' ');
                    column = line_indent = indent;
                    line_has_content = false;
                    line_indent_assigned = true;
                }
                else
                    append(query.substr(cursor, event.length));
                cursor += event.length;
            }
        }
        append(query.substr(cursor));
        return result;
    }
};

void ASTFormattingGroup::start(
    WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, Mode mode)
{
    if (&state.formatting->out != &out)
        return;
    auto & collector = *state.formatting;
    index = collector.groups.size();
    collector.groups.push_back({.parent = collector.current_group, .source_indent = size_t{4} * frame.indent, .mode = mode});
    collector.events.push_back({.kind = FormattingEvent::Open, .offset = out.count(), .group = index});
    collector.current_group = index;
    formatting = &collector;
}

void ASTFormattingGroup::close() noexcept
{
    chassert(formatting->current_group == index);
    auto & group = formatting->groups[index];
    group.end_offset = formatting->out.count();
    group.end_event = formatting->events.size();
    formatting->current_group = group.parent;
    formatting = nullptr;
}

void ASTFormattingDetail::writeBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, size_t nest, char flat, bool fill)
{
    if (&state.formatting->out == &out)
        state.formatting->events.push_back({.kind = FormattingEvent::Break, .offset = out.count(),
            .group = state.formatting->current_group, .indent = 4 * indent, .nest = nest, .length = flat ? size_t{1} : size_t{0}, .fill = fill});
    if (flat)
        out.write(flat);
}

void ASTFormattingDetail::writeIndent(WriteBuffer & out, IASTFormatState & state, size_t indent)
{
    if (&state.formatting->out == &out)
        state.formatting->events.push_back({.kind = FormattingEvent::Indent, .offset = out.count(),
            .group = state.formatting->current_group, .indent = 4 * indent, .length = 4 * indent});
    for (size_t i = 0; i < 4 * indent; ++i)
        out.write(' ');
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
    return formatting.layout(out.stringView(), line_length);
}

}
