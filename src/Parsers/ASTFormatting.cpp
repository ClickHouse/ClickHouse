#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFormatting.h>
#include <Common/UTF8Helpers.h>

#include <algorithm>
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

bool fitsWidth(std::string_view query, size_t width)
{
    for (size_t newline = 0; newline != std::string_view::npos; query.remove_prefix(newline + 1))
    {
        newline = query.find('\n');
        if (displayWidth(query.substr(0, newline)) > width)
            return false;
    }
    return true;
}

/// Width of a fragment of text written without any line breaks except the ones it already contains.
struct FlatWidth
{
    size_t first_line = 0;
    size_t last_line = 0;
    size_t max_after_first_line = 0;
    bool multiline = false;

    void append(const FlatWidth & other)
    {
        if (!multiline)
        {
            first_line += other.first_line;
            multiline = other.multiline;
            last_line = other.last_line;
            max_after_first_line = other.max_after_first_line;
            return;
        }
        size_t joined = last_line + other.first_line;
        max_after_first_line = std::max({max_after_first_line, joined, other.max_after_first_line});
        last_line = other.multiline ? other.last_line : joined;
    }

    void append(std::string_view text)
    {
        FlatWidth width;
        size_t newline = text.find('\n');
        width.first_line = displayWidth(text.substr(0, newline));
        width.multiline = newline != std::string_view::npos;
        while (newline != std::string_view::npos)
        {
            text.remove_prefix(newline + 1);
            newline = text.find('\n');
            width.last_line = displayWidth(text.substr(0, newline));
            width.max_after_first_line = std::max(width.max_after_first_line, width.last_line);
        }
        append(width);
    }

    [[nodiscard]] bool fits(size_t column, size_t limit) const { return column <= limit && first_line <= limit - column; }
};

struct FormattingGroup
{
    size_t parent;
    /// Indentation of the frame where the group was written, and of the source line where it starts.
    size_t source_indent;
    size_t source_line_indent = 0;
    ASTFormattingGroup::Mode mode;
    FlatWidth width{};
    /// The text after the group up to the next break of the enclosing group.
    FlatWidth suffix{};
    /// Indentation of the output line where the group starts.
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
    ASTFormattingBreak break_kind = ASTFormattingBreak::Consistent;
    size_t offset;
    size_t group;
    size_t indent = 0;
    size_t nest = 0;
    /// Length of the flat text of a break or of the written indentation.
    size_t length = 0;
    /// The text that has to fit on the current line for the break not to be taken.
    FlatWidth following{};
};

}

class ASTFormatting
{
public:
    explicit ASTFormatting(WriteBuffer & out_) : out(out_) {}

    WriteBuffer & out;
    std::vector<FormattingGroup> groups;
    std::vector<FormattingEvent> events;
    size_t current_group = no_group;
    size_t open_groups = 0;

    void push(FormattingEvent event)
    {
        /// Keep space for closing all open groups, so closing a group (in a destructor) never allocates.
        size_t required = events.size() + open_groups + 1;
        if (events.capacity() < required)
            events.reserve(std::max(required, 2 * events.capacity()));
        events.push_back(event);
    }

    String layout(std::string_view query, size_t width)
    {
        chassert(current_group == no_group);

        /// Measure the groups and remember the indentation of the source line where each of them starts.
        size_t group = no_group;
        size_t cursor = 0;
        size_t source_line_indent = 0;
        bool source_line_has_content = false;
        for (const auto & event : events)
        {
            auto fragment = query.substr(cursor, event.offset - cursor);
            if (group != no_group)
                groups[group].width.append(fragment);
            if (size_t newline = fragment.rfind('\n'); newline != std::string_view::npos)
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

        /// Measure the text after each break: up to the next break of the same group with nested groups
        /// taken whole, and for optional breaks up to the next possible break at any depth.
        FlatWidth suffix;
        FlatWidth up_to_next_break;
        cursor = query.size();
        for (auto it = events.rbegin(); it != events.rend(); ++it)
        {
            FlatWidth fragment;
            fragment.append(query.substr(it->offset, cursor - it->offset));
            cursor = it->offset;
            FlatWidth preceding = fragment;
            preceding.append(suffix);
            suffix = preceding;
            fragment.append(up_to_next_break);
            up_to_next_break = fragment;
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
                it->following = it->break_kind == ASTFormattingBreak::Optional ? up_to_next_break : suffix;
                suffix = {};
                up_to_next_break = {};
            }
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
                column = line_indent = 0;
                line_has_content = line_indent_assigned = false;
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
        /// A moved group carries the indentation written inside it to its new position.
        auto shifted = [](const FormattingGroup & owner, size_t indent)
        {
            if (owner.indent == owner.source_line_indent)
                return indent;
            return owner.indent + (indent >= owner.source_indent ? indent - owner.source_indent : 0);
        };

        cursor = 0;
        for (const auto & event : events)
        {
            append(query.substr(cursor, event.offset - cursor));
            cursor = event.offset + event.length;
            if (event.kind == FormattingEvent::Open)
            {
                auto & opened = groups[event.group];
                opened.indent = line_indent;
                if (opened.mode == ASTFormattingGroup::Mode::List && !line_has_content)
                    opened.indent -= std::min<size_t>(opened.indent, 4);
                FlatWidth flat = opened.width;
                flat.append(opened.suffix);
                opened.broken = !opened.width.multiline && !flat.fits(column, width);
                /// Arguments break together if any line of the call or the text attached after it does not fit.
                if (opened.width.multiline && opened.mode == ASTFormattingGroup::Mode::ConsistentLines)
                    opened.broken = !opened.width.fits(column, width) || shifted(opened, opened.width.max_after_first_line) > width
                        || !opened.suffix.fits(shifted(opened, opened.width.last_line), width);
                /// When the nested indentation alone takes the whole width, breaking does not help
                /// and only makes the output grow quadratically with the nesting depth.
                if (opened.indent + 4 >= width)
                    opened.broken = false;
            }
            else if (event.kind == FormattingEvent::Indent)
            {
                size_t indent = event.indent;
                if (event.group != no_group && !line_has_content)
                    indent = shifted(groups[event.group], indent);
                append(String(indent, ' '));
                line_indent_assigned = !line_has_content;
            }
            else if (event.kind == FormattingEvent::Break)
            {
                bool independent = event.break_kind != ASTFormattingBreak::Consistent || event.group == no_group
                    || (groups[event.group].width.multiline && !groups[event.group].broken);
                bool broken = independent ? !event.following.fits(column, width) : groups[event.group].broken;

                size_t indent = (event.group == no_group ? event.indent : groups[event.group].indent) + 4 * event.nest;
                /// A leading break shares the indentation already written by the enclosing group.
                bool shares_line = !line_has_content && line_indent_assigned && !event.length && event.nest;
                if (shares_line)
                    indent = line_indent;

                if (!broken || indent >= width)
                {
                    /// A separator is not needed at the beginning of a line.
                    if (line_has_content || !line_indent_assigned)
                        append(query.substr(event.offset, event.length));
                    continue;
                }

                if (shares_line && event.group != no_group)
                {
                    auto & owner = groups[event.group];
                    owner.indent = std::min(owner.indent, indent - std::min(indent, 4 * event.nest));
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
        }
        append(query.substr(cursor));
        return result;
    }
};

void ASTFormattingGroup::start(WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, Mode mode)
{
    auto & collector = *state.formatting;
    if (&collector.out != &out)
        return;
    index = collector.groups.size();
    collector.groups.push_back({.parent = collector.current_group, .source_indent = size_t{4} * frame.indent, .mode = mode});
    ++collector.open_groups;
    collector.push({.kind = FormattingEvent::Open, .offset = out.count(), .group = index});
    collector.current_group = index;
    formatting = &collector;
}

void ASTFormattingGroup::setMode(Mode mode)
{
    if (formatting)
        formatting->groups[index].mode = mode;
}

void ASTFormattingGroup::close() noexcept
{
    chassert(formatting->current_group == index);
    formatting->events.push_back({.kind = FormattingEvent::Close, .offset = formatting->out.count(), .group = index});
    formatting->current_group = formatting->groups[index].parent;
    --formatting->open_groups;
    formatting = nullptr;
}

void ASTFormattingDetail::writeBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, size_t nest, char flat, ASTFormattingBreak kind)
{
    auto & collector = *state.formatting;
    if (&collector.out == &out)
        collector.push({.kind = FormattingEvent::Break, .break_kind = kind, .offset = out.count(), .group = collector.current_group,
                        .indent = 4 * indent, .nest = nest, .length = flat ? size_t{1} : size_t{0}});
    if (flat)
        out.write(flat);
}

void ASTFormattingDetail::writeIndent(WriteBuffer & out, IASTFormatState & state, size_t indent)
{
    auto & collector = *state.formatting;
    if (&collector.out == &out)
        collector.push({.kind = FormattingEvent::Indent, .offset = out.count(), .group = collector.current_group,
                        .indent = 4 * indent, .length = 4 * indent});
    for (size_t i = 0; i < 4 * indent; ++i)
        out.write(' ');
}

String formatASTWithWrapping(const IAST & ast, const IAST::FormatSettings & settings, size_t wrap_width)
{
    {
        WriteBufferFromOwnString out;
        ast.format(out, settings);
        if (!wrap_width || settings.one_line || fitsWidth(out.stringView(), wrap_width))
            return out.str();
    }

    WriteBufferFromOwnString out;
    ASTFormatting formatting(out);
    IAST::FormatState state;
    state.formatting = &formatting;
    ast.format(out, settings, state, {});
    return formatting.layout(out.stringView(), wrap_width);
}

}
