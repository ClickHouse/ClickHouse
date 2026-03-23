#include <Parsers/ASTWindowDefinition.h>

#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTWindowDefinition::clone() const
{
    auto result = make_intrusive<ASTWindowDefinition>();

    result->parent_window_name = parent_window_name;

    if (partition_by)
    {
        result->partition_by = partition_by->clone();
        result->children.push_back(result->partition_by);
    }

    if (order_by)
    {
        result->order_by = order_by->clone();
        result->children.push_back(result->order_by);
    }

    result->frame_is_default = frame_is_default;
    result->frame_type = frame_type;
    result->frame_begin_type = frame_begin_type;
    result->frame_begin_preceding = frame_begin_preceding;
    result->frame_end_type = frame_end_type;
    result->frame_end_preceding = frame_end_preceding;

    if (frame_begin_offset)
    {
        result->frame_begin_offset = frame_begin_offset->clone();
        result->children.push_back(result->frame_begin_offset);
    }

    if (frame_end_offset)
    {
        result->frame_end_offset = frame_end_offset->clone();
        result->children.push_back(result->frame_end_offset);
    }

    return result;
}

String ASTWindowDefinition::getID(char) const
{
    return "WindowDefinition";
}

void ASTWindowDefinition::formatImpl(WriteBuffer & ostr, const FormatSettings & settings,
                                     FormatState & state, FormatStateStacked format_frame) const
{
    format_frame.expression_list_prepend_whitespace = false;
    bool need_space = false;

    if (!parent_window_name.empty())
    {
        ostr << backQuoteIfNeed(parent_window_name);

        need_space = true;
    }

    if (partition_by)
    {
        if (need_space)
        {
            ostr << " ";
        }

        ostr << "PARTITION BY ";
        partition_by->format(ostr, settings, state, format_frame);

        need_space = true;
    }

    if (order_by)
    {
        if (need_space)
        {
            ostr << " ";
        }

        ostr << "ORDER BY ";
        order_by->format(ostr, settings, state, format_frame);

        need_space = true;
    }

    if (!frame_is_default)
    {
        if (need_space)
            ostr << " ";

        format_frame.need_parens = true;

        ostr << frame_type << " BETWEEN ";
        if (frame_begin_type == WindowFrame::BoundaryType::Current)
        {
            ostr << "CURRENT ROW";
        }
        else if (frame_begin_type == WindowFrame::BoundaryType::Unbounded)
        {
            ostr << "UNBOUNDED PRECEDING";
        }
        else
        {
            frame_begin_offset->format(ostr, settings, state, format_frame);
            ostr << " "
                << (!frame_begin_preceding ? "FOLLOWING" : "PRECEDING");
        }
        ostr << " AND ";
        if (frame_end_type == WindowFrame::BoundaryType::Current)
        {
            ostr << "CURRENT ROW";
        }
        else if (frame_end_type == WindowFrame::BoundaryType::Unbounded)
        {
            ostr << "UNBOUNDED FOLLOWING";
        }
        else
        {
            frame_end_offset->format(ostr, settings, state, format_frame);
            ostr << " "
                << (!frame_end_preceding ? "FOLLOWING" : "PRECEDING");
        }
    }
}

std::string ASTWindowDefinition::getDefaultWindowName() const
{
    WriteBufferFromOwnString ostr;
    FormatSettings settings{true /* one_line */};
    FormatState state;
    FormatStateStacked format_frame;
    formatImpl(ostr, settings, state, format_frame);
    return ostr.str();
}

ASTPtr ASTWindowListElement::clone() const
{
    auto result = make_intrusive<ASTWindowListElement>();

    result->name = name;
    result->definition = definition->clone();
    result->children.push_back(result->definition);

    return result;
}

String ASTWindowListElement::getID(char) const
{
    return "WindowListElement";
}

void ASTWindowListElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings,
                                      FormatState & state, FormatStateStacked frame) const
{
    ostr << backQuoteIfNeed(name);
    ostr << " AS (";
    definition->format(ostr, settings, state, frame);
    ostr << ")";
}

void ASTWindowDefinition::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "WindowDefinition");
    if (!parent_window_name.empty())
        w.writeString("parent_window_name", parent_window_name);
    w.writeChild("partition_by", partition_by);
    w.writeChild("order_by", order_by);
    if (!frame_is_default)
    {
        w.writeBool("frame_is_default", false);
        switch (frame_type)
        {
            case WindowFrame::FrameType::ROWS:
                w.writeString("frame_type", "ROWS");
                break;
            case WindowFrame::FrameType::GROUPS:
                w.writeString("frame_type", "GROUPS");
                break;
            case WindowFrame::FrameType::RANGE:
                w.writeString("frame_type", "RANGE");
                break;
        }
        switch (frame_begin_type)
        {
            case WindowFrame::BoundaryType::Unbounded:
                w.writeString("frame_begin_type", "Unbounded");
                break;
            case WindowFrame::BoundaryType::Current:
                w.writeString("frame_begin_type", "Current");
                break;
            case WindowFrame::BoundaryType::Offset:
                w.writeString("frame_begin_type", "Offset");
                break;
        }
        w.writeChild("frame_begin_offset", frame_begin_offset);
        w.writeBool("frame_begin_preceding", frame_begin_preceding);
        switch (frame_end_type)
        {
            case WindowFrame::BoundaryType::Unbounded:
                w.writeString("frame_end_type", "Unbounded");
                break;
            case WindowFrame::BoundaryType::Current:
                w.writeString("frame_end_type", "Current");
                break;
            case WindowFrame::BoundaryType::Offset:
                w.writeString("frame_end_type", "Offset");
                break;
        }
        w.writeChild("frame_end_offset", frame_end_offset);
        w.writeBool("frame_end_preceding", frame_end_preceding);
    }
}

void ASTWindowListElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "WindowListElement");
    w.writeString("name", name);
    w.writeChild("definition", definition);
}

static WindowFrame::FrameType parseFrameType(const String & s)
{
    if (s == "ROWS") return WindowFrame::FrameType::ROWS;
    if (s == "GROUPS") return WindowFrame::FrameType::GROUPS;
    if (s == "RANGE") return WindowFrame::FrameType::RANGE;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown WindowFrame FrameType: '{}'", s);
}

static WindowFrame::BoundaryType parseBoundaryType(const String & s)
{
    if (s == "Unbounded") return WindowFrame::BoundaryType::Unbounded;
    if (s == "Current") return WindowFrame::BoundaryType::Current;
    if (s == "Offset") return WindowFrame::BoundaryType::Offset;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown WindowFrame BoundaryType: '{}'", s);
}

void ASTWindowDefinition::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    parent_window_name = r.getString("parent_window_name");

    auto child = r.readChild("partition_by");
    if (child)
    {
        partition_by = child;
        children.push_back(partition_by);
    }

    child = r.readChild("order_by");
    if (child)
    {
        order_by = child;
        children.push_back(order_by);
    }

    if (r.has("frame_is_default"))
    {
        frame_is_default = r.getBool("frame_is_default");

        String frame_type_str = r.getString("frame_type");
        if (!frame_type_str.empty())
            frame_type = parseFrameType(frame_type_str);

        String frame_begin_type_str = r.getString("frame_begin_type");
        if (!frame_begin_type_str.empty())
            frame_begin_type = parseBoundaryType(frame_begin_type_str);

        child = r.readChild("frame_begin_offset");
        if (child)
        {
            frame_begin_offset = child;
            children.push_back(frame_begin_offset);
        }

        frame_begin_preceding = r.getBool("frame_begin_preceding");

        String frame_end_type_str = r.getString("frame_end_type");
        if (!frame_end_type_str.empty())
            frame_end_type = parseBoundaryType(frame_end_type_str);

        child = r.readChild("frame_end_offset");
        if (child)
        {
            frame_end_offset = child;
            children.push_back(frame_end_offset);
        }

        frame_end_preceding = r.getBool("frame_end_preceding");
    }
}

void ASTWindowListElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    name = r.getString("name");

    auto child = r.readChild("definition");
    if (child)
    {
        definition = child;
        children.push_back(definition);
    }
}

}
