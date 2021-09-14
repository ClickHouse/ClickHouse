#include <Parsers/ASTWindowDefinition.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTWindowDefinition::clone() const
{
    auto result = std::make_shared<ASTWindowDefinition>();

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

void ASTWindowDefinition::formatImpl(const FormatSettings & settings,
    FormatState & state, FormatStateStacked format_frame) const
{
    format_frame.expression_list_prepend_whitespace = false;
    bool need_space = false;

    if (!parent_window_name.empty())
    {
        settings.ostr << backQuoteIfNeed(parent_window_name);

        need_space = true;
    }

    if (partition_by)
    {
        if (need_space)
        {
            settings.ostr << " ";
        }

        settings.ostr << "PARTITION BY ";
        partition_by->formatImpl(settings, state, format_frame);

        need_space = true;
    }

    if (order_by)
    {
        if (need_space)
        {
            settings.ostr << " ";
        }

        settings.ostr << "ORDER BY ";
        order_by->formatImpl(settings, state, format_frame);

        need_space = true;
    }

    if (!frame_is_default)
    {
        if (need_space)
        {
            settings.ostr << " ";
        }

        settings.ostr << WindowFrame::toString(frame_type) << " BETWEEN ";
        if (frame_begin_type == WindowFrame::BoundaryType::Current)
        {
            settings.ostr << "CURRENT ROW";
        }
        else if (frame_begin_type == WindowFrame::BoundaryType::Unbounded)
        {
            settings.ostr << "UNBOUNDED PRECEDING";
        }
        else
        {
            frame_begin_offset->formatImpl(settings, state, format_frame);
            settings.ostr << " "
                << (!frame_begin_preceding ? "FOLLOWING" : "PRECEDING");
        }
        settings.ostr << " AND ";
        if (frame_end_type == WindowFrame::BoundaryType::Current)
        {
            settings.ostr << "CURRENT ROW";
        }
        else if (frame_end_type == WindowFrame::BoundaryType::Unbounded)
        {
            settings.ostr << "UNBOUNDED FOLLOWING";
        }
        else
        {
            frame_end_offset->formatImpl(settings, state, format_frame);
            settings.ostr << " "
                << (!frame_end_preceding ? "FOLLOWING" : "PRECEDING");
        }
    }
}

std::string ASTWindowDefinition::getDefaultWindowName() const
{
    WriteBufferFromOwnString ostr;
    FormatSettings settings{ostr, true /* one_line */};
    FormatState state;
    FormatStateStacked format_frame;
    formatImpl(settings, state, format_frame);
    return ostr.str();
}

ASTPtr ASTWindowListElement::clone() const
{
    auto result = std::make_shared<ASTWindowListElement>();

    result->name = name;
    result->definition = definition->clone();
    result->children.push_back(result->definition);

    return result;
}

String ASTWindowListElement::getID(char) const
{
    return "WindowListElement";
}

void ASTWindowListElement::formatImpl(const FormatSettings & settings,
    FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << backQuoteIfNeed(name);
    settings.ostr << " AS (";
    definition->formatImpl(settings, state, frame);
    settings.ostr << ")";
}

}
