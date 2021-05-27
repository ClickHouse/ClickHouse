#include <Parsers/ASTWindowDefinition.h>

#include <Common/quoteString.h>
#include <Common/FieldVisitors.h>
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

    result->frame = frame;

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

    if (!frame.is_default)
    {
        if (need_space)
        {
            settings.ostr << " ";
        }

        settings.ostr << WindowFrame::toString(frame.type) << " BETWEEN ";
        if (frame.begin_type == WindowFrame::BoundaryType::Current)
        {
            settings.ostr << "CURRENT ROW";
        }
        else if (frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        {
            settings.ostr << "UNBOUNDED PRECEDING";
        }
        else
        {
            settings.ostr << applyVisitor(FieldVisitorToString(),
                frame.begin_offset);
            settings.ostr << " "
                << (!frame.begin_preceding ? "FOLLOWING" : "PRECEDING");
        }
        settings.ostr << " AND ";
        if (frame.end_type == WindowFrame::BoundaryType::Current)
        {
            settings.ostr << "CURRENT ROW";
        }
        else if (frame.end_type == WindowFrame::BoundaryType::Unbounded)
        {
            settings.ostr << "UNBOUNDED FOLLOWING";
        }
        else
        {
            settings.ostr << applyVisitor(FieldVisitorToString(),
                frame.end_offset);
            settings.ostr << " "
                << (!frame.end_preceding ? "FOLLOWING" : "PRECEDING");
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
