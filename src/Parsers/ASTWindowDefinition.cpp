#include <Parsers/ASTWindowDefinition.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWindowDefinition::clone() const
{
    auto result = std::make_shared<ASTWindowDefinition>();

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
    if (partition_by)
    {
        settings.ostr << "PARTITION BY ";
        partition_by->formatImpl(settings, state, format_frame);
    }

    if (partition_by && order_by)
    {
        settings.ostr << " ";
    }

    if (order_by)
    {
        settings.ostr << "ORDER BY ";
        order_by->formatImpl(settings, state, format_frame);
    }

    if ((partition_by || order_by) && !frame.is_default)
    {
        settings.ostr << " ";
    }

    if (!frame.is_default)
    {
        const auto * name = frame.type == WindowFrame::FrameType::Rows
            ? "ROWS" : frame.type == WindowFrame::FrameType::Groups
                ? "GROUPS" : "RANGE";

        settings.ostr << name << " UNBOUNDED PRECEDING";
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
