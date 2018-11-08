#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Name, type, default-specifier, default-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    ASTPtr type;
    String default_specifier;
    ASTPtr default_expression;

    String getID() const override { return "ColumnDeclaration_" + name; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTColumnDeclaration>(*this);
        res->children.clear();

        if (type)
        {
            res->type = type;
            res->children.push_back(res->type);
        }

        if (default_expression)
        {
            res->default_expression = default_expression->clone();
            res->children.push_back(res->default_expression);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

        settings.ostr << settings.nl_or_ws << indent_str << backQuoteIfNeed(name);
        if (type)
        {
            settings.ostr << ' ';
            type->formatImpl(settings, state, frame);
        }

        if (default_expression)
        {
            settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << default_specifier << (settings.hilite ? hilite_none : "") << ' ';
            default_expression->formatImpl(settings, state, frame);
        }
    }
};

}
