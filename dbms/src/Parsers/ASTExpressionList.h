#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** List of expressions, for example "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
public:
    ASTExpressionList() = default;
    ASTExpressionList(const StringRange range_) : IAST(range_) {}

    String getID() const override { return "ExpressionList"; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTExpressionList>(*this);
        res->children.clear();

        for (const auto & child : children)
            res->children.emplace_back(child->clone());

        return res;
    }

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
        {
            if (it != children.begin())
                settings.ostr << ", ";

            (*it)->formatImpl(settings, state, frame);
        }
    }

    /** Вывести список выражений в секциях запроса SELECT - по одному выражению на строку.
      */
    void formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
    {
        std::string indent_str = "\n" + std::string(4 * (frame.indent + 1), ' ');

        ++frame.indent;
        for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
        {
            if (it != children.begin())
                settings.ostr << ", ";

            if (children.size() > 1)
                settings.ostr << indent_str;

            (*it)->formatImpl(settings, state, frame);
        }
    }
};

}
