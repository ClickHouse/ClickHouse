#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <Common/FieldVisitors.h>
#include <Common/quoteString.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>

#include <vector>


namespace DB
{

/** name BY expr TYPE typename(args) GRANULARITY int in create query
  */
class ASTIndexDeclaration : public IAST
{
public:
    String name;
    IAST * expr;
    ASTFunction * type;
    UInt64 granularity;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTIndexDeclaration>();

        res->name = name;
        res->granularity = granularity;

        if (expr)
            res->set(res->expr, expr->clone());
        if (type)
            res->set(res->type, type->clone());
        return res;
    }

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;
        std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

        s.ostr << s.nl_or_ws << indent_str;
        s.ostr << backQuoteIfNeed(name);
        s.ostr << " ";
        expr->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
        type->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << " GRANULARITY " << (s.hilite ? hilite_none : "");
        s.ostr << granularity;
    }
};

}
