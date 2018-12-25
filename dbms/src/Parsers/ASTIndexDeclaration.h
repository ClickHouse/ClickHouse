#pragma once

#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

#include <vector>


namespace DB
{


/** Index name(expr) TYPE typename(args) in create query
  */
class ASTIndexQuery : public IAST
{
public:
    struct Index
    {
        String name;
        ASTPtr expression_list;
        ASTFunction type;
    };

    using Indexes = std::vector<Index>;
    Indexes indexes;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override { return std::make_shared<ASTIndexQuery>(*this); }

    void formatImpl(const FormatSettings & s, FormatState &state, FormatStateStacked frame) const override
    {
        for (ASTIndexQuery::Indexes::const_iterator it = indexes.begin(); it != indexes.end(); ++it)
        {
            if (it != indexes.begin())
                s.ostr << s.nl_or_ws;

            s.ostr << (s.hilite ? hilite_keyword : "") << "INDEX" << (s.hilite ? hilite_none : "") << " " <<  it->name;
            s.ostr << "(";
            it->expression_list->formatImpl(s, state, frame);
            s.ostr << ") " << (s.hilite ? hilite_keyword : "") << "TYPE" << (s.hilite ? hilite_none : "");
            it->type.formatImpl(s, state, frame);
        }
    }
};

}
