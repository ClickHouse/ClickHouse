#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTNamedChildrenHelper.h>
#include <Common/quoteString.h>


namespace DB
{


/** USE query
  */
class DECLARE_SELF_AND_CHILDREN(ASTUseQuery, DATABASE) : public ASTWithNamedChildren<IAST, ASTUseQuery, ASTUseQueryChildren>
{
public:
    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "UseQuery" + (delim + getChild(ASTUseQueryChildren::DATABASE)->getID(delim)); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTUseQuery>(*this);

//        getChild(ASTUseQueryChildren::DATABASE)->clone();
//        res->named.clone(named);
        return std::make_shared<ASTUseQuery>(*this);
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "USE " << (settings.hilite ? hilite_none : "");
        getChild(ASTUseQueryChildren::DATABASE)->formatImpl(settings, state, frame);
        return;
    }
};

}
