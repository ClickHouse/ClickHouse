#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** USE query
  */
class ASTUseQuery : public IAST
{
public:
    String database;

    ASTUseQuery() = default;
    ASTUseQuery(const StringRange range_) : IAST(range_) {}

    /** Get the text that identifies this element. */
    String getID() const override { return "UseQuery_" + database; };

    ASTPtr clone() const override { return std::make_shared<ASTUseQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "USE " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);
        return;
    }
};

}
