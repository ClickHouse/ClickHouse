#pragma once

#include <Parsers/IAST.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{


/** USE query
  */
class ASTUseQuery : public IAST
{
public:
    String database;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "UseQuery" + (delim + database); }

    ASTPtr clone() const override { return std::make_shared<ASTUseQuery>(*this); }

    QueryKind getQueryKind() const override { return QueryKind::Use; }

protected:
    void formatImpl(FormattingBuffer out) const override
    {
        out.writeKeyword("USE ");
        out.ostr << backQuoteIfNeed(database);
    }
};

}
