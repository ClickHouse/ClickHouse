#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{


/** USE query
  */
class ASTUseQuery : public IAST
{
public:
    IAST * database;

    String getDatabase() const
    {
        String name;
        tryGetIdentifierNameInto(database, name);
        return name;
    }

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "UseQuery" + (delim + getDatabase()); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTUseQuery>(*this);
        res->children.clear();
        if (database)
            res->set(res->database, database->clone());
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Use; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "USE " << (settings.hilite ? hilite_none : "");
        database->formatImpl(settings, state, frame);
    }
};

}
