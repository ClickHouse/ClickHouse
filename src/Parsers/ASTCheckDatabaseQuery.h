#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Common/quoteString.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

struct ASTCheckDatabaseQuery : public ASTQueryWithOutput
{
    ASTPtr database;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CheckQuery" + (delim + getDatabase()); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCheckDatabaseQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneDatabaseOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Check; }

    void cloneDatabaseOptions(ASTCheckDatabaseQuery & cloned) const
    {
        if (database)
        {
            cloned.database = database->clone();
            cloned.children.push_back(cloned.database);
        }
    }

    String getDatabase() const
    {
        String name;
        tryGetIdentifierNameInto(database, name);
        return name;
    }
protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "CHECK DATABASE " << getDatabase() << '\n';

        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }
    }
};

}
