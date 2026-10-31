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
        auto res = make_intrusive<ASTCheckDatabaseQuery>(*this);
        res->children.clear();
        /// The parser adds the database child first and `ParserQueryWithOutput` appends the output
        /// options last; reproduce that order so the clone has the same tree hash.
        cloneDatabaseOptions(*res);
        cloneOutputOptions(*res);
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
        ostr << indent_str << "CHECK DATABASE ";

        if (database)
        {
            database->format(ostr, settings, state, frame);
        }
    }
};

}
