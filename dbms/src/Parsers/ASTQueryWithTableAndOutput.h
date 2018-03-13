#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
    */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    String database;
    String table;
    bool temporary{false};

protected:
    void formatHelper(const FormatSettings & settings, const char * name) const
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
    }
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID() const override { return AstIDAndQueryNames::ID + ("_" + database) + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        formatHelper(settings, AstIDAndQueryNames::Query);
    }
};

}
