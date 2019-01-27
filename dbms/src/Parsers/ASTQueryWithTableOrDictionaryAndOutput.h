#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query specifying the name of table or dictionary and, possibly, the database and the FORMAT section.
    */
class ASTQueryWithTableOrDictionaryAndOutput : public ASTQueryWithOutput
{
public:
    String database;
    String table;
    String dictionary;
    bool temporary{false};

protected:
    void formatHelper(const FormatSettings & settings, const char * name) const
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "");

        if (!table.empty())
            settings.ostr << backQuoteIfNeed(table);

        if (!dictionary.empty())
            settings.ostr << backQuoteIfNeed(dictionary);
    }
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableOrDictionaryAndOutput
{
public:
    String getID(char delim) const override
    {
        String id = AstIDAndQueryNames::ID + (delim + database);
        if (!table.empty())
            id += delim + table;

        if (!dictionary.empty())
            id += delim + dictionary;

        return id;
    }

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
