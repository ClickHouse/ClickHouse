#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Core/UUID.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    String database;
    String table;
    UUID uuid = UUIDHelpers::Nil;
    bool temporary{false};

protected:
    void formatHelper(const FormatSettings & settings, const char * name) const;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + database) + delim + table; }

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
        formatHelper(settings, temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query);
    }
};

}
