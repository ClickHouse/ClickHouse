#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr table;
    bool temporary{false};

    String tableName() const { return getIdentifierName(table); }
    String databaseName(const String & default_name = "") const { return database ? getIdentifierName(database) : default_name ; }

    bool onlyDatabase() const { return !table && database; }

protected:
    String getTableAndDatabaseID(char delim) const;

    void formatHelper(const FormatSettings & settings, const char * name) const;

    void formatTableAndDatabase(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + getTableAndDatabaseID(delim)); }

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
