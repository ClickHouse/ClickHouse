#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTIdentifier.h>
#include <Core/UUID.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr table;
    
    UUID uuid = UUIDHelpers::Nil;
    bool temporary{false};


    const String & getDatabase() const
    {
        return getIdentifierNameRef(database);
    }

    const String & getTable() const
    {
        return getIdentifierNameRef(table);
    }

    void setDatabase(const String & name)
    {
        database = std::make_shared<ASTIdentifier>(name);
    }

    void setTable(const String & name)
    {
        table = std::make_shared<ASTIdentifier>(name);
    }

    void cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
    {
        if (database)
        {
            cloned.database = database->clone();
            cloned.children.push_back(cloned.database);
        }
        if (table)
        {
            cloned.table = table->clone();
            cloned.children.push_back(cloned.table);
        }
    }

protected:
    void formatHelper(const FormatSettings & settings, const char * name) const;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneTableOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        formatHelper(settings, temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query);
    }
};

}
