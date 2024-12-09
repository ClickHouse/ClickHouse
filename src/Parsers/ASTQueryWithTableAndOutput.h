#pragma once

#include <base/defines.h>
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
    ASTPtr database;
    ASTPtr table;

    UUID uuid = UUIDHelpers::Nil;
    bool temporary{false};

    String getDatabase() const;
    String getTable() const;

    // Once database or table are set they cannot be assigned with empty value
    void setDatabase(const String & name);
    void setTable(const String & name);

    void cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const;
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

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        ostr << (settings.hilite ? hilite_keyword : "")
            << (temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query)
            << " " << (settings.hilite ? hilite_none : "");

        if (database)
        {
            database->formatImpl(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table != nullptr, "Table is empty for the ASTQueryWithTableAndOutputImpl.");
        table->formatImpl(ostr, settings, state, frame);
    }
};

}
