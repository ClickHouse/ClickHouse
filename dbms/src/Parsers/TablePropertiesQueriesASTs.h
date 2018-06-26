#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{

struct ASTExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsQuery";
    static constexpr auto Query = "EXISTS TABLE";
};

struct ASTShowCreateTableQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateTableQuery";
    static constexpr auto Query = "SHOW CREATE TABLE";
};

struct ASTShowCreateDatabaseQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateDatabaseQuery";
    static constexpr auto Query = "SHOW CREATE DATABASE";
};

struct ASTDescribeQueryExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "DescribeQuery";
    static constexpr auto Query = "DESCRIBE TABLE";
};

using ASTExistsQuery = ASTQueryWithTableAndOutputImpl<ASTExistsQueryIDAndQueryNames>;
using ASTShowCreateTableQuery = ASTQueryWithTableAndOutputImpl<ASTShowCreateTableQueryIDAndQueryNames>;

class ASTShowCreateDatabaseQuery : public ASTQueryWithTableAndOutputImpl<ASTShowCreateDatabaseQueryIDAndQueryNames>
{
protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << ASTShowCreateDatabaseQueryIDAndQueryNames::Query
                      << " " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);
    }
};

class ASTDescribeQuery : public ASTQueryWithOutput
{
public:
    ASTPtr table_expression;

    String getID() const override { return "DescribeQuery"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDescribeQuery>(*this);
        res->children.clear();
        if (table_expression)
        {
            res->table_expression = table_expression->clone();
            res->children.push_back(res->table_expression);
        }
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << "DESCRIBE TABLE " << (settings.hilite ? hilite_none : "");
        table_expression->formatImpl(settings, state, frame);
    }

};

}
