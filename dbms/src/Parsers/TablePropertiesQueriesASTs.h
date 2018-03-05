#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{

struct ASTExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsQuery";
    static constexpr auto Query = "EXISTS TABLE";
};

struct ASTShowCreateQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateQuery";
    static constexpr auto Query = "SHOW CREATE TABLE";
};

struct ASTDescribeQueryExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "DescribeQuery";
    static constexpr auto Query = "DESCRIBE TABLE";
};

using ASTExistsQuery = ASTQueryWithTableAndOutputImpl<ASTExistsQueryIDAndQueryNames>;
using ASTShowCreateQuery = ASTQueryWithTableAndOutputImpl<ASTShowCreateQueryIDAndQueryNames>;

class ASTDescribeQuery : public ASTQueryWithOutput
{
public:
    ASTPtr table_expression;

    String getID() const override { return "DescribeQuery"; };

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
