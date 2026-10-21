#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLMakeSeries : public ParserKQLBase
{
protected:
    struct AggregationColumn
    {
        String alias;
        String aggregation_fun;
        String column;
        double default_value;
        AggregationColumn(String alias_, String aggregation_fun_, String column_, double default_value_)
            : alias(alias_), aggregation_fun(aggregation_fun_), column(column_), default_value(default_value_)
        {
        }
    };
    using AggregationColumns = std::vector<AggregationColumn>;

    struct FromToStepClause
    {
        String from_str;
        String to_str;
        String step_str;
        bool is_timespan = false;
        double step;
    };

    struct KQLMakeSeries
    {
        AggregationColumns aggregation_columns;
        FromToStepClause from_to_step;
        String axis_column;
        String group_expression;
        String subquery_columns;
        String sub_query;
        String main_query;
    };

    static bool makeSeries(KQLMakeSeries & kql_make_series, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks);
    static bool parseAggregationColumns(AggregationColumns & aggregation_columns, Pos & pos);
    static bool parseFromToStepClause(FromToStepClause & from_to_step, Pos & pos);

    const char * getName() const override { return "KQL make-series"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
