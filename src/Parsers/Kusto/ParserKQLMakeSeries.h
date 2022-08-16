
#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLMakeSeries : public ParserKQLBase
{
public:
    ASTPtr group_expression_list;
    ASTPtr tables;
    void setTableName(String table_name_) {table_name = table_name_;}

protected:
    struct AggregationColumn {
        String alias;
        String aggregation_fun;
        String column;
        double default_value;
        AggregationColumn(String alias_, String aggregation_fun_, String column_, double default_value_ )
        :alias(alias_), aggregation_fun(aggregation_fun_), column(column_), default_value(default_value_){}
    };
    using AggregationColumns = std::vector<AggregationColumn>;

    struct FromToStepClause {
        String from_str;
        String to_str;
        String step_str;
        bool is_timespan = false;
        double step;
    };

    struct KQLMakeSeries {
        AggregationColumns aggregation_columns;
        FromToStepClause from_to_step;
        String axis_column;
        String group_expression;
        String subquery_columns;
        String sub_query;
        String main_query;
    };

    void makeNumericSeries(KQLMakeSeries & kql_make_series, const uint32_t & max_depth);
    void makeTimeSeries(KQLMakeSeries & kql_make_series, const uint32_t & max_depth);
    bool parseAggregationColumns(AggregationColumns & aggregation_columns, Pos & pos);
    bool parseFromToStepClause(FromToStepClause & from_to_step, Pos & pos);
    const char * getName() const override { return "KQL project"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    String table_name;
};

}



