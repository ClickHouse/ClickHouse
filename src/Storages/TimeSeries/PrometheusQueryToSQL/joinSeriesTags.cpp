#include <Storages/TimeSeries/PrometheusQueryToSQL/joinSeriesTags.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <base/defines.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr joinSeriesTags(ASTPtr && per_id_query, const String & series, const Strings & columns, ConverterContext & context)
{
    chassert(!series.empty());

    context.subqueries.emplace_back(context.subqueries.size(), std::move(per_id_query), SQLSubqueryType::TABLE);
    String per_id = context.subqueries.back().name;

    SelectQueryBuilder builder;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{series, ColumnNames::Tags}));
    builder.select_list.back()->setAlias(ColumnNames::Tags);

    for (const auto & column : columns)
    {
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{per_id, column}));
        builder.select_list.back()->setAlias(column);
    }

    builder.from_table = series;
    builder.join_table = per_id;
    builder.join_kind = JoinKind::Inner;
    builder.join_strictness = JoinStrictness::Any;
    builder.join_on = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(Strings{per_id, ColumnNames::ID}),
        make_intrusive<ASTIdentifier>(Strings{series, ColumnNames::ID}));

    return builder.getSelectQuery();
}

}
