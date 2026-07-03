#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>

#include <fmt/format.h>


namespace DB::PrometheusQueryToSQL
{

SQLSubquery::SQLSubquery(size_t index_, ASTPtr ast_, SQLSubqueryType subquery_type_)
    : name(fmt::format("prometheus_query_step_{}", index_ + 1)), ast(std::move(ast_)), subquery_type(subquery_type_)
{
}

}
