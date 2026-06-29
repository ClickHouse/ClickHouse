#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <vector>


namespace DB::PrometheusQueryToSQL
{

enum class SQLSubqueryType
{
    TABLE,  /// Named subquery: "WITH name AS (SELECT ...)"
    SCALAR, /// Scalar subquery: "WITH (SELECT ...) AS name"
};

/// Subqueries are used to calculate steps of prometheus query evaluation.
struct SQLSubquery
{
    SQLSubquery(size_t index_, ASTPtr ast_, SQLSubqueryType subquery_type_);
    String name;
    ASTPtr ast;
    SQLSubqueryType subquery_type;
};

using SQLSubqueries = std::vector<SQLSubquery>;

}
