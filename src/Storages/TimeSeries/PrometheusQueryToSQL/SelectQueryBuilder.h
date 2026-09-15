#pragma once

#include <Core/Joins.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <base/types.h>


namespace DB::PrometheusQueryToSQL
{

/// Builds a SELECT query reading or calculating something as a part of evaluation of a prometheus query.
class SelectQueryBuilder
{
public:
    ASTs select_list;

    /// The source of the query: a table by its name or by its id, or a table function (at most one of them is set).
    String from_table;
    StorageID from_table_id = StorageID::createEmpty();
    ASTPtr from_table_function;

    ASTPtr where;
    ASTs group_by;
    ASTPtr having;
    ASTs order_by;
    std::optional<size_t> limit;
    int order_direction = 0; /// 1 for ASC, -1 for DESC
    String join_table;
    JoinKind join_kind = JoinKind::Inner;
    JoinStrictness join_strictness = JoinStrictness::Unspecified;
    ASTPtr join_on;
    String union_table;
    SQLSubqueries with;

    ASTPtr getSelectQuery();
};

}
