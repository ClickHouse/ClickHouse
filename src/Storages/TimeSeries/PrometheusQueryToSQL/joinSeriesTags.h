#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>


namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Adds the tags of time series to a query outputting one row per identifier: the columns `id` and `columns`.
///
/// SELECT <series>.tags AS tags, <per_id>.<column> AS <column>, ...
/// FROM <per_id> ANY INNER JOIN <series> ON <per_id>.id = <series>.id
///
/// `per_id` is a new subquery made of `per_id_query`, `series` is the subquery from `SQLQueryPiece::series`.
/// `series` can contain an identifier in multiple rows (one per unmerged part of the tags table), ANY joins one of them.
ASTPtr joinSeriesTags(ASTPtr && per_id_query, const String & series, const Strings & columns, ConverterContext & context);

}
