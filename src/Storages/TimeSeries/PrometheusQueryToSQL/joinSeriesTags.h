#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>


namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Adds the tags of time series to a query outputting one row per identifier: the columns `id` and `columns`.
///
/// SELECT <series>.tags AS tags, <per_id>.<column> AS <column>, ...
/// FROM <series> ANY INNER JOIN <per_id> ON <per_id>.id = <series>.id
///
/// `per_id` is a new subquery made of `per_id_query`, `series` is the subquery from `SQLQueryPiece::series`.
/// `series` can contain an identifier in multiple rows (one per unmerged part of the tags table) and identifiers without samples:
/// ANY INNER JOIN outputs one row per key of `per_id`, which is the right side to keep the hash table small.
ASTPtr joinSeriesTags(ASTPtr && per_id_query, const String & series, const Strings & columns, ConverterContext & context);

}
