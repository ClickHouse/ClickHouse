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
/// `series` can contain an identifier in multiple rows (one per unmerged part of the tags table), ANY INNER JOIN outputs
/// one row per key. `series` can also contain identifiers without samples in the range, INNER drops them: `per_id` is the right
/// side, so the hash table holds only the time series with samples.
ASTPtr joinSeriesTags(ASTPtr && per_id_query, const String & series, const Strings & columns, ConverterContext & context);

}
