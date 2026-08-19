#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <unordered_map>


namespace DB::PrometheusQueryToSQL
{

/// Converts a single PromQL label matcher into a ClickHouse SQL predicate
/// against the columns of the "tags" target table.
///
/// For example with `column_name_by_tag_name = {"instance" -> "instance"}`:
///
///   PromQL matcher    Resulting AST
///   --------------    -------------
///   __name__="up"     metric_name == 'up'
///   job="api"         tags['job'] == 'api'
///   instance="a"      instance == 'a'
///   region!~"eu-.*"   not(match(tags['region'], '^eu-.*$'))
///
ASTPtr matcherToAST(
    const PrometheusQueryTree::Matcher & matcher,
    const std::unordered_map<String, String> & column_name_by_tag_name);

}
