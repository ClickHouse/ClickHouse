#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <unordered_map>


namespace DB
{

/// Makes an AST for the expression referencing the value of a tag in the 'tags' target table of a
/// TimeSeries storage: the 'metric_name' column for '__name__', a dedicated column for a tag configured
/// via 'tags_to_columns' with a fallback to the 'tags' map, and an element of the 'tags' map otherwise.
/// A dedicated column may be Nullable, in which case NULL (a missing tag) is normalized to '',
/// because Prometheus treats a missing label as equal to the empty label value. A supported external
/// tags table can also carry a 'tags_to_columns' tag in only the residual 'tags' map (e.g. an older
/// or external layout), so when the dedicated column is empty the expression falls back to the map,
/// using the same precedence as `timeSeriesStoreTags` on the write path. This helper does not validate
/// conflicting non-empty carriers; callers that emit complete series metadata validate selected rows
/// separately.
ASTPtr timeSeriesTagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name);

/// Makes an AST for the condition filtering rows of the 'tags' target table of a TimeSeries storage
/// by a single label matcher of a Prometheus instant selector, e.g. {job="prometheus"}.
/// A regexp matcher is anchored on both sides (as in Prometheus) before being passed to `match`.
/// Equality and inequality matchers on a tag configured via `tags_to_columns` expose a guarded
/// predicate over the dedicated column so MergeTree can use it for index pruning while retaining
/// the residual Map fallback for legacy rows.
ASTPtr timeSeriesMatcherToAST(
    const PrometheusQueryTree::Matcher & matcher,
    const std::unordered_map<String, String> & column_name_by_tag_name);

}
