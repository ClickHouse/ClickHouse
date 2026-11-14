#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <vector>


namespace DB::PrometheusQueryToSQL
{

/// A warning or an info returned with the result of a prometheus query, like an annotation in Prometheus.
struct Annotation
{
    bool is_info = false;

    /// The message without the position, e.g. "PromQL warning: quantile value should be between 0 and 1, got 2".
    String message;

    /// The byte offset in the query of the expression the annotation is about.
    size_t position = 0;

    /// If set, the annotation depends on the data: it's reported once per row of this query, which returns a column `name`.
    ASTPtr probe;

    /// Whether the quoted value of `name` is appended to `message`.
    bool append_name = false;
};

using Annotations = std::vector<Annotation>;

}
