#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// A Prometheus stale marker is a `NaN` with the exact payload 0x7ff0000000000002, and it is recognized
/// by that bit pattern only at finalization. Functions either quiet the payload into an ordinary `NaN`
/// (for example `floor` or `sqrt`), replace it with a number (for example `sign`), or fail on it
/// (for example the `DateTime` helpers), which would make a stale sample survive instead of being dropped
/// or break the whole query. Prometheus drops stale samples before it evaluates a function, so such samples
/// are passed through unchanged here and dropped later.

/// Builds an expression which is true when `value` carries the Prometheus stale marker.
ASTPtr isStaleMarker(const ASTPtr & value);

/// Returns `value` unchanged when it carries the Prometheus stale marker, and `transformed_value` otherwise.
ASTPtr keepStaleMarker(const ASTPtr & value, ASTPtr transformed_value);

/// Substitutes `replacement` for `value` when `value` carries the Prometheus stale marker.
/// Useful to keep a transformation which cannot handle a `NaN` input from throwing;
/// the result of such a transformation must be discarded with `keepStaleMarker`.
ASTPtr replaceStaleMarker(const ASTPtr & value, ASTPtr replacement);

/// Returns `NULL` when `value` carries the Prometheus stale marker, and `value` otherwise.
/// Useful where a stale sample must be treated as an absent sample rather than passed through,
/// for example before aggregations such as `count` or `any` which skip `NULL`s.
ASTPtr nullifyStaleMarker(const ASTPtr & value);

}
