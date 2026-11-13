#pragma once

#include <Core/Types.h>

#include <optional>


namespace DB
{

/// Returns the terms of a regex like `a|b` or `(a|b)|(?:c)` if every term is a plain literal.
/// A group is allowed only as a whole term, so `(a|b)c` is not a list.
std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex);

/// Anchors a PromQL regex so that it matches the whole value, like `^(?:regex)$` in Prometheus.
String toFullMatchRegex(std::string_view regex);

}
