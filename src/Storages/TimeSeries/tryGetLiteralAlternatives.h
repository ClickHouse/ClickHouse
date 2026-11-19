#pragma once

#include <Core/Types.h>

#include <optional>


namespace DB
{

/// Returns the terms of a regex like `a|b` or `(a|b)|(?:c)` if every term is a plain literal.
/// A group is allowed only as a whole term, so `(a|b)c` is not a list.
std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex);

/// Makes a regular expression of a PromQL matcher match only whole values, as Prometheus does.
/// Prometheus wraps the regex as `^(?:...)$`, but here the group is added only when the regex has an alternation
/// or an inline multiline flag at the top level: a leading group stops `extractFixedPrefixFromRegularExpression`,
/// so `{__name__=~"node_.*"}` or `{__name__=~"^(node_foo|node_bar)$"}` would lose the primary key on `metric_name`.
String anchorRegex(const String & regex);

}
