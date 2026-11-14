#pragma once

#include <base/types.h>

#include <string_view>
#include <utility>
#include <vector>


namespace DB
{

/// Returns `labels.Hash()` of the default (stringlabels) build of Prometheus 3.5 for labels sorted by name:
/// XXH64 of the names and values, each prefixed with its length. Prometheus orders exemplars with equal timestamps and values by it.
UInt64 getPrometheusLabelsHash(const std::vector<std::pair<std::string_view, std::string_view>> & sorted_labels);

}
