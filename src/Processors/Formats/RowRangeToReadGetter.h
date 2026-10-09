#pragma once

#include <functional>
#include <optional>
#include <utility>

namespace DB
{

using RowRangeToReadGetter = std::function<std::optional<std::pair<size_t, size_t>>()>;

}
