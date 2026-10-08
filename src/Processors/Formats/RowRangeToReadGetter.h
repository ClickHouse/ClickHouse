#pragma once

#include <functional>
#include <optional>
#include <utility>

namespace DB
{

/// Returns `IInputFormat::getRowRangeToRead` of the format a transform is attached to.
using RowRangeToReadGetter = std::function<std::optional<std::pair<size_t, size_t>>()>;

}
