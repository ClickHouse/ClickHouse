#pragma once

#include <absl/status/statusor.h>

#include <utility>

namespace scann_cmake
{

/// Keep ScaNN's `StatusOr` unwrapping in a system header because Clang 22's
/// `abseil-unchecked-statusor-access` dataflow analysis can fail internally.
template <typename T, typename ExceptionFactory>
T unwrapScannStatusOr(absl::StatusOr<T> result, ExceptionFactory && exception_factory)
{
    if (!result.ok())
        throw std::forward<ExceptionFactory>(exception_factory)(result.status());

    return std::move(result).value();
}

}
