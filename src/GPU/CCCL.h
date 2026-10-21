#pragma once

/// The CCCL headers that host code uses, for anything compiled by clang against ClickHouse's libc++.
///
/// libcu++ forward-declares `std::get` for `std::array`, `std::tuple` and `std::pair` without libc++'s
/// `abi_tag`, and clang then rejects libc++'s own declarations as adding the attribute in a
/// redeclaration. ClickHouse's libc++ is built with `_LIBCPP_REMOVE_TRANSITIVE_INCLUDES`, so nothing
/// brings those headers in before libcu++ does. Including them first makes libcu++'s declarations the
/// redeclarations instead.

#include <array>
#include <tuple>
#include <utility>

#include <cuda/memory_pool>
#include <cuda/stream_ref>
