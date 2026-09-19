#pragma once

#include "config.h"

#if USE_GPU

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

#include <fmt/format.h>

#include <cstddef>
#include <utility>

namespace DB::GPU
{

template <typename Call, typename... Args>
void call(Call && entry_point, fmt::format_string<Args...> what, Args &&... args)
{
    char error[1024] = {};

    if (entry_point(error, sizeof(error)) != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR, "{}: {}", fmt::format(what, std::forward<Args>(args)...), static_cast<const char *>(error));
}

}

#endif
