#pragma once

#include "config.h"

#if USE_AWS_S3

#include <iterator>
#include <utility>

#include <fmt/format.h>

#include <aws/core/utils/memory/stl/AWSString.h>

namespace DB::S3
{

template <typename... Args>
Aws::String awsFormat(fmt::format_string<Args...> fmt_str, Args &&... args)
{
    Aws::String result;
    fmt::format_to(std::back_inserter(result), fmt_str, std::forward<Args>(args)...);
    return result;
}

}

#endif
