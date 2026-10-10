#pragma once

#include <fmt/format.h>

#include <string>

namespace DB::Runtime::Wyrd
{

void abort(std::string message);

template <class... Args>
void abort(std::string_view format_string, Args &&... args)
{
    abort(fmt::vformat(format_string, fmt::make_format_args(args...)));
}

}
