#pragma once

#include <fmt/format.h>

#include <string>

namespace DB::Runtime::Wyrd
{

void unreachable(std::string message);
void abort(std::string message);

template <class... Args>
void unreachable(std::string_view format_string, Args &&... args)
{
    unreachable(fmt::vformat(format_string, fmt::make_format_args(args...)));
}

template <class... Args>
void abort(std::string_view format_string, Args &&... args)
{
    abort(fmt::vformat(format_string, fmt::make_format_args(args...)));
}

}
