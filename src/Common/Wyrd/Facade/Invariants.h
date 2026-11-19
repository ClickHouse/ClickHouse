#pragma once

#include <fmt/format.h>

#include <string>

namespace DB::Runtime::Wyrd
{

void abort(std::string message);
void verify(bool condition, std::string message);

template <class... Args>
void abort(std::string_view format_string, Args &&... args)
{
    abort(fmt::vformat(format_string, fmt::make_format_args(args...)));
}

template <class... Args>
void verify(bool condition, std::string_view format_string, Args &&... args)
{
    verify(condition, fmt::vformat(format_string, fmt::make_format_args(args...)));
}

}
