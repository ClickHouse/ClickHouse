#pragma once

#include <Common/Wyrd/Namespace.h>

#include <fmt/format.h>

#include <string>
#include <string_view>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

void logMessage(std::string message);

template <class... Args>
void log(std::string_view format_string, Args &&... args)
{
    logMessage(fmt::vformat(format_string, fmt::make_format_args(args...)));
}

}
