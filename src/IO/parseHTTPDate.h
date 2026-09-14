#pragma once

#include <optional>
#include <string_view>
#include <ctime>

namespace DB
{

std::optional<time_t> tryParseHTTPDate(std::string_view date);

std::optional<time_t> tryParseHTTPDate(std::string_view date, time_t reference_time);

}
