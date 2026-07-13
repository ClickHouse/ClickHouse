#pragma once

#include <string>

namespace DB
{

std::string base64Encode(const std::string & decoded, bool url_encoding = false, bool no_padding = false);

std::string base64Decode(const std::string & encoded, bool url_encoding = false, bool no_padding = false);

}
