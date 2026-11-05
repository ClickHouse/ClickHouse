#pragma once

#include <string>

namespace DB
{

/// bzip2 and liblzma have no counterpart of zlib's zError() for their return codes.
std::string bzip2ErrorToString(int code);
std::string lzmaErrorToString(int code);

}
