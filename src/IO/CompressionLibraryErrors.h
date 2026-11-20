#pragma once

#include <string>

namespace DB
{

/// bzip2 and liblzma have no counterpart of zlib's zError() for their return codes.
std::string bzip2ErrorToString(int code);
/// liblzma gives LZMA_DATA_ERROR and LZMA_BUF_ERROR different meanings in encoders and decoders.
std::string lzmaErrorToString(int code, bool is_encoder);

}
