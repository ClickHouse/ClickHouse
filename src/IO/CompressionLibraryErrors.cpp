#include <IO/CompressionLibraryErrors.h>

#include <fmt/format.h>
#include <lzma.h>

#include "config.h"

#if USE_BZIP2
#    include <bzlib.h>
#endif

namespace DB
{

#if USE_BZIP2
std::string bzip2ErrorToString(int code)
{
    switch (code)
    {
        case BZ_SEQUENCE_ERROR: return "Functions called in a wrong order (BZ_SEQUENCE_ERROR)";
        case BZ_PARAM_ERROR: return "Invalid parameter (BZ_PARAM_ERROR)";
        case BZ_MEM_ERROR: return "Cannot allocate memory (BZ_MEM_ERROR)";
        case BZ_DATA_ERROR: return "Compressed data is corrupt (BZ_DATA_ERROR)";
        case BZ_DATA_ERROR_MAGIC: return "Not a bzip2 stream (BZ_DATA_ERROR_MAGIC)";
        case BZ_IO_ERROR: return "I/O error (BZ_IO_ERROR)";
        case BZ_UNEXPECTED_EOF: return "Unexpected end of input (BZ_UNEXPECTED_EOF)";
        case BZ_OUTBUFF_FULL: return "Output buffer is full (BZ_OUTBUFF_FULL)";
        case BZ_CONFIG_ERROR: return "Library is miscompiled for this platform (BZ_CONFIG_ERROR)";
        default: return fmt::format("Unknown error code {}", code);
    }
}
#endif

std::string lzmaErrorToString(int code, bool is_encoder)
{
    switch (code)
    {
        case LZMA_NO_CHECK: return "No integrity check (LZMA_NO_CHECK)";
        case LZMA_UNSUPPORTED_CHECK: return "Unsupported type of integrity check (LZMA_UNSUPPORTED_CHECK)";
        case LZMA_MEM_ERROR: return "Cannot allocate memory (LZMA_MEM_ERROR)";
        case LZMA_MEMLIMIT_ERROR: return "Memory usage limit reached (LZMA_MEMLIMIT_ERROR)";
        case LZMA_FORMAT_ERROR: return "Not an xz stream (LZMA_FORMAT_ERROR)";
        case LZMA_OPTIONS_ERROR: return "Unsupported options (LZMA_OPTIONS_ERROR)";
        case LZMA_DATA_ERROR:
            return is_encoder ? "Data exceeds the size limits of the xz format (LZMA_DATA_ERROR)"
                              : "Compressed data is corrupt (LZMA_DATA_ERROR)";
        case LZMA_BUF_ERROR:
            return is_encoder ? "No progress is possible (LZMA_BUF_ERROR)" : "Unexpected end of input (LZMA_BUF_ERROR)";
        case LZMA_PROG_ERROR: return "Invalid arguments (LZMA_PROG_ERROR)";
        default: return fmt::format("Unknown error code {}", code);
    }
}

}
