#pragma once

#include <cstdint>

namespace DB
{

enum class StreamingHandleErrorMode : uint8_t
{
    DEFAULT = 0, // Ignore errors with threshold.
    STREAM, // Put errors to stream in the virtual column named ``_error.
};

enum class ExtStreamingHandleErrorMode : uint8_t
{
    DEFAULT = 0, // Ignore errors with threshold.
    STREAM, // Put errors to stream in the virtual column named ``_error.
    DEAD_LETTER_QUEUE
    /*CUSTOM_SYSTEM_TABLE, Put errors to in a custom system table. This is not implemented now.  */
};

}
