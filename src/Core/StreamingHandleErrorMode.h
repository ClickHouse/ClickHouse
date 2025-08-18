#pragma once

#include <cstdint>

namespace DB
{

enum class StreamingHandleErrorMode : uint8_t
{
    DEFAULT = 0, // Ignore errors with threshold.
    STREAM, // Put errors to stream in the virtual column named ``_error.
    /*FIXED_SYSTEM_TABLE, Put errors to in a fixed system table likely system.kafka_errors. This is not implemented now.  */
    /*CUSTOM_SYSTEM_TABLE, Put errors to in a custom system table. This is not implemented now.  */
};

}
