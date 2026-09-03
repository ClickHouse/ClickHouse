#pragma once

namespace DB
{

/// Mode of opening a file for write.
enum class WriteMode : uint8_t
{
    Rewrite,
    Append
};

}
