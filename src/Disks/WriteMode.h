#pragma once

namespace DB
{

/**
 * Mode of opening a file for write.
 */
enum class WriteMode
{
    Rewrite,
    Append
};

}
