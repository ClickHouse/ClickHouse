#pragma once

#include <Common/Exception.h>

namespace DB
{

namespace extractKV
{

struct DuplicateKeyFoundException : Exception
{
    DuplicateKeyFoundException(std::string_view key_) : key(key_) {}

    std::string_view key;
};

}

}
