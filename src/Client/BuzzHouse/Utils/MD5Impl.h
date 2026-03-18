#pragma once

#include <base/types.h>
#include <Poco/MD5Engine.h>

#include <cstdint>

namespace BuzzHouse
{

class MD5Impl
{
private:
    /// 8 KB buffer
    static const constexpr size_t input_buffer_size = 8192;

    Poco::MD5Engine ctx;
    uint8_t input_buffer[input_buffer_size];

public:
    void hashFile(const String & file_path, Poco::DigestEngine::Digest & res);
};

}
