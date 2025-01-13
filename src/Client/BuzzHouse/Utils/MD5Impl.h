#pragma once

#include <Poco/MD5Engine.h>

#include <cstdint>
#include <string>

namespace BuzzHouse
{

class MD5Impl
{
private:
    static const constexpr size_t input_buffer_size = 8192; // 8 KB buffer

    Poco::MD5Engine ctx;
    uint8_t input_buffer[input_buffer_size];

public:
    void hashFile(const std::string & file_path, Poco::DigestEngine::Digest & res);
};

}
