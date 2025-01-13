#pragma once

#include "config.h"
#if USE_SSL
#    include <openssl/md5.h>
#endif

#include <cstdint>
#include <string>

namespace BuzzHouse
{

class MD5Impl
{
private:
    static const constexpr size_t input_buffer_size = 8192; // 8 KB buffer

    MD5_CTX ctx;
    uint8_t input_buffer[input_buffer_size];

public:
    void hashFile(const std::string & file_path, uint8_t digest[16]);
};

}
