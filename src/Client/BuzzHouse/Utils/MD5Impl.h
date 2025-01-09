#pragma once

#include "config.h"
#if USE_SSL
#    include <openssl/md5.h>
#endif

#include <cstdint>
#include <fstream>

class MD5Impl
{
private:
    static const constexpr size_t input_buffer_size = 8192; // 8 KB buffer

    MD5_CTX ctx;
    uint8_t input_buffer[input_buffer_size];

public:
    void hashFile(const std::string & file_path, uint8_t digest[16])
    {
        std::ifstream file(file_path, std::ios::binary);

        if (!file)
        {
            throw std::runtime_error("Could not open file: " + file_path);
        }
        MD5_Init(&ctx);
        while (file.read(reinterpret_cast<char *>(input_buffer), input_buffer_size) || file.gcount() > 0)
        {
            MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(input_buffer), file.gcount());
        }
        MD5_Final(digest, &ctx);
    }
};
