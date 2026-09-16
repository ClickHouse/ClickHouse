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
    static const constexpr size_t inputBufferSize = 8192;

    Poco::MD5Engine ctx;
    uint8_t inputBuffer[inputBufferSize];

public:
    void hashFile(const String & filePath, Poco::DigestEngine::Digest & res);
};

}
