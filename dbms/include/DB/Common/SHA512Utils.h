#pragma once

#include <openssl/sha.h>
#include <string>

namespace SHA512Utils
{

void updateHash(SHA512_CTX & ctx, const std::string & path);
std::string computeHashFromString(const std::string & in);
std::string computeHashFromFolder(const std::string & path);

}
