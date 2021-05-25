#pragma once
#include <string>

namespace FS
{
bool createFile(const std::string & path);
bool canRead(const std::string & path);
bool canWrite(const std::string & path);
}
