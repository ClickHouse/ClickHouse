#pragma once
#include <string>

namespace FS
{
bool createFile(const std::string & path);

bool canRead(const std::string & path);
bool canWrite(const std::string & path);

time_t getModificationTime(const std::string & path);
Poco::Timestamp getModificationTimestamp(const std::string & path);
void setModificationTime(const std::string & path, time_t time);
}
