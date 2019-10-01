#pragma once

#include <string>
#include <memory>

#include <Poco/TemporaryFile.h>

namespace DB
{

using TemporaryFile = Poco::TemporaryFile;

bool checkFreeSpace(const std::string & path, size_t data_size);
std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path);

}
