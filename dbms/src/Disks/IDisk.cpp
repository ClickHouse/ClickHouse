#include "IDisk.h"

namespace DB
{

std::mutex IDisk::reservation_mutex;

bool IDisk::isDirectoryEmpty(const String & path)
{
    return !iterateDirectory(path)->isValid();
}
}
