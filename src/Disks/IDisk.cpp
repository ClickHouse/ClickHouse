#include "IDisk.h"

namespace DB
{
bool IDisk::isDirectoryEmpty(const String & path)
{
    return !iterateDirectory(path)->isValid();
}
}
