#include <Storages/MergeTree/TemporaryParts.h>

namespace DB
{

bool TemporaryParts::contains(const std::string & basename) const
{
    std::lock_guard lock(mutex);
    return parts.contains(basename);
}

void TemporaryParts::add(std::string basename)
{
    std::lock_guard lock(mutex);
    parts.emplace(std::move(basename));
}

void TemporaryParts::remove(const std::string & basename)
{
    std::lock_guard lock(mutex);
    parts.erase(basename);
}

}
