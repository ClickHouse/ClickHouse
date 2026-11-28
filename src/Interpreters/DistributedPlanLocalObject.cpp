#include <Interpreters/DistributedPlanLocalObject.h>

namespace DB
{

void DistributedPlanLocalObject::add(Kind kind, const String & name)
{
    if (recorded.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(mutex);
    if (entry)
        return;
    entry = Entry{kind, name};
    recorded.store(true, std::memory_order_release);
}

std::optional<DistributedPlanLocalObject::Entry> DistributedPlanLocalObject::get() const
{
    std::lock_guard lock(mutex);
    return entry;
}

std::string_view DistributedPlanLocalObject::kindName(Kind kind)
{
    switch (kind)
    {
        case Kind::Dictionary: return "dictionary";
        case Kind::JoinTable: return "Join table";
        case Kind::EmbeddedDictionaries: return "the embedded dictionaries";
    }
}

}
