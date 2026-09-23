#include <Interpreters/UsedServerLocalObjects.h>

namespace DB
{

void UsedServerLocalObjects::add(Kind kind, const String & name)
{
    std::lock_guard lock(mutex);
    entries.push_back({kind, name});
}

std::optional<UsedServerLocalObjects::Entry> UsedServerLocalObjects::first() const
{
    std::lock_guard lock(mutex);
    if (entries.empty())
        return std::nullopt;
    return entries.front();
}

size_t UsedServerLocalObjects::size() const
{
    std::lock_guard lock(mutex);
    return entries.size();
}

std::optional<UsedServerLocalObjects::Entry> UsedServerLocalObjects::at(size_t position) const
{
    std::lock_guard lock(mutex);
    if (position >= entries.size())
        return std::nullopt;
    return entries[position];
}

std::string_view UsedServerLocalObjects::kindName(Kind kind)
{
    switch (kind)
    {
        case Kind::Dictionary: return "dictionary";
        case Kind::JoinTable: return "Join table";
        case Kind::EmbeddedDictionaries: return "the embedded dictionaries";
    }
}

}
