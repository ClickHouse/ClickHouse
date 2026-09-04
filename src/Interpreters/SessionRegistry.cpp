#include <Interpreters/SessionRegistry.h>

namespace DB
{

SessionRegistry::Handle::Handle(SessionRegistry & registry_, std::list<Entry>::iterator entry_iter_) noexcept
    : registry(registry_), entry_iter(entry_iter_)
{
}

SessionRegistry::Handle::~Handle()
{
    registry.unregisterSession(entry_iter);
}

SessionRegistry::HandlePtr SessionRegistry::registerSession(Entry entry)
{
    std::lock_guard lock(mutex);
    entries.push_front(std::move(entry));
    return std::make_unique<Handle>(*this, entries.begin());
}

void SessionRegistry::unregisterSession(std::list<Entry>::iterator entry_iter)
{
    std::lock_guard lock(mutex);
    entries.erase(entry_iter);
}

std::vector<SessionRegistry::EntryPtr> SessionRegistry::getEntries() const
{
    std::vector<EntryPtr> result;

    std::lock_guard lock(mutex);
    result.reserve(entries.size());
    for (const auto & entry : entries)
        result.push_back(std::make_shared<const Entry>(entry));

    return result;
}

}
