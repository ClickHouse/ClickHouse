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

void SessionRegistry::Handle::updateConnection(
    ClientInfo client_info, std::optional<ClientCertificateInfo> certificate_info,
    AuthenticationType auth_type, std::vector<QuotaUsage> quotas)
{
    std::lock_guard lock(registry.mutex);
    entry_iter->client_info = std::move(client_info);
    entry_iter->certificate_info = std::move(certificate_info);
    entry_iter->auth_type = auth_type;
    entry_iter->quotas = std::move(quotas);
}

void SessionRegistry::Handle::updateQuotas(std::vector<QuotaUsage> quotas)
{
    std::lock_guard lock(registry.mutex);
    entry_iter->quotas = std::move(quotas);
}

SessionRegistry::HandlePtr SessionRegistry::registerSession(Entry entry)
{
    std::lock_guard lock(mutex);
    entries.push_front(std::move(entry));
    try
    {
        return std::make_unique<Handle>(*this, entries.begin());
    }
    catch (...)
    {
        /// A failed handle allocation must not leave a session without an owner.
        entries.pop_front();
        throw;
    }
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
