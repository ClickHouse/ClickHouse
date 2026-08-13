#pragma once

#include <Access/Common/AuthenticationType.h>
#include <Access/QuotaUsage.h>
#include <Core/Types_fwd.h>
#include <Interpreters/ClientCertificateInfo.h>
#include <Interpreters/ClientInfo.h>
#include <base/defines.h>
#include <base/types.h>

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include <boost/noncopyable.hpp>

namespace DB
{

/// Tracks currently logged-in sessions (TCP/HTTP-named/MySQL/PostgreSQL) so that they can be
/// listed (`system.sessions`) and, in the future, terminated (`KILL SESSION`). Unlike SessionTracker,
/// which only counts sessions per user, this registry keeps enough information about each session to
/// display it, mirroring what SessionLog records at login time.
class SessionRegistry
{
public:
    struct Entry
    {
        UUID auth_id;
        String session_id;

        time_t event_time{};
        Decimal64 event_time_microseconds{};

        std::optional<String> user;
        std::optional<AuthenticationType> auth_type;
        Strings roles;
        Strings profiles;
        std::vector<std::pair<String, String>> settings;
        std::vector<QuotaUsage> quotas;

        ClientInfo client_info;
        std::optional<ClientCertificateInfo> certificate_info;
    };

    using EntryPtr = std::shared_ptr<const Entry>;

    class Handle : boost::noncopyable
    {
    public:
        Handle(SessionRegistry & registry_, std::list<Entry>::iterator entry_iter_) noexcept;
        ~Handle();

    private:
        friend class SessionRegistry;

        SessionRegistry & registry;
        std::list<Entry>::iterator entry_iter;
    };

    using HandlePtr = std::unique_ptr<Handle>;

    HandlePtr registerSession(Entry entry);

    std::vector<EntryPtr> getEntries() const;

private:
    friend class Handle;

    mutable std::mutex mutex;
    std::list<Entry> entries TSA_GUARDED_BY(mutex);

    void unregisterSession(std::list<Entry>::iterator entry_iter);
};

}
