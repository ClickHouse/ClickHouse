#include "config.h"

#if USE_LANCE

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>

namespace ProfileEvents
{
extern const Event LanceDatasetCacheHit;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool lance_query_dataset_reuse;
extern const SettingsUInt64 lance_runtime_threads;
}
}

namespace DB::Lance
{

namespace
{
constexpr const char * session_type_tag = "Lance::QuerySession";
}

QuerySession::QuerySession()
    : cancel_handle(std::make_shared<CancelHandle>())
{
}

void QuerySession::bindToQueryCancellation(const ContextPtr & context)
{
    if (!context)
        return;

    const auto query_status = context->getProcessListElementSafe();
    if (!query_status)
        return;

    std::lock_guard lock(mutex);
    if (query_cancel_callback)
        return;

    query_cancel_callback = std::make_unique<StopCallback>(
        query_status->getCancellationToken(),
        [handle = cancel_handle]
        {
            handle->requestCancel();
        });
}

std::shared_ptr<QuerySession> QuerySession::get(const ContextPtr & context)
{
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance QuerySession requires a non-null Context");

    ContextMutablePtr query_context;
    if (context->hasQueryContext())
        query_context = context->getQueryContext();
    else
    {
        /// No query context (unit tests / background paths): return a private ephemeral session
        /// that is not shared across calls. Callers still get correct single-handle reuse within
        /// one returned shared_ptr lifetime only when they keep it.
        return std::make_shared<QuerySession>();
    }

    auto & holder = query_context->kitchen_sink.lance_query_session;
    if (!holder)
    {
        auto session = std::make_shared<QuerySession>();
        /// Apply runtime setting on first session attach for this query.
        const auto threads = static_cast<UInt32>(context->getSettingsRef()[Setting::lance_runtime_threads]);
        ensureRuntime(threads);
        session->setReuseEnabled(context->getSettingsRef()[Setting::lance_query_dataset_reuse]);
        session->bindToQueryCancellation(query_context);
        holder = session;
        return session;
    }

    auto session = std::static_pointer_cast<QuerySession>(holder);
    if (!session)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query kitchen_sink holds unexpected type for {}", session_type_tag);
    session->bindToQueryCancellation(query_context);
    return session;
}

DatasetHandle QuerySession::getOrOpen(const DatasetOptions & options)
{
    if (!reuse_enabled)
        return DatasetHandle::openEphemeral(options, cancel_handle);

    const auto key = options.identityKey();

    std::lock_guard lock(mutex);
    if (auto it = open_datasets.find(key); it != open_datasets.end())
    {
        ProfileEvents::increment(ProfileEvents::LanceDatasetCacheHit);
        return it->second;
    }

    auto handle = DatasetHandle::openEphemeral(options, cancel_handle);
    open_datasets.emplace(key, handle);
    ++open_count;
    return handle;
}

void QuerySession::pinVersion(const String & identity_key, UInt64 version)
{
    if (version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot pin Lance dataset version 0");

    std::lock_guard lock(mutex);
    if (auto it = pinned_versions.find(identity_key); it != pinned_versions.end())
    {
        if (it->second != version)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Lance dataset identity already pinned to version {}, cannot re-pin to {}",
                it->second,
                version);
        }
        return;
    }
    pinned_versions.emplace(identity_key, version);
}

std::optional<UInt64> QuerySession::getPinnedVersion(const String & identity_key) const
{
    std::lock_guard lock(mutex);
    if (auto it = pinned_versions.find(identity_key); it != pinned_versions.end())
        return it->second;
    return std::nullopt;
}

DatasetHandle QuerySession::getPinned(const DatasetOptions & options, UInt64 pinned_version)
{
    if (pinned_version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Lance dataset for pinned version 0");

    if (!reuse_enabled)
        return DatasetHandle::openEphemeral(options, cancel_handle);

    const auto key = options.identityKey();
    std::lock_guard lock(mutex);

    if (auto pin_it = pinned_versions.find(key); pin_it != pinned_versions.end())
    {
        if (pin_it->second != pinned_version)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Requested Lance pinned version {} does not match session pin {}",
                pinned_version,
                pin_it->second);
        }
    }
    else
    {
        /// Analysis normally pins first; allow first pin at execution for paths that bypass analysis.
        pinned_versions.emplace(key, pinned_version);
    }

    if (auto it = open_datasets.find(key); it != open_datasets.end())
    {
        ProfileEvents::increment(ProfileEvents::LanceDatasetCacheHit);
        return it->second;
    }

    auto handle = DatasetHandle::openEphemeral(options, cancel_handle);
    open_datasets.emplace(key, handle);
    ++open_count;
    return handle;
}

size_t QuerySession::openCount() const
{
    std::lock_guard lock(mutex);
    return open_count;
}

}

#endif
