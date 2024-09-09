#pragma once

#include <Interpreters/Context_fwd.h>

#include <Coordination/KeeperDispatcher.h>

#include <Common/MultiVersion.h>
#include <Common/RemoteHostFilter.h>
#include <Common/SharedMutex.h>

#include <Disks/IO/getThreadPoolReader.h>

#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Core/BackgroundSchedulePool.h>

#include <IO/AsyncReadCounters.h>
#include <Common/Scheduler/IResourceManager.h>

#include <Poco/Util/Application.h>

#include <memory>

#include "config.h"
namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

struct ContextSharedPart;
class Macros;
class FilesystemCacheLog;
class FilesystemReadPrefetchesLog;
class BlobStorageLog;
class IOUringReader;
class S3SettingsByEndpoint;

/// A small class which owns ContextShared.
/// We don't use something like unique_ptr directly to allow ContextShared type to be incomplete.
struct SharedContextHolder
{
    ~SharedContextHolder();
    SharedContextHolder();
    explicit SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context);
    SharedContextHolder(SharedContextHolder &&) noexcept;

    SharedContextHolder & operator=(SharedContextHolder &&) noexcept;

    ContextSharedPart * get() const { return shared.get(); }
    void reset();
private:
    std::unique_ptr<ContextSharedPart> shared;
};

class ContextData
{
protected:
    ContextWeakMutablePtr global_context;
    inline static ContextPtr global_context_instance;
    ContextSharedPart * shared;

    /// Query metrics for reading data asynchronously with IAsynchronousReader.
    mutable std::shared_ptr<AsyncReadCounters> async_read_counters;

    Settings settings;  /// Setting for query execution.

public:
    /// Use copy constructor or createGlobal() instead
    ContextData();
    ContextData(const ContextData &);
};

class Context : public ContextData, public std::enable_shared_from_this<Context>
{
private:
    /// ContextData mutex
    mutable SharedMutex mutex;

    Context();
    Context(const Context &);

    std::unique_lock<SharedMutex> getGlobalLock() const;

    std::shared_lock<SharedMutex> getGlobalSharedLock() const;

    std::unique_lock<SharedMutex> getLocalLock() const;

    std::shared_lock<SharedMutex> getLocalSharedLock() const;

public:
    /// Create initial Context with ContextShared and etc.
    static ContextMutablePtr createGlobal(ContextSharedPart * shared_part);
    static SharedContextHolder createShared();

    ContextMutablePtr getGlobalContext() const;
    static ContextPtr getGlobalContextInstance() { return global_context_instance; }

    void makeGlobalContext();
    void initGlobal();

    ~Context();

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    const Settings & getSettingsRef() const { return settings; }

    String getPath() const;
    void setPath(const String & path);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    BackgroundSchedulePool & getSchedulePool() const;

    /// Storage of allowed hosts from config.xml
    void setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config);
    const RemoteHostFilter & getRemoteHostFilter() const;

    std::shared_ptr<FilesystemCacheLog> getFilesystemCacheLog() const;
    std::shared_ptr<FilesystemReadPrefetchesLog> getFilesystemReadPrefetchesLog() const;
    std::shared_ptr<BlobStorageLog> getBlobStorageLog() const;

    enum class ApplicationType : uint8_t
    {
        KEEPER,
        SERVER,
    };

    void setApplicationType(ApplicationType) {}
    ApplicationType getApplicationType() const { return ApplicationType::KEEPER; }

    IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type) const;
#if USE_LIBURING
    IOUringReader & getIOUringReader() const;
#endif
    std::shared_ptr<AsyncReadCounters> getAsyncReadCounters() const;
    ThreadPool & getThreadPoolWriter() const;

    ThrottlerPtr getRemoteReadThrottler() const;
    ThrottlerPtr getRemoteWriteThrottler() const;

    ThrottlerPtr getLocalReadThrottler() const;
    ThrottlerPtr getLocalWriteThrottler() const;

    ReadSettings getReadSettings() const;

    /// Resource management related
    ResourceManagerPtr getResourceManager() const;
    ClassifierPtr getWorkloadClassifier() const;

    std::shared_ptr<KeeperDispatcher> getKeeperDispatcher() const;
    std::shared_ptr<KeeperDispatcher> tryGetKeeperDispatcher() const;
    void initializeKeeperDispatcher(bool start_async) const;
    void shutdownKeeperDispatcher() const;
    void updateKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    const S3SettingsByEndpoint & getStorageS3Settings() const;

    const String & getUserName() const { static std::string user; return user; }

    const ServerSettings & getServerSettings() const;

    bool hasTraceCollector() const;

    bool isBackgroundOperationContext() const;
};

}
