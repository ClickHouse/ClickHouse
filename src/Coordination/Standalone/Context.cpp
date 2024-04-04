#include <Interpreters/Context.h>

#include <Common/Config/ConfigProcessor.h>
#include <Common/Macros.h>
#include <Common/ThreadPool.h>

#include <Core/ServerSettings.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <cassert>

namespace ProfileEvents
{
    extern const Event ContextLock;
}

namespace CurrentMetrics
{
    extern const Metric ContextLockWait;
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric BackgroundSchedulePoolSize;
    extern const Metric IOWriterThreads;
    extern const Metric IOWriterThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct ContextSharedPart : boost::noncopyable
{
    ContextSharedPart()
        : macros(std::make_unique<Macros>())
    {}

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;

    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher TSA_GUARDED_BY(keeper_dispatcher_mutex);

    ServerSettings server_settings;

    String path;                                            /// Path to the data directory, with a slash at the end.
    ConfigurationPtr config;                                /// Global configuration settings.
    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    mutable std::unique_ptr<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background
    RemoteHostFilter remote_host_filter; /// Allowed URL from config.xml
                                         ///
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_remote_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_local_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> synchronous_local_fs_reader;

    mutable std::unique_ptr<ThreadPool> threadpool_writer;

    mutable ThrottlerPtr remote_read_throttler;             /// A server-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_throttler;            /// A server-wide throttler for remote IO writes

    mutable ThrottlerPtr local_read_throttler;              /// A server-wide throttler for local IO reads
    mutable ThrottlerPtr local_write_throttler;             /// A server-wide throttler for local IO writes

};

Context::Context() = default;
Context::~Context() = default;
Context::Context(const Context &) = default;
Context & Context::operator=(const Context &) = default;

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) noexcept = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context)
    : shared(std::move(shared_context)) {}

void SharedContextHolder::reset() { shared.reset(); }

void Context::makeGlobalContext()
{
    initGlobal();
    global_context = shared_from_this();
}

ContextMutablePtr Context::createGlobal(ContextSharedPart * shared)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared;
    return res;
}

void Context::initGlobal()
{
    assert(!global_context_instance);
    global_context_instance = shared_from_this();
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

ContextMutablePtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no global context or global context has expired");
    return ptr;
}

std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(shared->mutex);
}

String Context::getPath() const
{
    auto lock = getLock();
    return shared->path;
}

void Context::setPath(const String & path)
{
    auto lock = getLock();
    shared->path = path;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

BackgroundSchedulePool & Context::getSchedulePool() const
{
    auto lock = getLock();
    if (!shared->schedule_pool)
    {
        shared->schedule_pool = std::make_unique<BackgroundSchedulePool>(
            shared->server_settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            CurrentMetrics::BackgroundSchedulePoolSize,
            "BgSchPool");
    }

    return *shared->schedule_pool;
}

void Context::setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config)
{
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    return shared->remote_host_filter;
}

IAsynchronousReader & Context::getThreadPoolReader(FilesystemReaderType type) const
{
    auto lock = getLock();

    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            if (!shared->asynchronous_remote_fs_reader)
                shared->asynchronous_remote_fs_reader = createThreadPoolReader(type, getConfigRef());
            return *shared->asynchronous_remote_fs_reader;
        }
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            if (!shared->asynchronous_local_fs_reader)
                shared->asynchronous_local_fs_reader = createThreadPoolReader(type, getConfigRef());

            return *shared->asynchronous_local_fs_reader;
        }
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            if (!shared->synchronous_local_fs_reader)
                shared->synchronous_local_fs_reader = createThreadPoolReader(type, getConfigRef());

            return *shared->synchronous_local_fs_reader;
        }
    }
}

std::shared_ptr<FilesystemCacheLog> Context::getFilesystemCacheLog() const
{
    return nullptr;
}

std::shared_ptr<FilesystemReadPrefetchesLog> Context::getFilesystemReadPrefetchesLog() const
{
    return nullptr;
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->config = config;
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock();
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}

std::shared_ptr<AsyncReadCounters> Context::getAsyncReadCounters() const
{
    auto lock = getLock();
    if (!async_read_counters)
        async_read_counters = std::make_shared<AsyncReadCounters>();
    return async_read_counters;
}

ThreadPool & Context::getThreadPoolWriter() const
{
    const auto & config = getConfigRef();

    auto lock = getLock();

    if (!shared->threadpool_writer)
    {
        auto pool_size = config.getUInt(".threadpool_writer_pool_size", 100);
        auto queue_size = config.getUInt(".threadpool_writer_queue_size", 1000000);

        shared->threadpool_writer = std::make_unique<ThreadPool>(
            CurrentMetrics::IOWriterThreads, CurrentMetrics::IOWriterThreadsActive, pool_size, pool_size, queue_size);
    }

    return *shared->threadpool_writer;
}

ThrottlerPtr Context::getRemoteReadThrottler() const
{
    return nullptr;
}

ThrottlerPtr Context::getRemoteWriteThrottler() const
{
    return nullptr;
}

ThrottlerPtr Context::getLocalReadThrottler() const
{
    return nullptr;
}

ThrottlerPtr Context::getLocalWriteThrottler() const
{
    return nullptr;
}

ReadSettings Context::getReadSettings() const
{
    return ReadSettings{};
}

void Context::initializeKeeperDispatcher([[maybe_unused]] bool start_async) const
{
    const auto & config_ref = getConfigRef();

    std::lock_guard lock(shared->keeper_dispatcher_mutex);

    if (shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    if (config_ref.has("keeper_server"))
    {
        shared->keeper_dispatcher = std::make_shared<KeeperDispatcher>();
        shared->keeper_dispatcher->initialize(config_ref, true, start_async, getMacros());
    }
}

std::shared_ptr<KeeperDispatcher> Context::getKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->keeper_dispatcher;
}

std::shared_ptr<KeeperDispatcher> Context::tryGetKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    return shared->keeper_dispatcher;
}

void Context::shutdownKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (shared->keeper_dispatcher)
    {
        shared->keeper_dispatcher->shutdown();
        shared->keeper_dispatcher.reset();
    }
}

void Context::updateKeeperConfiguration([[maybe_unused]] const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        return;

    shared->keeper_dispatcher->updateConfiguration(getConfigRef(), getMacros());
}

}
