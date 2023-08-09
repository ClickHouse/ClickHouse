#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/ThreadPool.h>

#include <chrono>


namespace DB
{

class StorageLiveView;
struct StorageID;

/// This class removes temporary live views in the background thread when it's possible.
/// There should only a single instance of this class.
class TemporaryLiveViewCleaner : WithMutableContext
{
public:
    static TemporaryLiveViewCleaner & instance() { return *the_instance; }

    /// Drops a specified live view after a while if it's temporary.
    void addView(const std::shared_ptr<StorageLiveView> & view);

    /// Should be called once.
    static void init(ContextMutablePtr global_context_);
    static void shutdown();

    void startup();

private:
    friend std::unique_ptr<TemporaryLiveViewCleaner>::deleter_type;

    explicit TemporaryLiveViewCleaner(ContextMutablePtr global_context_);
    ~TemporaryLiveViewCleaner();

    void backgroundThreadFunc();
    void startBackgroundThread();
    void stopBackgroundThread();

    struct StorageAndTimeOfCheck
    {
        std::weak_ptr<StorageLiveView> storage;
        std::chrono::system_clock::time_point time_of_check;
        bool operator <(const StorageAndTimeOfCheck & other) const { return time_of_check < other.time_of_check; }
    };

    static std::unique_ptr<TemporaryLiveViewCleaner> the_instance;
    std::mutex mutex;
    std::vector<StorageAndTimeOfCheck> views;
    ThreadFromGlobalPool background_thread;
    std::atomic<bool> background_thread_can_start = false;
    std::atomic<bool> background_thread_should_exit = false;
    std::condition_variable background_thread_wake_up;
};

}
