//#pragma once
//
//namespace DB
//{
//class DiskObjectStorageVFS;
//
//// Despite the name, this thread handles not only garbage collection but also snapshot making and
//// uploading it to corresponding object storage
//class ObjectStorageVFSGCThread
//{
//public:
//    explicit ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_);
//
//    void start() { task->activateAndSchedule(); }
//    void wakeup() { task->schedule(); }
//    void stop() { task->deactivate(); }
//
//    void wakeupEarlierIfNeeded();
//
//    ActionLock getCleanupLock() { return cleanup_blocker.cancel(); }
//
//private:
//    DiskObjectStorageVFS & storage;
//    String log_name;
//    Poco::Logger * log;
//    BackgroundSchedulePool::TaskHolder task;
//    pcg64 rng{randomSeed()};
//
//    UInt64 sleep_ms;
//
//    std::atomic<UInt64> prev_cleanup_timestamp_ms = 0;
//    std::atomic<bool> is_running = false;
//
//    AtomicStopwatch wakeup_check_timer;
//
//    ActionBlocker cleanup_blocker;
//
//    void run();
//};
//}
