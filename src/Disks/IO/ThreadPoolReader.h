#pragma once

#include <IO/AsynchronousReader.h>
#include <Common/ThreadPool.h>


namespace DB
{

/** Perform reads from separate thread pool of specified size.
  *
  * Note: doing reads from thread pool is usually bad idea for the following reasons:
  * - for fast devices or when data is in page cache, it is less cache-friendly and less NUMA friendly
  *   and also involves extra synchronization overhead;
  * - for fast devices and lots of small random reads, it cannot utilize the device, because
  *   OS will spent all the time in switching between threads and wasting CPU;
  * - you don't know how many threads do you need, for example, when reading from page cache,
  *   you need the number of threads similar to the number of CPU cores;
  *   when reading from HDD array you need the number of threads as the number of disks times N;
  *   when reading from SSD you may need at least hundreds of threads for efficient random reads,
  *   but it is impractical;
  * For example, this method is used in POSIX AIO that is notoriously useless (in contrast to Linux AIO).
  *
  * This is intended only as example for implementation of readers from remote filesystems,
  * where this method can be feasible.
  */
class ThreadPoolReader final : public IAsynchronousReader
{
private:
    ThreadPool pool;

public:
    ThreadPoolReader(size_t pool_size, size_t queue_size_);
    std::future<Result> submit(Request request) override;

    /// pool automatically waits for all tasks in destructor.
};

}
