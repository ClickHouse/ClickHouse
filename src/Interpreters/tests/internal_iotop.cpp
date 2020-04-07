#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Poco/File.h>
#include <Common/Stopwatch.h>
#include <common/getThreadId.h>
#include <IO/WriteBufferFromString.h>
#include <linux/taskstats.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>


std::mutex mutex;


static std::ostream & operator << (std::ostream & stream, const ::taskstats & stat)
{
#define PRINT(field) (stream << #field << " " << stat.field)

    PRINT(ac_pid) << ", ";

    PRINT(read_bytes) << ", ";
    PRINT(write_bytes) << ", ";

    PRINT(read_char) << ", ";
    PRINT(write_char) << ", ";

    PRINT(swapin_delay_total) << ", ";
    PRINT(blkio_delay_total) << ", ";
    PRINT(cpu_delay_total) << ", ";

    PRINT(ac_pid) << ", ";

    PRINT(ac_utime) << ", ";
    PRINT(ac_stime) << ", ";

#undef PRINT

    return stream;
}

using namespace DB;


static void do_io(size_t id)
{
    ::taskstats stat;
    int tid = getThreadId();
    TaskStatsInfoGetter get_info;

    get_info.getStat(stat, tid);
    {
        std::lock_guard lock(mutex);
        std::cerr << "#" << id << ", tid " << tid << ", intitial\n" << stat << "\n";
    }

    size_t copy_size = 1048576 * (1 + id);
    std::string path_dst = "test_out_" + std::to_string(id);

    {
        ReadBufferFromFile rb("/dev/urandom");
        WriteBufferFromFile wb(path_dst, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0666, nullptr, 4096);
        copyData(rb, wb, copy_size);
        wb.close();
    }

    get_info.getStat(stat, tid);
    {
        std::lock_guard lock(mutex);
        std::cerr << "#" << id << ", tid " << tid << ", step1\n" << stat << "\n";
    }

    {
        ReadBufferFromFile rb(path_dst);
        WriteBufferFromOwnString wb;
        copyData(rb, wb, copy_size);
    }

    get_info.getStat(stat, tid);
    {
        std::lock_guard lock(mutex);
        std::cerr << "#" << id << ", tid " << tid << ", step2\n" << stat << "\n";
    }

    {
        ReadBufferFromFile rb(path_dst);
        WriteBufferFromOwnString wb;
        copyData(rb, wb, copy_size);
    }

    get_info.getStat(stat, tid);
    {
        std::lock_guard lock(mutex);
        std::cerr << "#" << id << ", tid " << tid << ", step3\n" << stat << "\n";
    }

    Poco::File(path_dst).remove(false);
}

static void test_perf()
{

    ::taskstats stat;
    int tid = getThreadId();
    TaskStatsInfoGetter get_info;

    rusage rusage;

    constexpr size_t num_samples = 1000000;
    {
        Stopwatch watch;
        for (size_t i = 0; i < num_samples; ++i)
            getrusage(RUSAGE_THREAD, &rusage);

        auto ms = watch.elapsedMilliseconds();
        if (ms > 0)
            std::cerr << "RUsage: " << double(ms) / num_samples << " ms per call, " << 1000 * num_samples / ms << " calls per second\n";
    }

    {
        Stopwatch watch;
        for (size_t i = 0; i < num_samples; ++i)
            get_info.getStat(stat, tid);

        auto ms = watch.elapsedMilliseconds();
        if (ms > 0)
            std::cerr << "Netlink: " << double(ms) / num_samples << " ms per call, " << 1000 * num_samples / ms << " calls per second\n";
    }

    std::cerr << stat << "\n";
}

int main()
try
{
    std::cerr << "pid " << getpid() << "\n";

    size_t num_threads = 2;
    ThreadPool pool(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        pool.scheduleOrThrowOnError([i]() { do_io(i); });
    pool.wait();

    test_perf();
    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true);
    return -1;
}

