#include <Processors/Executors/PollingQueue.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Common/Exception.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <unistd.h>

using namespace DB;
using namespace std::chrono;

namespace
{

struct TestPipe
{
    int fds[2]{-1, -1};

    TestPipe()
    {
        EXPECT_EQ(0, ::pipe(fds));
        for (int fd : fds)
            EXPECT_NE(-1, ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK));
    }

    ~TestPipe()
    {
        ::close(fds[0]);
        ::close(fds[1]);
    }

    int readFd() const { return fds[0]; }

    void makeReady() const
    {
        char byte = 0;
        EXPECT_EQ(1, ::write(fds[1], &byte, 1));
    }
};

}

TEST(PollingQueue, ReturnsTaskWhenFdIsReady)
{
    PollingQueue queue;
    TestPipe pipe;
    std::mutex mutex;

    pipe.makeReady();

    std::unique_lock lock(mutex);
    queue.addTask(0, &pipe, pipe.readFd());
    EXPECT_EQ(queue.size(), 1);

    auto res = queue.wait(lock);
    EXPECT_EQ(res.data, &pipe);
    EXPECT_TRUE(queue.empty());
}

TEST(PollingQueue, TryGetReadyTaskDoesNotBlock)
{
    PollingQueue queue;
    TestPipe pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    queue.addTask(0, &pipe, pipe.readFd());

    EXPECT_FALSE(queue.tryGetReadyTask(lock));

    pipe.makeReady();
    auto res = queue.tryGetReadyTask(lock);
    EXPECT_EQ(res.data, &pipe);
}

TEST(PollingQueue, ReturnsTaskOnTimeout)
{
    PollingQueue queue;
    TestPipe pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    auto start = steady_clock::now();
    queue.addTask(0, &pipe, pipe.readFd(), EPOLLIN | EPOLLERR, 50);

    auto res = queue.wait(lock);
    EXPECT_EQ(res.data, &pipe);
    EXPECT_GE(steady_clock::now() - start, milliseconds(45));
    EXPECT_TRUE(queue.empty());
}

TEST(PollingQueue, EarliestDeadlineFiresFirst)
{
    PollingQueue queue;
    TestPipe slow_pipe;
    TestPipe fast_pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    queue.addTask(0, &slow_pipe, slow_pipe.readFd(), EPOLLIN | EPOLLERR, 300);
    queue.addTask(0, &fast_pipe, fast_pipe.readFd(), EPOLLIN | EPOLLERR, 50);

    EXPECT_EQ(queue.wait(lock).data, &fast_pipe);
    EXPECT_EQ(queue.wait(lock).data, &slow_pipe);
    EXPECT_TRUE(queue.empty());
}

TEST(PollingQueue, ReadyFdCancelsDeadline)
{
    PollingQueue queue;
    TestPipe first_pipe;
    TestPipe second_pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    auto start = steady_clock::now();
    queue.addTask(0, &first_pipe, first_pipe.readFd(), EPOLLIN | EPOLLERR, 10000);
    first_pipe.makeReady();

    EXPECT_EQ(queue.wait(lock).data, &first_pipe);
    EXPECT_LT(steady_clock::now() - start, seconds(10));

    queue.addTask(0, &second_pipe, second_pipe.readFd(), EPOLLIN | EPOLLERR, 50);
    EXPECT_EQ(queue.wait(lock).data, &second_pipe);
    EXPECT_TRUE(queue.empty());
}

TEST(PollingQueue, TaskCanBeReAddedAfterTimeout)
{
    PollingQueue queue;
    TestPipe pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    queue.addTask(0, &pipe, pipe.readFd(), EPOLLIN | EPOLLERR, 20);
    EXPECT_EQ(queue.wait(lock).data, &pipe);

    queue.addTask(0, &pipe, pipe.readFd(), EPOLLIN | EPOLLERR, 20);
    EXPECT_EQ(queue.wait(lock).data, &pipe);
    EXPECT_TRUE(queue.empty());
}

TEST(PollingQueue, DeadlineArmedWhileWaiting)
{
    PollingQueue queue;
    TestPipe idle_pipe;
    TestPipe timed_pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    queue.addTask(0, &idle_pipe, idle_pipe.readFd());

    std::thread adder([&]
    {
        std::this_thread::sleep_for(milliseconds(100));
        std::lock_guard guard(mutex);
        queue.addTask(0, &timed_pipe, timed_pipe.readFd(), EPOLLIN | EPOLLERR, 50);
    });

    auto res = queue.wait(lock);
    adder.join();

    EXPECT_EQ(res.data, &timed_pipe);
    EXPECT_EQ(queue.size(), 1);
}

TEST(PollingQueue, FinishInterruptsWait)
{
    PollingQueue queue;
    TestPipe pipe;
    std::mutex mutex;

    std::unique_lock lock(mutex);
    queue.addTask(0, &pipe, pipe.readFd());

    std::thread finisher([&]
    {
        std::this_thread::sleep_for(milliseconds(50));
        std::lock_guard guard(mutex);
        queue.finish();
    });

    EXPECT_FALSE(queue.wait(lock));
    finisher.join();
}

#endif
