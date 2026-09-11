#include <Processors/Executors/Runtime/Engine/Poller.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

#include <gtest/gtest.h>

#include <chrono>
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
    ProcessorState state;

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

std::vector<ProcessorState *> one(ProcessorState & state)
{
    return {&state};
}

}

TEST(Poller, ReturnsStateWhenFdIsReady)
{
    Poller poller;
    TestPipe pipe;

    pipe.makeReady();

    poller.add(pipe.state, pipe.readFd());
    EXPECT_EQ(1u, poller.pending());

    EXPECT_EQ(one(pipe.state), poller.poll(-1));
    EXPECT_EQ(0u, poller.pending());
}

TEST(Poller, NonBlockingPollReturnsNothingUntilReady)
{
    Poller poller;
    TestPipe pipe;

    poller.add(pipe.state, pipe.readFd());
    EXPECT_TRUE(poller.poll(0).empty());
    EXPECT_EQ(1u, poller.pending());

    pipe.makeReady();
    EXPECT_EQ(one(pipe.state), poller.poll(0));
}

TEST(Poller, ReturnsStateOnTimeout)
{
    Poller poller;
    TestPipe pipe;

    auto start = steady_clock::now();
    poller.add(pipe.state, pipe.readFd(), EPOLLIN | EPOLLERR, 50);

    EXPECT_EQ(one(pipe.state), poller.poll(-1));
    EXPECT_GE(steady_clock::now() - start, milliseconds(45));
    EXPECT_EQ(0u, poller.pending());
}

TEST(Poller, EarliestDeadlineFiresFirst)
{
    Poller poller;
    TestPipe slow_pipe;
    TestPipe fast_pipe;

    poller.add(slow_pipe.state, slow_pipe.readFd(), EPOLLIN | EPOLLERR, 300);
    poller.add(fast_pipe.state, fast_pipe.readFd(), EPOLLIN | EPOLLERR, 50);

    EXPECT_EQ(one(fast_pipe.state), poller.poll(-1));
    EXPECT_EQ(one(slow_pipe.state), poller.poll(-1));
    EXPECT_EQ(0u, poller.pending());
}

TEST(Poller, ReadyFdCancelsDeadline)
{
    Poller poller;
    TestPipe first_pipe;
    TestPipe second_pipe;

    auto start = steady_clock::now();
    poller.add(first_pipe.state, first_pipe.readFd(), EPOLLIN | EPOLLERR, 10000);
    first_pipe.makeReady();

    EXPECT_EQ(one(first_pipe.state), poller.poll(-1));
    EXPECT_LT(steady_clock::now() - start, seconds(10));

    poller.add(second_pipe.state, second_pipe.readFd(), EPOLLIN | EPOLLERR, 50);
    EXPECT_EQ(one(second_pipe.state), poller.poll(-1));
    EXPECT_EQ(0u, poller.pending());
}

TEST(Poller, StateCanBeReAddedAfterTimeout)
{
    Poller poller;
    TestPipe pipe;

    poller.add(pipe.state, pipe.readFd(), EPOLLIN | EPOLLERR, 20);
    EXPECT_EQ(one(pipe.state), poller.poll(-1));

    poller.add(pipe.state, pipe.readFd(), EPOLLIN | EPOLLERR, 20);
    EXPECT_EQ(one(pipe.state), poller.poll(-1));
    EXPECT_EQ(0u, poller.pending());
}

TEST(Poller, DeadlineArmedWhileWaiting)
{
    Poller poller;
    TestPipe idle_pipe;
    TestPipe timed_pipe;

    poller.add(idle_pipe.state, idle_pipe.readFd());

    std::thread adder([&]
    {
        std::this_thread::sleep_for(milliseconds(100));
        poller.add(timed_pipe.state, timed_pipe.readFd(), EPOLLIN | EPOLLERR, 50);
    });

    auto fired = poller.poll(-1);
    adder.join();

    EXPECT_EQ(one(timed_pipe.state), fired);
    EXPECT_EQ(1u, poller.pending());
}

TEST(Poller, WakeupInterruptsPollAndIsDrained)
{
    Poller poller;
    TestPipe pipe;

    poller.add(pipe.state, pipe.readFd());

    std::thread waker([&]
    {
        std::this_thread::sleep_for(milliseconds(50));
        poller.wakeup();
    });

    EXPECT_TRUE(poller.poll(-1).empty());
    waker.join();
    EXPECT_EQ(1u, poller.pending());

    EXPECT_TRUE(poller.poll(0).empty());

    pipe.makeReady();
    EXPECT_EQ(one(pipe.state), poller.poll(0));
}

TEST(Poller, SeveralReadyFdsComeInOnePoll)
{
    Poller poller;
    TestPipe first_pipe;
    TestPipe second_pipe;

    poller.add(first_pipe.state, first_pipe.readFd());
    poller.add(second_pipe.state, second_pipe.readFd());
    first_pipe.makeReady();
    second_pipe.makeReady();

    auto fired = poller.poll(0);
    std::ranges::sort(fired);
    std::vector<ProcessorState *> expected{&first_pipe.state, &second_pipe.state};
    std::ranges::sort(expected);
    EXPECT_EQ(expected, fired);
    EXPECT_EQ(0u, poller.pending());
}

#endif
