#include <gtest/gtest.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <array>
#include <unistd.h>

#include <Common/Epoll.h>

namespace
{

/// A pipe whose read end stands in for the plain descriptors a poller holds (sockets, timers).
struct Pipe
{
    std::array<int, 2> fds{-1, -1};

    Pipe() { EXPECT_EQ(::pipe(fds.data()), 0); }
    ~Pipe()
    {
        for (int fd : fds)
            if (fd != -1)
                ::close(fd);
    }

    int readEnd() const { return fds[0]; }
    void signal() const { EXPECT_EQ(::write(fds[1], "x", 1), 1); }
};

}

/// A poller may nest a deeper poller after it has itself been registered somewhere. macOS pins a
/// kqueue's nesting level the first time it is registered, so without a reserved level the
/// `outer.add(middle)` below would make the later `middle.add(inner)` fail with EINVAL and abort a
/// `Distributed` read. Both the depth and the order matter and are the ones observed in CI: the
/// hedged poller has already nested a packet receiver, while the read context has only reported a
/// raw replica socket when the pipeline poller registers it, so the read context is the shallower of
/// the two by the time it has to nest the hedged poller.
TEST(Epoll, NestsADeeperPollerAfterBeingRegisteredItself)
{
    const Pipe pipe;

    DB::Epoll leaf{DB::EpollNesting::Leaf};
    DB::Epoll inner{DB::EpollNesting::HedgedConnections};
    DB::Epoll middle{DB::EpollNesting::AsyncReadContext};
    DB::Epoll outer{DB::EpollNesting::PipelinePoller};

    leaf.add(pipe.readEnd());
    inner.add(leaf.getFileDescriptor());

    const Pipe raw_descriptor;
    middle.add(raw_descriptor.readEnd());
    outer.add(middle.getFileDescriptor());
    ASSERT_NO_THROW(middle.add(inner.getFileDescriptor()));

    /// Readiness must still travel the whole chain.
    pipe.signal();
    std::array<epoll_event, 4> events{};
    ASSERT_EQ(outer.getManyReady(static_cast<int>(events.size()), events.data(), 10000), 1u);
    EXPECT_EQ(events[0].data.fd, middle.getFileDescriptor());
}

/// Reserving a level must be invisible to the Epoll's own bookkeeping: on macOS the reservation
/// registers throwaway descriptors in this very kqueue, and they must not count as events or block
/// a later add of a descriptor that reuses one of their numbers.
TEST(Epoll, ReservationLeavesNoTraceInTheDescriptorSet)
{
    for (auto nesting : {DB::EpollNesting::Leaf,
                         DB::EpollNesting::ConnectionsFactory,
                         DB::EpollNesting::HedgedConnections,
                         DB::EpollNesting::AsyncReadContext,
                         DB::EpollNesting::PipelinePoller})
    {
        DB::Epoll epoll{nesting};
        EXPECT_TRUE(epoll.empty());
        EXPECT_EQ(epoll.size(), 0);

        const Pipe pipe;
        epoll.add(pipe.readEnd());
        EXPECT_EQ(epoll.size(), 1);

        epoll.remove(pipe.readEnd());
        EXPECT_TRUE(epoll.empty());
    }
}

#endif
