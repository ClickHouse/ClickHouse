#include <Relay.h>

#if USE_SILK

#include <Common/Exception.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <algorithm>
#include <atomic>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}
}

namespace DB::Proxy
{

namespace
{

/// The state shared by the two directions of a relay.
struct RelayState
{
    std::atomic<int> aborted{0};
    int client_fd;
    int backend_fd;
};

/// Tear the whole relay down: the first caller shuts down (not closes) both underlying fds so the other
/// fiber's in-flight io_uring operation returns. Shutting down an fd is safe to race with a pending
/// io_uring op; the fds stay valid until both fibers have joined and the caller closes the sockets.
void abortRelay(RelayState & state) noexcept
{
    if (state.aborted.fetch_add(1, std::memory_order_acq_rel) == 0)
    {
        ::shutdown(state.client_fd, SHUT_RDWR);
        ::shutdown(state.backend_fd, SHUT_RDWR);
    }
}

/// One direction of the relay has ended. A clean end of stream is forwarded one way, as a half-close of
/// the destination, so the opposite direction keeps flowing: a peer that has finished sending can still
/// receive the rest of the response, as on a direct connection. An error tears the relay down.
/// A TLS-terminated destination cannot be half-closed independently of its other direction (both share
/// one TLS session), so its stream is torn down as well.
void finishDirection(RelayState & state, bool clean_end_of_stream, int dst_fd, bool dst_plaintext) noexcept
{
    if (clean_end_of_stream && dst_plaintext)
        ::shutdown(dst_fd, SHUT_WR);
    else
        abortRelay(state);
}

/// --- Copy relay: reads into a user-space buffer and writes it out. Used when a leg is TLS-terminated
/// (its bytes must be decrypted/encrypted in user space and cannot be spliced), or when the pipes for
/// the splice relay cannot be created. ---

struct CopyDirection
{
    FiberSocket * src;
    FiberSocket * dst;
    Backend * backend;
    bool to_client;
    size_t buffer_size;
    RelayState * state;
};

int copyLoop(CopyDirection * d) noexcept
{
    bool clean_end_of_stream = false;
    try
    {
        std::vector<char> buffer(d->buffer_size);
        while (true)
        {
            int n = d->src->receive(buffer.data(), static_cast<int>(buffer.size()));
            if (n <= 0)
            {
                clean_end_of_stream = (n == 0);
                break;
            }
            d->dst->sendAll(buffer.data(), n);
            if (d->backend)
            {
                if (d->to_client)
                    d->backend->addBytesToClient(n);
                else
                    d->backend->addBytesFromClient(n);
            }
        }
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        /// A read or write error simply ends the relay for this connection, so it is Ok to swallow it.
    }

    finishDirection(*d->state, clean_end_of_stream, d->dst->fd(), d->dst->plaintext());
    return 0;
}

/// --- Splice relay: moves bytes socket -> pipe -> socket entirely inside the kernel, so plaintext
/// traffic is never copied through user space. splice(2) requires a pipe on one side, hence the
/// per-direction pipe. Used only when both legs are plaintext. ---

struct SpliceDirection
{
    int src_fd;
    int dst_fd;
    int pipe_read_fd;
    int pipe_write_fd;
    Backend * backend;
    bool to_client;
    unsigned int chunk;
    RelayState * state;
};

int spliceLoop(SpliceDirection * d) noexcept
{
    bool clean_end_of_stream = false;

    /// Enlarge the pipe so a single splice can carry a full chunk (best-effort; capped by
    /// /proc/sys/fs/pipe-max-size). SPLICE_F_MOVE only: SPLICE_F_MORE would cork the socket and
    /// re-introduce the Nagle-like latency that TCP_NODELAY removes.
    ::fcntl(d->pipe_write_fd, F_SETPIPE_SZ, static_cast<int>(d->chunk));
    while (true)
    {
        uint64_t in_bytes = 0;
        int r = silk::FiberScheduler::splice(d->src_fd, -1, d->pipe_write_fd, -1, d->chunk, SPLICE_F_MOVE, &in_bytes);
        if (r != 0 || in_bytes == 0)
        {
            clean_end_of_stream = (r == 0);
            break;   // error or end of input
        }

        uint64_t remaining = in_bytes;
        while (remaining > 0)
        {
            uint64_t out_bytes = 0;
            int w = silk::FiberScheduler::splice(
                d->pipe_read_fd, -1, d->dst_fd, -1, static_cast<unsigned int>(remaining), SPLICE_F_MOVE, &out_bytes);
            if (w != 0 || out_bytes == 0)
            {
                in_bytes = 0;   // signal the outer loop to stop
                break;          // exits this inner loop; `remaining` is not read afterwards
            }
            remaining -= out_bytes;
        }
        if (in_bytes == 0)
            break;

        if (d->backend)
        {
            if (d->to_client)
                d->backend->addBytesToClient(in_bytes);
            else
                d->backend->addBytesFromClient(in_bytes);
        }
    }

    finishDirection(*d->state, clean_end_of_stream, d->dst_fd, /*dst_plaintext=*/ true);
    return 0;
}

/// The two pipes of a splice relay, closed on destruction.
struct SplicePipes
{
    int to_backend[2] = {-1, -1};
    int to_client[2] = {-1, -1};

    /// Returns false if the pipes cannot be created (e.g. `EMFILE`).
    bool create()
    {
        return ::pipe2(to_backend, O_CLOEXEC) == 0 && ::pipe2(to_client, O_CLOEXEC) == 0;
    }

    ~SplicePipes()
    {
        for (int fd : {to_backend[0], to_backend[1], to_client[0], to_client[1]})
            if (fd >= 0)
                [[maybe_unused]] int err = ::close(fd);
    }
};

}

void runRelay(
    FiberSocket & client,
    FiberSocket & backend_socket,
    Backend * backend,
    const String & initial_to_backend,
    size_t buffer_size,
    UInt64 relay_timeout_ms)
{
    /// The handshake is over: leave the short handshake timeout behind, so an ordinary idle gap
    /// between commands, a slow upload, or a long-running query does not tear down the session.
    /// This governs the user-space copy path; the zero-copy splice path does not consult it.
    client.setTimeouts(relay_timeout_ms, relay_timeout_ms);
    backend_socket.setTimeouts(relay_timeout_ms, relay_timeout_ms);

    /// Handshake bytes the proxy already parsed live in user space; forward them with a normal write.
    if (!initial_to_backend.empty())
    {
        backend_socket.sendAll(initial_to_backend.data(), initial_to_backend.size());
        if (backend)
            backend->addBytesFromClient(initial_to_backend.size());
    }

    RelayState state{.client_fd = client.fd(), .backend_fd = backend_socket.fd()};

    /// The zero-copy path needs a pipe per direction. Under file descriptor pressure, when they cannot
    /// be created, relay through user space instead of failing the connection.
    SplicePipes pipes;
    if (client.plaintext() && backend_socket.plaintext() && pipes.create())
    {
        const unsigned int chunk = static_cast<unsigned int>(std::max<size_t>(buffer_size, 4096));
        SpliceDirection to_backend{state.client_fd, state.backend_fd, pipes.to_backend[0], pipes.to_backend[1], backend, false, chunk, &state};
        SpliceDirection to_client{state.backend_fd, state.client_fd, pipes.to_client[0], pipes.to_client[1], backend, true, chunk, &state};

        silk::FiberFuture future;
        if (silk::FiberScheduler::run(spliceLoop, SpliceDirection(to_client), &future) != 0)
            throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot allocate a fiber for the relay");
        spliceLoop(&to_backend);
        future.wait();
        return;
    }

    CopyDirection to_backend{&client, &backend_socket, backend, false, buffer_size, &state};
    CopyDirection to_client{&backend_socket, &client, backend, true, buffer_size, &state};

    silk::FiberFuture future;
    if (silk::FiberScheduler::run(copyLoop, CopyDirection(to_client), &future) != 0)
        throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot allocate a fiber for the relay");
    copyLoop(&to_backend);
    future.wait();
}

}

#endif
