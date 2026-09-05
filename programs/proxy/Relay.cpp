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


namespace DB::Proxy
{

namespace
{

/// When one direction of a relay finishes, the first to do so shuts down (not closes) both underlying
/// fds so the other fiber's in-flight io_uring operation returns. Shutting down an fd is safe to race
/// with a pending io_uring op; the fds stay valid until both fibers have joined and the caller closes
/// the sockets.
void endRelay(std::atomic<int> & finished, int client_fd, int backend_fd) noexcept
{
    if (finished.fetch_add(1, std::memory_order_acq_rel) == 0)
    {
        ::shutdown(client_fd, SHUT_RDWR);
        ::shutdown(backend_fd, SHUT_RDWR);
    }
}

/// --- Copy relay: reads into a user-space buffer and writes it out. Used when a leg is TLS-terminated
/// (its bytes must be decrypted/encrypted in user space and cannot be spliced). ---

struct CopyDirection
{
    FiberSocket * src;
    FiberSocket * dst;
    Backend * backend;
    bool to_client;
    size_t buffer_size;
    std::atomic<int> * finished;
    int client_fd;
    int backend_fd;
};

int copyLoop(CopyDirection * d) noexcept
{
    try
    {
        std::vector<char> buffer(d->buffer_size);
        while (true)
        {
            int n = d->src->receive(buffer.data(), static_cast<int>(buffer.size()));
            if (n <= 0)
                break;
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

    endRelay(*d->finished, d->client_fd, d->backend_fd);
    return 0;
}

/// --- Splice relay: moves bytes socket -> pipe -> socket entirely inside the kernel, so plaintext
/// traffic is never copied through user space. splice(2) requires a pipe on one side, hence the
/// per-direction pipe. Used only when both legs are plaintext. ---

struct SpliceDirection
{
    int src_fd;
    int dst_fd;
    Backend * backend;
    bool to_client;
    unsigned int chunk;
    std::atomic<int> * finished;
    int client_fd;
    int backend_fd;
};

int spliceLoop(SpliceDirection * d) noexcept
{
    int pipe_fd[2] = {-1, -1};
    if (::pipe2(pipe_fd, O_CLOEXEC) == 0)
    {
        /// Enlarge the pipe so a single splice can carry a full chunk (best-effort; capped by
        /// /proc/sys/fs/pipe-max-size). SPLICE_F_MOVE only: SPLICE_F_MORE would cork the socket and
        /// re-introduce the Nagle-like latency that TCP_NODELAY removes.
        ::fcntl(pipe_fd[1], F_SETPIPE_SZ, static_cast<int>(d->chunk));
        while (true)
        {
            uint64_t in_bytes = 0;
            int r = silk::FiberScheduler::splice(d->src_fd, -1, pipe_fd[1], -1, d->chunk, SPLICE_F_MOVE, &in_bytes);
            if (r != 0 || in_bytes == 0)
                break;   // error or end of input

            uint64_t remaining = in_bytes;
            while (remaining > 0)
            {
                uint64_t out_bytes = 0;
                int w = silk::FiberScheduler::splice(
                    pipe_fd[0], -1, d->dst_fd, -1, static_cast<unsigned int>(remaining), SPLICE_F_MOVE, &out_bytes);
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
        [[maybe_unused]] int err0 = ::close(pipe_fd[0]);
        [[maybe_unused]] int err1 = ::close(pipe_fd[1]);
    }

    endRelay(*d->finished, d->client_fd, d->backend_fd);
    return 0;
}

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

    std::atomic<int> finished{0};
    const int client_fd = client.fd();
    const int backend_fd = backend_socket.fd();

    if (client.plaintext() && backend_socket.plaintext())
    {
        /// Zero-copy fast path.
        const unsigned int chunk = static_cast<unsigned int>(std::max<size_t>(buffer_size, 4096));
        SpliceDirection to_backend{client_fd, backend_fd, backend, false, chunk, &finished, client_fd, backend_fd};
        SpliceDirection to_client{backend_fd, client_fd, backend, true, chunk, &finished, client_fd, backend_fd};

        silk::FiberFuture future;
        if (silk::FiberScheduler::run(spliceLoop, SpliceDirection(to_client), &future) != 0)
        {
            spliceLoop(&to_backend);
            return;
        }
        spliceLoop(&to_backend);
        future.wait();
        return;
    }

    /// A TLS-terminated leg is present: copy through user space.
    CopyDirection to_backend{&client, &backend_socket, backend, false, buffer_size, &finished, client_fd, backend_fd};
    CopyDirection to_client{&backend_socket, &client, backend, true, buffer_size, &finished, client_fd, backend_fd};

    silk::FiberFuture future;
    if (silk::FiberScheduler::run(copyLoop, CopyDirection(to_client), &future) != 0)
    {
        copyLoop(&to_backend);
        return;
    }
    copyLoop(&to_backend);
    future.wait();
}

}

#endif
