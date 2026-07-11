#include "Relay.h"

#if USE_SILK

#include <Common/Exception.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <atomic>
#include <vector>

#include <sys/socket.h>


namespace DB::Proxy
{

namespace
{

struct Direction
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

/// Copy bytes from src to dst until end of stream or error. On completion, the first direction to
/// finish shuts down both underlying fds (not close) so the other fiber's pending read returns.
/// Shutting down an fd is safe to race with an in-flight io_uring read; the fds stay valid until
/// both fibers have joined and the caller closes the sockets.
int copyLoop(Direction * d) noexcept
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
        /// A read or write error simply ends the relay for this connection.
    }

    if (d->finished->fetch_add(1, std::memory_order_acq_rel) == 0)
    {
        ::shutdown(d->client_fd, SHUT_RDWR);
        ::shutdown(d->backend_fd, SHUT_RDWR);
    }
    return 0;
}

}

void runRelay(
    FiberSocket & client,
    FiberSocket & backend_socket,
    Backend * backend,
    const String & initial_to_backend,
    size_t buffer_size)
{
    if (!initial_to_backend.empty())
    {
        backend_socket.sendAll(initial_to_backend.data(), initial_to_backend.size());
        if (backend)
            backend->addBytesFromClient(initial_to_backend.size());
    }

    std::atomic<int> finished{0};
    const int client_fd = client.raw().impl()->sockfd();
    const int backend_fd = backend_socket.raw().impl()->sockfd();

    Direction to_backend{&client, &backend_socket, backend, false, buffer_size, &finished, client_fd, backend_fd};
    Direction to_client{&backend_socket, &client, backend, true, buffer_size, &finished, client_fd, backend_fd};

    /// Run the backend-to-client direction on a child fiber and the client-to-backend direction inline.
    /// silk copies the parameter struct into the fiber; its pointers reference this frame, which
    /// outlives the child because we wait on the future below.
    silk::FiberFuture future;
    int run_result = silk::FiberScheduler::run(copyLoop, Direction(to_client), &future);
    if (run_result != 0)
    {
        /// Could not allocate a fiber: fall back to a single direction so the connection still drains.
        copyLoop(&to_backend);
        return;
    }

    copyLoop(&to_backend);
    future.wait();
}

}

#endif
