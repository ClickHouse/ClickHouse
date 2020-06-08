#include "NetworkDump.h"

#include <dlfcn.h>
#include <cassert>
#include <atomic>
#include <mutex>
#include <cstdint>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>


namespace NetworkDump
{

namespace
{

std::atomic<SendFunction *> original_send{};
std::atomic<RecvFunction *> original_recv{};

SendHook send_hook;
RecvHook recv_hook;

std::atomic_bool initialized{};

}

void initialize(SendHook new_send_hook, RecvHook new_recv_hook)
{
    send_hook = new_send_hook;
    recv_hook = new_recv_hook;

    initialized.store(true, std::memory_order_release);
}

struct FileWriter
{
    std::mutex mutex;
    DB::WriteBufferFromFile out;

    FileWriter(const std::string & path) : out(path) {}

    void write(uint8_t type, int fd, size_t size, const char * data)
    {
        std::lock_guard lock(mutex);

        writeBinary(type, out);
        writeBinary(fd, out);
        writeVarUInt(size, out);
        out.write(data, size);
    }
};

void dumpToFile(const std::string & path)
{
    static FileWriter writer(path);

    initialize(
        [&](ssize_t size, int fd, const void * data, size_t, int)
        {
            writer.write(0, fd, size, static_cast<const char *>(data));
        },
        [&](ssize_t size, int fd, void * data, size_t, int)
        {
            writer.write(1, fd, size, static_cast<const char *>(data));
        });
}

}


extern "C"
{

ssize_t send(int sockfd, const void * buf, size_t len, int flags)
{
    using namespace NetworkDump;

    auto original_send_loaded = original_send.load(std::memory_order_relaxed);
    if (!original_send_loaded)
    {
        void * sym = dlsym(RTLD_NEXT, "send");
        assert(sym);
        original_send_loaded = reinterpret_cast<NetworkDump::SendFunction *>(sym);
        original_send.store(original_send_loaded, std::memory_order_relaxed);
    }

    ssize_t res = original_send_loaded(sockfd, buf, len, flags);
    if (initialized.load(std::memory_order_acquire))
        send_hook(res, sockfd, buf, len, flags);
    return res;
}

ssize_t recv(int sockfd, void * buf, size_t len, int flags)
{
    using namespace NetworkDump;

    auto original_recv_loaded = original_recv.load(std::memory_order_relaxed);
    if (!original_recv_loaded)
    {
        void * sym = dlsym(RTLD_NEXT, "recv");
        assert(sym);
        original_recv_loaded = reinterpret_cast<NetworkDump::RecvFunction *>(sym);
        original_recv.store(original_recv_loaded, std::memory_order_relaxed);
    }

    ssize_t res = original_recv_loaded(sockfd, buf, len, flags);
    if (initialized.load(std::memory_order_acquire))
        recv_hook(res, sockfd, buf, len, flags);
    return res;
}

}
