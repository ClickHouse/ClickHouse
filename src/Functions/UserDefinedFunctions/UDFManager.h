#ifndef CLICKHOUSE_UDFMANAGER_H
#define CLICKHOUSE_UDFMANAGER_H

#include <stdexcept>
#include <mutex>
#include <unistd.h>

#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/anonymous_shared_memory.hpp>

#include "fdstream.h"

namespace bint = boost::interprocess;

constexpr auto SharedMemSize = 1u << 22u;

class TestAllocator {
public:
    struct Chunk {
        char * data;
        size_t size;
    };

    TestAllocator() : mem(bint::anonymous_shared_memory(SharedMemSize)) { }

    Chunk alloc(size_t size) {
        auto ptr = static_cast<char*>(mem.get_address()) + cur;
        cur += size;
        return {ptr, size};
    }

    void free(Chunk) { }

private:
    bint::mapped_region mem;
    size_t cur;
};

namespace DB
{
class UDFManager
{
public:
    UDFManager() = default;

    void initIsolatedProcess()
    {
        int pipe1[2];
        int pipe2[2];
        if (pipe(pipe1) != 0)
        {
            throw std::runtime_error("Pipe failed");
        }
        if (pipe(pipe2) != 0)
        {
            throw std::runtime_error("Pipe failed");
        }

        auto child = fork();
        if (child == -1)
        {
            throw std::runtime_error("Fork failed");
        }
        if (child == 0)
        {
            /// Child
            close(pipe1[0]);
            outCommands_.set_fd(pipe1[1]);
            inCommands_.set_fd(pipe2[0]);
            close(pipe2[1]);
            // setuid(uid);
            runIsolated();
        }
        else
        {
            /// Main
            inCommands_.set_fd(pipe1[0]);
            close(pipe1[1]);
            close(pipe2[0]);
            outCommands_.set_fd(pipe2[1]);
            return;
        }
    }

    void run();

    void load(std::string_view filename)
    {
        std::lock_guard lock(mx_);
        outCommands_ << "LoadLib " << filename << '\n';
    }

private:
    [[noreturn]] void runIsolated();

    TestAllocator memInput_;
    TestAllocator memOutput_;
    FDIStream inCommands_;
    FDOStream outCommands_;
    std::mutex mx_;
};

}

#endif //CLICKHOUSE_UDFMANAGER_H
