#pragma once

#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <port/unistd.h>
#include <csignal>

namespace DB {

    struct Pipe
    {
        union
        {
            int fds[2];
            struct
            {
                int read_fd;
                int write_fd;
            };
        };

        Pipe()
        {
            read_fd = -1;
            write_fd = -1;

            if (0 != pipe(fds))
                DB::throwFromErrno("Cannot create pipe", 0);
        }

        void close()
        {
            if (-1 != read_fd)
            {
                ::close(read_fd);
                read_fd = -1;
            }

            if (-1 != write_fd)
            {
                ::close(write_fd);
                write_fd = -1;
            }

            LOG_INFO(&Logger::get("TraceCollector"), "Pipe is closed");
        }

        ~Pipe()
        {
            close();
        }
    };

    class PipeSingleton : public ext::singleton<Pipe>
    {
    };

}