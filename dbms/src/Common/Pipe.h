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
    namespace ErrorCodes
    {
        extern const int CANNOT_PIPE;
    }

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

#ifndef __APPLE__
            if (0 != pipe2(fds, O_CLOEXEC))
                DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
#else
            if (0 != pipe(fds))
                DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
            if (0 != fcntl(read_fd, F_SETFD, FD_CLOEXEC))
                DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
            if (0 != fcntl(write_fd, F_SETFD, FD_CLOEXEC))
                DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
#endif
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
        }

        ~Pipe()
        {
            close();
        }
    };
}
