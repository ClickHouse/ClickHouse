#pragma once

#include <Common/Exception.h>

/** For transferring information from signal handler to a separate thread.
  * If you need to do something serious in case of a signal (example: write a message to the log),
  *  then sending information to a separate thread through pipe and doing all the stuff asynchronously
  *  - is probably the only safe method for doing it.
  * (Because it's only safe to use reentrant functions in signal handlers.)
  */
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
    }

    ~Pipe()
    {
        close();
    }
};