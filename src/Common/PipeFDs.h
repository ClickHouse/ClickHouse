#pragma once

/// A POSIX pipe with the fault handling around it. Not built on Windows: its users here -
/// `TraceSender`, `SignalHandlers` and `ShellCommand` - are not built either, and a Windows
/// pipe is not pollable alongside sockets, so a port would want a socket pair instead.
#if !defined(OS_WINDOWS)


namespace DB
{

/** Struct containing a pipe with lazy initialization.
  * Use `open` and `close` methods to manipulate pipe and `fds_rw` field to access
  * pipe's file descriptors.
  */
struct LazyPipeFDs
{
    int fds_rw[2] = {-1, -1};

    void open();
    void close();

    /// Set O_NONBLOCK to different ends of pipe preserving existing flags.
    /// Throws an exception if fcntl was not successful.
    void setNonBlockingWrite();
    void setNonBlockingRead();
    void setNonBlockingReadWrite();

    void tryIncreaseSize(int desired_size);

    ~LazyPipeFDs();
};


/** Struct which opens new pipe on creation and closes it on destruction.
  * Use `fds_rw` field to access pipe's file descriptors.
  */
struct PipeFDs : public LazyPipeFDs
{
    PipeFDs();
};

}

#endif
