#pragma once

#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>

/**
  * Struct containing a pipe with lazy initialization.
  * Use `open` and `close` methods to manipulate pipe and `fds_rw` field to access
  * pipe's file descriptors.
  */
struct LazyPipe
{
    int fds_rw[2] = {-1, -1};

    LazyPipe() = default;

    void open();

    void close();

    virtual ~LazyPipe() = default;
};

/**
  * Struct which opens new pipe on creation and closes it on destruction.
  * Use `fds_rw` field to access pipe's file descriptors.
  */
struct Pipe : public LazyPipe
{
    Pipe();

    ~Pipe();
};
