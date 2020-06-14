#pragma once

#include <cstddef>


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

    void setNonBlocking();
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
