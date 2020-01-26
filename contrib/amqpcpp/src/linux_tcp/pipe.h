/**
 *  Pipe.h
 *
 *  Pipe of two filedescriptors, used to communicate between master and child thread
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <fcntl.h>
#include <unistd.h>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class Pipe
{
private:
    /**
     *  The two filedescriptors that make up the pipe
     *  @var int[]
     */
    int _fds[2];

public:
    /**
     *  Constructor
     */
    Pipe()
    {
        // construct the pipe
#ifdef _GNU_SOURCE
        if (pipe2(_fds, O_CLOEXEC) == 0) return;
#else
        if (
            pipe(_fds) == 0 &&
            fcntl(_fds[0], F_SETFD, FD_CLOEXEC) == 0 &&
            fcntl(_fds[1], F_SETFD, FD_CLOEXEC) == 0
        ) return;
#endif

        // something went wrong
        throw std::runtime_error(strerror(errno));
    }
    
    /**
     *  Destructor
     */
    virtual ~Pipe()
    {
        // close the two filedescriptors
        close(_fds[0]);
        close(_fds[1]);
    }
    
    /**
     *  Expose the filedescriptors
     *  @return int
     */
    int in() const { return _fds[0]; }
    int out() const { return _fds[1]; }
    
    /**
     *  Notify the pipe, so that the other thread wakes up
     *  @return bool success/failure (errno is set on failure)
     */
    bool notify()
    {
        // one byte to send
        char byte = 0;
        
        // send one byte over the pipe - this will wake up the other thread
        return write(_fds[1], &byte, 1) == 1;
    }
};

/**
 *  End of namespace
 */
}

