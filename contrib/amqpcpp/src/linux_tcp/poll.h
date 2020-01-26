/**
 *  Poll.h
 *
 *  Class to check or wait for a socket to become readable and/or writable
 *
 *  @copyright 2018 Copernica BV
 */
 
 /**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class Poll
{
private:
    /**
     *  Set with just one filedescriptor
     *  @var fd_set
     */
    fd_set _set;

    /**
     *  The socket filedescriptor
     *  @var int
     */
    int _socket;
    
public:
    /**
     *  Constructor
     *  @param  fd      the filedescriptor that we're waiting on
     */
    Poll(int fd) : _socket(fd)
    {
        // initialize the set
        FD_ZERO(&_set);
        
        // add the one socket
        FD_SET(_socket, &_set);
    }
    
    /**
     *  No copying
     *  @param  that
     */
    Poll(const Poll &that) = delete;
    
    /**
     *  Destructor
     */
    virtual ~Poll() = default;
    
    /**
     *  Wait until the filedescriptor becomes readable
     *  @param  block       block until readable
     *  @return bool
     */
    bool readable(bool block) 
    {
        // wait for the socket
        if (block) return select(_socket + 1, &_set, nullptr, nullptr, nullptr) > 0;
        
        // we do not want to block, so we use a small timeout
        struct timeval timeout;
        
        // no timeout at all
        timeout.tv_sec = timeout.tv_usec = 0;
        
        // no timeout at all
        return select(_socket + 1, &_set, nullptr, nullptr, &timeout) > 0;
    }
        
    /**
     *  Wait until the filedescriptor becomes writable
     *  @param  block       block until readable
     *  @return bool
     */
    bool writable(bool block) 
    {
        // wait for the socket
        if (block) return select(_socket + 1, nullptr, &_set, nullptr, nullptr) > 0;

        // we do not want to block, so we use a small timeout
        struct timeval timeout;
        
        // no timeout at all
        timeout.tv_sec = timeout.tv_usec = 0;
        
        // no timeout at all
        return select(_socket + 1, nullptr, &_set, nullptr, &timeout) > 0;
    }
    
    /**
     *  Wait until a filedescriptor becomes active (readable or writable)
     *  @param  block       block until readable
     *  @return bool
     */
    bool active(bool block)
    {
        // accommodate restrict qualifier on fd_set params
        fd_set set2 = _set;

        // wait for the socket
        if (block) return select(_socket + 1, &_set, &set2, nullptr, nullptr) > 0;

        // we do not want to block, so we use a small timeout
        struct timeval timeout;
        
        // no timeout at all
        timeout.tv_sec = timeout.tv_usec = 0;
        
        // no timeout at all
        return select(_socket + 1, &_set, &set2, nullptr, &timeout) > 0;
    }
};

/**
 *  End of namespace
 */
}
