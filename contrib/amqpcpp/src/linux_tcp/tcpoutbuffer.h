/**
 *  TcpOutBuffer.h
 *
 *  When data could not be sent out immediately, it is buffered in a temporary
 *  output buffer. This is the implementation of that buffer
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <sys/ioctl.h>
#include <sys/uio.h>
#include "openssl.h"

/**
 *  FIONREAD on Solaris is defined elsewhere
 */
#ifdef __sun
#include <sys/filio.h>
#endif

/**
 *  Set up namespace
 */
namespace AMQP {
    
/**
 *  Class definition
 */
class TcpOutBuffer
{
private:
    /**
     *  All output buffers
     *  @var std::deque
     */
    mutable std::deque<std::vector<char>> _buffers;

    /**
     *  Number of bytes in first buffer that is no longer in use
     *  @var size_t
     */
    size_t _skip = 0;
    
    /**
     *  Total number of bytes in the buffer
     *  @var size_t
     */
    size_t _size = 0;

public:
    /**
     *  Regular constructor
     */
    TcpOutBuffer() {}
    
    /**
     *  No copy'ing allowed
     *  @param  that
     */
    TcpOutBuffer(const TcpOutBuffer &that) = delete;

    /**
     *  Move operator
     *  @param  that
     */
    TcpOutBuffer(TcpOutBuffer &&that) : 
        _buffers(std::move(that._buffers)), 
        _skip(that._skip), 
        _size(that._size)
    {
        // reset other object
        that._skip = 0;
        that._size = 0;
    }
    
    /**
     *  Move assignment operator
     *  @param  that
     */
    TcpOutBuffer &operator=(TcpOutBuffer &&that)
    {
        // skip self-assignment
        if (this == &that) return *this;
        
        // swap buffers
        _buffers.swap(that._buffers);
        
        // swap integers
        std::swap(_skip, that._skip);
        std::swap(_size, that._size);
        
        // done
        return *this;
    }
    
    /**
     *  Does the buffer exist (is it non-empty)
     *  @return bool
     */
    operator bool () const
    {
        // there must be a size
        return _size > 0;
    }
    
    /**
     *  Is the buffer empty
     *  @return bool
     */
    bool operator!() const
    {
        // size should be zero
        return _size == 0;
    }

    /**
     *  Total size of the buffer
     *  @return size_t
     */
    size_t size() const
    {
        // this simply is a member
        return _size;
    }

    /**
     *  Add data to the buffer
     *  @param  buffer
     *  @param  size
     */
    void add(const char *buffer, size_t size)
    {
        // add element
        _buffers.emplace_back(buffer, buffer + size);
    
        // update total size
        _size += size;
    }
    
    /**
     *  Shrink the buffer with a number of bytes
     *  @param  toremove
     */
    void shrink(size_t toremove)
    {
        // are we removing everything?
        if (toremove >= _size)
        {
            // reset all
            _buffers.clear(); 
            _skip = _size = 0;
        }
        else
        {
            // keep looping
            while (toremove > 0)
            {
                // access to the first buffer
                const auto &first = _buffers.front();
                
                // actual used bytes in first buffer
                size_t bytes = first.size() - _skip;
                
                // can we remove the first buffer completely?
                if (toremove >= bytes)
                {
                    // we're going to remove the first item, update sizes
                    _size -= bytes;
                    _skip = 0;
                    
                    // number of bytes that still have to be removed
                    toremove -= bytes;
                    
                    // remove first buffer
                    _buffers.pop_front();
                }
                else
                {
                    // we should remove the first buffer partially
                    _skip += toremove;
                    _size -= toremove;
                    
                    // done
                    toremove = 0;
                }
            }
        }
    }
    
    /**
     *  Clear the buffer
     */
    void clear()
    {
        // clear all buffers
        _buffers.clear();
        
        // reset members
        _skip = _size = 0;
    }
    
    /**
     *  Fill an iovec buffer
     *  @param  buffers     the buffers to be filled
     *  @param  count       number of buffers available
     *  @return size_t      the number of buffers that were filled
     */
    size_t fill(struct iovec buffers[], size_t count) const
    {
        // index counter
        size_t index = 0;

        // iterate over the buffers
        for (const auto &str : _buffers)
        {
            // fill buffer
            buffers[index].iov_base = (void *)(index == 0 ? str.data() + _skip : str.data());
            buffers[index].iov_len = index == 0 ? str.size() - _skip : str.size();
            
            // update counter for next iteration
            if (++index >= count) return count;
        }
        
        // done
        return index;
    }
    
    /**
     *  Send the buffer to a socket
     *  @param  socket          the socket to send data to
     *  @return ssize_t         number of bytes sent (or the same result as sendmsg() in case of an error)
     */
    ssize_t sendto(int socket)
    {
        // total number of bytes written
        ssize_t total = 0;
        
        // keep looping
        while (_size > 0)
        {
            // we're going to fill a lot of buffers (64 should normally be enough)
            struct iovec buffer[64];
            
            // create the message header
            struct msghdr header;

            // make sure the members of the header are empty
            memset(&header, 0, sizeof(header));

            // save the buffers in the message header
            header.msg_iov = buffer;
            header.msg_iovlen = fill(buffer, 64);
            
            // do nothing if no buffers were filled
            if (header.msg_iovlen == 0) break;

            // send the data
            auto result = sendmsg(socket, &header, AMQP_CPP_MSG_NOSIGNAL);

            // skip on error, or when nothing was written
            if (result <= 0) return total > 0 ? total : result;

            // shrink the buffer
            shrink(result);

            // update total number of bytes written
            total += result;
        }
        
        // done
        return total;
    }
    
    /**
     *  Send the buffer to an SSL connection
     *  @param  ssl         the ssl context to send data to
     *  @return ssize_t     number of bytes sent, or the return value of ssl_write
     */
    ssize_t sendto(SSL *ssl)
    {
        // we're going to fill a lot of buffers (for ssl only one buffer at a time can be sent)
        struct iovec buffer[1];
        
        // fill the buffers, and leap out if there is no data
        auto buffers = fill(buffer, 1);
        
        // just to be sure we do this check
        if (buffers == 0) return 0;
        
        // make sure that the error queue is currently completely empty, so the error queue can be checked
        OpenSSL::ERR_clear_error();

        // send the data
        auto result = OpenSSL::SSL_write(ssl, buffer[0].iov_base, buffer[0].iov_len);
        
        // on success we shrink the buffer
        if (result > 0) shrink(result);
        
        // done
        return result;
    }
};
    
/**
 *  End of namespace
 */
}

