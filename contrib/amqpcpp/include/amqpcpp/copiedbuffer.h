/**
 *  CopiedBuffer.h
 *
 *  If an output buffer (frame) cannot immediately be sent, we copy it to
 *  memory using this CopiedBuffer class
 *
 *  @copyright 2017 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <memory>
#include <cstring>
#include "endian.h"
#include "frame.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class CopiedBuffer : public OutBuffer
{
private:
    /**
     *  The total capacity of the out buffer
     *  @var size_t
     */
    size_t _capacity;

    /**
     *  Pointer to the beginning of the buffer
     *  @var const char *
     */
    char *_buffer;

    /**
     *  Current size of the buffer
     *  @var size_t
     */
    size_t _size = 0;


protected:
    /**
     *  The method that adds the actual data
     *  @param  data
     *  @param  size
     */
    virtual void append(const void *data, size_t size) override
    {
        // copy into the buffer
        memcpy(_buffer + _size, data, size);
        
        // update the size
        _size += size;
    }

public:
    /**
     *  Constructor
     *  @param  frame
     */
    CopiedBuffer(const Frame &frame) :
        _capacity(frame.totalSize()),
        _buffer((char *)malloc(_capacity)) 
    {
        // tell the frame to fill this buffer
        frame.fill(*this);
        
        // append an end of frame byte (but not when still negotiating the protocol)
        if (frame.needsSeparator()) add((uint8_t)206);
    }

    /**
     *  Disabled copy constructor to prevent expensive copy operations
     *  @param  that
     */
    CopiedBuffer(const CopiedBuffer &that) = delete;

    /**
     *  Move constructor
     *  @param  that
     */
    CopiedBuffer(CopiedBuffer &&that) :
        _capacity(that._capacity),
        _buffer(that._buffer),
        _size(that._size)
    {
        // reset the other object
        that._buffer = nullptr;
        that._size = 0;
        that._capacity = 0;
    }

    /**
     *  Destructor
     */
    virtual ~CopiedBuffer()
    {
        // deallocate the buffer
        free(_buffer);
    }

    /**
     *  Get access to the internal buffer
     *  @return const char*
     */
    const char *data() const
    {
        // expose member
        return _buffer;
    }

    /**
     *  Current size of the output buffer
     *  @return size_t
     */
    size_t size() const
    {
        // expose member
        return _size;
    }
};

/**
 *  End of namespace
 */
}
