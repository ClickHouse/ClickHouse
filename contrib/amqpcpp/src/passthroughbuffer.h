/**
 *  PassthroughBuffer.h
 *
 *  If we can immediately pass on data to the TCP layer, we use a passthrough
 *  buffer so that we do not have to dynamically allocate memory
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
#include "amqpcpp/frame.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class PassthroughBuffer : public OutBuffer
{
private:
    /**
     *  The actual buffer
     *  @var const char *
     */
    char _buffer[4096];

    /**
     *  Current size of the buffer
     *  @var size_t
     */
    size_t _size = 0;
    
    /**
     *  Connection object (needs to be passed to the handler)
     *  @var Connection
     */
    Connection *_connection;
    
    /**
     *  Object that will send the data when the buffer is full
     *  @var ConnectionHandler
     */
    ConnectionHandler *_handler;


    /**
     *  Flush the object
     */
    void flush()
    {
        // notify the handler
        _handler->onData(_connection, _buffer, _size);

        // all data has been sent
        _size = 0;
    }

protected:
    /**
     *  The method that adds the actual data
     *  @param  data
     *  @param  size
     */
    virtual void append(const void *data, size_t size) override
    {
        // flush existing buffers if data would not fit
        if (_size > 0 && _size + size > 4096) flush();
        
        // if data would not fit anyway, we send it immediately
        if (size > 4096) return _handler->onData(_connection, (const char *)data, size);
        
        // copy data into the buffer
        memcpy(_buffer + _size, data, size);
        
        // update the size
        _size += size;
    }

public:
    /**
     *  Constructor
     *  @param  connection
     *  @param  handler
     *  @param  frame
     */
    PassthroughBuffer(Connection *connection, ConnectionHandler *handler, const Frame &frame) : _connection(connection), _handler(handler)
    {
        // tell the frame to fill this buffer
        frame.fill(*this);
        
        // append an end of frame byte (but not when still negotiating the protocol)
        if (frame.needsSeparator()) add((uint8_t)206);
    }

    /**
     *  No copying, because that would be too expensive
     *  @param  that
     */
    PassthroughBuffer(const CopiedBuffer &that) = delete;

    /**
     *  Moving is also not necessary
     *  @param  that
     */
    PassthroughBuffer(CopiedBuffer &&that) = delete;
    
    /**
     *  Destructor
     */
    virtual ~PassthroughBuffer()
    {
        // pass data to the handler
        if (_size > 0) flush();
    }
};

/**
 *  End of namespace
 */
}
