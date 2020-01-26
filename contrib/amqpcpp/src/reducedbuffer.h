/**
 *  ReducedBuffer.h
 *
 *  Wrapper around a buffer with a number of bytes to skip
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2014 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Open namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class ReducedBuffer : public Buffer
{
private:
    /**
     *  Pointer to the original buffer
     *  @var    Buffer
     */
    const Buffer &_buffer;
    
    /**
     *  Number of bytes to skip
     *  @var    size_t
     */
    size_t _skip;

public:
    /**
     *  Constructor
     *  @param  buffer
     *  @param  skip
     */
    ReducedBuffer(const Buffer &buffer, size_t skip) : _buffer(buffer), _skip(skip) {}
    
    /**
     *  Destructor
     */
    virtual ~ReducedBuffer() {}
    
    /**
     *  Total size of the buffer
     *  @return size_t
     */
    virtual size_t size() const override
    {
        return _buffer.size() - _skip;
    }

    /**
     *  Get access to a single byte
     *  @param  pos         position in the buffer
     *  @return char        value of the byte in the buffer
     */
    virtual char byte(size_t pos) const override
    {
        return _buffer.byte(pos + _skip);
    }

    /**
     *  Get access to the raw data
     *  @param  pos         position in the buffer
     *  @param  size        number of continuous bytes
     *  @return char*
     */
    virtual const char *data(size_t pos, size_t size) const override
    {
        return _buffer.data(pos + _skip, size);
    }
    
    /**
     *  Copy bytes to a buffer
     * 
     *  No safety checks are necessary: this method will only be called
     *  for bytes that actually exist
     * 
     *  @param  pos         position in the buffer
     *  @param  size        number of bytes to copy
     *  @param  buffer      buffer to copy into
     *  @return void*       pointer to buffer
     */
    virtual void *copy(size_t pos, size_t size, void *buffer) const override
    {
        return _buffer.copy(pos + _skip, size, buffer);
    }
};

/**
 *  End of namespace
 */
}


