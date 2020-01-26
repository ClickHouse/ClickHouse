/**
 *  OutBuffer.h
 *
 *  This is a utility class for writing various data types to a binary
 *  string, and converting the values to network byte order
 *
 *  @copyright 2014 - 2017 Copernica BV
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

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class OutBuffer
{
protected:
    /**
     *  The method that adds the actual data
     *  @param  data
     *  @param  size
     */
    virtual void append(const void *data, size_t size) = 0;

public:
    /**
     *  Destructor
     */
    virtual ~OutBuffer()
    {
    }

    /**
     *  Add a binary buffer to the buffer
     *  @param  string  char* to the string
     *  @param  size    size of string
     */
    void add(const char *string, uint32_t size)
    {
        // append data
        append(string, size);
    }

    /**
     *  Add a binary buffer to the buffer
     *  @param  string  char* to the string
     *  @param  size    size of string
     */
    void add(const std::string &string)
    {
        // add data
        append(string.c_str(), string.size());
    }

    /**
     *  add a uint8_t to the buffer
     *  @param value    value to add
     */
    void add(uint8_t value)
    {
        // append one byte
        append(&value, sizeof(value));
    }

    /**
     *  add a uint16_t to the buffer
     *  @param value    value to add
     */
    void add(uint16_t value)
    {
        // convert to network byte order
        uint16_t v = htobe16(value);
        
        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a uint32_t to the buffer
     *  @param value    value to add
     */
    void add(uint32_t value)
    {
        // convert to network byte order
        uint32_t v = htobe32(value);
        
        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a uint64_t to the buffer
     *  @param value    value to add
     */
    void add(uint64_t value)
    {
        // convert to network byte order
        uint64_t v = htobe64(value);
        
        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a int8_t to the buffer
     *  @param value    value to add
     */
    void add(int8_t value)
    {
        // append the data
        append(&value, sizeof(value));
    }

    /**
     *  add a int16_t to the buffer
     *  @param value    value to add
     */
    void add(int16_t value)
    {
        // convert to network byte order
        int16_t v = htobe16(value);
        
        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a int32_t to the buffer
     *  @param value    value to add
     */
    void add(int32_t value)
    {
        // convert into network byte order
        int32_t v = htobe32(value);

        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a int64_t to the buffer
     *  @param value    value to add
     */
    void add(int64_t value)
    {
        // copy into the buffer
        int64_t v = htobe64(value);
        
        // append the data
        append(&v, sizeof(v));
    }

    /**
     *  add a float to the buffer
     *  @param value    value to add
     */
    void add(float value)
    {
        // append the data
        append(&value, sizeof(value));
    }

    /**
     *  add a double to the buffer
     *  @param value    value to add
     */
    void add(double value)
    {
        // append the data
        append(&value, sizeof(value));
    }
};

/**
 *  End of namespace
 */
}
