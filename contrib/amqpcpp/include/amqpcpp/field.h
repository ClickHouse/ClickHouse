/**
 *  Available field types for AMQP
 *
 *  @copyright 2014, 2015 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <memory>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class ReceivedFrame;
class OutBuffer;
class Array;
class Table;

/**
 *  Base field class
 *
 *  This class cannot be constructed, but serves
 *  as the base class for all AMQP field types
 */
class Field
{
protected:
    /**
     *  Decode a field by fetching a type and full field from a frame
     *  The returned field is allocated on the heap!
     *  @param  frame
     *  @return Field*
     */
    static Field *decode(ReceivedFrame &frame);

public:
    /**
     *  Destructor
     */
    virtual ~Field() {}

    /**
     *  Create a new instance on the heap of this object, identical to the object passed
     *  @return Field*
     */
    virtual std::shared_ptr<Field> clone() const = 0;

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     *  @return size_t
     */
    virtual size_t size() const = 0;

    /**
     *  Write encoded payload to the given buffer.
     *  @param  buffer
     */
    virtual void fill(OutBuffer& buffer) const = 0;

    /**
     *  Get the type ID that is used to identify this type of
     *  field in a field table
     *  @return char
     */
    virtual char typeID() const = 0;

    /**
     *  Output the object to a stream
     *  @param std::ostream
     */
    virtual void output(std::ostream &stream) const = 0;

    /**
     *  Casting operators
     *  @return mixed
     */
    virtual operator const std::string& () const;
    virtual operator const char * () const { return nullptr; }
    virtual operator uint8_t () const { return 0; }
    virtual operator uint16_t () const { return 0; }
    virtual operator uint32_t () const { return 0; }
    virtual operator uint64_t () const { return 0; }
    virtual operator int8_t () const { return 0; }
    virtual operator int16_t () const { return 0; }
    virtual operator int32_t () const { return 0; }
    virtual operator int64_t () const { return 0; }
    virtual operator float () const { return 0; }
    virtual operator double () const { return 0; }
    virtual operator const Array& () const;
    virtual operator const Table& () const;

    /**
     *  Check the field type
     *
     *  @return Is the field a specific type?
     */
    virtual bool isInteger() const { return false; }
    virtual bool isDecimal() const { return false; }
    virtual bool isArray()   const { return false; }
    virtual bool isTable()   const { return false; }
    virtual bool isBoolean() const { return false; }
    virtual bool isString()  const { return false; }
};

/**
 *  Custom output stream operator
 *  @param  stream
 *  @param  field
 *  @return ostream
 */
inline std::ostream &operator<<(std::ostream &stream, const Field &field)
{
    field.output(stream);
    return stream;
}

/**
 *  end namespace
 */
}

