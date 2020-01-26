/**
 *  Numeric field types for AMQP
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <memory>
#include <type_traits>
#include "receivedframe.h"
#include "outbuffer.h"
#include "field.h"
#include <ostream>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Template for numeric field types
 */
template<
    typename T,
    char F,
    typename = typename std::enable_if<std::is_arithmetic<T>::value, T>
>
class NumericField : public Field
{
private:
    /**
     *  Field value
     */
    T _value;

public:
    using Type = T;

    /**
     *  Default constructor, assign 0
     */
    NumericField() : _value(0) {}

    /**
     *  Construct numeric field from
     *  one of numeric types
     *
     *  @param  value   field value
     */
    NumericField(T value) : _value(value) {}

    /**
     *  Parse based on incoming buffer
     *  @param  frame
     */
    NumericField(ReceivedFrame &frame)
    {
// The Microsoft Visual Studio compiler thinks that there is an issue
// with the following code, so we temporarily disable a specific warning
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4244)
#endif

        if      (std::is_same<int8_t,   typename std::remove_cv<T>::type>::value) _value = frame.nextInt8();
        else if (std::is_same<int16_t,  typename std::remove_cv<T>::type>::value) _value = frame.nextInt16();
        else if (std::is_same<int32_t,  typename std::remove_cv<T>::type>::value) _value = frame.nextInt32();
        else if (std::is_same<int64_t,  typename std::remove_cv<T>::type>::value) _value = frame.nextInt64();
        else if (std::is_same<uint8_t,  typename std::remove_cv<T>::type>::value) _value = frame.nextUint8();
        else if (std::is_same<uint16_t, typename std::remove_cv<T>::type>::value) _value = frame.nextUint16();
        else if (std::is_same<uint32_t, typename std::remove_cv<T>::type>::value) _value = frame.nextUint32();
        else if (std::is_same<uint64_t, typename std::remove_cv<T>::type>::value) _value = frame.nextUint64();
        else if (std::is_same<float,    typename std::remove_cv<T>::type>::value) _value = frame.nextFloat();
        else if (std::is_same<double,   typename std::remove_cv<T>::type>::value) _value = frame.nextDouble();

// re-enable the warning
#if defined(_MSC_VER)
#pragma warning( pop )
#endif
    }

    /**
     *  Destructor
     */
    virtual ~NumericField() {}

    /**
     *  Create a new instance of this object
     *  @return Field*
     */
    virtual std::shared_ptr<Field> clone() const override
    {
        // create a new copy of ourselves and return it
        return std::make_shared<NumericField>(_value);
    }

    /**
     *  Assign a new value
     *
     *  @param  value   new value for field
     *  @return NumericField
     */
    NumericField& operator=(T value)
    {
        _value = value;
        return *this;
    };

    /**
     *  Get the value
     *  @return mixed
     */
    operator uint8_t () const override { return (uint8_t)_value; }
    operator uint16_t() const override { return (uint16_t)_value; }
    operator uint32_t() const override { return (uint32_t)_value; }
    operator uint64_t() const override { return (uint64_t)_value; }
    operator int8_t  () const override { return (int8_t)_value; }
    operator int16_t () const override { return (int16_t)_value; }
    operator int32_t () const override { return (int32_t)_value; }
    operator int64_t () const override { return (int64_t)_value; }
    operator float () const override { return (float)_value; }
    operator double () const override { return (double)_value; }

    /**
     *  Get the value
     *  @return mixed
     */
    T value() const
    {
        // return internal value
        return _value;
    }

    /**
     *  We are an integer field
     *
     *  @return true, because we are an integer
     */
    bool isInteger() const override
    {
        return std::is_integral<T>::value;
    }

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     *  @return size_t
     */
    virtual size_t size() const override
    {
        // numeric types have no extra storage requirements
        return sizeof(_value);
    }

    /**
     *  Write encoded payload to the given buffer.
     *  @param  buffer      OutBuffer to write to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // store converted value
        T value = _value;

        // write to buffer
        // adding a value takes care of host to network byte order
        buffer.add(value);
    }

    /**
     *  Get the type ID that is used to identify this type of
     *  field in a field table
     */
    virtual char typeID() const override
    {
        return F;
    }

    /**
     *  Output the object to a stream
     *  @param std::ostream
     */
    virtual void output(std::ostream &stream) const override
    {
        // show
        stream << "numeric(" << value() << ")";
    }
};

/**
 *  Concrete numeric types for AMQP
 */
typedef NumericField<int8_t, 'b'>   Octet;
typedef NumericField<uint8_t, 'B'>  UOctet;
typedef NumericField<int16_t, 'U'>  Short;
typedef NumericField<uint16_t, 'u'> UShort;
typedef NumericField<int32_t, 'I'>  Long;
typedef NumericField<uint32_t, 'i'> ULong;
typedef NumericField<int64_t, 'L'>  LongLong;
typedef NumericField<uint64_t, 'l'> ULongLong;
typedef NumericField<uint64_t, 'T'> Timestamp;

/**
 *  Concrete floating-point types for AMQP
 */
typedef NumericField<float, 'f'>    Float;
typedef NumericField<double, 'd'>   Double;

/**
 *  end namespace
 */
}
