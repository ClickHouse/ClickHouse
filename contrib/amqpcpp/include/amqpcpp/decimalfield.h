/**
 *  Decimal field type for AMQP
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
#include <cmath>
#include <ostream>
#include "field.h"
#include "outbuffer.h"
#include "receivedframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class DecimalField : public Field
{
/**
 *  To preserve precision the decision is made to work with the places and number.
 *  These values are sent in the framedata, so no precision will be lost.
 *  Other options, such as floats, doubles, Decimal32 etc result in loss of precision
 *  and this is something which is not acceptable.
 *
 *  Only (in)equality and assignment operators are implemented since the decimalfield
 *  is not supposed to be altered.
 *  e.q. ==, != and =
 *
 *  When requesting the value of this object there are 3 choices;
 *  float, double or DecimalField
 *  e.g. valueFloat(), valueDouble() and value()
 */
private:
    /**
     *  The number of places, which means the number of decimals
     *  e.g. number = 1234, places = 2, true value is 12.34
     */
    uint8_t _places;

    /**
     *  The number without the decimals
     */
    uint32_t _number;

protected:
    /**
     *  Write encoded payload to the given buffer.
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // encode fields
        buffer.add(_places);
        buffer.add(_number);
    }

public:
    /**
     *  Construct decimal field
     *
     *  @param  places  the number of places
     *  @param  number  the integer number
     */
    DecimalField(uint8_t places = 0, uint32_t number = 0) :
        _places(places),
        _number(number)
    {}

    /**
     *  Construct based on incoming data
     *  @param  frame
     */
    DecimalField(ReceivedFrame &frame)
    {
        _places = frame.nextUint8();
        _number = frame.nextUint32();
    }

    /**
     *  Destructor
     */
    virtual ~DecimalField() {}

    /**
     *  Create a new identical instance of this object
     *  @return Field*
     */
    virtual std::shared_ptr<Field> clone() const override
    {
        return std::make_shared<DecimalField>(_places, _number);
    }

    /**
     *  Output the object to a stream
     *  @param std::ostream
     */
    virtual void output(std::ostream &stream) const override
    {
        // output floating point value
        stream << "decimal(" << _number / pow(10.0f, _places) << ")";
    }

    /**
     *  Assign a new value
     *
     *  @param  value   new value for field
     *  @return DecimalField
     */
    DecimalField& operator=(const DecimalField& value)
    {
        // if it's the same object, skip assignment and just return *this
        if (this == &value) return *this;

        // not the same object, copy values to this object.
        _places = value._places;
        _number = value._number;

        // allow chaining
        return *this;
    }

    /**
     *  Casts decimalfield to double
     *  e.g. "double x = decimalfield" will work
     *
     *  @return  double     value of decimalfield in double format
     */
    virtual operator double() const override
    {
        return _number / pow(10.0f, _places);
    }

    /**
     *  Casts decimalfield to float
     *  e.g. "float x = decimalfield" will work
     *
     *  @return  float     value of decimalfield in float format
     */
    virtual operator float() const override
    {
        return static_cast<float>(_number / pow(10.0f, _places));
    }

    /**
     *  Check for equality between this and another DecimalField
     *
     *  @param  value   value to be checked for equality
     *  @return boolean whether values are equal
     */
    bool operator==(const DecimalField& value) const
    {
        // check if everything is the same
        // precision is taken into account, e.q. 1.0 != 1.00
        // meaning number:10, places:1 is not equal to number:100, places:2
        return _number == value.number() && _places == value.places();
    }

    /**
     *  Check for inequality between this and another DecimalField
     *
     *  @param  value    value to be checked for inequality
     *  @return boolean  whether values are inequal
     */
    bool operator!=(const DecimalField& value) const
    {
        return !(*this == value);
    }

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     */
    virtual size_t size() const override
    {
        // the sum of all fields
        return 5;
    }

    /**
     *  Get the number of places
     *  @return uint8_t
     */
    uint8_t places() const
    {
        return _places;
    }

    /**
     *  Get the number without decimals
     *  @return uint32_t
     */
    uint32_t number() const
    {
        return _number;
    }

    /**
     *  Return the DecimalField
     *  To preserve precision DecimalField is returned, containing the number and places.
     *  @return return DecimalField
     */
    DecimalField value() const
    {
        return *this;
    }

    /**
     *  We are a decimal field
     *
     *  @return true, because we are a decimal field
     */
    bool isDecimal() const override
    {
        return true;
    }

    /**
     *  Get the type ID that is used to identify this type of
     *  field in a field table
     */
    virtual char typeID() const override
    {
        return 'D';
    }
};

/**
 *  end namespace
 */
}

