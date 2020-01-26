/**
 *  BooleanSet.h
 *
 *  AMQP can store eight booleans in a single byte. This class
 *  is a utility class for setting and getting the eight
 *  booleans
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
#include <ostream>
#include "field.h"
#include "outbuffer.h"
#include "receivedframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class BooleanSet : public Field
{
private:
    /**
     *  The actual byte
     *  @var    uint8_t
     */
    uint8_t _byte;

public:
    /**
     *  Constructor
     *  @param  first   the first bool to set
     *  @param  second  the second bool to set
     *  @param  third   the third bool to set
     *  @param  fourth  the fourth bool to set
     *  @param  fifth   the fifth bool to set
     *  @param  sixth   the sixth bool to set
     *  @param  seventh the seventh bool to set
     *  @param  eigth   the eigth bool to set
     */
    BooleanSet(bool first = false, bool second = false, bool third = false, bool fourth = false, bool fifth = false, bool sixth = false, bool seventh = false, bool eigth = false)
    {
        _byte = 0;
        set(0, first);
        set(1, second);
        set(2, third);
        set(3, fourth);
        set(4, fifth);
        set(5, sixth);
        set(6, seventh);
        set(7, eigth);
    }

    /**
     *  Constructor based on incoming data
     *  @param  frame
     */
    BooleanSet(ReceivedFrame &frame)
    {
        _byte = frame.nextUint8();
    }

    /**
     *  Copy constructor
     *  @param  that
     */
    BooleanSet(const BooleanSet &that)
    {
        _byte = that._byte;
    }

    /**
     *  Destructor
     */
    virtual ~BooleanSet() {}

    /**
     *  Extending from field forces us to implement a clone function.
     *  @return shared_ptr
     */
    virtual std::shared_ptr<Field> clone() const override
    {
        return std::make_shared<BooleanSet>(*this);
    }

    /**
     *  Output the object to a stream
     *  @param std::ostream
     */
    virtual void output(std::ostream &stream) const override
    {
        // prefix
        stream << "booleanset(";

        // the members
        for (int i=0; i<8; i++) stream << (i == 0 ? "" : ",") << (get(i) ? 1 : 0);

        // postfix
        stream << ")";
    }

    /**
     *  Get one of the booleans
     *  @param  index   from 0 to 7, where 0 is rightmost bit
     *  @return bool
     */
    bool get(uint32_t index) const
    {
        // bigger than seven is not an option
        if (index > 7) return false;

        // magic bit manipulation...
        return 0 != ((1 << index) & _byte);
    }

    /**
     *  Set a boolean in the set
     *  @param  index
     *  @param  value
     */
    void set(uint32_t index, bool value)
    {
        // index must be valid
        if (index > 7) return;

        // are we setting or unsetting
        if (value)
        {
            // magic bit manipulation...
            _byte |= (1 << index);
        }
        else
        {
            // magic bit manipulation...
            _byte &= ~(1 << index);
        }
    }

    /**
     *  Fill the buffer
     *  @param  buffer
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        buffer.add(_byte);
    }

    /**
     *  Get the byte value
     *  @return value
     */
    uint8_t value() const
    {
        return _byte;
    }

    /**
     *  Type ID
     *  @return char
     */
    virtual char typeID() const override
    {
        return 't';
    }

    /**
     *  We are a boolean field
     *
     *  @return true, because we are a boolean
     */
    bool isBoolean() const override
    {
        return true;
    }

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     */
    virtual size_t size() const override
    {
        // booleanset takes up a single byte.
        return 1;
    }
};

/**
 *  End of namespace
 */
}
