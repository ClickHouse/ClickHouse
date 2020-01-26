/**
 *  AMQP field array
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
#include "field.h"
#include "fieldproxy.h"
#include <vector>
#include <ostream>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  AMQP field array
 */
class Array : public Field
{
private:
    /**
     *  Definition of an array as a vector
     *  @typedef
     */
    typedef std::vector<std::shared_ptr<Field>> FieldArray;

    /**
     *  The actual fields
     *  @var FieldArray
     */
    FieldArray _fields;

public:
    /**
     *  Constructor to construct an array from a received frame
     *
     *  @param  frame   received frame
     */
    Array(ReceivedFrame &frame);

    /**
     *  Copy constructor
     *  @param  array
     */
    Array(const Array &array);

    /**
     *  Move constructor
     *  @param  array
     */
    Array(Array &&array) : _fields(std::move(array._fields)) {}

    /**
     *  Constructor for an empty Array
     */
    Array() {}

    /**
     * Destructor
     */
    virtual ~Array() {}

    /**
     *  Create a new instance of this object
     *  @return Field*
     */
    virtual std::shared_ptr<Field> clone() const override
    {
        return std::make_shared<Array>(*this);
    }

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     *  @return size_t
     */
    virtual size_t size() const override;

    /**
     *  Set a field
     *
     *  @param  index   field index
     *  @param  value   field value
     *  @return Array
     */
    Array set(uint8_t index, const Field &value)
    {
        // construct a shared pointer
        auto ptr = value.clone();

        // should we overwrite an existing record?
        if (index >= _fields.size())
        {
            // append index
            _fields.push_back(ptr);
        }
        else
        {
            // overwrite pointer
            _fields[index] = ptr;
        }

        // allow chaining
        return *this;
    }

    /**
     *  Get a field
     *
     *  If the field does not exist, an empty string is returned
     *
     *  @param  index   field index
     *  @return Field
     */
    const Field &get(uint8_t index) const;

    /**
     *  Get number of elements on this array
     *
     *  @return array size
     */
    uint32_t count() const;

    /**
     *  Remove last element from array
     */
    void pop_back();

    /**
     *  Add field to end of array
     *
     *  @param value
     */
    void push_back(const Field &value);

    /**
     *  Get a field
     *
     *  @param  index   field index
     *  @return ArrayFieldProxy
     */
    ArrayFieldProxy operator[](uint8_t index)
    {
        return ArrayFieldProxy(this, index);
    }

    /**
     *  Get a const field
     *  @param  index   field index
     *  @return Field
     */
    const Field &operator[](uint8_t index) const
    {
        return get(index);
    }

    /**
     *  Write encoded payload to the given buffer.
     *  @param  buffer
     */
    virtual void fill(OutBuffer& buffer) const override;

    /**
     *  Get the type ID that is used to identify this type of
     *  field in a field table
     *  @return char
     */
    virtual char typeID() const override
    {
        return 'A';
    }

    /**
     *  We are an array field
     *
     *  @return true, because we are an array
     */
    bool isArray() const override
    {
        return true;
    }

    /**
     *  Output the object to a stream
     *  @param std::ostream
     */
    virtual void output(std::ostream &stream) const override
    {
        // prefix
        stream << "array(";

        // is this the first iteration
        bool first = true;

        // loop through all members
        for (auto &iter : _fields)
        {
            // split with comma
            if (!first) stream << ",";

            // show output
            stream << *iter;

            // no longer first iter
            first = false;
        }

        // postfix
        stream << ")";
    }

    /**
     *  Cast to array.
     *
     *  @note:  This function may look silly and unnecessary. We are, after all, already
     *          an array. The whole reason we still have this function is that it is virtual
     *          and if we do not declare a cast to array on a pointer to base (i.e. Field)
     *          will return an empty field instead of the expected array.
     *
     *          Yes, clang gets this wrong and gives incorrect warnings here. See
     *          https://llvm.org/bugs/show_bug.cgi?id=28263 for more information
     *
     *  @return Ourselves
     */
    virtual operator const Array& () const override
    {
        // this already is an array, so no cast is necessary
        return *this;
    }
};

/**
 *  Custom output stream operator
 *  @param  stream
 *  @param  field
 *  @return ostream
 */
inline std::ostream &operator<<(std::ostream &stream, const ArrayFieldProxy &field)
{
    // get underlying field, and output that
    return stream << field.get();
}

/**
 *  end namespace
 */
}

