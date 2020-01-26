/**
 *  Field proxy. Returned by the table. Can be casted to the
 *  relevant native type (std::string or numeric)
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
#include <cstdint>
#include <string>
#include "stringfield.h"
#include "booleanset.h"
#include "decimalfield.h"
#include "numericfield.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class Table;
class Array;
class Field;

/**
 *  Class implementation
 */
template <typename T, typename I>
class FieldProxy
{
private:
    /**
     *  The table or array possibly holding the requested field
     */
    T *_source;

    /**
     *  The key in the table
     */
    I _index;

public:
    /**
     *  Construct the field proxy
     *
     *  @param  table   the table possibly holding the field
     *  @oaram  index   key in table map
     */
    FieldProxy(T *source, I index) :
        _source(source),
        _index(index)
    {}

    /**
     *  Assign a boolean value
     *
     *  @param  value
     */
    FieldProxy& operator=(bool value)
    {
        // assign value and allow chaining
        _source->set(_index, BooleanSet(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(uint8_t value)
    {
        // assign value and allow chaining
        _source->set(_index, UOctet(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(int8_t value)
    {
        // assign value and allow chaining
        _source->set(_index, Octet(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(uint16_t value)
    {
        // assign value and allow chaining
        _source->set(_index, UShort(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(int16_t value)
    {
        // assign value and allow chaining
        _source->set(_index, Short(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(uint32_t value)
    {
        // assign value and allow chaining
        _source->set(_index, ULong(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(int32_t value)
    {
        // assign value and allow chaining
        _source->set(_index, Long(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(uint64_t value)
    {
        // assign value and allow chaining
        _source->set(_index, ULongLong(value));
        return *this;
    }

    /**
     *  Assign a numeric value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(int64_t value)
    {
        // assign value and allow chaining
        _source->set(_index, LongLong(value));
        return *this;
    }

    /**
     *  Assign a decimal value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy& operator=(const DecimalField &value)
    {
        // assign value and allow chaining
        _source->set(_index, DecimalField(value));
        return *this;
    }

    /**
     *  Assign a string value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy &operator=(const std::string &value)
    {
        // in theory we should make a distinction between short and long string,
        // but in practive only long strings are accepted
        _source->set(_index, LongString(value));

        // allow chaining
        return *this;
    }

    /**
     *  Assign a string value
     *
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy &operator=(const char *value)
    {
        // cast to a string
        return operator=(std::string(value));
    }

    /**
     *  Assign an array value
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy &operator=(const Array &value)
    {
        // assign value and allow chaining
        _source->set(_index, value);
        return *this;
    }

    /**
     *  Assign a table value
     *  @param  value
     *  @return FieldProxy
     */
    FieldProxy &operator=(const Table &value)
    {
        // assign value and allow chaining
        _source->set(_index, value);
        return *this;
    }

    /**
     *  Get the underlying field
     *  @return Field
     */
    const Field &get() const
    {
        return _source->get(_index);
    }

    /**
     *  Get a boolean
     *  @return bool
     */
    template <typename TARGET>
    operator TARGET () const
    {
        // retrieve the value
        return _source->get(_index);
    }
};

// define types for array- and table-based field proxy
using AssociativeFieldProxy = FieldProxy<Table, std::string>;
using ArrayFieldProxy       = FieldProxy<Array, uint8_t>;

/**
 *  end namespace
 */
}

