/**
 *  Array.cpp
 *
 *  Implementation of an array
 *
 */
#include "includes.h"

// we live in the copernica namespace
namespace AMQP {

/**
 *  Constructor based on incoming frame
 *  @param  frame
 */
Array::Array(ReceivedFrame &frame)
{
    // use this to see if we've read too many bytes.
    uint32_t charsToRead = frame.nextUint32();

    // keep going until all data is read
    while (charsToRead > 0)
    {
        // one byte less for the field type
        charsToRead -= 1;

        // read the field type and construct the field
        Field *field = Field::decode(frame);
        if (!field) continue;

        // less bytes to read
        charsToRead -= (uint32_t)field->size();

        // add the additional field
        _fields.push_back(std::shared_ptr<Field>(field));
    }
}

/**
 *  Copy constructor
 *  @param  array
 */
Array::Array(const Array &array)
{
    // loop through the other array
    for (auto iter = array._fields.begin(); iter != array._fields.end(); iter++)
    {
        // add to this vector
        _fields.push_back(std::shared_ptr<Field>((*iter)->clone()));
    }
}

/**
 *  Get a field
 *
 *  If the field does not exist, an empty string is returned
 *
 *  @param  index   field index
 *  @return Field
 */
const Field &Array::get(uint8_t index) const
{
    // used if index does not exist
    static ShortString empty;

    // check whether we have that many elements
    if (index >= _fields.size()) return empty;

    // get value
    return *_fields[index];
}

/**
 *  Number of entries in the array
 *  @return uint32_t
 */
uint32_t Array::count() const
{
  return (uint32_t)_fields.size();
}

/**
 *  Remove a field from the array
 */
void Array::pop_back()
{
    _fields.pop_back();
}

/**
 *  Add a field to the array
 *  @param  value
 */
void Array::push_back(const Field& value)
{
    _fields.push_back(std::shared_ptr<Field>(value.clone()));
}

/**
 *  Get the size this field will take when
 *  encoded in the AMQP wire-frame format
 *  @return size_t
 */
size_t Array::size() const
{
    // store the size (four bytes for the initial size)
    size_t size = 4;

    // iterate over all elements
    for (auto item : _fields)
    {
        // add the size of the field type and size of element
        size += sizeof(item->typeID());
        size += item->size();
    }

    // return the result
    return size;
}

/**
 *  Write encoded payload to the given buffer.
 *  @param  buffer
 */
void Array::fill(OutBuffer& buffer) const
{
    // store total size for all elements
    buffer.add(static_cast<uint32_t>(size()-4));

    // iterate over all elements
    for (auto item : _fields)
    {
        // encode the element type and element
        buffer.add((uint8_t)item->typeID());
        item->fill(buffer);
    }
}

// end namespace
}
