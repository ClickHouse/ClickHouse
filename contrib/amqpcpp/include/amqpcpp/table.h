/**
 *  AMQP field table
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
#include "field.h"
#include "fieldproxy.h"
#include <vector>
#include <map>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  AMQP field table
 */
class Table : public Field
{
private:
    /**
     *  We define a custom type for storing fields
     *  @typedef    FieldMap
     */
    typedef std::map<std::string, std::shared_ptr<Field> > FieldMap;

    /**
     *  Store the fields
     *  @var    FieldMap
     */
    FieldMap _fields;

public:
    /**
     *  Constructor that creates an empty table
     */
    Table() {}

    /**
     *  Decode the data from a received frame into a table
     *
     *  @param  frame   received frame to decode
     */
    Table(ReceivedFrame &frame);

    /**
     *  Copy constructor
     *  @param  table
     */
    Table(const Table &table);

    /**
     *  Move constructor
     *  @param  table
     */
    Table(Table &&table) : _fields(std::move(table._fields)) {}

    /**
     *  Destructor
     */
    virtual ~Table() {}

    /**
     *  Assignment operator
     *  @param  table
     *  @return Table
     */
    Table &operator=(const Table &table);

    /**
     *  Move assignment operator
     *  @param  table
     *  @return Table
     */
    Table &operator=(Table &&table);

    /**
     *  Retrieve all keys in the table
     *
     *  @return Vector with all keys in the table
     */
    std::vector<std::string> keys() const;

    /**
     *  Create a new instance on the heap of this object, identical to the object passed
     *  @return Field*
     */
    virtual std::shared_ptr<Field> clone() const override
    {
        return std::make_shared<Table>(*this);
    }

    /**
     *  Get the size this field will take when
     *  encoded in the AMQP wire-frame format
     */
    virtual size_t size() const override;

    /**
     *  Set a field
     *  @param  name    field name
     *  @param  value   field value
     *  @return Table
     */
    Table &set(const std::string& name, const Field &value)
    {
        // copy to a new pointer and store it
        _fields[name] = value.clone();

        // allow chaining
        return *this;
    }

    /**
     *  Aliases for setting values
     *  @param  name
     *  @param  value
     *  @return Table&
     */
    Table &set(const std::string &name, bool value) { return set(name, BooleanSet(value)); }
    Table &set(const std::string &name, uint8_t value) { return set(name, UOctet(value)); }
    Table &set(const std::string &name, int8_t value) { return set(name, Octet(value)); }
    Table &set(const std::string &name, uint16_t value) { return set(name, UShort(value)); }
    Table &set(const std::string &name, int16_t value) { return set(name, Short(value)); }
    Table &set(const std::string &name, uint32_t value) { return set(name, ULong(value)); }
    Table &set(const std::string &name, int32_t value) { return set(name, Long(value)); }
    Table &set(const std::string &name, uint64_t value) { return set(name, ULongLong(value)); }
    Table &set(const std::string &name, int64_t value) { return set(name, LongLong(value)); }
    Table &set(const std::string &name, const std::string &value) { return set(name, LongString(value)); }
    Table &set(const std::string &name, const char *value) { return set(name, LongString(std::string(value))); }

    /**
     *  Is a certain field set in the table
     *  @param  name
     *  @return bool
     */
    bool contains(const std::string &name) const
    {
        return _fields.find(name) != _fields.end();
    }

    /**
     *  Get a field
     *
     *  If the field does not exist, an empty string field is returned
     *
     *  @param  name    field name
     *  @return         the field value
     */
    const Field &get(const std::string &name) const;

    /**
     *  Get a field
     *
     *  @param  name    field name
     */
    AssociativeFieldProxy operator[](const std::string& name)
    {
        return AssociativeFieldProxy(this, name);
    }

    /**
     *  Get a field
     *
     *  @param  name    field name
     */
    AssociativeFieldProxy operator[](const char *name)
    {
        return AssociativeFieldProxy(this, name);
    }

    /**
     *  Get a const field
     *
     *  @param  name    field name
     */
    const Field &operator[](const std::string& name) const
    {
        return get(name);
    }

    /**
     *  Get a const field
     *
     *  @param  name    field name
     */
    const Field &operator[](const char *name) const
    {
        return get(name);
    }

    /**
     *  Write encoded payload to the given buffer.
     *  @param  buffer
     */
    virtual void fill(OutBuffer& buffer) const override;

    /**
     *  Get the type ID that is used to identify this type of
     *  field in a field table
     */
    virtual char typeID() const override
    {
        return 'F';
    }

    /**
     *  We are a table
     *
     *  @return true, because we are a table
     */
    bool isTable() const override
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
        stream << "table(";

        // is this the first iteration
        bool first = true;

        // loop through all members
        for (auto &iter : _fields)
        {
            // split with comma
            if (!first) stream << ",";

            // show output
            stream << iter.first << ":" << *iter.second;

            // no longer first iter
            first = false;
        }

        // postfix
        stream << ")";
    }

    /**
     *  Cast to table.
     *
     *  @note:  This function may look silly and unnecessary. We are, after all, already
     *          a table. The whole reason we still have this function is that it is virtual
     *          and if we do not declare a cast to table on a pointer to base (i.e. Field)
     *          will return an empty field instead of the expected table.
     *
     *          Yes, clang gets this wrong and gives incorrect warnings here. See
     *          https://llvm.org/bugs/show_bug.cgi?id=28263 for more information
     *
     *  @return Ourselves
     */
    virtual operator const Table& () const override
    {
        // this already is a table, so no cast is necessary
        return *this;
    }
};

/**
 *  Custom output stream operator
 *  @param  stream
 *  @param  field
 *  @return ostream
 */
inline std::ostream &operator<<(std::ostream &stream, const AssociativeFieldProxy &field)
{
    // get underlying field, and output that
    return stream << field.get();
}

/**
 *  end namespace
 */
}

