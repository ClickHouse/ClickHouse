#pragma once

#include <Poco/NumberFormatter.h>
#include "Document.h"


namespace DB
{
namespace BSON
{

class Array : public Document
/// This class represents a BSON Array.
{
public:
    using Ptr = Poco::SharedPtr<Array>;

    Array();
    /// Creates an empty Array.

    ~Array() override;
    /// Destroys the Array.

    // Document template functions available for backward compatibility
    using Document::add;
    using Document::get;
    using Document::size;

    template <typename T>
    Document & add(T value)
    /// Creates an element with the name from the current pos and value and
    /// adds it to the array document.
    ///
    /// The active document is returned to allow chaining of the add methods.
    {
        return Document::add<T>(Poco::NumberFormatter::format(size()), value);
    }

    Document & add(const char * value)
    /// Creates an element with a name from the current pos and value and
    /// adds it to the array document.
    ///
    /// The active document is returned to allow chaining of the add methods.
    {
        return Document::add(Poco::NumberFormatter::format(size()), value);
    }

    template <typename T>
    T get(std::size_t pos) const
    /// Returns the element at the given index and tries to convert
    /// it to the template type. If the element is not found, a
    /// Poco::NotFoundException will be thrown. If the element cannot be
    /// converted a BadCastException will be thrown.
    {
        return Document::get<T>(Poco::NumberFormatter::format(pos));
    }

    template <typename T>
    T & get(std::size_t pos)
    /// Returns the element at the given index and tries to convert
    /// it to the template type. If the element is not found, a
    /// Poco::NotFoundException will be thrown. If the element cannot be
    /// converted a BadCastException will be thrown.
    {
        return Document::get<T>(Poco::NumberFormatter::format(pos));
    }

    template <typename T>
    T get(std::size_t pos, const T & deflt) const
    /// Returns the element at the given index and tries to convert
    /// it to the template type. If the element is not found, or
    /// has the wrong type, the deflt argument will be returned.
    {
        return Document::get<T>(Poco::NumberFormatter::format(pos), deflt);
    }

    Element::Ptr get(std::size_t pos) const;
    /// Returns the element at the given index.
    /// An empty element will be returned if the element is not found.

    template <typename T>
    bool isType(std::size_t pos) const
    /// Returns true if the type of the element equals the TypeId of ElementTrait,
    /// otherwise false.
    {
        return Document::isType<T>(Poco::NumberFormatter::format(pos));
    }

    std::size_t size() const { return Document::size(); }

    std::string toString() const override;
    /// Returns a string representation of the Array.

private:
    friend Array::Ptr BSONReader::read<Array::Ptr>();
};


// BSON Embedded Array
// spec: document
template <>
struct ElementTraits<Array::Ptr>
{
    enum
    {
        TypeId = 0x04
    };

    static std::string toString(const Array::Ptr & value) { return value.isNull() ? "null" : value->toString(); }

    static Array::Ptr fromString(const std::string & str)
    {
        throw Poco::NotImplementedException("Array from string is not implemented, str: {}", str);
        return nullptr;
    }
};


template <>
inline Array::Ptr BSONReader::read<Array::Ptr>()
{
    Array::Ptr array = new Array();
    array->read(reader);
    return array;
}


template <>
inline void BSONWriter::write<Array::Ptr>(const Array::Ptr & t)
{
    t->write(writer);
}

template <>
inline Int32 BSONWriter::getLength<Array::Ptr>(const Array::Ptr & t)
{
    return t->getLength();
}
}
}
