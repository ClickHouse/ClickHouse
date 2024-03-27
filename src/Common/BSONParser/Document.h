#pragma once
#include <string>
#include <IO/ReadBuffer.h>
#include <Poco/SharedPtr.h>
#include <Common/logger_useful.h>
#include "BSONReader.h"
#include "Element.h"

namespace DB
{
namespace BSON
{

class Array;

class ElementFindByName
{
public:
    explicit ElementFindByName(const Element::Key & name_) : name(name_) { }

    bool operator()(const Element::Ptr & element) { return !element.isNull() && element->getName() == name; }

private:
    Element::Key name;
};


class Document
/// Represents a MongoDB (BSON) document.
{
public:
    using Ptr = Poco::SharedPtr<Document>;
    using Vector = std::vector<Document::Ptr>;
    using Key = std::string;
    using Keys = std::vector<Key>;

    Document() = default;
    /// Creates an empty Document.

    virtual ~Document();
    /// Destroys the Document.

    Document & addElement(Element::Ptr element);
    /// Add an element to the document.
    ///
    /// The active document is returned to allow chaining of the add methods.

    template <typename T>
    Document & add(const Key & name, T value)
    /// Creates an element with the given name and value and
    /// adds it to the document.
    ///
    /// The active document is returned to allow chaining of the add methods.
    {
        return addElement(new ConcreteElement<T>(name, value));
    }

    template <typename T>
    Document & add(const char * name, T value)
    {
        return add(std::string(name), value);
    }

    Document & add(const Key & name, const char * value)
    /// Creates an element with the given name and value and
    /// adds it to the document.
    ///
    /// The active document is returned to allow chaining of the add methods.
    {
        addElement(new ConcreteElement<std::string>(name, std::string(value)));
        return *this;
    }

    Document & addNewDocument(const Key & name);
    /// Create a new document and add it to this document.
    /// Unlike the other add methods, this method returns
    /// a reference to the new document.

    Array & addNewArray(const std::string & name);
    /// Create a new array and add it to this document.
    /// Method returns a reference to the new array.

    void clear();
    /// Removes all elements from the document.

    void elementNames(Keys & keys) const;
    /// Puts all element names into std::vector.

    bool empty() const;
    /// Returns true if the document doesn't contain any documents.

    bool exists(const Key & name) const;
    /// Returns true if the document has an element with the given name.

    template <typename T>
    T get(const Key & name) const
    /// Returns the element with the given name and tries to convert
    /// it to the template type. When the element is not found, a
    /// NotFoundException will be thrown. When the element can't be
    /// converted a BadCastException will be thrown.
    {
        Element::Ptr element = get(name);
        if (element.isNull())
        {
            throw Poco::NotFoundException(name);
        }
        else
        {
            if (ElementTraits<T>::TypeId == element->type())
            {
                ConcreteElement<T> * concrete = dynamic_cast<ConcreteElement<T> *>(element.get());
                if (concrete != 0)
                    return concrete->value();
            }
            throw Poco::BadCastException("Invalid type mismatch!");
        }
    }

    template <typename T>
    T get(const Key & name, const T & def) const
    /// Returns the element with the given name and tries to convert
    /// it to the template type. When the element is not found, or
    /// has the wrong type, the def argument will be returned.
    {
        Element::Ptr element = get(name);
        if (element.isNull())
            return def;

        if (ElementTraits<T>::TypeId == element->type())
        {
            ConcreteElement<T> * concrete = dynamic_cast<ConcreteElement<T> *>(element.get());
            if (concrete != 0)
                return concrete->value();
        }

        return def;
    }

    Element::Ptr get(const Key & name) const;
    /// Returns the element with the given name.
    /// An empty element will be returned when the element is not found.

    Int64 getInteger(const Key & name) const;
    /// Returns an integer. Useful when MongoDB returns Int32, Int64
    /// or double for a number (count for example). This method will always
    /// return an Int64. When the element is not found, a
    /// Poco::NotFoundException will be thrown.

    bool remove(const Key & name);
    /// Removes an element from the document.

    template <typename T>
    bool isType(const Key & name) const
    /// Returns true when the type of the element equals the TypeId of ElementTrait.
    {
        Element::Ptr element = get(name);
        if (element.isNull())
            return false;

        return ElementTraits<T>::TypeId == element->type();
    }

    Int32 read(ReadBuffer & reader);
    /// Reads a document from the reader

    std::size_t size() const;
    /// Returns the number of elements in the document.

    virtual std::string toString() const;
    /// Returns a String representation of the document.

    void write(WriteBuffer & writer) const;
    /// Writes a document to the writer

    virtual Int32 getLength() const;
    /// How many bytes will by written if `write` is called

    Document(Document && other) noexcept;

    Document & operator=(Document && other) noexcept;

protected:
    ElementSet elements;
};

Document::~Document() = default;

//
// inlines
//
inline Document & Document::addElement(Element::Ptr element)
{
    elements.push_back(element);
    return *this;
}


inline Document & Document::addNewDocument(const Document::Key & name)
{
    Document::Ptr new_doc = new Document();
    add(name, new_doc);
    return *new_doc;
}


inline void Document::clear()
{
    elements.clear();
}


inline bool Document::empty() const
{
    return elements.empty();
}


inline void Document::elementNames(Document::Keys & keys) const
{
    for (const auto & element : elements)
        keys.push_back(element->getName());
}


inline bool Document::exists(const Document::Key & name) const
{
    return std::find_if(elements.begin(), elements.end(), ElementFindByName(name)) != elements.end();
}


inline bool Document::remove(const Document::Key & name)
{
    auto it = std::find_if(elements.begin(), elements.end(), ElementFindByName(name));
    if (it == elements.end())
        return false;

    elements.erase(it);
    return true;
}


inline std::size_t Document::size() const
{
    return elements.size();
}


void Document::write(WriteBuffer & writer) const
{
    if (elements.empty())
    {
        Int32 magic = sizeof(Int32) + sizeof('\0');
        writeIntBinary(magic, writer);
        writer.write('\0');
        return;
    }
    Int32 doc_size = this->getLength();
    writeIntBinary(doc_size, writer);
    for (const auto & element : elements)
        element->write(writer);
    writer.write('\0');
}

Int32 Document::getLength() const
{
    if (elements.empty())
        return sizeof(Int32) + sizeof('\0');
    Int32 length = sizeof(Int32);
    for (const auto & element : elements)
        length += element->getLength();
    return length + sizeof('\0');
}


// BSON Embedded Document
// spec: document
template <>
struct ElementTraits<Document::Ptr>
{
    enum
    {
        TypeId = 0x03
    };

    static std::string toString(const Document::Ptr & value) { return value.isNull() ? "null" : value->toString(); }
};


template <>
inline Document::Ptr BSONReader::read<Document::Ptr>()
{
    Document::Ptr new_doc = new Document();
    new_doc->read(reader);
    return new_doc;
}


template <>
inline void BSONWriter::write<Document::Ptr>(const Document::Ptr & t)
{
    t->write(writer);
}

template <>
inline Int32 BSONWriter::getLength<Document::Ptr>(const Document::Ptr & t)
{
    return t->getLength();
}

Element::Ptr Document::get(const Document::Key & name) const
{
    Element::Ptr element;

    ElementSet::const_iterator it = std::find_if(elements.begin(), elements.end(), ElementFindByName(name));
    if (it != elements.end())
        return *it;

    return element;
}


Int64 Document::getInteger(const Document::Key & name) const
{
    Element::Ptr element = get(name);
    if (element.isNull())
        throw Poco::NotFoundException(name);

    if (ElementTraits<double>::TypeId == element->type())
    {
        ConcreteElement<double> * concrete = dynamic_cast<ConcreteElement<double> *>(element.get());
        if (concrete)
            return static_cast<Int64>(concrete->getValue());
    }
    else if (ElementTraits<Int32>::TypeId == element->type())
    {
        ConcreteElement<Int32> * concrete = dynamic_cast<ConcreteElement<Int32> *>(element.get());
        if (concrete)
            return concrete->getValue();
    }
    else if (ElementTraits<Int64>::TypeId == element->type())
    {
        ConcreteElement<Int64> * concrete = dynamic_cast<ConcreteElement<Int64> *>(element.get());
        if (concrete)
            return concrete->getValue();
    }
    throw Poco::BadCastException("Invalid type mismatch!");
}


std::string Document::toString() const
{
    std::ostringstream oss;
    oss << '{';
    for (ElementSet::const_iterator it = elements.begin(); it != elements.end(); ++it)
    {
        if (it != elements.begin())
            oss << ',';

        oss << '"' << (*it)->getName() << '"';

        oss << (*it)->toString();
    }
    oss << '}';
    return oss.str();
}

Document::Document(Document && other) noexcept
{
    this->elements = other.elements;
}

Document & Document::operator=(Document && other) noexcept
{
    swap(this->elements, other.elements);
    return *this;
}


}
} // namespace DB::BSON
