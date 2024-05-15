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

    Document & addElements(const std::vector<Element::Ptr> & elements);

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

    Keys elementNames() const;
    /// Puts all element names into std::vector.

    bool empty() const;
    /// Returns true if the document doesn't contain any documents.

    bool exists(const Key & name) const;
    /// Returns true if the document has an element with the given name.

    template <typename T>
    const T & get(const Key & name) const
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
            if (ElementTraits<T>::TypeId == element->getType())
            {
                ConcreteElement<T> * concrete = dynamic_cast<ConcreteElement<T> *>(element.get());
                if (concrete != nullptr)
                    return concrete->getValue();
            }
            throw Poco::BadCastException("Invalid type mismatch!");
        }
    }

    template <typename T>
    T & get(const Key & name)
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
            if (ElementTraits<T>::TypeId == element->getType())
            {
                ConcreteElement<T> * concrete = dynamic_cast<ConcreteElement<T> *>(element.get());
                if (concrete != nullptr)
                    return concrete->getValue();
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

        if (ElementTraits<T>::TypeId == element->getType())
        {
            ConcreteElement<T> * concrete = dynamic_cast<ConcreteElement<T> *>(element.get());
            if (concrete != nullptr)
                return concrete->getValue();
        }

        return def;
    }

    const Element::Ptr get(const Key & name) const;
    /// Returns the element with the given name.
    /// An empty element will be returned when the element is not found.

    const Element::Ptr getLast() const;

    template <typename T>
    T takeValue(const Key & name)
    {
        Element::Ptr element = take(name);
        auto casted = element.cast<ConcreteElement<T>>();
        return casted->getValue();
    }
    /// Returns Element and removes it from document

    template <typename T>
    T takeLastValue()
    {
        const Key & name = getLast()->getName();
        return takeValue<T>(name);
    }

    Element::Ptr take(const Key & name);
    Element::Ptr takeLast();

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

        return ElementTraits<T>::TypeId == element->getType();
    }

    std::vector<BSON::Element::Ptr> deconstruct() &&;

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


//
// inlines
//
inline Document & Document::addElement(Element::Ptr element)
{
    elements.push_back(element);
    return *this;
}

inline Document & Document::addElements(const std::vector<Element::Ptr> & elements_)
{
    for (const auto & element : elements_)
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


inline Document::Keys Document::elementNames() const
{
    Keys names;
    for (const auto & element : elements)
        names.push_back(element->getName());
    return names;
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

    static Document::Ptr fromString(const std::string & str)
    {
        throw Poco::NotImplementedException("Document from string is not implemented, str: {}", str);
        return nullptr;
    }
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

template <>
inline void BSONWriter::write<Document::Vector>(const Document::Vector & t)
{
    for (const auto & element : t)
        element->write(writer);
}

template <>
inline Int32 BSONWriter::getLength<Document::Vector>(const Document::Vector & t)
{
    Int32 length = 0;
    for (const auto & element : t)
        length += element->getLength();
    return length;
}


}
} // namespace DB::BSON
