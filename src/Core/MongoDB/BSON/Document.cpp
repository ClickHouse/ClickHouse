#include "Document.h"
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "Array.h"
#include "Binary.h"
#include "ObjectId.h"

namespace DB
{
namespace BSON
{

Int32 Document::read(ReadBuffer & reader)
{
    Int32 size;
    try
    {
        readIntBinary(size, reader);
    }
    catch (const std::exception &)
    {
        return 0;
    }

    char type;
    if (!reader.read(type))
        throw std::exception(); // FIXME


    while (type != '\0')
    {
        Element::Ptr element;

        std::string name;
        readNullTerminated(name, reader);
        element = Element::fromTypeId(type, name);
        element->read(reader);
        elements.push_back(element);

        if (!reader.read(type))
            throw std::exception(); // FIXME
    }
    return size;
}

Array & Document::addNewArray(const std::string & name)
{
    Array::Ptr new_array = new Array();
    add(name, new_array);
    return *new_array;
}


Document::~Document() = default;


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


const Element::Ptr Document::get(const Document::Key & name) const
{
    Element::Ptr element;

    ElementSet::const_iterator it = std::find_if(elements.begin(), elements.end(), ElementFindByName(name));
    if (it != elements.end())
        return *it;

    return element;
}

const Element::Ptr Document::getLast() const
{
    if (elements.empty())
        throw Poco::RuntimeException("Getting last element from an empty document");
    return elements.back();
}


Element::Ptr Document::take(const Key & name)
{
    ElementSet::const_iterator it = std::find_if(elements.begin(), elements.end(), ElementFindByName(name));
    if (it != elements.end())
    {
        Element::Ptr elem = *it;
        remove(name);
        return elem;
    }
    return nullptr;
}

Element::Ptr Document::takeLast()
{
    auto last = elements.back();
    elements.pop_back();
    return last;
}


Int64 Document::getInteger(const Document::Key & name) const
{
    Element::Ptr element = get(name);
    if (element.isNull())
        throw Poco::NotFoundException(name);

    if (ElementTraits<double>::TypeId == element->getType())
    {
        ConcreteElement<double> * concrete = dynamic_cast<ConcreteElement<double> *>(element.get());
        if (concrete)
            return static_cast<Int64>(concrete->getValue());
    }
    else if (ElementTraits<Int32>::TypeId == element->getType())
    {
        ConcreteElement<Int32> * concrete = dynamic_cast<ConcreteElement<Int32> *>(element.get());
        if (concrete)
            return concrete->getValue();
    }
    else if (ElementTraits<Int64>::TypeId == element->getType())
    {
        ConcreteElement<Int64> * concrete = dynamic_cast<ConcreteElement<Int64> *>(element.get());
        if (concrete)
            return concrete->getValue();
    }
    throw Poco::BadCastException("Invalid type mismatch!");
}

std::vector<BSON::Element::Ptr> Document::deconstruct() &&
{
    return std::move(elements);
}


std::string Document::toString() const
{
    WriteBufferFromOwnString writer;
    writeChar('{', writer);
    for (size_t i = 0; i < elements.size(); i++)
    {
        if (i != 0)
            writeChar(',', writer);

        writeChar('"', writer);
        writeText(elements[i]->getName(), writer);
        writeChar('"', writer);
        writeChar(':', writer);
        writeText(elements[i]->toString(), writer);
    }
    writeChar('}', writer);
    return writer.str();
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
}
