#include "Element.h"
#include "Array.h"
#include "Binary.h"
#include "Document.h"
#include "ObjectId.h"


namespace DB
{
namespace BSON
{

Element::~Element() = default;


BSON::Element::Ptr Element::fromTypeId(UInt8 typeId, const std::string & name)
{
    BSON::Element::Ptr element;
    switch (typeId)
    {
        case ElementTraits<double>::TypeId:
            element = new ConcreteElement<double>(name, 0);
            break;
        case ElementTraits<Int32>::TypeId:
            element = new ConcreteElement<Int32>(name, 0);
            break;
        case ElementTraits<std::string>::TypeId:
            element = new ConcreteElement<std::string>(name, "");
            break;
        case ElementTraits<Document::Ptr>::TypeId:
            element = new ConcreteElement<Document::Ptr>(name, new Document);
            break;
        case ElementTraits<bool>::TypeId:
            element = new ConcreteElement<bool>(name, false);
            break;
        case ElementTraits<Int64>::TypeId:
            element = new ConcreteElement<Int64>(name, 0);
            break;
        case ElementTraits<Array::Ptr>::TypeId:
            element = new ConcreteElement<Array::Ptr>(name, new Array);
            break;
        case ElementTraits<Binary::Ptr>::TypeId:
            element = new ConcreteElement<Binary::Ptr>(name, new Binary);
            break;
        case ElementTraits<Poco::Timestamp>::TypeId:
            element = new ConcreteElement<Poco::Timestamp>(name, Poco::Timestamp());
            break;
        case ElementTraits<ObjectId::Ptr>::TypeId:
            element = new ConcreteElement<ObjectId::Ptr>(name, new ObjectId);
            break;
        case ElementTraits<NullValue>::TypeId:
            element = new ConcreteElement<NullValue>(name, NullValue());
            break;
        default: {
            throw Poco::NotImplementedException(fmt::format("Element {} contains an unsupported type {}", name, static_cast<int>(typeId)));
        }
            // TODO: everything else:)
    }
    return element;
}


UInt8 Element::typeIdFromString(const std::string & type)
{
    // TODO add other types
    if (type == "Int32")
        return ElementTraits<Int32>::TypeId;
    else if (type == "Int64")
        return ElementTraits<Int64>::TypeId;
    else if (type == "UInt64")
    {
        LOG_WARNING(getLogger("MongoDB::typeIdFromString"), "Converting UInt64 to Int64");
        return ElementTraits<Int64>::TypeId;
    }
    else if (type == "Float64")
        return ElementTraits<double>::TypeId;
    else if (type == "String")
        return ElementTraits<std::string>::TypeId;
    else if (type == "Boolean")
        return ElementTraits<bool>::TypeId;
    else if (type == "DateTime")
        return ElementTraits<Poco::Timestamp>::TypeId;
    throw Poco::NotImplementedException(fmt::format("Cannot get TypeId from type: {}", type));
}


BSON::Element::Ptr Element::createElementWithType(const std::string & type, const std::string & name, const std::string & value)
{
    auto typeId = Element::typeIdFromString(type);
    auto element = Element::fromTypeId(typeId, name);
    element->valueFromString(value);
    return element;
}

}
}
