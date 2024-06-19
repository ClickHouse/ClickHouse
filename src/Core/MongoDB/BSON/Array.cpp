#include "Array.h"


namespace DB
{
namespace BSON
{

Array::Array() = default;


Array::~Array()
{
}


Element::Ptr Array::get(std::size_t pos) const
{
    std::string name = Poco::NumberFormatter::format(pos);
    return Document::get(name);
}


std::string Array::toString() const
{
    WriteBufferFromOwnString writer;
    writeChar('[', writer);
    for (size_t i = 0; i < elements.size(); i++)
    {
        if (i != 0)
            writeChar(',', writer);
        writeText(elements[i]->toString(), writer);
    }
    writeChar(']', writer);
    return writer.str();
}
}
}
