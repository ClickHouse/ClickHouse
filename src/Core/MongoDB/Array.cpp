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
    std::ostringstream oss;
    oss << "[";
    for (ElementSet::const_iterator it = elements.begin(); it != elements.end(); ++it)
    {
        if (it != elements.begin())
            oss << ",";
        oss << (*it)->toString();
    }
    oss << "]";
    return oss.str();
}
}
} // Namespace DB::BSON
