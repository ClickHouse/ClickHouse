
#include <DataStreams/NullBlockInputStream.h>

#include <sstream>

namespace DB
{
String NullBlockInputStream::getID() const
{
    std::stringstream res;
    res << this;
    return res.str();
}
}
