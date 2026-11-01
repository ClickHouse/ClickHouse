#include <Core/Names.h>

#include <Common/SipHash.h>

namespace DB
{

size_t NamesHash::operator()(const Names & column_names) const
{
    SipHash hash;
    for (const auto & column_name : column_names)
        hash.update(column_name);
    return hash.get64();
}

}
