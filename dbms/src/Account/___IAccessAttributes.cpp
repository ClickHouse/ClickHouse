#if 0
#include <Account/IAccessAttributes.h>
#include <assert.h>


namespace DB
{
void IAccessAttributes::copyTo(IAccessAttributes & dest) const
{
    assert(type == dest.type);
    dest.name = name;
}


bool IAccessAttributes::isEqual(const IAccessAttributes & other) const
{
    return (type == other.type) && (name == other.name);
}
}
#endif
