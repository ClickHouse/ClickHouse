#include <Interpreters/DistributedPlanLocalObject.h>

namespace DB
{

void DistributedPlanLocalObject::add(Kind kind, const String & name)
{
    std::call_once(once, [&] { entry = Entry{kind, name}; });
}

std::string_view DistributedPlanLocalObject::kindName(Kind kind)
{
    switch (kind)
    {
        case Kind::Dictionary: return "dictionary";
        case Kind::JoinTable: return "Join table";
        case Kind::EmbeddedDictionaries: return "the embedded dictionaries";
    }
}

}
