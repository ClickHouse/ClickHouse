#include <Databases/LoadingStrictnessLevel.h>

#include <base/defines.h>

namespace DB
{

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore, bool secondary, bool full_definition)
{
    if (force_restore)
    {
        chassert(attach);
        chassert(force_attach);
        return LoadingStrictnessLevel::FORCE_RESTORE;
    }

    if (force_attach)
    {
        chassert(attach);
        return LoadingStrictnessLevel::FORCE_ATTACH;
    }

    if (attach)
        return full_definition ? LoadingStrictnessLevel::ATTACH_FULL_DEFINITION : LoadingStrictnessLevel::ATTACH;

    if (secondary)
        return LoadingStrictnessLevel::SECONDARY_CREATE;

    return LoadingStrictnessLevel::CREATE;
}

}
