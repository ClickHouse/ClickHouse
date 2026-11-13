#include <Databases/LoadingStrictnessLevel.h>

#include <base/defines.h>

namespace DB
{

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore, bool secondary, bool full_definition, bool restore)
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

    /// A secondary replica replaying a restored definition gets it validated by the initiator.
    if (secondary)
        return LoadingStrictnessLevel::SECONDARY_CREATE;

    if (restore)
        return LoadingStrictnessLevel::RESTORE;

    return LoadingStrictnessLevel::CREATE;
}

}
