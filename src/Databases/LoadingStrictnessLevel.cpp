#include <Databases/LoadingStrictnessLevel.h>
#include <cassert>

namespace DB
{

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore)
{
    if (force_restore)
    {
        assert(attach);
        assert(force_attach);
        return LoadingStrictnessLevel::FORCE_RESTORE;
    }

    if (force_attach)
    {
        assert(attach);
        return LoadingStrictnessLevel::FORCE_ATTACH;
    }

    if (attach)
        return LoadingStrictnessLevel::ATTACH;

    return LoadingStrictnessLevel::CREATE;
}

}
