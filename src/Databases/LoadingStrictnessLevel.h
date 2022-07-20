#pragma once

namespace DB
{

enum class LoadingStrictnessLevel
{
    CREATE = 0,
    ATTACH = 1,
    FORCE_ATTACH = 2,
    FORCE_RESTORE = 3,
};

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore);

}
