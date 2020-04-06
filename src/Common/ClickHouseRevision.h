#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace ClickHouseRevision
{

inline unsigned get()
{
    return VERSION_REVISION;
}

inline unsigned getVersionInteger()
{
    return VERSION_INTEGER;
}

}
