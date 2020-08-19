#pragma once

#include <vector>
#include <utility>
#include <ostream>
#include <Core/Types.h>

namespace DB
{
    enum class ExternalLoaderStatus
    {
        NOT_LOADED, /// Object hasn't been tried to load. This is an initial state.
        LOADED, /// Object has been loaded successfully.
        FAILED, /// Object has been failed to load.
        LOADING, /// Object is being loaded right now for the first time.
        FAILED_AND_RELOADING, /// Object was failed to load before and it's being reloaded right now.
        LOADED_AND_RELOADING, /// Object was loaded successfully before and it's being reloaded right now.
        NOT_EXIST, /// Object with this name wasn't found in the configuration.
    };

    String toString(ExternalLoaderStatus status);
    std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues();
    std::ostream & operator<<(std::ostream & out, ExternalLoaderStatus status);
}
