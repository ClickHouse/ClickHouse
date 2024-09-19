#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{
enum class ExternalLoaderStatus : int8_t
{
    NOT_LOADED, /// Object hasn't been tried to load. This is an initial state.
    LOADED, /// Object has been loaded successfully.
    FAILED, /// Object has been failed to load.
    LOADING, /// Object is being loaded right now for the first time.
    FAILED_AND_RELOADING, /// Object was failed to load before and it's being reloaded right now.
    LOADED_AND_RELOADING, /// Object was loaded successfully before and it's being reloaded right now.
    NOT_EXIST, /// Object with this name wasn't found in the configuration.
};

std::vector<std::pair<String, Int8>> getExternalLoaderStatusEnumAllPossibleValues();
}
