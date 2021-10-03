#pragma once

#include <vector>
#include <base/EnumReflection.h>
#include <base/types.h>

namespace DB
{
    enum class ExternalLoaderStatus : Int8
    {
        NOT_LOADED, /// Object hasn't been tried to load. This is an initial state.
        LOADED, /// Object has been loaded successfully.
        FAILED, /// Object has been failed to load.
        LOADING, /// Object is being loaded right now for the first time.
        FAILED_AND_RELOADING, /// Object was failed to load before and it's being reloaded right now.
        LOADED_AND_RELOADING, /// Object was loaded successfully before and it's being reloaded right now.
        NOT_EXIST, /// Object with this name wasn't found in the configuration.
    };

    inline std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues()
    {
        std::vector<std::pair<String, Int8>> out;
        out.reserve(magic_enum::enum_count<ExternalLoaderStatus>());

        for (const auto & [value, str] : magic_enum::enum_entries<ExternalLoaderStatus>())
            out.emplace_back(std::string{str}, static_cast<Int8>(value));

        return out;
    }
}
