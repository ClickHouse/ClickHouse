#pragma once

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

    String toString(ExternalLoaderStatus status)
    {
        using Status = ExternalLoaderStatus;
        switch (status)
        {
            case Status::NOT_LOADED: return "NOT_LOADED";
            case Status::LOADED: return "LOADED";
            case Status::FAILED: return "FAILED";
            case Status::LOADING: return "LOADING";
            case Status::FAILED_AND_RELOADING: return "FAILED_AND_RELOADING";
            case Status::LOADED_AND_RELOADING: return "LOADED_AND_RELOADING";
            case Status::NOT_EXIST: return "NOT_EXIST";
        }
    __builtin_unreachable();
    }

    std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues()
    {
        using Status = ExternalLoaderStatus;
        return std::vector<std::pair<String, Int8>>{
            {toString(Status::NOT_LOADED), static_cast<Int8>(Status::NOT_LOADED)},
            {toString(Status::LOADED), static_cast<Int8>(Status::LOADED)},
            {toString(Status::FAILED), static_cast<Int8>(Status::FAILED)},
            {toString(Status::LOADING), static_cast<Int8>(Status::LOADING)},
            {toString(Status::LOADED_AND_RELOADING), static_cast<Int8>(Status::LOADED_AND_RELOADING)},
            {toString(Status::FAILED_AND_RELOADING), static_cast<Int8>(Status::FAILED_AND_RELOADING)},
            {toString(Status::NOT_EXIST), static_cast<Int8>(Status::NOT_EXIST)},
        };
    }

    std::ostream & operator<<(std::ostream & out, ExternalLoaderStatus status)
    {
        return out << toString(status);
    }
}
