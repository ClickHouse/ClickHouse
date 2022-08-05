#pragma once

#include "CatBoostLibraryHandler.h"

#include <base/defines.h>

#include <mutex>
#include <unordered_map>


namespace DB
{

class CatBoostLibraryHandlerFactory final : private boost::noncopyable
{
public:
    static CatBoostLibraryHandlerFactory & instance();

    CatBoostLibraryHandlerPtr get(const String & model_path);

    void create(const String & library_path, const String & model_path);

    void remove(const String & model_path);

private:
    /// map: model path -> shared library handler
    std::unordered_map<String, CatBoostLibraryHandlerPtr> library_handlers TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
