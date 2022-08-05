#include "CatBoostLibraryHandlerFactory.h"

#include <Common/logger_useful.h>

namespace DB
{

CatBoostLibraryHandlerFactory & CatBoostLibraryHandlerFactory::instance()
{
    static CatBoostLibraryHandlerFactory instance;
    return instance;
}

CatBoostLibraryHandlerPtr CatBoostLibraryHandlerFactory::get(const String & model_path)
{
    std::lock_guard lock(mutex);

    if (auto handler = library_handlers.find(model_path); handler != library_handlers.end())
        return handler->second;
    return nullptr;
}

void CatBoostLibraryHandlerFactory::create(const String & library_path, const String & model_path)
{
    std::lock_guard lock(mutex);

    if (library_handlers.contains(model_path))
    {
        LOG_DEBUG(&Poco::Logger::get("CatBoostLibraryHandlerFactory"), "Cannot load catboost library handler for model path {} because it exists already", model_path);
        return;
    }

    library_handlers.emplace(std::make_pair(model_path, std::make_shared<CatBoostLibraryHandler>(library_path, model_path)));
    LOG_DEBUG(&Poco::Logger::get("CatBoostLibraryHandlerFactory"), "Loaded catboost library handler for model path {}.", model_path);
}

void CatBoostLibraryHandlerFactory::remove(const String & model_path)
{
    std::lock_guard lock(mutex);
    bool deleted = library_handlers.erase(model_path);
    if (!deleted) {
        LOG_DEBUG(&Poco::Logger::get("CatBoostLibraryHandlerFactory"), "Cannot unload catboost library handler for model path: {}", model_path);
        return;
    }
    LOG_DEBUG(&Poco::Logger::get("CatBoostLibraryHandlerFactory"), "Unloaded catboost library handler for model path: {}", model_path);
}

}
