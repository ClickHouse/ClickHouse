#include "CatBoostLibraryHandlerFactory.h"

#include <Common/logger_useful.h>


namespace DB
{

CatBoostLibraryHandlerFactory & CatBoostLibraryHandlerFactory::instance()
{
    static CatBoostLibraryHandlerFactory instance;
    return instance;
}

CatBoostLibraryHandlerFactory::CatBoostLibraryHandlerFactory()
    : log(getLogger("CatBoostLibraryHandlerFactory"))
{
}

CatBoostLibraryHandlerPtr CatBoostLibraryHandlerFactory::tryGetModel(const String & model_path, const String & library_path, bool create_if_not_found)
{
    std::lock_guard lock(mutex);

    auto handler = library_handlers.find(model_path);
    bool found = (handler != library_handlers.end());

    if (found)
        return handler->second;

    if (create_if_not_found)
    {
        auto new_handler = std::make_shared<CatBoostLibraryHandler>(library_path, model_path);
        library_handlers.emplace(model_path, new_handler);
        LOG_DEBUG(log, "Loaded catboost library handler for model path '{}'", model_path);
        return new_handler;
    }
    return nullptr;
}

void CatBoostLibraryHandlerFactory::removeModel(const String & model_path)
{
    std::lock_guard lock(mutex);

    bool deleted = library_handlers.erase(model_path);
    if (!deleted)
    {
        LOG_DEBUG(log, "Cannot unload catboost library handler for model path '{}'", model_path);
        return;
    }
    LOG_DEBUG(log, "Unloaded catboost library handler for model path '{}'", model_path);
}

void CatBoostLibraryHandlerFactory::removeAllModels()
{
    std::lock_guard lock(mutex);
    library_handlers.clear();
    LOG_DEBUG(log, "Unloaded all catboost library handlers");
}

ExternalModelInfos CatBoostLibraryHandlerFactory::getModelInfos()
{
    std::lock_guard lock(mutex);

    ExternalModelInfos result;

    for (const auto & handler : library_handlers)
        result.push_back({
            .model_path = handler.first,
            .model_type = "catboost",
            .loading_start_time = handler.second->getLoadingStartTime(),
            .loading_duration = handler.second->getLoadingDuration()
        });

    return result;

}

}
