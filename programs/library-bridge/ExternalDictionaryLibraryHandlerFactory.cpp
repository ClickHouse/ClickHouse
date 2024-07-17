#include "ExternalDictionaryLibraryHandlerFactory.h"

#include <Common/logger_useful.h>

namespace DB
{

ExternalDictionaryLibraryHandlerPtr ExternalDictionaryLibraryHandlerFactory::get(const String & dictionary_id)
{
    std::lock_guard lock(mutex);

    if (auto handler = library_handlers.find(dictionary_id); handler != library_handlers.end())
        return handler->second;
    return nullptr;
}


void ExternalDictionaryLibraryHandlerFactory::create(
    const String & dictionary_id,
    const String & library_path,
    const std::vector<String> & library_settings,
    const Block & sample_block,
    const std::vector<String> & attributes_names)
{
    std::lock_guard lock(mutex);

    if (library_handlers.contains(dictionary_id))
    {
        LOG_WARNING(getLogger("ExternalDictionaryLibraryHandlerFactory"), "Library handler with dictionary id {} already exists", dictionary_id);
        return;
    }

    library_handlers.emplace(std::make_pair(dictionary_id, std::make_shared<ExternalDictionaryLibraryHandler>(library_path, library_settings, sample_block, attributes_names)));
}


bool ExternalDictionaryLibraryHandlerFactory::clone(const String & from_dictionary_id, const String & to_dictionary_id)
{
    std::lock_guard lock(mutex);
    auto from_library_handler = library_handlers.find(from_dictionary_id);

    if (from_library_handler == library_handlers.end())
        return false;

    /// extDict_libClone method will be called in copy constructor
    library_handlers[to_dictionary_id] = std::make_shared<ExternalDictionaryLibraryHandler>(*from_library_handler->second);
    return true;
}


bool ExternalDictionaryLibraryHandlerFactory::remove(const String & dictionary_id)
{
    std::lock_guard lock(mutex);
    /// extDict_libDelete is called in destructor.
    return library_handlers.erase(dictionary_id);
}


ExternalDictionaryLibraryHandlerFactory & ExternalDictionaryLibraryHandlerFactory::instance()
{
    static ExternalDictionaryLibraryHandlerFactory instance;
    return instance;
}

}
