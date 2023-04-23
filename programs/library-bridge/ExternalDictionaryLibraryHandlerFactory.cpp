#include "ExternalDictionaryLibraryHandlerFactory.h"


namespace DB
{

SharedLibraryHandlerPtr ExternalDictionaryLibraryHandlerFactory::get(const std::string & dictionary_id)
{
    std::lock_guard lock(mutex);
    auto library_handler = library_handlers.find(dictionary_id);

    if (library_handler != library_handlers.end())
        return library_handler->second;

    return nullptr;
}


void ExternalDictionaryLibraryHandlerFactory::create(
    const std::string & dictionary_id,
    const std::string & library_path,
    const std::vector<std::string> & library_settings,
    const Block & sample_block,
    const std::vector<std::string> & attributes_names)
{
    std::lock_guard lock(mutex);
    if (!library_handlers.contains(dictionary_id))
        library_handlers.emplace(std::make_pair(dictionary_id, std::make_shared<ExternalDictionaryLibraryHandler>(library_path, library_settings, sample_block, attributes_names)));
    else
        LOG_WARNING(&Poco::Logger::get("ExternalDictionaryLibraryHandlerFactory"), "Library handler with dictionary id {} already exists", dictionary_id);
}


bool ExternalDictionaryLibraryHandlerFactory::clone(const std::string & from_dictionary_id, const std::string & to_dictionary_id)
{
    std::lock_guard lock(mutex);
    auto from_library_handler = library_handlers.find(from_dictionary_id);

    if (from_library_handler == library_handlers.end())
        return false;

    /// extDict_libClone method will be called in copy constructor
    library_handlers[to_dictionary_id] = std::make_shared<ExternalDictionaryLibraryHandler>(*from_library_handler->second);
    return true;
}


bool ExternalDictionaryLibraryHandlerFactory::remove(const std::string & dictionary_id)
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
