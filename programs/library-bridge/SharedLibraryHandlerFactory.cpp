#include "SharedLibraryHandlerFactory.h"


namespace DB
{

SharedLibraryHandlerPtr SharedLibraryHandlerFactory::get(const std::string & dictionary_id)
{
    std::lock_guard lock(mutex);
    auto library_handler = library_handlers.find(dictionary_id);

    if (library_handler != library_handlers.end())
        return library_handler->second;

    return nullptr;
}


void SharedLibraryHandlerFactory::create(
    const std::string & dictionary_id,
    const std::string & library_path,
    const std::vector<std::string> & library_settings,
    const Block & sample_block,
    const std::vector<std::string> & attributes_names)
{
    std::lock_guard lock(mutex);
    if (!library_handlers.contains(dictionary_id))
        library_handlers.emplace(std::make_pair(dictionary_id, std::make_shared<SharedLibraryHandler>(library_path, library_settings, sample_block, attributes_names)));
    else
        LOG_WARNING(&Poco::Logger::get("SharedLibraryHandlerFactory"), "Library handler with dictionary id {} already exists", dictionary_id);
}


bool SharedLibraryHandlerFactory::clone(const std::string & from_dictionary_id, const std::string & to_dictionary_id)
{
    std::lock_guard lock(mutex);
    auto from_library_handler = library_handlers.find(from_dictionary_id);

    if (from_library_handler == library_handlers.end())
        return false;

    /// libClone method will be called in copy constructor
    library_handlers[to_dictionary_id] = std::make_shared<SharedLibraryHandler>(*from_library_handler->second);
    return true;
}


bool SharedLibraryHandlerFactory::remove(const std::string & dictionary_id)
{
    std::lock_guard lock(mutex);
    /// libDelete is called in destructor.
    return library_handlers.erase(dictionary_id);
}


SharedLibraryHandlerFactory & SharedLibraryHandlerFactory::instance()
{
    static SharedLibraryHandlerFactory ret;
    return ret;
}

}
