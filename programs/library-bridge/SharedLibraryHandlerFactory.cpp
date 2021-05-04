#include "SharedLibraryHandlerFactory.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    library_handlers[dictionary_id] = std::make_shared<SharedLibraryHandler>(library_path, library_settings, sample_block, attributes_names);
}


void SharedLibraryHandlerFactory::clone(const std::string & from_dictionary_id, const std::string & to_dictionary_id)
{
    std::lock_guard lock(mutex);
    auto from_library_handler = library_handlers.find(from_dictionary_id);

    /// This is not supposed to happen as libClone is called from copy constructor of LibraryDictionarySource
    /// object, and shared library handler of from_dictionary is removed only in its destructor.
    /// And if for from_dictionary there was no shared library handler, it would have received and exception in
    /// its constructor, so no libClone would be made from it.
    if (from_library_handler == library_handlers.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No shared library handler found");

    /// libClone method will be called in copy constructor
    library_handlers[to_dictionary_id] = std::make_shared<SharedLibraryHandler>(*from_library_handler->second);
}


void SharedLibraryHandlerFactory::remove(const std::string & dictionary_id)
{
    std::lock_guard lock(mutex);
    /// libDelete is called in destructor.
    library_handlers.erase(dictionary_id);
}


SharedLibraryHandlerFactory & SharedLibraryHandlerFactory::instance()
{
    static SharedLibraryHandlerFactory ret;
    return ret;
}

}
