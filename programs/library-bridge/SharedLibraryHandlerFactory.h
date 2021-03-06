#pragma once

#include "SharedLibraryHandler.h"
#include <unordered_map>
#include <mutex>


namespace DB
{

/// Creates map object: dictionary_id -> SharedLibararyHandler.
class SharedLibraryHandlerFactory final : private boost::noncopyable
{
public:
    static SharedLibraryHandlerFactory & instance();

    SharedLibraryHandlerPtr get(const std::string & dictionary_id);

    bool create(const std::string & dictionary_id, const std::string & library_path, const std::string & library_settings);

    bool clone(const std::string & from_dictionary_id, const std::string & to_dictionary_id);

    bool remove(const std::string & dictionary_id);

private:
    std::unordered_map<std::string, SharedLibraryHandlerPtr> library_handlers;
    std::mutex mutex;
};

}
