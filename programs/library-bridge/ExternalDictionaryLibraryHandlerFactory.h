#pragma once

#include "ExternalDictionaryLibraryHandler.h"
#include <base/defines.h>

#include <unordered_map>
#include <mutex>


namespace DB
{

/// Each library dictionary source has unique UUID. When clone() method is called, a new UUID is generated.
/// There is a unique mapping from dictionary UUID to sharedLibraryHandler.
class ExternalDictionaryLibraryHandlerFactory final : private boost::noncopyable
{
public:
    static ExternalDictionaryLibraryHandlerFactory & instance();

    ExternalDictionaryLibraryHandlerPtr get(const String & dictionary_id);

    void create(
        const String & dictionary_id,
        const String & library_path,
        const std::vector<String> & library_settings,
        const Block & sample_block,
        const std::vector<String> & attributes_names);

    bool clone(const String & from_dictionary_id, const String & to_dictionary_id);

    bool remove(const String & dictionary_id);

private:
    /// map: dict_id -> sharedLibraryHandler
    std::unordered_map<String, ExternalDictionaryLibraryHandlerPtr> library_handlers TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
