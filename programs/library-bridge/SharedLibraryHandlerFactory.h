#pragma once

#include "SharedLibraryHandler.h"
#include <base/defines.h>

#include <unordered_map>
#include <mutex>


namespace DB
{

/// Each library dictionary source has unique UUID. When clone() method is called, a new UUID is generated.
/// There is a unique mapping from diciotnary UUID to sharedLibraryHandler.
class SharedLibraryHandlerFactory final : private boost::noncopyable
{
public:
    static SharedLibraryHandlerFactory & instance();

    SharedLibraryHandlerPtr get(const std::string & dictionary_id);

    void create(
        const std::string & dictionary_id,
        const std::string & library_path,
        const std::vector<std::string> & library_settings,
        const Block & sample_block,
        const std::vector<std::string> & attributes_names);

    bool clone(const std::string & from_dictionary_id, const std::string & to_dictionary_id);

    bool remove(const std::string & dictionary_id);

private:
    /// map: dict_id -> sharedLibraryHandler
    std::unordered_map<std::string, SharedLibraryHandlerPtr> library_handlers TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
