#pragma once

#include <Common/FileCache_fwd.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <mutex>

namespace DB
{

/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory final : private boost::noncopyable
{
public:
    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_base_path, const FileCacheSettings & file_cache_settings);

private:
    FileCachePtr getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &);

    std::mutex mutex;
    std::unordered_map<std::string, FileCachePtr> caches;
};

}
