#pragma once

#include <map>
#include <mutex>

#include <Core/Types.h>
#include <Common/ProfileEvents.h>
#include <IO/OpenedFile.h>


namespace ProfileEvents
{
    extern const Event OpenedFileCacheHits;
    extern const Event OpenedFileCacheMisses;
}

namespace DB
{


/** Cache of opened files for reading.
  * It allows to share file descriptors when doing reading with 'pread' syscalls on readonly files.
  * Note: open/close of files is very cheap on Linux and we should not bother doing it 10 000 times a second.
  * (This may not be the case on Windows with WSL. This is also not the case if strace is active. Neither when some eBPF is loaded).
  * But sometimes we may end up opening one file multiple times, that increases chance exhausting opened files limit.
  */
class OpenedFileCache
{
private:
    using Key = std::pair<std::string /* path */, int /* flags */>;

    using OpenedFileWeakPtr = std::weak_ptr<OpenedFile>;
    using Files = std::map<Key, OpenedFileWeakPtr>;

    Files files;
    std::mutex mutex;

public:
    using OpenedFilePtr = std::shared_ptr<OpenedFile>;

    OpenedFilePtr get(const std::string & path, int flags)
    {
        Key key(path, flags);

        std::lock_guard lock(mutex);

        auto [it, inserted] = files.emplace(key, OpenedFilePtr{});
        if (!inserted)
            if (auto res = it->second.lock())
                return res;

        OpenedFilePtr res
        {
            new OpenedFile(path, flags),
            [key, this](auto ptr)
            {
                {
                    std::lock_guard another_lock(mutex);
                    files.erase(key);
                }
                delete ptr;
            }
        };

        it->second = res;
        return res;
    }

    static OpenedFileCache & instance()
    {
        static OpenedFileCache res;
        return res;
    }
};

using OpenedFileCachePtr = std::shared_ptr<OpenedFileCache>;

}


