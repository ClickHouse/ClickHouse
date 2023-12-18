#include "VFSLogItem.h"
#include "IO/ReadBufferFromString.h"
#include "IO/ReadHelpers.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void VFSLogItem::merge(VFSLogItem && other)
{
    for (auto && elem : other)
        if (elem.second <= 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} references to {}", elem.second, elem.first);
        else if (auto it = find(elem.first); it == end())
            insert(std::move(elem));
        else
            // As we have only link + unlink, we can't distinguish between 2 situations:
            // 1. We created an object and deleted it in the log batch -- then we need to remove it
            // 2. We created and removed link to object -- we don't need to remove the object.
            // So we leave objects with 0 or negative references and postpone the decision till we
            // merge with snapshot
            it->second += elem.second;
}

String VFSLogItem::getSerialised(StoredObjects && link, StoredObjects && unlink)
{
    return fmt::format("{}\n{}\n{}\n{}", link.size(), fmt::join(link, "\n"), unlink.size(), fmt::join(unlink, "\n"));
}

template <>
VFSLogItem parseFromString<VFSLogItem>(std::string_view str)
{
    VFSLogItem out;
    ReadBufferFromString buf{str};
    int cnt;
    readIntTextUnsafe(cnt, buf);

    for (int i = 0; i < cnt; ++i)
    {
        String path;
        readStringUntilNewlineInto(path, buf);
        out.emplace(std::move(path), 1);
    }

    readIntTextUnsafe(cnt, buf);

    for (int i = 0; i < cnt; ++i)
    {
        String path;
        readStringUntilNewlineInto(path, buf);
        out.emplace(std::move(path), -1);
    }
    assertEOF(buf);
    return out;
}
}
