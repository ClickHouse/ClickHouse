#include "VFSLogItem.h"

#include <Disks/ObjectStorages/VFSSnapshotStorage.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

using ItemPair = DB::VFSLogItemStorage::value_type;
template <>
struct fmt::formatter<ItemPair>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const ItemPair & item, auto & ctx) { return fmt::format_to(ctx.out(), "{} {}", item.first, item.second); }
};

namespace DB
{
VFSLogItem VFSLogItem::parse(std::string_view str)
{
    if (str.empty() || str == "\n")
        return VFSLogItem{};
    VFSLogItem out;
    ReadBufferFromMemory buf{std::move(str)};
    ItemPair pair;
    while (!buf.eof())
    {
        readStringUntilWhitespace(pair.first, buf);
        checkChar(' ', buf);
        readIntTextUnsafe(pair.second, buf);
        checkChar('\n', buf);
        out.emplace(std::exchange(pair, {}));
    }
    return out;
}

constexpr size_t zk_node_limit = 1_MiB;
Strings VFSLogItem::serialize() const
{
    if (empty())
        return {};
    Strings out(1);
    String & ref = out[0];
    for (const auto & obj : *this)
    {
        String item_serialized = fmt::format("{}\n", obj);
        if (ref.size() + item_serialized.size() <= zk_node_limit)
            ref += item_serialized;
        else
            ref = out.emplace_back(std::move(item_serialized));
    }
    return out;
}

void VFSLogItem::add(const StoredObject & obj)
{
    (*this)[obj.remote_path]++;
}

void VFSLogItem::remove(const StoredObject & obj)
{
    (*this)[obj.remote_path]--;
}

void VFSLogItem::merge(VFSLogItem && other)
{
    reserve(size() + other.size());
    // Leave objects with 0 references and postpone the decision till we merge with snapshot as we can't
    // distinguish between 2 situations:
    // 1. We created an object and deleted it in the log batch -- then we need to remove it
    // 2. We created and removed link to object -- we don't need to remove the object.
    for (auto & elem : other)
        (*this)[std::move(elem.first)] += elem.second;
}

VFSMergeResult
VFSLogItem::mergeWithSnapshot(IVFSSnapshotReadStream & snapshot, IVFSSnapshotWriteStream & new_snapshot, Poco::Logger * log) &&
{
    // TODO myrrc this algo is ugly
    /// Both snapshot and batch data are sorted so we can merge them in one traversal
    VFSMergeResult out;
    auto batch_it = begin();

    auto read_left = [&]()
    {
        auto entry = snapshot.next();
        if (!entry)
            return entry;
        LOG_TRACE(log, "Old snapshot entry: {} {}", entry->remote_path, entry->link_count);
        return entry;
    };
    auto left = read_left();

    auto write = [&](std::string_view remote, int links)
    {
        if (links < 1)
        {
            out.invalid.emplace(remote, links);
            return;
        }
        LOG_TRACE(log, "New snapshot entry: {} {}", remote, links);
        new_snapshot.write(VFSSnapshotEntry{String(remote), links});
    };

    while (left && batch_it != cend())
    {
        auto & [left_remote, left_links] = *left;
        const auto & [right_remote, right_links] = *batch_it;

        if (const int res = left_remote.compare(right_remote); res == 0)
        {
            if (int delta = left_links + right_links; delta == 0)
                out.obsolete.emplace_back(StoredObject{left_remote});
            else
                write(left_remote, delta);

            left = read_left();
            ++batch_it;
        }
        else if (res < 0)
        {
            write(left_remote, left_links);
            left = read_left();
        }
        else
        {
            if (right_links == 0) // Object's lifetime is local to batch
                out.obsolete.emplace_back(StoredObject{right_remote});
            else
                write(right_remote, right_links);
            ++batch_it;
        }
    }

    //TODO myrrc if one side is off, write in blocks rather than in single entry
    while (left)
    {
        auto & [left_remote, left_links] = *left;
        write(left_remote, left_links);
        left = read_left();
    }

    while (batch_it != cend())
    {
        const auto & [right_remote, right_links] = *batch_it;
        if (right_links == 0) // Object's lifetime is local to batch
            out.obsolete.emplace_back(StoredObject{right_remote});
        else
            write(right_remote, right_links);
        ++batch_it;
    }

    return out;
}
}
