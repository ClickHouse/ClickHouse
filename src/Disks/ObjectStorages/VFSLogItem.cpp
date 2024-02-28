#include "VFSLogItem.h"
#include "Common/logger_useful.h"
#include "IO/ReadBufferFromMemory.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"

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

void VFSLogItem::merge(VFSLogItem && other)
{
    reserve(size() + other.size());
    // As we have only link + unlink, we can't distinguish 2 situations:
    // 1. We created an object and deleted it in the log batch -- then we need to remove it
    // 2. We created and removed link to object -- we don't need to remove the object.
    // So we leave objects with non-positive references and postpone the decision till we merge with snapshot
    for (auto & elem : other)
        if (auto it = find(elem.first); it == end())
            emplace(std::move(elem));
        else
            it->second += elem.second;
}

VFSMergeResult VFSLogItem::mergeWithSnapshot(ReadBuffer & snapshot, WriteBuffer & new_snapshot, Poco::Logger * log) &&
{
    // TODO myrrc this algo is ugly
    /// Both snapshot and batch data are sorted so we can merge them in one traversal
    VFSMergeResult out;

    using Pair = std::remove_cvref_t<decltype(out.invalid)::reference>;
    std::optional<Pair> left;
    auto batch_it = begin();

    auto read_left = [&] -> decltype(left)
    {
        if (snapshot.eof())
            return {};
        Pair entry;
        readStringUntilWhitespace(entry.first, snapshot);
        checkChar(' ', snapshot);
        readIntTextUnsafe(entry.second, snapshot);
        checkChar('\n', snapshot);
        LOG_TRACE(log, "Old snapshot entry: {} {}", entry.first, entry.second);
        return entry;
    };
    left = read_left();

    auto write = [&](std::string_view remote, int links)
    {
        if (links < 1)
        {
            out.invalid.emplace(remote, links);
            return;
        }
        const String entry = fmt::format("{} {}\n", remote, links);
        LOG_TRACE(log, "New snapshot entry: {}", entry);
        writeString(entry, new_snapshot);
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
