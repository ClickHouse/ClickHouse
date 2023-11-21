#include "VFSTransactionLog.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include "IO/ReadHelpers.h"
#include "base/EnumReflection.h"
#include "base/defines.h"
#include "base/find_symbols.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

String VFSTransactionLogItem::serialize() const
{
    if (type == Type::CreateInode)
        return fmt::format("{} {} {} {}", type, remote_path, local_path, bytes_size);
    return fmt::format("{} {}", type, remote_path);
}

VFSTransactionLogItem VFSTransactionLogItem::deserialize(std::string_view str)
{
    VFSTransactionLogItem out;

    // TODO myrrc proper checking
    std::vector<std::string_view> parts;
    splitInto<' '>(parts, str);

    if (parts.size() == 4)
    {
        out.type = Type::CreateInode;
        out.remote_path = parts[1];
        out.local_path = parts[2];
        out.bytes_size = parseFromString<size_t>(parts[3]);
        return out;
    }

    chassert(parts.size() == 2);
    out.type = *magic_enum::enum_cast<Type>(parts[0]);
    out.remote_path = parts[1];
    return out;
}

void getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type type, const StoredObjects & objects, Coordination::Requests & ops)
{
    for (const StoredObject & object : objects)
    {
        VFSTransactionLogItem item = static_cast<const VFSTransactionLogItem &>(object);
        item.type = type;
        ops.emplace_back(zkutil::makeCreateRequest(VFS_LOG_ITEM, item.serialize(), zkutil::CreateMode::PersistentSequential));
    }
}

VFSSnapshot::ObsoleteObjects VFSSnapshot::update(const std::vector<VFSTransactionLogItem> & logs)
{
    using enum VFSTransactionLogItem::Type;
    ObsoleteObjects out;

    for (const VFSTransactionLogItem & item : logs)
        switch (item.type)
        {
            case CreateInode: {
                if (auto it = items.find(item.remote_path); it != items.end()) [[unlikely]]
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Items with same remote path found in snapshot ({}) and log ({})", it->second.obj, item);
                items.emplace(item.remote_path, ObjectWithRefcount{item, 0});
                break;
            }
            case Link: {
                auto it = items.find(item.remote_path);
                if (it == items.end()) [[unlikely]]
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Item {} not found in snapshot", item);

                ++it->second.links;
                break;
            }
            case Unlink: {
                auto it = items.find(item.remote_path);
                if (it == items.end()) [[unlikely]]
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Item {} not found in snapshot", item);

                if (--it->second.links == 0)
                {
                    out.emplace_back(it->second.obj);
                    items.erase(it);
                }
                break;
            }
        }

    return out;
}

VFSSnapshot VFSSnapshot::deserialize(std::string_view str)
{
    VFSSnapshot out;

    // TODO myrrc proper checking and proper code
    std::vector<std::string_view> objects;
    splitInto<'\n'>(objects, str);

    for (std::string_view object : objects)
    {
        std::vector<std::string> object_parts;
        splitInto<' '>(object_parts, object);
        chassert(object_parts.size() == 4);
        const size_t links = parseFromString<size_t>(object_parts[0]);
        out.items.emplace(
            object_parts[2],
            ObjectWithRefcount{
                StoredObject(
                    /*remote_path*/ object_parts[2],
                    /*local_path*/ object_parts[1],
                    /*bytes_size*/ parseFromString<size_t>(object_parts[3])),
                links});
    }

    return out;
}

String VFSSnapshot::serialize() const
{
    String out;
    for (const auto & [_, item] : items)
        out += fmt::format("{} {} {} {}\n", item.links, item.obj.local_path, item.obj.remote_path, item.obj.bytes_size);
    return out;
}
}
