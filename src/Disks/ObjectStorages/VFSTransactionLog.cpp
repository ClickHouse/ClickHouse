#include "VFSTransactionLog.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include "IO/ReadHelpers.h"
#include "base/EnumReflection.h"
#include "base/defines.h"
#include "base/find_symbols.h"

namespace DB
{
String VFSTransactionLogItem::serialize() const
{
    return fmt::format("{} {}", type, object_storage_path);
}

VFSTransactionLogItem & VFSTransactionLogItem::deserialize(std::string_view str)
{
    // TODO myrrc proper checking
    std::vector<std::string_view> parts;
    splitInto<' '>(parts, str);
    chassert(parts.size() == 2);
    type = *magic_enum::enum_cast<Type>(parts[0]);
    object_storage_path = parts[1];
    return *this;
}

void getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type type, const StoredObjects & objects, Coordination::Requests & ops)
{
    // TODO myrrc for CreateInode we should store and propagate more information
    // so that object storage along with log should be self-contained
    for (const StoredObject & object : objects)
        ops.emplace_back(zkutil::makeCreateRequest(
            VFS_LOG_ITEM, VFSTransactionLogItem{type, object.remote_path}.serialize(), zkutil::CreateMode::PersistentSequential));
}

VFSSnapshot::ObsoleteObjects VFSSnapshot::update(const std::vector<VFSTransactionLogItem> & logs)
{
    using enum VFSTransactionLogItem::Type;
    ObsoleteObjects out;

    for (const auto & item : logs)
        switch (item.type)
        {
            case CreateInode: {
                chassert(!items.contains(item.object_storage_path));
                items.emplace(item.object_storage_path, 0);
                break;
            }
            case Link: {
                auto it = items.find(item.object_storage_path);
                chassert(it != items.end());
                ++it->second;
                break;
            }
            case Unlink: {
                auto it = items.find(item.object_storage_path);
                chassert(it != items.end());
                if (--it->second == 0)
                {
                    out.emplace_back(VFSTransactionLogItem{VFSTransactionLogItem::Type::Unlink, it->first});
                    items.erase(it);
                }
                break;
            }
        }

    return out;
}

VFSSnapshot & VFSSnapshot::deserialize(std::string_view str)
{
    // TODO myrrc proper checking
    std::vector<std::string_view> parts;
    splitInto<'\n'>(parts, str);

    for (std::string_view part : parts)
    {
        std::vector<std::string_view> item_parts;
        splitInto<' '>(item_parts, part);
        chassert(item_parts.size() == 2);
        const size_t links = parseFromString<size_t>(item_parts[1]);
        items.emplace(item_parts[0], links);
    }

    return *this;
}

String VFSSnapshot::serialize() const
{
    String out;
    for (const auto & [path, links] : items)
        out += fmt::format("{} {}\n", path, links);
    return out;
}
}
