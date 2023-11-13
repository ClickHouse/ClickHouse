#include "VFSTransactionLog.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include "base/EnumReflection.h"
#include "base/defines.h"
#include "base/find_symbols.h"

namespace DB
{
String VFSTransactionLogItem::serialize() const
{
    return fmt::format("{} {}", type, object_storage_path);
}

void VFSTransactionLogItem::deserialize(std::string_view str)
{
    // TODO myrrc proper checking
    std::vector<std::string_view> parts;
    splitInto<' '>(parts, str);
    chassert(parts.size() == 2);
    type = *magic_enum::enum_cast<Type>(parts[0]);
    object_storage_path = parts[1];
}

void getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type type, const StoredObjects & objects, Coordination::Requests & ops)
{
    // TODO myrrc for CreateInode we should store and propagate more information
    // so that object storage along with log should be self-contained
    for (const StoredObject & object : objects)
        ops.emplace_back(zkutil::makeCreateRequest(
            VFS_LOG_ITEM, VFSTransactionLogItem{type, object.remote_path}.serialize(), zkutil::CreateMode::PersistentSequential));
}

void VFSSnapshot::add(const VFSTransactionLogItem & item)
{
    using enum VFSTransactionLogItem::Type;
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
                items.erase(it);
            break;
        }
    }
}

String VFSSnapshot::serializeItems() const
{
    String out;
    for (const auto &[path, links] : items)
        out += fmt::format("{} {}\n", path, links);
    return out;
}
}
