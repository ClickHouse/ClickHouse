#pragma once

#include <Common/AllocatorWithMemoryTracking.h>
#include <Storages/ColumnsDescription.h>

#include <memory>
#include <unordered_map>

namespace DB
{

/// This struct is shared between all parts with the same structure
/// and stored in a table.
struct ColumnsDescriptionCache
{
    std::shared_ptr<const ColumnsDescription> original;
    std::shared_ptr<const ColumnsDescription> with_collected_nested;
    /// Column name to position mapping, derived from columns list
    using NameToPositionAllocator = BytesAwareAllocatorWithMemoryTracking<std::pair<const std::string, size_t>>;
    using NameToPositionMap = std::unordered_map<std::string, size_t, std::hash<std::string>, std::equal_to<std::string>, NameToPositionAllocator>;
    std::shared_ptr<const NameToPositionMap> column_name_to_position;
};

}
