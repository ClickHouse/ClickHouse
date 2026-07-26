#pragma once

#include <Core/UUID.h>
#include <Interpreters/StorageID.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// This class manages dependencies between definers and the objects (views and other engines that
/// support the DEFINER SQL SECURITY clause) they define. It maintains a global mapping from definer
/// names to the set of objects they define, so that a definer cannot be dropped while still in use.
class DefinerDependencies
{
public:
    /// Get the global instance
    static DefinerDependencies & instance();

    /// Add a dependency of an object on a definer
    void addDependency(const String & definer, const StorageID & object_id);

    /// Remove all dependencies for a specific object (when it is dropped)
    void removeDependencies(const StorageID & object_id);

    /// Get the UUIDs of all objects defined by a specific definer
    std::vector<UUID> getObjectsForDefiner(const String & definer) const;

    /// Check if a definer has any dependencies
    bool hasDependencies(const String & definer) const;

private:
    DefinerDependencies() = default;

    std::unordered_map<String, std::unordered_set<UUID>> definer_to_objects;

    std::unordered_map<UUID, String> object_to_definer;

    mutable std::mutex mutex;

    /// Global instance
    static std::unique_ptr<DefinerDependencies> global_instance;
    static std::once_flag instance_flag;
};

}
