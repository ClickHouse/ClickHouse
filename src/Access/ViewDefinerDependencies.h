#pragma once

#include <Interpreters/StorageID.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// This class manages dependencies between view definers and the views they define.
/// It maintains a global mapping from definer names to the set of views they have defined.
/// This is useful for tracking which views need to be checked when the definer is dropped.
class ViewDefinerDependencies
{
public:
    using String = String;
    using ViewSet = std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    /// Get the global instance
    static ViewDefinerDependencies & instance();

    /// Add a view dependency for a definer
    void addViewDependency(const String & definer, const StorageID & view_id);

    /// Remove all dependencies for a specific view (when view is dropped)
    void removeViewDependencies(const StorageID & view_id);

    /// Get all views defined by a specific definer
    std::vector<StorageID> getViewsForDefiner(const String & definer) const;

    /// Check if a definer has any view dependencies
    bool hasViewDependencies(const String & definer) const;

private:
    ViewDefinerDependencies() = default;

    std::unordered_map<String, ViewSet> definer_to_views;

    std::unordered_map<StorageID, String, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> view_to_definer;

    mutable std::mutex mutex;

    /// Global instance
    static std::unique_ptr<ViewDefinerDependencies> global_instance;
    static std::once_flag instance_flag;
};

}
