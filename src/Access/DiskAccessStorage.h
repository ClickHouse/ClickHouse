#pragma once

#include <condition_variable>
#include <Access/MemoryAccessStorage.h>
#include <Common/ThreadPool_fwd.h>
#include <boost/container/flat_set.hpp>


namespace DB
{
class AccessChangesNotifier;

/// Loads and saves access entities on a local disk to a specified directory.
class DiskAccessStorage : public MemoryAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "local_directory";

    DiskAccessStorage(const String & storage_name_, const String & directory_path_, AccessChangesNotifier & changes_notifier_, bool readonly_, bool allow_backup_, UInt64 access_entities_num_limit_);
    ~DiskAccessStorage() override;

    void shutdown() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;

    String getPath() const { return directory_path; }
    bool isPathEqual(const String & directory_path_) const;

    void setReadOnly(bool readonly_) { readonly = readonly_; }
    bool isReadOnly() const override { return readonly; }

    void reload(ReloadMode reload_mode) override;

    bool isBackupAllowed() const override { return backup_allowed; }

private:
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    bool insertImpl(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool readLists();
    void writeLists();
    void scheduleWriteLists(AccessEntityType type);
    void reloadAllAndRebuildLists();
    void setAllInMemory(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

    void listsWritingThreadFunc() TSA_NO_THREAD_SAFETY_ANALYSIS;
    void stopListsWritingThread();

    bool insertWithDiskNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, bool write_on_disk);
    bool removeWithDiskNoLock(const UUID & id, bool throw_if_not_exists);

    AccessEntityPtr readAccessEntityFromDisk(const UUID & id) const;
    void writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const;
    void deleteAccessEntityOnDisk(const UUID & id) const;

    using NameToIDMap = std::unordered_map<String, UUID>;

    String directory_path;

    boost::container::flat_set<AccessEntityType> types_of_lists_to_write;

    /// Whether writing of the list files has been failed since the recent restart of the server.
    bool failed_to_write_lists = false;

    /// List files are written in a separate thread.
    std::unique_ptr<ThreadFromGlobalPool> lists_writing_thread;

    /// Signals `lists_writing_thread` to exit.
    std::condition_variable_any lists_writing_thread_should_exit;

    bool lists_writing_thread_is_waiting = false;

    std::atomic<bool> readonly;
    std::atomic<bool> backup_allowed;

    /// Making some public methods inaccessible
    using MemoryAccessStorage::setAll;
    using MemoryAccessStorage::removeAllExcept;
};
}
