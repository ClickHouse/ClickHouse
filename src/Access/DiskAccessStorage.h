#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Common/ThreadPool_fwd.h>
#include <boost/container/flat_set.hpp>


namespace DB
{
class AccessChangesNotifier;

/// Loads and saves access entities on a local disk to a specified directory.
class DiskAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "local_directory";

    DiskAccessStorage(const String & storage_name_, const String & directory_path_, AccessChangesNotifier & changes_notifier_, bool readonly_, bool allow_backup_);
    ~DiskAccessStorage() override;

    void shutdown() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;

    String getPath() const { return directory_path; }
    bool isPathEqual(const String & directory_path_) const;

    void setReadOnly(bool readonly_) { readonly = readonly_; }
    bool isReadOnly() const override { return readonly; }

    void reload(ReloadMode reload_mode) override;

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    bool insertImpl(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool readLists() TSA_REQUIRES(mutex);
    void writeLists() TSA_REQUIRES(mutex);
    void scheduleWriteLists(AccessEntityType type) TSA_REQUIRES(mutex);
    void reloadAllAndRebuildLists() TSA_REQUIRES(mutex);
    void setAllInMemory(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities) TSA_REQUIRES(mutex);
    void removeAllExceptInMemory(const boost::container::flat_set<UUID> & ids_to_keep) TSA_REQUIRES(mutex);

    void listsWritingThreadFunc() TSA_NO_THREAD_SAFETY_ANALYSIS;
    void stopListsWritingThread();

    bool insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id, bool write_on_disk) TSA_REQUIRES(mutex);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, bool write_on_disk) TSA_REQUIRES(mutex);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists, bool write_on_disk) TSA_REQUIRES(mutex);

    AccessEntityPtr readAccessEntityFromDisk(const UUID & id) const;
    void writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const;
    void deleteAccessEntityOnDisk(const UUID & id) const;

    using NameToIDMap = std::unordered_map<String, UUID>;
    struct Entry
    {
        UUID id;
        String name;
        AccessEntityType type;
        mutable AccessEntityPtr entity; /// may be nullptr, if the entity hasn't been loaded yet.
    };

    String directory_path;

    std::unordered_map<UUID, Entry> entries_by_id TSA_GUARDED_BY(mutex);
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)] TSA_GUARDED_BY(mutex);
    boost::container::flat_set<AccessEntityType> types_of_lists_to_write TSA_GUARDED_BY(mutex);

    /// Whether writing of the list files has been failed since the recent restart of the server.
    bool failed_to_write_lists TSA_GUARDED_BY(mutex) = false;

    /// List files are written in a separate thread.
    std::unique_ptr<ThreadFromGlobalPool> lists_writing_thread;

    /// Signals `lists_writing_thread` to exit.
    std::condition_variable lists_writing_thread_should_exit;

    bool lists_writing_thread_is_waiting = false;

    AccessChangesNotifier & changes_notifier;
    std::atomic<bool> readonly;
    std::atomic<bool> backup_allowed;
    mutable std::mutex mutex;
};
}
