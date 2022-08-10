#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Common/ThreadPool.h>
#include <boost/container/flat_set.hpp>


namespace DB
{
/// Loads and saves access entities on a local disk to a specified directory.
class DiskAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "local directory";

    DiskAccessStorage(const String & storage_name_, const String & directory_path_, bool readonly_ = false);
    DiskAccessStorage(const String & directory_path_, bool readonly_ = false);
    ~DiskAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;

    String getPath() const { return directory_path; }
    bool isPathEqual(const String & directory_path_) const;

    void setReadOnly(bool readonly_) { readonly = readonly_; }
    bool isReadOnly() const override { return readonly; }

    bool exists(const UUID & id) const override;
    bool hasSubscription(const UUID & id) const override;
    bool hasSubscription(AccessEntityType type) const override;

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const override;

    void clear();
    bool readLists();
    bool writeLists();
    void scheduleWriteLists(AccessEntityType type);
    bool rebuildLists();

    void listsWritingThreadFunc();
    void stopListsWritingThread();

    bool insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, Notifications & notifications);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists, Notifications & notifications);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, Notifications & notifications);

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
        mutable std::list<OnChangedHandler> handlers_by_id;
    };

    void prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const;

    String directory_path;
    std::atomic<bool> readonly;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];
    boost::container::flat_set<AccessEntityType> types_of_lists_to_write;
    bool failed_to_write_lists = false;                          /// Whether writing of the list files has been failed since the recent restart of the server.
    ThreadFromGlobalPool lists_writing_thread;                   /// List files are written in a separate thread.
    std::condition_variable lists_writing_thread_should_exit;    /// Signals `lists_writing_thread` to exit.
    bool lists_writing_thread_is_waiting = false;
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(AccessEntityType::MAX)];
    mutable std::mutex mutex;
};
}
