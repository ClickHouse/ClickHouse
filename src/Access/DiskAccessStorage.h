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
    DiskAccessStorage();
    ~DiskAccessStorage() override;

    void setDirectory(const String & directory_path_);

private:
    std::optional<UUID> findImpl(std::type_index type, const String & name) const override;
    std::vector<UUID> findAllImpl(std::type_index type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr & entity) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    ext::scope_guard subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(std::type_index type) const override;

    void initialize(const String & directory_path_, Notifications & notifications);
    bool readLists();
    bool writeLists();
    void scheduleWriteLists(std::type_index type);
    bool rebuildLists();

    void startListsWritingThread();
    void stopListsWritingThread();
    void listsWritingThreadFunc();

    void insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications);
    void removeNoLock(const UUID & id, Notifications & notifications);
    void updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications);

    AccessEntityPtr readAccessEntityFromDisk(const UUID & id) const;
    void writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const;
    void deleteAccessEntityOnDisk(const UUID & id) const;

    using NameToIDMap = std::unordered_map<String, UUID>;
    struct Entry
    {
        Entry(const std::string_view & name_, std::type_index type_) : name(name_), type(type_) {}
        std::string_view name;          /// view points to a string in `name_to_id_maps`.
        std::type_index type;
        mutable AccessEntityPtr entity; /// may be nullptr, if the entity hasn't been loaded yet.
        mutable std::list<OnChangedHandler> handlers_by_id;
    };

    void prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const;

    String directory_path;
    bool initialized = false;
    std::unordered_map<std::type_index, NameToIDMap> name_to_id_maps;
    std::unordered_map<UUID, Entry> id_to_entry_map;
    boost::container::flat_set<std::type_index> types_of_lists_to_write;
    bool failed_to_write_lists = false;                          /// Whether writing of the list files has been failed since the recent restart of the server.
    ThreadFromGlobalPool lists_writing_thread;                   /// List files are written in a separate thread.
    std::condition_variable lists_writing_thread_should_exit;    /// Signals `lists_writing_thread` to exit.
    std::atomic<bool> lists_writing_thread_exited = false;
    mutable std::unordered_multimap<std::type_index, OnChangedHandler> handlers_by_type;
    mutable std::mutex mutex;
};
}
