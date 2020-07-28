#include <Access/DiskAccessStorage.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/RowPolicy.h>
#include <Access/Quota.h>
#include <Access/SettingsProfile.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ParserGrantQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Interpreters/InterpreterCreateSettingsProfileQuery.h>
#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <filesystem>
#include <fstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_ACCESS_ENTITY_DEFINITION;
    extern const int LOGICAL_ERROR;
}


namespace
{
    using EntityType = IAccessStorage::EntityType;
    using EntityTypeInfo = IAccessStorage::EntityTypeInfo;

    /// Special parser for the 'ATTACH access entity' queries.
    class ParserAttachAccessEntity : public IParserBase
    {
    protected:
        const char * getName() const override { return "ATTACH access entity query"; }

        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            ParserCreateUserQuery create_user_p;
            ParserCreateRoleQuery create_role_p;
            ParserCreateRowPolicyQuery create_policy_p;
            ParserCreateQuotaQuery create_quota_p;
            ParserCreateSettingsProfileQuery create_profile_p;
            ParserGrantQuery grant_p;

            create_user_p.useAttachMode();
            create_role_p.useAttachMode();
            create_policy_p.useAttachMode();
            create_quota_p.useAttachMode();
            create_profile_p.useAttachMode();
            grant_p.useAttachMode();

            return create_user_p.parse(pos, node, expected) || create_role_p.parse(pos, node, expected)
                || create_policy_p.parse(pos, node, expected) || create_quota_p.parse(pos, node, expected)
                || create_profile_p.parse(pos, node, expected) || grant_p.parse(pos, node, expected);
        }
    };


    /// Reads a file containing ATTACH queries and then parses it to build an access entity.
    AccessEntityPtr readEntityFile(const std::filesystem::path & file_path)
    {
        /// Read the file.
        ReadBufferFromFile in{file_path};
        String file_contents;
        readStringUntilEOF(file_contents, in);

        /// Parse the file contents.
        ASTs queries;
        ParserAttachAccessEntity parser;
        const char * begin = file_contents.data(); /// begin of current query
        const char * pos = begin; /// parser moves pos from begin to the end of current query
        const char * end = begin + file_contents.size();
        while (pos < end)
        {
            queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
            while (isWhitespaceASCII(*pos) || *pos == ';')
                ++pos;
        }

        /// Interpret the AST to build an access entity.
        std::shared_ptr<User> user;
        std::shared_ptr<Role> role;
        std::shared_ptr<RowPolicy> policy;
        std::shared_ptr<Quota> quota;
        std::shared_ptr<SettingsProfile> profile;
        AccessEntityPtr res;

        for (const auto & query : queries)
        {
            if (auto * create_user_query = query->as<ASTCreateUserQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = user = std::make_unique<User>();
                InterpreterCreateUserQuery::updateUserFromQuery(*user, *create_user_query);
            }
            else if (auto * create_role_query = query->as<ASTCreateRoleQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = role = std::make_unique<Role>();
                InterpreterCreateRoleQuery::updateRoleFromQuery(*role, *create_role_query);
            }
            else if (auto * create_policy_query = query->as<ASTCreateRowPolicyQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = policy = std::make_unique<RowPolicy>();
                InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(*policy, *create_policy_query);
            }
            else if (auto * create_quota_query = query->as<ASTCreateQuotaQuery>())
            {
                if (res)
                    throw Exception("Two access entities are attached in the same file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = quota = std::make_unique<Quota>();
                InterpreterCreateQuotaQuery::updateQuotaFromQuery(*quota, *create_quota_query);
            }
            else if (auto * create_profile_query = query->as<ASTCreateSettingsProfileQuery>())
            {
                if (res)
                    throw Exception("Two access entities are attached in the same file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = profile = std::make_unique<SettingsProfile>();
                InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(*profile, *create_profile_query);
            }
            else if (auto * grant_query = query->as<ASTGrantQuery>())
            {
                if (!user && !role)
                    throw Exception("A user or role should be attached before grant in file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                if (user)
                    InterpreterGrantQuery::updateUserFromQuery(*user, *grant_query);
                else
                    InterpreterGrantQuery::updateRoleFromQuery(*role, *grant_query);
            }
            else
                throw Exception("No interpreter found for query " + query->getID(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
        }

        if (!res)
            throw Exception("No access entities attached in file " + file_path.string(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);

        return res;
    }


    AccessEntityPtr tryReadEntityFile(const std::filesystem::path & file_path, Poco::Logger & log)
    {
        try
        {
            return readEntityFile(file_path);
        }
        catch (...)
        {
            tryLogCurrentException(&log, "Could not parse " + file_path.string());
            return nullptr;
        }
    }


    /// Writes ATTACH queries for building a specified access entity to a file.
    void writeEntityFile(const std::filesystem::path & file_path, const IAccessEntity & entity)
    {
        /// Build list of ATTACH queries.
        ASTs queries;
        queries.push_back(InterpreterShowCreateAccessEntityQuery::getAttachQuery(entity));
        if ((entity.getType() == EntityType::USER) || (entity.getType() == EntityType::ROLE))
            boost::range::push_back(queries, InterpreterShowGrantsQuery::getAttachGrantQueries(entity));

        /// Serialize the list of ATTACH queries to a string.
        std::stringstream ss;
        for (const ASTPtr & query : queries)
            ss << *query << ";\n";
        String file_contents = std::move(ss).str();

        /// First we save *.tmp file and then we rename if everything's ok.
        auto tmp_file_path = std::filesystem::path{file_path}.replace_extension(".tmp");
        bool succeeded = false;
        SCOPE_EXIT(
        {
            if (!succeeded)
                std::filesystem::remove(tmp_file_path);
        });

        /// Write the file.
        WriteBufferFromFile out{tmp_file_path.string()};
        out.write(file_contents.data(), file_contents.size());

        /// Rename.
        std::filesystem::rename(tmp_file_path, file_path);
        succeeded = true;
    }


    /// Calculates the path to a file named <id>.sql for saving an access entity.
    std::filesystem::path getEntityFilePath(const String & directory_path, const UUID & id)
    {
        return std::filesystem::path(directory_path).append(toString(id)).replace_extension(".sql");
    }


    /// Reads a map of name of access entity to UUID for access entities of some type from a file.
    std::vector<std::pair<UUID, String>> readListFile(const std::filesystem::path & file_path)
    {
        ReadBufferFromFile in(file_path);

        size_t num;
        readVarUInt(num, in);
        std::vector<std::pair<UUID, String>> id_name_pairs;
        id_name_pairs.reserve(num);

        for (size_t i = 0; i != num; ++i)
        {
            String name;
            readStringBinary(name, in);
            UUID id;
            readUUIDText(id, in);
            id_name_pairs.emplace_back(id, std::move(name));
        }

        return id_name_pairs;
    }


    /// Writes a map of name of access entity to UUID for access entities of some type to a file.
    void writeListFile(const std::filesystem::path & file_path, const std::vector<std::pair<UUID, std::string_view>> & id_name_pairs)
    {
        WriteBufferFromFile out(file_path);
        writeVarUInt(id_name_pairs.size(), out);
        for (const auto & [id, name] : id_name_pairs)
        {
            writeStringBinary(name, out);
            writeUUIDText(id, out);
        }
    }


    /// Calculates the path for storing a map of name of access entity to UUID for access entities of some type.
    std::filesystem::path getListFilePath(const String & directory_path, EntityType type)
    {
        String file_name = EntityTypeInfo::get(type).plural_raw_name;
        boost::to_lower(file_name);
        file_name += ".list";
        return std::filesystem::path(directory_path).append(file_name);
    }


    /// Calculates the path to a temporary file which existence means that list files are corrupted
    /// and need to be rebuild.
    std::filesystem::path getNeedRebuildListsMarkFilePath(const String & directory_path)
    {
        return std::filesystem::path(directory_path).append("need_rebuild_lists.mark");
    }


    bool tryParseUUID(const String & str, UUID & id)
    {
        try
        {
            id = parseFromString<UUID>(str);
            return true;
        }
        catch (...)
        {
            return false;
        }
    }
}


DiskAccessStorage::DiskAccessStorage()
    : IAccessStorage("disk")
{
}


DiskAccessStorage::~DiskAccessStorage()
{
    stopListsWritingThread();
    writeLists();
}


void DiskAccessStorage::setDirectory(const String & directory_path_)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    initialize(directory_path_, notifications);
}


void DiskAccessStorage::initialize(const String & directory_path_, Notifications & notifications)
{
    auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path_);

    if (initialized)
    {
        if (directory_path == canonical_directory_path)
            return;
        throw Exception("Storage " + getStorageName() + " already initialized with another directory", ErrorCodes::LOGICAL_ERROR);
    }

    std::filesystem::create_directories(canonical_directory_path);
    if (!std::filesystem::exists(canonical_directory_path) || !std::filesystem::is_directory(canonical_directory_path))
        throw Exception("Couldn't create directory " + canonical_directory_path.string(), ErrorCodes::DIRECTORY_DOESNT_EXIST);

    directory_path = canonical_directory_path;
    initialized = true;

    bool should_rebuild_lists = std::filesystem::exists(getNeedRebuildListsMarkFilePath(directory_path));
    if (!should_rebuild_lists)
    {
        if (!readLists())
            should_rebuild_lists = true;
    }

    if (should_rebuild_lists)
    {
        rebuildLists();
        writeLists();
    }

    for (const auto & [id, entry] : entries_by_id)
        prepareNotifications(id, entry, false, notifications);
}


void DiskAccessStorage::clear()
{
    entries_by_id.clear();
    for (auto type : ext::range(EntityType::MAX))
        entries_by_name_and_type[static_cast<size_t>(type)].clear();
}


bool DiskAccessStorage::readLists()
{
    clear();

    bool ok = true;
    for (auto type : ext::range(EntityType::MAX))
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        auto file_path = getListFilePath(directory_path, type);
        if (!std::filesystem::exists(file_path))
        {
            LOG_WARNING(getLogger(), "File {} doesn't exist", file_path.string());
            ok = false;
            break;
        }

        try
        {
            for (const auto & [id, name] : readListFile(file_path))
            {
                auto & entry = entries_by_id[id];
                entry.id = id;
                entry.type = type;
                entry.name = name;
                entries_by_name[entry.name] = &entry;
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not read " + file_path.string());
            ok = false;
            break;
        }
    }

    if (!ok)
        clear();
    return ok;
}


bool DiskAccessStorage::writeLists()
{
    if (failed_to_write_lists)
        return false; /// We don't try to write list files after the first fail.
                      /// The next restart of the server will invoke rebuilding of the list files.

    if (types_of_lists_to_write.empty())
        return true;

    for (const auto & type : types_of_lists_to_write)
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        auto file_path = getListFilePath(directory_path, type);
        try
        {
            std::vector<std::pair<UUID, std::string_view>> id_name_pairs;
            id_name_pairs.reserve(entries_by_name.size());
            for (const auto * entry : entries_by_name | boost::adaptors::map_values)
                id_name_pairs.emplace_back(entry->id, entry->name);
            writeListFile(file_path, id_name_pairs);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not write " + file_path.string());
            failed_to_write_lists = true;
            types_of_lists_to_write.clear();
            return false;
        }
    }

    /// The list files was successfully written, we don't need the 'need_rebuild_lists.mark' file any longer.
    std::filesystem::remove(getNeedRebuildListsMarkFilePath(directory_path));
    types_of_lists_to_write.clear();
    return true;
}


void DiskAccessStorage::scheduleWriteLists(EntityType type)
{
    if (failed_to_write_lists)
        return;

    bool already_scheduled = !types_of_lists_to_write.empty();
    types_of_lists_to_write.insert(type);

    if (already_scheduled)
        return;

    /// Create the 'need_rebuild_lists.mark' file.
    /// This file will be used later to find out if writing lists is successful or not.
    std::ofstream{getNeedRebuildListsMarkFilePath(directory_path)};

    startListsWritingThread();
}


void DiskAccessStorage::startListsWritingThread()
{
    if (lists_writing_thread.joinable())
    {
        if (!lists_writing_thread_exited)
            return;
        lists_writing_thread.detach();
    }

    lists_writing_thread_exited = false;
    lists_writing_thread = ThreadFromGlobalPool{&DiskAccessStorage::listsWritingThreadFunc, this};
}


void DiskAccessStorage::stopListsWritingThread()
{
    if (lists_writing_thread.joinable())
    {
        lists_writing_thread_should_exit.notify_one();
        lists_writing_thread.join();
    }
}


void DiskAccessStorage::listsWritingThreadFunc()
{
    std::unique_lock lock{mutex};
    SCOPE_EXIT({ lists_writing_thread_exited = true; });

    /// It's better not to write the lists files too often, that's why we need
    /// the following timeout.
    const auto timeout = std::chrono::minutes(1);
    if (lists_writing_thread_should_exit.wait_for(lock, timeout) != std::cv_status::timeout)
        return; /// The destructor requires us to exit.

    writeLists();
}


/// Reads and parses all the "<id>.sql" files from a specified directory
/// and then saves the files "users.list", "roles.list", etc. to the same directory.
bool DiskAccessStorage::rebuildLists()
{
    LOG_WARNING(getLogger(), "Recovering lists in directory {}", directory_path);
    clear();

    for (const auto & directory_entry : std::filesystem::directory_iterator(directory_path))
    {
        if (!directory_entry.is_regular_file())
            continue;
        const auto & path = directory_entry.path();
        if (path.extension() != ".sql")
            continue;

        UUID id;
        if (!tryParseUUID(path.stem(), id))
            continue;

        const auto access_entity_file_path = getEntityFilePath(directory_path, id);
        auto entity = tryReadEntityFile(access_entity_file_path, *getLogger());
        if (!entity)
            continue;

        const String & name = entity->getName();
        auto type = entity->getType();
        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.type = type;
        entry.name = name;
        entry.entity = entity;
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name[entry.name] = &entry;
    }

    for (auto type : ext::range(EntityType::MAX))
        types_of_lists_to_write.insert(type);

    return true;
}


std::optional<UUID> DiskAccessStorage::findImpl(EntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    return it->second->id;
}


std::vector<UUID> DiskAccessStorage::findAllImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    std::vector<UUID> res;
    res.reserve(entries_by_name.size());
    for (const auto * entry : entries_by_name | boost::adaptors::map_values)
        res.emplace_back(entry->id);
    return res;
}

bool DiskAccessStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.count(id);
}


AccessEntityPtr DiskAccessStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    const auto & entry = it->second;
    if (!entry.entity)
        entry.entity = readAccessEntityFromDisk(id);
    return entry.entity;
}


String DiskAccessStorage::readNameImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);
    return String{it->second.name};
}


bool DiskAccessStorage::canInsertImpl(const AccessEntityPtr &) const
{
    return initialized;
}


UUID DiskAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = generateRandomID();
    std::lock_guard lock{mutex};
    insertNoLock(generateRandomID(), new_entity, replace_if_exists, notifications);
    return id;
}


void DiskAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications)
{
    const String & name = new_entity->getName();
    EntityType type = new_entity->getType();
    if (!initialized)
        throw Exception(
            "Cannot insert " + new_entity->outputTypeAndName() + " to storage [" + getStorageName()
                + "] because the output directory is not set",
            ErrorCodes::LOGICAL_ERROR);

    /// Check that we can insert.
    auto it_by_id = entries_by_id.find(id);
    if (it_by_id != entries_by_id.end())
    {
        const auto & existing_entry = it_by_id->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
    }

    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it_by_name = entries_by_name.find(name);
    bool name_collision = (it_by_name != entries_by_name.end());

    if (name_collision && !replace_if_exists)
        throwNameCollisionCannotInsert(type, name);

    scheduleWriteLists(type);
    writeAccessEntityToDisk(id, *new_entity);

    if (name_collision && replace_if_exists)
        removeNoLock(it_by_name->second->id, notifications);

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.type = type;
    entry.name = name;
    entry.entity = new_entity;
    entries_by_name[entry.name] = &entry;
    prepareNotifications(id, entry, false, notifications);
}


void DiskAccessStorage::removeImpl(const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    removeNoLock(id, notifications);
}


void DiskAccessStorage::removeNoLock(const UUID & id, Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    Entry & entry = it->second;
    EntityType type = entry.type;

    scheduleWriteLists(type);
    deleteAccessEntityOnDisk(id);

    /// Do removing.
    prepareNotifications(id, entry, true, notifications);
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(entry.name);
    entries_by_id.erase(it);
}


void DiskAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    updateNoLock(id, update_func, notifications);
}


void DiskAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    Entry & entry = it->second;
    if (!entry.entity)
        entry.entity = readAccessEntityFromDisk(id);
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return;

    const String & new_name = new_entity->getName();
    const String & old_name = old_entity->getName();
    const EntityType type = entry.type;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

    bool name_changed = (new_name != old_name);
    if (name_changed)
    {
        if (entries_by_name.count(new_name))
            throwNameCollisionCannotRename(type, old_name, new_name);
        scheduleWriteLists(type);
    }

    writeAccessEntityToDisk(id, *new_entity);
    entry.entity = new_entity;

    if (name_changed)
    {
        entries_by_name.erase(entry.name);
        entry.name = new_name;
        entries_by_name[entry.name] = &entry;
    }

    prepareNotifications(id, entry, false, notifications);
}


AccessEntityPtr DiskAccessStorage::readAccessEntityFromDisk(const UUID & id) const
{
    return readEntityFile(getEntityFilePath(directory_path, id));
}


void DiskAccessStorage::writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const
{
    writeEntityFile(getEntityFilePath(directory_path, id), entity);
}


void DiskAccessStorage::deleteAccessEntityOnDisk(const UUID & id) const
{
    auto file_path = getEntityFilePath(directory_path, id);
    if (!std::filesystem::remove(file_path))
        throw Exception("Couldn't delete " + file_path.string(), ErrorCodes::FILE_DOESNT_EXIST);
}


void DiskAccessStorage::prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const
{
    if (!remove && !entry.entity)
        return;

    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, id, entity});

    for (const auto & handler : handlers_by_type[static_cast<size_t>(entry.type)])
        notifications.push_back({handler, id, entity});
}


ext::scope_guard DiskAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto it2 = entries_by_id.find(id);
        if (it2 != entries_by_id.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}

ext::scope_guard DiskAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
    };
}

bool DiskAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it != entries_by_id.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}

bool DiskAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}

}
