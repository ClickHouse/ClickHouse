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
    /// Special parser for the 'ATTACH access entity' queries.
    class ParserAttachAccessEntity : public IParserBase
    {
    protected:
        const char * getName() const override { return "ATTACH access entity query"; }

        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            if (ParserCreateUserQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            if (ParserCreateRoleQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            if (ParserCreateRowPolicyQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            if (ParserCreateQuotaQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            if (ParserCreateSettingsProfileQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            if (ParserGrantQuery{}.enableAttachMode(true).parse(pos, node, expected))
                return true;
            return false;
        }
    };


    /// Reads a file containing ATTACH queries and then parses it to build an access entity.
    AccessEntityPtr readAccessEntityFile(const std::filesystem::path & file_path)
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


    AccessEntityPtr tryReadAccessEntityFile(const std::filesystem::path & file_path, Poco::Logger & log)
    {
        try
        {
            return readAccessEntityFile(file_path);
        }
        catch (...)
        {
            tryLogCurrentException(&log, "Could not parse " + file_path.string());
            return nullptr;
        }
    }


    /// Writes ATTACH queries for building a specified access entity to a file.
    void writeAccessEntityFile(const std::filesystem::path & file_path, const IAccessEntity & entity)
    {
        /// Build list of ATTACH queries.
        ASTs queries;
        queries.push_back(InterpreterShowCreateAccessEntityQuery::getAttachQuery(entity));
        if (entity.getType() == typeid(User) || entity.getType() == typeid(Role))
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
    std::filesystem::path getAccessEntityFilePath(const String & directory_path, const UUID & id)
    {
        return std::filesystem::path(directory_path).append(toString(id)).replace_extension(".sql");
    }


    /// Reads a map of name of access entity to UUID for access entities of some type from a file.
    std::unordered_map<String, UUID> readListFile(const std::filesystem::path & file_path)
    {
        ReadBufferFromFile in(file_path);

        size_t num;
        readVarUInt(num, in);
        std::unordered_map<String, UUID> res;
        res.reserve(num);

        for (size_t i = 0; i != num; ++i)
        {
            String name;
            readStringBinary(name, in);
            UUID id;
            readUUIDText(id, in);
            res[name] = id;
        }

        return res;
    }


    /// Writes a map of name of access entity to UUID for access entities of some type to a file.
    void writeListFile(const std::filesystem::path & file_path, const std::unordered_map<String, UUID> & map)
    {
        WriteBufferFromFile out(file_path);
        writeVarUInt(map.size(), out);
        for (const auto & [name, id] : map)
        {
            writeStringBinary(name, out);
            writeUUIDText(id, out);
        }
    }


    /// Calculates the path for storing a map of name of access entity to UUID for access entities of some type.
    std::filesystem::path getListFilePath(const String & directory_path, std::type_index type)
    {
        std::string_view file_name;
        if (type == typeid(User))
            file_name = "users";
        else if (type == typeid(Role))
            file_name = "roles";
        else if (type == typeid(Quota))
            file_name = "quotas";
        else if (type == typeid(RowPolicy))
            file_name = "row_policies";
        else if (type == typeid(SettingsProfile))
            file_name = "settings_profiles";
        else
            throw Exception("Unexpected type of access entity: " + IAccessEntity::getTypeName(type),
                            ErrorCodes::LOGICAL_ERROR);

        return std::filesystem::path(directory_path).append(file_name).replace_extension(".list");
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


    const std::vector<std::type_index> & getAllAccessEntityTypes()
    {
        static const std::vector<std::type_index> res = {typeid(User), typeid(Role), typeid(RowPolicy), typeid(Quota), typeid(SettingsProfile)};
        return res;
    }
}


DiskAccessStorage::DiskAccessStorage()
    : IAccessStorage("disk")
{
    for (auto type : getAllAccessEntityTypes())
        name_to_id_maps[type];
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

    for (const auto & [id, entry] : id_to_entry_map)
        prepareNotifications(id, entry, false, notifications);
}


bool DiskAccessStorage::readLists()
{
    assert(id_to_entry_map.empty());
    bool ok = true;
    for (auto type : getAllAccessEntityTypes())
    {
        auto & name_to_id_map = name_to_id_maps.at(type);
        auto file_path = getListFilePath(directory_path, type);
        if (!std::filesystem::exists(file_path))
        {
            LOG_WARNING(getLogger(), "File " + file_path.string() + " doesn't exist");
            ok = false;
            break;
        }

        try
        {
            name_to_id_map = readListFile(file_path);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not read " + file_path.string());
            ok = false;
            break;
        }

        for (const auto & [name, id] : name_to_id_map)
            id_to_entry_map.emplace(id, Entry{name, type});
    }

    if (!ok)
    {
        id_to_entry_map.clear();
        for (auto & name_to_id_map : name_to_id_maps | boost::adaptors::map_values)
            name_to_id_map.clear();
    }
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
        const auto & name_to_id_map = name_to_id_maps.at(type);
        auto file_path = getListFilePath(directory_path, type);
        try
        {
            writeListFile(file_path, name_to_id_map);
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


void DiskAccessStorage::scheduleWriteLists(std::type_index type)
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
    LOG_WARNING(getLogger(), "Recovering lists in directory " + directory_path);
    assert(id_to_entry_map.empty());

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

        const auto access_entity_file_path = getAccessEntityFilePath(directory_path, id);
        auto entity = tryReadAccessEntityFile(access_entity_file_path, *getLogger());
        if (!entity)
            continue;

        auto type = entity->getType();
        auto & name_to_id_map = name_to_id_maps.at(type);
        auto it_by_name = name_to_id_map.emplace(entity->getName(), id).first;
        id_to_entry_map.emplace(id, Entry{it_by_name->first, type});
    }

    for (auto type : getAllAccessEntityTypes())
        types_of_lists_to_write.insert(type);

    return true;
}


std::optional<UUID> DiskAccessStorage::findImpl(std::type_index type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & name_to_id_map = name_to_id_maps.at(type);
    auto it = name_to_id_map.find(name);
    if (it == name_to_id_map.end())
        return {};

    return it->second;
}


std::vector<UUID> DiskAccessStorage::findAllImpl(std::type_index type) const
{
    std::lock_guard lock{mutex};
    const auto & name_to_id_map = name_to_id_maps.at(type);
    std::vector<UUID> res;
    res.reserve(name_to_id_map.size());
    boost::range::copy(name_to_id_map | boost::adaptors::map_values, std::back_inserter(res));
    return res;
}

bool DiskAccessStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return id_to_entry_map.count(id);
}


AccessEntityPtr DiskAccessStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = id_to_entry_map.find(id);
    if (it == id_to_entry_map.end())
        throwNotFound(id);

    const auto & entry = it->second;
    if (!entry.entity)
        entry.entity = readAccessEntityFromDisk(id);
    return entry.entity;
}


String DiskAccessStorage::readNameImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = id_to_entry_map.find(id);
    if (it == id_to_entry_map.end())
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
    std::type_index type = new_entity->getType();
    if (!initialized)
        throw Exception(
            "Cannot insert " + new_entity->getTypeName() + " " + backQuote(name) + " to " + getStorageName()
                + " because the output directory is not set",
            ErrorCodes::LOGICAL_ERROR);

    /// Check that we can insert.
    auto it_by_id = id_to_entry_map.find(id);
    if (it_by_id != id_to_entry_map.end())
    {
        const auto & existing_entry = it_by_id->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
    }

    auto & name_to_id_map = name_to_id_maps.at(type);
    auto it_by_name = name_to_id_map.find(name);
    bool name_collision = (it_by_name != name_to_id_map.end());

    if (name_collision && !replace_if_exists)
        throwNameCollisionCannotInsert(type, name);

    scheduleWriteLists(type);
    writeAccessEntityToDisk(id, *new_entity);

    if (name_collision && replace_if_exists)
        removeNoLock(it_by_name->second, notifications);

    /// Do insertion.
    it_by_name = name_to_id_map.emplace(name, id).first;
    it_by_id = id_to_entry_map.emplace(id, Entry{it_by_name->first, type}).first;
    auto & entry = it_by_id->second;
    entry.entity = new_entity;
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
    auto it = id_to_entry_map.find(id);
    if (it == id_to_entry_map.end())
        throwNotFound(id);

    Entry & entry = it->second;
    String name{it->second.name};
    std::type_index type = it->second.type;

    scheduleWriteLists(type);
    deleteAccessEntityOnDisk(id);

    /// Do removing.
    prepareNotifications(id, entry, true, notifications);
    id_to_entry_map.erase(it);
    auto & name_to_id_map = name_to_id_maps.at(type);
    name_to_id_map.erase(name);
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
    auto it = id_to_entry_map.find(id);
    if (it == id_to_entry_map.end())
        throwNotFound(id);

    Entry & entry = it->second;
    if (!entry.entity)
        entry.entity = readAccessEntityFromDisk(id);
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (*new_entity == *old_entity)
        return;

    String new_name = new_entity->getName();
    auto old_name = entry.name;
    const std::type_index type = entry.type;
    bool name_changed = (new_name != old_name);
    if (name_changed)
    {
        const auto & name_to_id_map = name_to_id_maps.at(type);
        if (name_to_id_map.count(new_name))
            throwNameCollisionCannotRename(type, String{old_name}, new_name);
        scheduleWriteLists(type);
    }

    writeAccessEntityToDisk(id, *new_entity);
    entry.entity = new_entity;

    if (name_changed)
    {
        auto & name_to_id_map = name_to_id_maps.at(type);
        name_to_id_map.erase(String{old_name});
        auto it_by_name = name_to_id_map.emplace(new_name, id).first;
        entry.name = it_by_name->first;
    }

    prepareNotifications(id, entry, false, notifications);
}


AccessEntityPtr DiskAccessStorage::readAccessEntityFromDisk(const UUID & id) const
{
    return readAccessEntityFile(getAccessEntityFilePath(directory_path, id));
}


void DiskAccessStorage::writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const
{
    writeAccessEntityFile(getAccessEntityFilePath(directory_path, id), entity);
}


void DiskAccessStorage::deleteAccessEntityOnDisk(const UUID & id) const
{
    auto file_path = getAccessEntityFilePath(directory_path, id);
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

    auto range = handlers_by_type.equal_range(entry.type);
    for (auto it = range.first; it != range.second; ++it)
        notifications.push_back({it->second, id, entity});
}


ext::scope_guard DiskAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto it = id_to_entry_map.find(id);
    if (it == id_to_entry_map.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto it2 = id_to_entry_map.find(id);
        if (it2 != id_to_entry_map.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}

ext::scope_guard DiskAccessStorage::subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto handler_it = handlers_by_type.emplace(type, handler);

    return [this, handler_it]
    {
        std::lock_guard lock2{mutex};
        handlers_by_type.erase(handler_it);
    };
}

bool DiskAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = id_to_entry_map.find(id);
    if (it != id_to_entry_map.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}

bool DiskAccessStorage::hasSubscriptionImpl(std::type_index type) const
{
    std::lock_guard lock{mutex};
    auto range = handlers_by_type.equal_range(type);
    return range.first != range.second;
}

}
