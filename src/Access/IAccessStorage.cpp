#include <Access/IAccessStorage.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int ACCESS_STORAGE_READONLY;
    extern const int LOGICAL_ERROR;
}


namespace
{
    using EntityType = IAccessStorage::EntityType;
    using EntityTypeInfo = IAccessStorage::EntityTypeInfo;


    String outputID(const UUID & id)
    {
        return "ID(" + toString(id) + ")";
    }

    String outputTypeAndNameOrID(const IAccessStorage & storage, const UUID & id)
    {
        auto entity = storage.tryRead(id);
        if (entity)
            return entity->outputTypeAndName();
        return outputID(id);
    }


    template <typename Func, typename ResultType = std::result_of_t<Func()>>
    ResultType doTry(const Func & func)
    {
        try
        {
            return func();
        }
        catch (Exception &)
        {
            return {};
        }
    }


    template <bool ignore_errors, typename T, typename ApplyFunc, typename GetNameFunc = std::nullptr_t,
              typename ResultTypeOfApplyFunc = std::result_of_t<ApplyFunc(T)>,
              typename ResultType = std::conditional_t<std::is_same_v<ResultTypeOfApplyFunc, void>, void, std::vector<ResultTypeOfApplyFunc>>>
    ResultType applyToMultipleEntities(
        const std::vector<T> & multiple_entities,
        const ApplyFunc & apply_function,
        const char * error_message_format [[maybe_unused]] = nullptr,
        const GetNameFunc & get_name_function [[maybe_unused]] = nullptr)
    {
        std::optional<Exception> exception;
        std::vector<bool> success;

        auto helper = [&](const auto & apply_and_store_result_function)
        {
            for (size_t i = 0; i != multiple_entities.size(); ++i)
            {
                try
                {
                    apply_and_store_result_function(multiple_entities[i]);
                    if constexpr (!ignore_errors)
                        success[i] = true;
                }
                catch (Exception & e)
                {
                    if (!ignore_errors && !exception)
                        exception.emplace(e);
                }
                catch (Poco::Exception & e)
                {
                    if (!ignore_errors && !exception)
                        exception.emplace(Exception::CreateFromPocoTag{}, e);
                }
                catch (std::exception & e)
                {
                    if (!ignore_errors && !exception)
                        exception.emplace(Exception::CreateFromSTDTag{}, e);
                }
            }
        };

        if constexpr (std::is_same_v<ResultType, void>)
        {
            if (multiple_entities.empty())
                return;

            if (multiple_entities.size() == 1)
            {
                apply_function(multiple_entities.front());
                return;
            }

            if constexpr (!ignore_errors)
                success.resize(multiple_entities.size(), false);

            helper(apply_function);

            if (ignore_errors || !exception)
                return;
        }
        else
        {
            ResultType result;
            if (multiple_entities.empty())
                return result;

            if (multiple_entities.size() == 1)
            {
                result.emplace_back(apply_function(multiple_entities.front()));
                return result;
            }

            result.reserve(multiple_entities.size());
            if constexpr (!ignore_errors)
                success.resize(multiple_entities.size(), false);

            helper([&](const T & entity) { result.emplace_back(apply_function(entity)); });

            if (ignore_errors || !exception)
                return result;
        }

        if constexpr (!ignore_errors)
        {
            Strings succeeded_names_list;
            Strings failed_names_list;
            for (size_t i = 0; i != multiple_entities.size(); ++i)
            {
                const auto & entity = multiple_entities[i];
                String name = get_name_function(entity);
                if (success[i])
                    succeeded_names_list.emplace_back(name);
                else
                    failed_names_list.emplace_back(name);
            }
            String succeeded_names = boost::algorithm::join(succeeded_names_list, ", ");
            String failed_names = boost::algorithm::join(failed_names_list, ", ");
            if (succeeded_names.empty())
                succeeded_names = "none";

            String error_message = error_message_format;
            boost::replace_all(error_message, "{succeeded_names}", succeeded_names);
            boost::replace_all(error_message, "{failed_names}", failed_names);
            exception->addMessage(error_message);
            exception->rethrow();
        }
        __builtin_unreachable();
    }
}


std::vector<UUID> IAccessStorage::findAll(EntityType type) const
{
    return findAllImpl(type);
}


std::optional<UUID> IAccessStorage::find(EntityType type, const String & name) const
{
    return findImpl(type, name);
}


std::vector<UUID> IAccessStorage::find(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
    {
        auto id = findImpl(type, name);
        if (id)
            ids.push_back(*id);
    }
    return ids;
}


UUID IAccessStorage::getID(EntityType type, const String & name) const
{
    auto id = findImpl(type, name);
    if (id)
        return *id;
    throwNotFound(type, name);
}


std::vector<UUID> IAccessStorage::getIDs(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
        ids.push_back(getID(type, name));
    return ids;
}


bool IAccessStorage::exists(const UUID & id) const
{
    return existsImpl(id);
}


AccessEntityPtr IAccessStorage::tryReadBase(const UUID & id) const
{
    return doTry([&] { return readImpl(id); });
}


String IAccessStorage::readName(const UUID & id) const
{
    return readNameImpl(id);
}


std::optional<String> IAccessStorage::tryReadName(const UUID & id) const
{
    return doTry([&] { return std::optional<String>{readNameImpl(id)}; });
}


UUID IAccessStorage::insert(const AccessEntityPtr & entity)
{
    return insertImpl(entity, false);
}


std::vector<UUID> IAccessStorage::insert(const std::vector<AccessEntityPtr> & multiple_entities)
{
    return applyToMultipleEntities</* ignore_errors = */ false>(
        multiple_entities,
        [this](const AccessEntityPtr & entity) { return insertImpl(entity, /* replace_if_exists = */ false); },
        "Couldn't insert {failed_names}. Successfully inserted: {succeeded_names}",
        [](const AccessEntityPtr & entity) { return entity->outputTypeAndName(); });
}


std::optional<UUID> IAccessStorage::tryInsert(const AccessEntityPtr & entity)
{
    return doTry([&] { return std::optional<UUID>{insertImpl(entity, false)}; });
}


std::vector<UUID> IAccessStorage::tryInsert(const std::vector<AccessEntityPtr> & multiple_entities)
{
    return applyToMultipleEntities</* ignore_errors = */ true>(
        multiple_entities,
        [this](const AccessEntityPtr & entity) { return insertImpl(entity, /* replace_if_exists = */ false); });
}


UUID IAccessStorage::insertOrReplace(const AccessEntityPtr & entity)
{
    return insertImpl(entity, true);
}


std::vector<UUID> IAccessStorage::insertOrReplace(const std::vector<AccessEntityPtr> & multiple_entities)
{
    return applyToMultipleEntities</* ignore_errors = */ false>(
        multiple_entities,
        [this](const AccessEntityPtr & entity) { return insertImpl(entity, /* replace_if_exists = */ true); },
        "Couldn't insert {failed_names}. Successfully inserted: {succeeded_names}",
        [](const AccessEntityPtr & entity) -> String { return entity->outputTypeAndName(); });
}


void IAccessStorage::remove(const UUID & id)
{
    removeImpl(id);
}


void IAccessStorage::remove(const std::vector<UUID> & ids)
{
    applyToMultipleEntities</* ignore_errors = */ false>(
        ids,
        [this](const UUID & id) { removeImpl(id); },
        "Couldn't remove {failed_names}. Successfully removed: {succeeded_names}",
        [this](const UUID & id) { return outputTypeAndNameOrID(*this, id); });
}


bool IAccessStorage::tryRemove(const UUID & id)
{
    return doTry([&] { removeImpl(id); return true; });
}


std::vector<UUID> IAccessStorage::tryRemove(const std::vector<UUID> & ids)
{
    return applyToMultipleEntities</* ignore_errors = */ true>(
        ids,
        [this](const UUID & id) { removeImpl(id); return id; });
}


void IAccessStorage::update(const UUID & id, const UpdateFunc & update_func)
{
    updateImpl(id, update_func);
}


void IAccessStorage::update(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    applyToMultipleEntities</* ignore_errors = */ false>(
        ids,
        [this, &update_func](const UUID & id) { updateImpl(id, update_func); },
        "Couldn't update {failed_names}. Successfully updated: {succeeded_names}",
        [this](const UUID & id) { return outputTypeAndNameOrID(*this, id); });
}


bool IAccessStorage::tryUpdate(const UUID & id, const UpdateFunc & update_func)
{
    return doTry([&] { updateImpl(id, update_func); return true; });
}


std::vector<UUID> IAccessStorage::tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    return applyToMultipleEntities</* ignore_errors = */ true>(
        ids,
        [this, &update_func](const UUID & id) { updateImpl(id, update_func); return id; });
}


ext::scope_guard IAccessStorage::subscribeForChanges(EntityType type, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(type, handler);
}


ext::scope_guard IAccessStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(id, handler);
}


ext::scope_guard IAccessStorage::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const
{
    ext::scope_guard subscriptions;
    for (const auto & id : ids)
        subscriptions.join(subscribeForChangesImpl(id, handler));
    return subscriptions;
}


bool IAccessStorage::hasSubscription(EntityType type) const
{
    return hasSubscriptionImpl(type);
}


bool IAccessStorage::hasSubscription(const UUID & id) const
{
    return hasSubscriptionImpl(id);
}


void IAccessStorage::notify(const Notifications & notifications)
{
    for (const auto & [fn, id, new_entity] : notifications)
        fn(id, new_entity);
}


UUID IAccessStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}


Poco::Logger * IAccessStorage::getLogger() const
{
    Poco::Logger * ptr = log.load();
    if (!ptr)
        log.store(ptr = &Poco::Logger::get("Access(" + storage_name + ")"), std::memory_order_relaxed);
    return ptr;
}


void IAccessStorage::throwNotFound(const UUID & id) const
{
    throw Exception(outputID(id) + " not found in [" + getStorageName() + "]", ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwNotFound(EntityType type, const String & name) const
{
    int error_code = EntityTypeInfo::get(type).not_found_error_code;
    throw Exception("There is no " + outputEntityTypeAndName(type, name) + " in [" + getStorageName() + "]", error_code);
}


void IAccessStorage::throwBadCast(const UUID & id, EntityType type, const String & name, EntityType required_type)
{
    throw Exception(
        outputID(id) + ": " + outputEntityTypeAndName(type, name) + " expected to be of type " + toString(required_type),
        ErrorCodes::LOGICAL_ERROR);
}


void IAccessStorage::throwIDCollisionCannotInsert(const UUID & id, EntityType type, const String & name, EntityType existing_type, const String & existing_name) const
{
    throw Exception(
        outputEntityTypeAndName(type, name) + ": cannot insert because the " + outputID(id) + " is already used by "
            + outputEntityTypeAndName(existing_type, existing_name) + " in [" + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotInsert(EntityType type, const String & name) const
{
    throw Exception(
        outputEntityTypeAndName(type, name) + ": cannot insert because " + outputEntityTypeAndName(type, name) + " already exists in ["
            + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotRename(EntityType type, const String & old_name, const String & new_name) const
{
    throw Exception(
        outputEntityTypeAndName(type, old_name) + ": cannot rename to " + backQuote(new_name) + " because "
            + outputEntityTypeAndName(type, new_name) + " already exists in [" + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwReadonlyCannotInsert(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot insert " + outputEntityTypeAndName(type, name) + " to [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotUpdate(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot update " + outputEntityTypeAndName(type, name) + " in [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotRemove(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot remove " + outputEntityTypeAndName(type, name) + " from [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}
}
