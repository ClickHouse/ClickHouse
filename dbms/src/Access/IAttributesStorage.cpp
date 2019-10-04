#include <Access/IAttributesStorage.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTRIBUTES_NOT_FOUND;
}

String backQuoteIfNeed(const String & x);


UUID IAttributesStorage::getID(const String & name, const Type & type) const
{
    auto id = find(name, type);
    if (id)
        return *id;
    throwNotFound(name, type);
}


AttributesPtr IAttributesStorage::tryReadHelper(const UUID & id) const
{
    try
    {
        return readImpl(id);
    }
    catch (...)
    {
        return nullptr;
    }
}


String IAttributesStorage::readName(const UUID & id) const
{
    return readNameImpl(id);
}


std::optional<String> IAttributesStorage::tryReadName(const UUID & id) const
{
    try
    {
        return readNameImpl(id);
    }
    catch (...)
    {
        return {};
    }
}


UUID insert(const IAttributes & attrs, bool replace_if_exists = false);
UUID insert(const AttributesPtr & attrs, bool replace_if_exists = false);
std::vector<UUID> insert(const std::vector<AttributesPtr> & attrs, bool replace_if_exists = false);

/// Inserts attributes to the storage.
/// Returns `{id, true}` if successfully inserted or `{id, false}` if the specified name is already in use.
std::pair<UUID, bool> tryInsert(const IAttributes & attrs);
std::pair<UUID, bool> tryInsert(const AttributesPtr & attrs);
std::vector<std::pair<UUID, bool>> tryInsert(const std::vector<AttributesPtr> & attrs);


UUID IAttributesStorage::insert(const IAttributes & attrs, bool replace_if_exists)
{
    return insertImpl(attrs, replace_if_exists);
}


UUID IAttributesStorage::insert(const AttributesPtr & attrs, bool replace_if_exists)
{
    return insert(*attrs, replace_if_exists);
}


std::vector<UUID> IAttributesStorage::insert(const std::vector<AttributesPtr> & multiple_attrs, bool replace_if_exists)
{
    std::vector<UUID> ids;
    Strings failed_to_insert;
    String exception_message;
    int exception_code;
    for (const auto & attrs : multiple_attrs)
    {
        try
        {
            ids.emplace_back(insertImpl(*attrs, replace_if_exists));
        }
        catch (...)
        {
            failed_to_insert.emplace_back(attrs->name);
            exception_message = getCurrentExceptionMessage(false);
            exception_code = getCurrentExceptionCode();
        }
    }

    if (!failed_to_insert.empty())
    {
        String msg = "Couldn't insert ";
        for (size_t i = 0; i != failed_to_insert.size(); ++i)
            msg += String(i ? ", " : "") + backQuoteIfNeed(failed_to_insert[i]);
        msg += ": " + exception_message;
        throw Exception(msg, exception_code);
    }
    return ids;
}


std::optional<UUID> IAttributesStorage::tryInsert(const IAttributes & attrs)
{
    try
    {
        return insertImpl(attrs, false);
    }
    catch (...)
    {
        return std::nullopt;
    }
}


std::optional<UUID> IAttributesStorage::tryInsert(const AttributesPtr & attrs)
{
    return tryInsert(*attrs);
}


std::vector<std::optional<UUID>> IAttributesStorage::tryInsert(const std::vector<AttributesPtr> & multiple_attrs)
{
    std::vector<std::optional<UUID>> ids;
    for (const auto & attrs : multiple_attrs)
    {
        std::optional<UUID> id;
        try
        {
            id = insertImpl(*attrs, false);
        }
        catch (...)
        {
        }
        ids.emplace_back(id);
    }
    return ids;
}


void IAttributesStorage::remove(const std::vector<UUID> & ids)
{
    std::vector<UUID> failed_to_remove;
    String exception_message;
    int exception_code;
    for (const UUID & id : ids)
    {
        try
        {
            removeImpl(id);
        }
        catch (...)
        {
            failed_to_remove.emplace_back(id);
            exception_message = getCurrentExceptionMessage(false);
            exception_code = getCurrentExceptionCode();
        }
    }

    if (!failed_to_remove.empty())
    {
        String msg = "Couldn't remove ";
        for (size_t i = 0; i != failed_to_remove.size(); ++i)
            msg += String(i ? ", " : "") + "{" + toString(failed_to_remove[i]) + "}";
        msg += ": " + exception_message;
        throw Exception(msg, exception_code);
    }
}


void IAttributesStorage::remove(const Strings & names, const Type & type)
{
    Strings failed_to_remove;
    String exception_message;
    int exception_code;
    for (const String & name : names)
    {
        try
        {
            removeImpl(getID(name, type));
        }
        catch (...)
        {
            failed_to_remove.emplace_back(name);
            exception_message = getCurrentExceptionMessage(false);
            exception_code = getCurrentExceptionCode();
        }
    }

    if (!failed_to_remove.empty())
    {
        String msg = "Couldn't remove ";
        for (size_t i = 0; i != failed_to_remove.size(); ++i)
            msg += String(i ? ", " : "") + backQuoteIfNeed(failed_to_remove[i]);
        msg += ": " + exception_message;
        throw Exception(msg, exception_code);
    }
}


bool IAttributesStorage::tryRemove(const UUID & id)
{
    try
    {
        removeImpl(id);
        return true;
    }
    catch (...)
    {
        return false;
    }
}


bool IAttributesStorage::tryRemove(const String & name, const Type & type)
{
    try
    {
        removeImpl(getID(name, type));
        return true;
    }
    catch (...)
    {
        return false;
    }
}


void IAttributesStorage::tryRemove(const std::vector<UUID> & ids, std::vector<UUID> * failed_to_remove)
{
    if (failed_to_remove)
        failed_to_remove->clear();
    for (const UUID & id : ids)
    {
        try
        {
            removeImpl(id);
        }
        catch (...)
        {
            if (failed_to_remove)
                failed_to_remove->emplace_back(id);
        }
    }
}


void IAttributesStorage::tryRemove(const Strings & names, const Type & type, Strings * failed_to_remove)
{
    if (failed_to_remove)
        failed_to_remove->clear();
    for (const String & name : names)
    {
        try
        {
            removeImpl(getID(name, type));
        }
        catch (...)
        {
            if (failed_to_remove)
                failed_to_remove->emplace_back(name);
        }
    }
}


void IAttributesStorage::updateHelper(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    std::vector<UUID> failed_to_update;
    String exception_message;
    int exception_code;
    for (const UUID & id : ids)
    {
        try
        {
            updateImpl(id, update_func);
        }
        catch (...)
        {
            failed_to_update.emplace_back(id);
            exception_message = getCurrentExceptionMessage(false);
            exception_code = getCurrentExceptionCode();
        }
    }

    if (!failed_to_update.empty())
    {
        String msg = "Couldn't update ";
        for (size_t i = 0; i != failed_to_update.size(); ++i)
            msg += String(i ? ", " : "") + "{" + toString(failed_to_update[i]) + "}";
        msg += ": " + exception_message;
        throw Exception(msg, exception_code);
    }
}


void IAttributesStorage::updateHelper(const Strings & names, const Type & type, const UpdateFunc & update_func)
{
    Strings failed_to_update;
    String exception_message;
    int exception_code;
    for (const String & name : names)
    {
        try
        {
            updateImpl(getID(name, type), update_func);
        }
        catch (...)
        {
            failed_to_update.emplace_back(name);
            exception_message = getCurrentExceptionMessage(false);
            exception_code = getCurrentExceptionCode();
        }
    }

    if (!failed_to_update.empty())
    {
        String msg = "Couldn't update ";
        for (size_t i = 0; i != failed_to_update.size(); ++i)
            msg += String(i ? ", " : "") + "{" + failed_to_update[i] + "}";
        msg += ": " + exception_message;
        throw Exception(msg, exception_code);
    }
}


bool IAttributesStorage::tryUpdateHelper(const UUID & id, const UpdateFunc & update_func)
{
    try
    {
        updateImpl(id, update_func);
        return true;
    }
    catch (...)
    {
        return false;
    }
}


bool IAttributesStorage::tryUpdateHelper(const String & name, const Type & type, const UpdateFunc & update_func)
{
    try
    {
        updateImpl(getID(name, type), update_func);
        return true;
    }
    catch (...)
    {
        return false;
    }
}


void IAttributesStorage::tryUpdateHelper(const std::vector<UUID> & ids, const UpdateFunc & update_func, std::vector<UUID> * failed_to_update)
{
    if (failed_to_update)
        failed_to_update->clear();
    for (const UUID & id : ids)
    {
        try
        {
            updateImpl(id, update_func);
        }
        catch (...)
        {
            if (failed_to_update)
                failed_to_update->emplace_back(id);
        }
    }
}


void IAttributesStorage::tryUpdateHelper(const Strings & names, const Type & type, const UpdateFunc & update_func, Strings * failed_to_update)
{
    if (failed_to_update)
        failed_to_update->clear();
    for (const String & name : names)
    {
        try
        {
            updateImpl(getID(name, type), update_func);
        }
        catch (...)
        {
            if (failed_to_update)
                failed_to_update->emplace_back(name);
        }
    }
}


UUID IAttributesStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}


void IAttributesStorage::throwNotFound(const UUID & id)
{
    throw Exception("ID {" + toString(id) + "} not found", ErrorCodes::ATTRIBUTES_NOT_FOUND);
}


void IAttributesStorage::throwNotFound(const String & name, const Type & type)
{
    throw Exception(String(type.name) + " " + backQuoteIfNeed(name) + " not found", type.error_code_not_found);
}


void IAttributesStorage::throwNameCollisionCannotInsert(const String & name, const Type & type, const Type & type_of_existing)
{
    throw Exception(
        String(type.name) + " " + backQuoteIfNeed(name) + ": cannot insert because " + type_of_existing.name + " " + backQuoteIfNeed(name)
            + " already exists",
        type.error_code_already_exists);
}


void IAttributesStorage::throwNameCollisionCannotRename(const String & old_name, const String & new_name, const Type & type, const Type & type_of_existing)
{
    throw Exception(
        String(type.name) + " " + backQuoteIfNeed(old_name) + ": cannot rename to " + backQuoteIfNeed(new_name) + " because "
            + type_of_existing.name + " " + backQuoteIfNeed(new_name) + " already exists",
        type.error_code_already_exists);
}


void IAttributesStorage::notify(const OnChangeNotifications & notifications)
{
    for (const auto & [fn, attrs] : notifications)
        fn(attrs);
}


void IAttributesStorage::notify(const OnNewNotifications & notifications)
{
    for (const auto & [fn, id] : notifications)
        fn(id);
}
}
