#pragma once

#include <ACL/IControlAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <optional>


namespace DB
{
class IControlAttributesStorage;

class IControlAttributesStorageManager
{
public:
    using Type = IControlAttributes::Type;
    using Storage = IControlAttributesStorage;

    virtual ~IControlAttributesStorageManager() {}

    virtual std::pair<std::optional<UUID>, Storage *> findInAllStorages(const String & name, const Type & type) const = 0;
    virtual Storage * findStorage(UUID id) const = 0;
    virtual Storage * getCurrentStorage() const = 0;
    virtual const std::vector<Storage *> & getAllStorages() const = 0;
};

}
