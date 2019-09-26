#pragma once

#include <Core/Types.h>
#include <functional>
#include <memory>
#include <vector>


namespace DB
{
class IAttributesStorage;


/// Manages access control entities.
class AccessControlManager
{
public:
    AccessControlManager();
    ~AccessControlManager();

    template <typename ACLType>
    ACLType create(const typename ACLType::Attributes & attrs, bool if_not_exists = false);

    template <typename ACLType>
    using UpdateFunction = std::function<void(typename ACLType::Attributes &)>;

    template <typename ACLType>
    using UpdateFunctions = std::vector<std::pair<String, UpdateFunction<ACLType>>>;

    template <typename ACLType>
    void update(const String & name, const UpdateFunction<ACLType> & update_function);

    template <typename ACLType>
    void update(const UpdateFunctions<ACLType> & update_functions);

    template <typename ACLType>
    bool drop(const String & name, bool if_not_exists = false);

    template <typename ACLType>
    void drop(const Strings & names, bool if_not_exists = false);

    template <typename ACLType>
    ACLType get(const String & name);

    template <typename ACLType>
    ACLType get(const String & name) const;

    template <typename ACLType>
    std::optional<ACLType> find(const String & name);

    template <typename ACLType>
    std::optional<ACLType> find(const String & name) const;

private:
    std::unique_ptr<IAttributesStorage> storage;
};

}
