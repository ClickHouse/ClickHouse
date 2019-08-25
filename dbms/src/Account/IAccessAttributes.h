#pragma once

#include <Core/Types.h>
#include <memory>


namespace DB
{
class RoleAttributes;
class UserAttributes;
class RLSPolicyAttributes;

/// Base class for access-control attributes.
struct IAccessAttributes
{
    enum class Type
    {
        ROLE,
        USER,
        RLS_POLICY,
    };

    String name;
    const Type type;

    IAccessAttributes(Type type_) : type(type_) {}
    virtual ~IAccessAttributes() {}
    virtual std::shared_ptr<IAccessAttributes> clone() const = 0;
    virtual bool isEqual(const IAccessAttributes & other) const;

    template <typename ToType>
    ToType & as();

    template <typename ToType>
    const ToType & as() const;

protected:
    void copyTo(IAccessAttributes & dest) const;
};

using AccessAttributesPtr = std::shared_ptr<const IAccessAttributes>;
}
