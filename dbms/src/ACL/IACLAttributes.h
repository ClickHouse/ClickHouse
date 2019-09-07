#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <memory>


namespace DB
{
/// Access control attributes.
/// Attributes are part of the element's data that can be stored and loaded to a file or another storage.
struct IACLAttributes
{
    enum class Type
    {
        USER,
        ROLE,
        QUOTA,
        ROW_FILTER_POLICY,
    };

    String name;

    virtual ~IACLAttributes() {}
    virtual Type getType() const = 0;
    virtual std::shared_ptr<IACLAttributes> clone() const = 0;
    virtual bool hasReferences(UUID) const { return false; }
    virtual void removeReferences(UUID) {}

    friend bool operator ==(const IACLAttributes & lhs, const IACLAttributes & rhs) { return lhs.equal(rhs); }
    friend bool operator !=(const IACLAttributes & lhs, const IACLAttributes & rhs) { return !(lhs == rhs); }
protected:
    virtual bool equal(const IACLAttributes & other) const { return (name == other.name) && (getType() == other.getType()); }
};

using ACLAttributesPtr = std::shared_ptr<const IACLAttributes>;
}
