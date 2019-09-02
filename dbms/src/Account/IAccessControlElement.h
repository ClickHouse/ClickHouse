#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <memory>
#include <utility>


namespace DB
{
class IAccessControlStorage;
class AccessControlManager;


/// A named object that controls access somehow.
/// Role, user, and row level security policy are derived from this class.
class IAccessControlElement
{
public:
    enum class Type
    {
        USER,
        ROLE,
        ROW_LEVEL_SECURITY_POLICY,
        QUOTA,
        SETTINGS_PROFILE,
    };

    /// Attributes of the element.
    /// Attributes are part of the element's data that can be stored and loaded to a file or another storage.
    struct Attributes
    {
        String name;

        virtual ~Attributes() {}
        virtual Type getType() const = 0;
        virtual std::shared_ptr<Attributes> clone() const = 0;
        virtual bool hasReferences(UUID) const { return false; }
        virtual void removeReferences(UUID) {}

        friend bool operator ==(const Attributes & lhs, const Attributes & rhs) { return lhs.equal(rhs); }
        friend bool operator !=(const Attributes & lhs, const Attributes & rhs) { return !(lhs == rhs); }
    protected:
        virtual bool equal(const Attributes & other) const { return (name == other.name) && (getType() == other.getType()); }
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;

    using Manager = AccessControlManager;
    using Storage = IAccessControlStorage;
    using StoragePtr = std::shared_ptr<IAccessControlStorage>;

    IAccessControlElement();
    IAccessControlElement(const UUID & id_, Manager * manager_);
    IAccessControlElement(const UUID & id_, Manager * manager_, const StoragePtr & storage_);
    IAccessControlElement(const IAccessControlElement & src);
    IAccessControlElement & operator =(const IAccessControlElement & src);
    virtual ~IAccessControlElement();

    const UUID & getID() const { return id; }
    Manager * getManager() const { return manager; }

    /// Whether this access control element is valid, i.e. controls the access somehow.
    bool isValid() const;

    /// Returns the attributes. Throws an exception if not valid.
    AttributesPtr getAttributes() const { return getAttributesImpl<Attributes>(); }

    /// Returns the attributes. Returns nullptr if not valid.
    AttributesPtr tryGetAttributes() const { return tryGetAttributesImpl<Attributes>(); }

    /// Operation which can be performed on the attributes.
    struct Operation
    {
        Operation & then(Operation && src);
        Operation & then(const std::function<void(Attributes &)> & fn);

        std::vector<std::function<void(Attributes &)>> ops;
    };

    /// Performs the operation. Throws an exception on fail.
    void perform(const Operation & operation);

    /// Returns the name of this access control element.
    String getName() const;

    /// Changes the name of this access control element.
    void setName(const String & name) { perform(setNameOp(name)); }
    Operation setNameOp(const String & name) const;

    /// Returns the name or ID. This function returns a string representation of this element
    /// for logging or exception message. This function tries not to throw exceptions.
    String getNameOrID() const;

    /// Drops this access control element.
    /// Returns true if successfully dropped, or false if this element didn't exist.
    bool drop();

protected:
    virtual const String & getTypeName() const = 0;
    virtual int getNotFoundErrorCode() const = 0;
    virtual int getAlreadyExistsErrorCode() const = 0;

    template <typename AttributesType>
    std::shared_ptr<const AttributesType> getAttributesImpl() const;

    template <typename AttributesType>
    std::shared_ptr<const AttributesType> tryGetAttributesImpl() const;

    template <typename AttributesType>
    Operation prepareOperationImpl(const std::function<void(AttributesType &)> & fn) const;

private:
    Storage * getStorage() const;
    AttributesPtr getAttributesInternal() const;
    Operation prepareOperation(const std::function<void(Attributes &)> & fn) const { return prepareOperationImpl<Attributes>(fn); }
    [[noreturn]] static void throwException(const String & message, int error_code);

    UUID id = UUID(UInt128(0));
    Manager * manager = nullptr;
    mutable std::optional<StoragePtr> storage_cached;
};


template <typename AttributesType>
std::shared_ptr<const AttributesType> IAccessControlElement::getAttributesImpl() const
{
    auto attrs = tryGetAttributesImpl<AttributesType>();
    if (!attrs)
        throwException("There is no such " + getTypeName() + " " + getNameOrID(), getNotFoundErrorCode());
    return attrs;
}


template <typename AttributesType>
std::shared_ptr<const AttributesType> IAccessControlElement::tryGetAttributesImpl() const
{
    return std::dynamic_pointer_cast<const AttributesType>(getAttributesInternal());
}


template <typename AttributesType>
IAccessControlElement::Operation IAccessControlElement::prepareOperationImpl(const std::function<void(AttributesType &)> & fn) const
{
    const String & type_name = getTypeName();
    int error_code = getNotFoundErrorCode();
    return Operation{}.then([fn, &type_name, error_code](Attributes & attrs)
    {
        AttributesType * a = dynamic_cast<AttributesType *>(&attrs);
        if (!a)
            throwException("There is no such " + type_name + " " + attrs.name, error_code);
        fn(*a);
    });
}

}
