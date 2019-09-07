#pragma once

#include <ACL/IACLAttributesStorage.h>
#include <memory>
#include <atomic>


namespace DB
{
class IACLAttributableManager;


/// A named object that controls access somehow.
/// Role, user, and row level security policy are derived from this class.
/// This class is thread-safe.
class IACLAttributable
{
public:
    using Attributes = IACLAttributes;
    using AttributesPtr = ACLAttributesPtr;

    IACLAttributable();
    IACLAttributable(const UUID & id_, IACLAttributableManager * manager_);
    IACLAttributable(const UUID & id_, IACLAttributableManager * manager_, IACLAttributesStorage * storage_);
    IACLAttributable(const IACLAttributable & src);
    IACLAttributable & operator =(const IACLAttributable & src);
    virtual ~IACLAttributable();

    UUID getID() const;
    IACLAttributableManager * getManager() const;

    /// Whether this access control element is valid, i.e. controls the access somehow.
    bool isValid() const;

    /// Returns the attributes. Returns nullptr if not valid.
    AttributesPtr getAttributes() const { AttributesPtr attrs; readAttributes(attrs); return attrs; }

    /// Returns the attributes. Never returns nullptr.
    /// Throws an exception if can't get the attributes.
    AttributesPtr getAttributesStrict() const { AttributesPtr attrs; readAttributesStrict(attrs); return attrs; }

    /// Performs the operation. Throws an exception on fail.
    using Operation = IACLAttributesStorage::Operation;
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
    void readAttributes(std::shared_ptr<const AttributesType> & attrs) const;

    template <typename AttributesType>
    void readAttributesStrict(std::shared_ptr<const AttributesType> & attrs) const;

private:
    struct Data
    {
        UInt128 id;
        IACLAttributableManager * manager;
        std::optional<IACLAttributesStorage *> storage;
    };

    Data loadDataWithStorage() const;
    static String getNameOrID(const Data & data);
    [[noreturn]] static void throwException(const String & message, int error_code);

    mutable std::atomic<Data> atomic_data;
};


template <typename AttributesType>
void IACLAttributable::readAttributes(std::shared_ptr<const AttributesType> & attrs) const
{
    Data data = loadDataWithStorage();
    attrs = nullptr;
    if (*data.storage)
    {
        if ((*data.storage)->read(UUID(data.id), attrs) == IACLAttributesStorage::Status::OK)
            return;
        attrs = nullptr;
    }
}


template <typename AttributesType>
void IACLAttributable::readAttributesStrict(std::shared_ptr<const AttributesType> & attrs) const
{
    Data data = loadDataWithStorage();
    attrs = nullptr;
    if (*data.storage)
    {
        if ((*data.storage)->read(UUID(data.id), attrs) == IACLAttributesStorage::Status::OK)
            return;
        attrs = nullptr;
    }
    throwException("There is no such " + getTypeName() + " " + getNameOrID(data), getNotFoundErrorCode());
}
}
