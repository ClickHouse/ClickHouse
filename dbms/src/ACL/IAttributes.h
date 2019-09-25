#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <boost/noncopyable.hpp>
#include <memory>


namespace DB
{
/// Attributes are a set of data which have a name and a type. Attributes control something.
/// Attributes can be stored and loaded to a file or another storage, see IAttributesStorage.
struct IAttributes : public std::enable_shared_from_this<IAttributes>
{
    struct Type : private boost::noncopyable
    {
        const char * name;
        const size_t namespace_idx;
        const Type * const base_type;
        const int error_code_not_found;
        const int error_code_already_exists;

        Type(const char * name_, size_t namespace_idx_, const Type *  base_type_, int error_code_not_found_, int error_code_already_exists_);
        bool isDerived(const Type & base_type_) const;
        friend bool operator ==(const Type & lhs, const Type & rhs) { return &lhs == &rhs; }
        friend bool operator !=(const Type & lhs, const Type & rhs) { return !(lhs == rhs); }
    };

    String name;

    virtual ~IAttributes() {}

    virtual const Type & getType() const = 0;
    virtual std::shared_ptr<IAttributes> clone() const = 0;
    virtual bool hasReferences(const UUID &) const { return false; }
    virtual void removeReferences(const UUID &) {}

    bool isDerived(const Type & base_type) const;
    void checkIsDerived(const Type & base_type) const;

    template <typename AttributesT>
    std::shared_ptr<AttributesT> cast();

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> cast() const;

    template <typename AttributesT>
    std::shared_ptr<AttributesT> tryCast();

    template <typename AttributesT>
    std::shared_ptr<const AttributesT> tryCast() const;

    friend bool operator ==(const IAttributes & lhs, const IAttributes & rhs) { return lhs.equal(rhs); }
    friend bool operator !=(const IAttributes & lhs, const IAttributes & rhs) { return !(lhs == rhs); }

protected:
    template <typename AttributesT>
    std::shared_ptr<IAttributes> cloneImpl() const;

    virtual bool equal(const IAttributes & other) const;
};

using ControlAttributesPtr = std::shared_ptr<const IAttributes>;


template <typename AttributesT>
std::shared_ptr<IAttributes> IAttributes::cloneImpl() const
{
    return std::make_shared<AttributesT>(*cast<AttributesT>());
}


template <typename AttributesT>
std::shared_ptr<AttributesT> IAttributes::cast()
{
    const Type & to_type = AttributesT::TYPE;
    checkIsDerived(to_type);
    return std::static_pointer_cast<AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributes::cast() const
{
    const Type & to_type = AttributesT::TYPE;
    checkIsDerived(to_type);
    return std::static_pointer_cast<const AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<AttributesT> IAttributes::tryCast()
{
    const Type & to_type = AttributesT::TYPE;
    if (!isDerived(to_type))
        return nullptr;
    return std::static_pointer_cast<AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributes::tryCast() const
{
    const Type & to_type = AttributesT::TYPE;
    if (!isDerived(to_type))
        return nullptr;
    return std::static_pointer_cast<const AttributesT>(shared_from_this());
}
}
