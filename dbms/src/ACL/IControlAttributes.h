#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <memory>


namespace DB
{
/// Access control attributes.
/// Attributes are part of the element's data that can be stored and loaded to a file or another storage.
struct IControlAttributes : public std::enable_shared_from_this<IControlAttributes>
{
    UUID id;
    String name;

    virtual ~IControlAttributes() {}

    struct Type
    {
        using CreateFunc = std::shared_ptr<IControlAttributes> (*)(const UUID &);

        const char * name;
        const CreateFunc create_func;
        const Type * const base_type;
        const int error_code_not_found;
        const int error_code_already_exists;
        const size_t namespace_idx;

        bool isDerived(const Type & base_type_) const;
        friend bool operator ==(const Type & lhs, const Type & rhs) { return &lhs == &rhs; }
        friend bool operator !=(const Type & lhs, const Type & rhs) { return !(lhs == rhs); }
    };

    virtual const Type & getType() const = 0;

    virtual std::shared_ptr<IControlAttributes> clone() const = 0;
    virtual bool hasReferences(UUID) const { return false; }
    virtual void removeReferences(UUID) {}

    friend bool operator ==(const IControlAttributes & lhs, const IControlAttributes & rhs) { return lhs.equal(rhs); }
    friend bool operator !=(const IControlAttributes & lhs, const IControlAttributes & rhs) { return !(lhs == rhs); }

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

protected:
    template <typename AttributesT>
    static std::shared_ptr<IControlAttributes> createImpl(const UUID & id);

    template <typename AttributesT>
    std::shared_ptr<IControlAttributes> cloneImpl() const;

    virtual bool equal(const IControlAttributes & other) const;
};

using ControlAttributesPtr = std::shared_ptr<const IControlAttributes>;


template <typename AttributesT>
static std::shared_ptr<IControlAttributes> IControlAttributes::createImpl(const UUID & id)
{
    auto result = std::make_shared<AttributesT>();
    result->id = id;
    return result;
}

template <typename AttributesT>
std::shared_ptr<IControlAttributes> IControlAttributes::cloneImpl() const
{
    auto result = createImpl<AttributesT>(id);
    *result = *this;
    return result;
}


template <typename AttributesT>
std::shared_ptr<AttributesT> IControlAttributes::cast()
{
    const Type & to_type = AttributesT::TYPE;
    checkIsDerived(to_type);
    return std::static_pointer_cast<AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IControlAttributes::cast() const
{
    const Type & to_type = AttributesT::TYPE;
    checkIsDerived(to_type);
    return std::static_pointer_cast<const AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<AttributesT> IControlAttributes::tryCast()
{
    const Type & to_type = AttributesT::TYPE;
    if (!isDerived(to_type))
        return nullptr;
    return std::static_pointer_cast<AttributesT>(shared_from_this());
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IControlAttributes::tryCast() const
{
    const Type & to_type = AttributesT::TYPE;
    if (!isDerived(to_type))
        return nullptr;
    return std::static_pointer_cast<const AttributesT>(shared_from_this());
}
}
