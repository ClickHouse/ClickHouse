#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <boost/noncopyable.hpp>
#include <functional>
#include <memory>


namespace DB
{
class IAttributes;


struct ACLAttributesType
{
    static const ACLAttributesType USER;
    static const ACLAttributesType ROLE;
    static const ACLAttributesType QUOTA;
    static const ACLAttributesType ROW_FILTER_POLICY;

    const String name;
    const std::function<std::shared_ptr<IAttributes>()> create_func;
    const ACLAttributesType * const base_type;
    const int not_found_error_code;
    const int already_exists_error_code;
    const int namespace_idx;
    static const int namespace_max_idx;

    bool isDerived(const ACLAttributesType & base_type_) const;
    friend bool operator ==(const ACLAttributesType & lhs, const ACLAttributesType & rhs) { return &lhs == &rhs; }
    friend bool operator !=(const ACLAttributesType & lhs, const ACLAttributesType & rhs) { return !(lhs == rhs); }
};

}
