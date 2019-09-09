#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <optional>


namespace DB
{
enum class ACLAttributesType
{
    USER,
    ROLE,
    QUOTA,
    ROW_FILTER_POLICY,
    MAX,
};


struct ACLAttributesTypeInfo
{
    ACLAttributesType type;
    String title;
    int not_found_error_code;
    int already_exists_error_code;
    std::optional<ACLAttributesType> base_type;

    static const ACLAttributesTypeInfo & get(ACLAttributesType type);
};

}
