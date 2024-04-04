#include <base/types.h>
#include <Common/Exception.h>

#include <magic_enum.hpp>

#include <Storages/MergeTree/MergeTreeDataPartType.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

template <typename E>
requires std::is_enum_v<E>
static E parseEnum(const String & str)
{
    auto value = magic_enum::enum_cast<E>(str);
    if (!value || *value == E::Unknown)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected string {} for enum {}", str, magic_enum::enum_type_name<E>());

    return *value;
}

String MergeTreeDataPartType::toString() const
{
    return String(magic_enum::enum_name(value));
}

void MergeTreeDataPartType::fromString(const String & str)
{
    value = parseEnum<Value>(str);
}

String MergeTreeDataPartStorageType::toString() const
{
    return String(magic_enum::enum_name(value));
}

void MergeTreeDataPartStorageType::fromString(const String & str)
{
    value = parseEnum<Value>(str);
}

}
