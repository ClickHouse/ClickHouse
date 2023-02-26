#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PART_TYPE;
}

void MergeTreeDataPartType::fromString(const String & str)
{
    auto maybe_value = magic_enum::enum_cast<MergeTreeDataPartType::Value>(str);
    if (!maybe_value || *maybe_value == Value::Unknown)
        throw DB::Exception(ErrorCodes::UNKNOWN_PART_TYPE, "Unexpected string for part type: {}", str);

    value = *maybe_value;
}

String MergeTreeDataPartType::toString() const
{
    return String(magic_enum::enum_name(value));
}

}
