#pragma once

#include <Common/Exception.h>
#include <base/types.h>
#include <magic_enum.hpp>

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
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unexpected string {} for enum {}", str, magic_enum::enum_type_name<E>());

    return *value;
}

/// It's a bug in clang with three-way comparison operator
/// https://github.com/llvm/llvm-project/issues/55919
#ifdef __clang__
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif

/// Types of data part format.
class MergeTreeDataPartType
{
public:
    enum Value
    {
        /// Data of each column is stored in one or several (for complex types) files.
        /// Every data file is followed by marks file.
        Wide,

        /// Data of all columns is stored in one file. Marks are also stored in single file.
        Compact,

        /// Format with buffering data in RAM. Obsolete - new parts cannot be created in this format.
        InMemory,

        Unknown,
    };

    MergeTreeDataPartType() : value(Value::Unknown) {}
    MergeTreeDataPartType(Value value_) : value(value_) {} /// NOLINT

    auto operator<=>(const MergeTreeDataPartType &) const = default;

    Value getValue() const { return value; }
    String toString() const { return String(magic_enum::enum_name(value)); }
    void fromString(const String & str) { value = parseEnum<Value>(str); }

private:
    Value value;
};

/// Types of data part storage format.
class MergeTreeDataPartStorageType
{
public:
    enum Value
    {
        Full,
        Unknown,
    };

    MergeTreeDataPartStorageType() : value(Value::Unknown) {}
    MergeTreeDataPartStorageType(Value value_) : value(value_) {} /// NOLINT

    auto operator<=>(const MergeTreeDataPartStorageType &) const = default;

    Value getValue() const { return value; }
    String toString() const { return String(magic_enum::enum_name(value)); }
    void fromString(const String & str) { value = parseEnum<Value>(str); }

private:
    Value value;
};

#ifdef __clang__
    #pragma clang diagnostic pop
#endif

struct MergeTreeDataPartFormat
{
    MergeTreeDataPartType part_type;
    MergeTreeDataPartStorageType storage_type;
};

}
