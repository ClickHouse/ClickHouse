#pragma once

#include <base/types.h>

namespace DB
{
/// It's a bug in clang with three-way comparison operator
/// https://github.com/llvm/llvm-project/issues/55919
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"

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

        Unknown,
    };

    MergeTreeDataPartType() : value(Value::Unknown) {}
    MergeTreeDataPartType(Value value_) : value(value_) {} /// NOLINT

    auto operator<=>(const MergeTreeDataPartType &) const = default;

    Value getValue() const { return value; }
    String toString() const;
    void fromString(const String & str);

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
        Packed,
        Unknown,
    };

    MergeTreeDataPartStorageType() : value(Value::Unknown) {}
    MergeTreeDataPartStorageType(Value value_) : value(value_) {} /// NOLINT

    auto operator<=>(const MergeTreeDataPartStorageType &) const = default;

    Value getValue() const { return value; }
    String toString() const;
    void fromString(const String & str);

private:
    Value value;
};

#pragma clang diagnostic pop

struct MergeTreeDataPartFormat
{
    MergeTreeDataPartType part_type;
    MergeTreeDataPartStorageType storage_type;
};

}
