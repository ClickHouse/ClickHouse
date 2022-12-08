#pragma once

#include <base/types.h>

namespace DB
{

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

        /// Format with buffering data in RAM.
        InMemory,

        /// Data stored in a buffer (backed by WAL) only, so until committed:
        /// - data from Buffer part is not available to the SELECT,
        /// - no data in coordinator.
        Buffer,

        Unknown,
    };

    MergeTreeDataPartType() : value(Unknown) {}
    MergeTreeDataPartType(Value value_) : value(value_) {} /// NOLINT

    bool operator==(const MergeTreeDataPartType & other) const
    {
        return value == other.value;
    }

    bool operator!=(const MergeTreeDataPartType & other) const
    {
        return !(*this == other);
    }

    bool operator<(const MergeTreeDataPartType & other) const
    {
        return value < other.value;
    }

    bool operator>(const MergeTreeDataPartType & other) const
    {
        return value > other.value;
    }

    void fromString(const String & str);
    String toString() const;

    Value getValue() const { return value; }

private:
    Value value;
};

}
