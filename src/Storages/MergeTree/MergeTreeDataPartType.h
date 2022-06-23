#pragma once

#include <common/types.h>

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
        WIDE,

        /// Data of all columns is stored in one file. Marks are also stored in single file.
        COMPACT,

        /// Format with buffering data in RAM.
        IN_MEMORY,

        UNKNOWN,
    };

    MergeTreeDataPartType() : value(UNKNOWN) {}
    MergeTreeDataPartType(Value value_) : value(value_) {}

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
