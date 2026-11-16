#pragma once

#include <Core/Range.h>

#include <algorithm>
#include <vector>

namespace DB
{

/// Which key columns are sorted in reverse (`ORDER BY (g, r DESC)`).
class KeyOrder
{
public:
    /// All columns ascending.
    KeyOrder() = default;

    explicit KeyOrder(std::vector<bool> reverse_flags_)
        : reverse_flags(std::move(reverse_flags_))
        , has_any_reversed(std::find(reverse_flags.begin(), reverse_flags.end(), true) != reverse_flags.end())
    {
    }

    bool hasAnyReversed() const { return has_any_reversed; }

    /// Positions beyond the stored flags are ascending; an empty vector means the whole key is ascending.
    bool isReversed(size_t column) const { return column < reverse_flags.size() && reverse_flags[column]; }

    /// Value-space stand-in for the value at the unknown physical end of a part (the side past the
    /// last mark). Values ascend toward +inf on an ascending column and descend toward -inf on a
    /// descending one.
    FieldRef physicalEndExtreme(size_t column) const
    {
        return isReversed(column) ? FieldRef(NEGATIVE_INFINITY) : FieldRef(POSITIVE_INFINITY);
    }

    /// Mirror of physicalEndExtreme: the stand-in for an unknown value on the physical-start side.
    FieldRef physicalStartExtreme(size_t column) const
    {
        return isReversed(column) ? FieldRef(POSITIVE_INFINITY) : FieldRef(NEGATIVE_INFINITY);
    }

    /// Whether NULLs are stored physically last on this column: they are on an ascending column and
    /// physically first on a descending one.
    /// mark range starting with a NULL known to be NULL up to the end of the part.
    bool nullsAreStoredLast(size_t column) const { return !isReversed(column); }

    /// Debug helper for the `checkInRange` contract: boundary tuples must be points of storage order,
    /// left before right.
    int compareTuples(const FieldRef * left, const FieldRef * right, size_t size) const;

    bool matchesPrefix(const std::vector<bool> & flags, size_t num_columns) const
    {
        for (size_t i = 0; i < num_columns; ++i)
            if (isReversed(i) != (i < flags.size() && flags[i]))
                return false;
        return true;
    }

private:
    /// Empty means all columns ascending.
    std::vector<bool> reverse_flags;
    bool has_any_reversed = false;
};

}
