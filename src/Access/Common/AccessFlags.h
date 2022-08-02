#pragma once

#include <Access/Common/AccessType.h>
#include <bitset>
#include <cstring>
#include <vector>


namespace DB
{
using Strings = std::vector<String>;

/// Represents a combination of access types which can be granted globally, on databases, tables, columns, etc.
/// For example "SELECT, CREATE USER" is an access type.
class AccessFlags
{
public:
    AccessFlags(AccessType type); /// NOLINT

    /// The same as AccessFlags(AccessType::NONE).
    AccessFlags() = default;

    /// Constructs from a string like "SELECT".
    AccessFlags(std::string_view keyword); /// NOLINT

    /// Constructs from a list of strings like "SELECT, UPDATE, INSERT".
    AccessFlags(const std::vector<std::string_view> & keywords); /// NOLINT
    AccessFlags(const Strings & keywords); /// NOLINT

    AccessFlags(const AccessFlags & src) = default;
    AccessFlags(AccessFlags && src) = default;
    AccessFlags & operator =(const AccessFlags & src) = default;
    AccessFlags & operator =(AccessFlags && src) = default;

    /// Returns the access type which contains two specified access types.
    AccessFlags & operator |=(const AccessFlags & other) { flags |= other.flags; return *this; }
    friend AccessFlags operator |(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) |= right; }

    /// Returns the access type which contains the common part of two access types.
    AccessFlags & operator &=(const AccessFlags & other) { flags &= other.flags; return *this; }
    friend AccessFlags operator &(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) &= right; }

    /// Returns the access type which contains only the part of the first access type which is not the part of the second access type.
    /// (lhs - rhs) is the same as (lhs & ~rhs).
    AccessFlags & operator -=(const AccessFlags & other) { flags &= ~other.flags; return *this; }
    friend AccessFlags operator -(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) -= right; }

    AccessFlags operator ~() const { AccessFlags res; res.flags = ~flags; return res; }

    bool isEmpty() const { return flags.none(); }
    explicit operator bool() const { return !isEmpty(); }
    bool contains(const AccessFlags & other) const { return (flags & other.flags) == other.flags; }

    friend bool operator ==(const AccessFlags & left, const AccessFlags & right) { return left.flags == right.flags; }
    friend bool operator !=(const AccessFlags & left, const AccessFlags & right) { return !(left == right); }
    friend bool operator <(const AccessFlags & left, const AccessFlags & right) { return memcmp(&left.flags, &right.flags, sizeof(Flags)) < 0; }
    friend bool operator >(const AccessFlags & left, const AccessFlags & right) { return right < left; }
    friend bool operator <=(const AccessFlags & left, const AccessFlags & right) { return !(right < left); }
    friend bool operator >=(const AccessFlags & left, const AccessFlags & right) { return !(left < right); }

    void clear() { flags.reset(); }

    /// Returns a comma-separated list of keywords, like "SELECT, CREATE USER, UPDATE".
    String toString() const;

    /// Returns a list of access types.
    std::vector<AccessType> toAccessTypes() const;

    /// Returns a list of keywords.
    std::vector<std::string_view> toKeywords() const;

    /// Returns all the flags.
    /// These are the same as (allGlobalFlags() | allDatabaseFlags() | allTableFlags() | allColumnsFlags() | allDictionaryFlags()).
    static AccessFlags allFlags();

    /// Returns all the global flags.
    static AccessFlags allGlobalFlags();

    /// Returns all the flags related to a database.
    static AccessFlags allDatabaseFlags();

    /// Returns all the flags related to a table.
    static AccessFlags allTableFlags();

    /// Returns all the flags related to a column.
    static AccessFlags allColumnFlags();

    /// Returns all the flags related to a dictionary.
    static AccessFlags allDictionaryFlags();

    /// Returns all the flags which could be granted on the global level.
    /// The same as allFlags().
    static AccessFlags allFlagsGrantableOnGlobalLevel();

    /// Returns all the flags which could be granted on the database level.
    /// Returns allDatabaseFlags() | allTableFlags() | allDictionaryFlags() | allColumnFlags().
    static AccessFlags allFlagsGrantableOnDatabaseLevel();

    /// Returns all the flags which could be granted on the table level.
    /// Returns allTableFlags() | allDictionaryFlags() | allColumnFlags().
    static AccessFlags allFlagsGrantableOnTableLevel();

    /// Returns all the flags which could be granted on the global level.
    /// The same as allColumnFlags().
    static AccessFlags allFlagsGrantableOnColumnLevel();

    static constexpr size_t SIZE = 128;
private:
    using Flags = std::bitset<SIZE>;
    Flags flags;

    AccessFlags(const Flags & flags_) : flags(flags_) {} /// NOLINT
};

AccessFlags operator |(AccessType left, AccessType right);
AccessFlags operator &(AccessType left, AccessType right);
AccessFlags operator -(AccessType left, AccessType right);
AccessFlags operator ~(AccessType x);

}
