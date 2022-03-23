#pragma once

#include <Core/Types.h>
#include <Core/Field.h>
#include <bitset>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Class that represents path in document, e.g. JSON.
class PathInData
{
public:
    struct Part
    {
        Part() = default;
        Part(std::string_view key_, bool is_nested_, UInt8 anonymous_array_level_)
            : key(key_), is_nested(is_nested_), anonymous_array_level(anonymous_array_level_)
        {
        }

        /// Name of part of path.
        std::string_view key;

        /// If this part is Nested, i.e. element
        /// related to this key is the array of objects.
        bool is_nested = false;

        /// Number of array levels between current key and previous key.
        /// E.g. in JSON {"k1": [[[{"k2": 1, "k3": 2}]]]}
        /// "k1" is nested and has anonymous_array_level = 0.
        /// "k2" and "k3" are not nested and have anonymous_array_level = 2.
        UInt8 anonymous_array_level = 0;

        bool operator==(const Part & other) const = default;
    };

    using Parts = std::vector<Part>;

    PathInData() = default;
    explicit PathInData(std::string_view path_);
    explicit PathInData(const Parts & parts_);

    PathInData(const PathInData & other);
    PathInData & operator=(const PathInData & other);

    static UInt128 getPartsHash(const Parts & parts_);

    bool empty() const { return parts.empty(); }

    const String & getPath() const { return path; }
    const Parts & getParts() const  { return parts; }

    bool isNested(size_t i) const { return parts[i].is_nested; }
    bool hasNested() const { return has_nested; }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

    bool operator==(const PathInData & other) const { return parts == other.parts; }
    struct Hash { size_t operator()(const PathInData & value) const; };

private:
    /// Creates full path from parts.
    void buildPath(const Parts & other_parts);

    /// Creates new parts full from full path with correct string pointers.
    void buildParts(const Parts & other_parts);

    /// The full path. Parts are separated by dots.
    String path;

    /// Parts of the path. All string_view-s in parts must point to the @path.
    Parts parts;

    /// True if at least one part is nested.
    /// Cached to avoid linear complexity at 'hasNested'.
    bool has_nested = false;
};

class PathInDataBuilder
{
public:
    const PathInData::Parts & getParts() const { return parts; }

    PathInDataBuilder & append(std::string_view key, bool is_array);
    PathInDataBuilder & append(const PathInData::Parts & path, bool is_array);

    void popBack();
    void popBack(size_t n);

private:
    PathInData::Parts parts;

    /// Number of array levels without key to which
    /// next non-empty key will be nested.
    /// Example: for JSON { "k1": [[{"k2": 1, "k3": 2}] }
    // `k2` and `k3` has anonymous_array_level = 1 in that case.
    size_t current_anonymous_array_level = 0;
};

using PathsInData = std::vector<PathInData>;

/// Result of parsing of a document.
/// Contains all paths extracted from document
/// and values which are related to them.
struct ParseResult
{
    std::vector<PathInData> paths;
    std::vector<Field> values;
};

}
