#pragma once

#include <Core/Types.h>
#include <Core/Field.h>
#include <bitset>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

class PathInData
{
public:
    struct Part
    {
        Part() = default;
        Part(std::string_view key_, bool is_nested_)
            : key(key_), is_nested(is_nested_)
        {
        }

        std::string_view key;
        bool is_nested = false;
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
    bool hasNested() const { return std::any_of(parts.begin(), parts.end(), [](const auto & part) { return part.is_nested; }); }

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

    bool operator==(const PathInData & other) const { return path == other.path; }
    bool operator!=(const PathInData & other) const { return !(*this == other); }
    struct Hash { size_t operator()(const PathInData & value) const; };

private:
    static String buildPath(const Parts & other_parts);
    static Parts buildParts(const String & path, const Parts & other_parts);

    String path;
    Parts parts;
};

class PathInDataBuilder
{
public:
    const PathInData::Parts & getParts() const { return parts; }

    PathInDataBuilder & append(std::string_view key, bool is_nested);
    PathInDataBuilder & append(const PathInData::Parts & path, bool is_nested);

    void popBack();
    void popBack(size_t n);

private:
    PathInData::Parts parts;
};

using PathsInData = std::vector<PathInData>;

struct ParseResult
{
    std::vector<PathInData> paths;
    std::vector<Field> values;
};

}
