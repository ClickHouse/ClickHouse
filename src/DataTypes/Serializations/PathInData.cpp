#include <DataTypes/Serializations/PathInData.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Common/SipHash.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

namespace DB
{

PathInData::PathInData(std::string_view path_)
    : path(path_)
{
    const char * begin = path.data();
    const char * end = path.data() + path.size();

    for (const char * it = path.data(); it != end; ++it)
    {
        if (*it == '.')
        {
            size_t size = static_cast<size_t>(it - begin);
            parts.emplace_back(std::string_view{begin, size}, false, 0);
            begin = it + 1;
        }
    }

    size_t size = static_cast<size_t>(end - begin);
    parts.emplace_back(std::string_view{begin, size}, false, 0.);
}

PathInData::PathInData(const Parts & parts_)
    : path(buildPath(parts_))
    , parts(buildParts(path, parts_))
{
}

PathInData::PathInData(const PathInData & other)
    : path(other.path)
    , parts(buildParts(path, other.getParts()))
{
}

PathInData & PathInData::operator=(const PathInData & other)
{
    if (this != &other)
    {
        path = other.path;
        parts = buildParts(path, other.parts);
    }
    return *this;
}

UInt128 PathInData::getPartsHash(const Parts & parts_)
{
    SipHash hash;
    hash.update(parts_.size());
    for (const auto & part : parts_)
    {
        hash.update(part.key.data(), part.key.length());
        hash.update(part.is_nested);
        hash.update(part.anonymous_array_level);
    }

    UInt128 res;
    hash.get128(res);
    return res;
}

void PathInData::writeBinary(WriteBuffer & out) const
{
    writeVarUInt(parts.size(), out);
    for (const auto & part : parts)
    {
        writeStringBinary(part.key, out);
        writeVarUInt(part.is_nested, out);
        writeVarUInt(part.anonymous_array_level, out);
    }
}

void PathInData::readBinary(ReadBuffer & in)
{
    size_t num_parts;
    readVarUInt(num_parts, in);

    Arena arena;
    Parts temp_parts;
    temp_parts.reserve(num_parts);

    for (size_t i = 0; i < num_parts; ++i)
    {
        bool is_nested;
        UInt8 anonymous_array_level;

        auto ref = readStringBinaryInto(arena, in);
        readVarUInt(is_nested, in);
        readVarUInt(anonymous_array_level, in);

        temp_parts.emplace_back(static_cast<std::string_view>(ref), is_nested, anonymous_array_level);
    }

    /// Recreate path and parts.
    path = buildPath(temp_parts);
    parts = buildParts(path, temp_parts);
}

String PathInData::buildPath(const Parts & other_parts)
{
    if (other_parts.empty())
        return "";

    String res;
    auto it = other_parts.begin();
    res += it->key;
    ++it;
    for (; it != other_parts.end(); ++it)
    {
        res += ".";
        res += it->key;
    }

    return res;
}

PathInData::Parts PathInData::buildParts(const String & other_path, const Parts & other_parts)
{
    if (other_parts.empty())
        return {};

    Parts res;
    const char * begin = other_path.data();
    for (const auto & part : other_parts)
    {
        res.emplace_back(std::string_view{begin, part.key.length()}, part.is_nested, part.anonymous_array_level);
        begin += part.key.length() + 1;
    }
    return res;
}

size_t PathInData::Hash::operator()(const PathInData & value) const
{
    auto hash = getPartsHash(value.parts);
    return hash.items[0] ^ hash.items[1];
}

PathInDataBuilder & PathInDataBuilder::append(std::string_view key, bool is_array)
{
    if (parts.empty())
        current_anonymous_array_level += is_array;

    if (!key.empty())
    {
        if (!parts.empty())
            parts.back().is_nested = is_array;

        parts.emplace_back(key, false, current_anonymous_array_level);
        current_anonymous_array_level = 0;
    }

    return *this;
}

PathInDataBuilder & PathInDataBuilder::append(const PathInData::Parts & path, bool is_array)
{
    if (parts.empty())
        current_anonymous_array_level += is_array;

    if (!path.empty())
    {
        if (!parts.empty())
            parts.back().is_nested = is_array;

        auto it = parts.insert(parts.end(), path.begin(), path.end());
        for (; it != parts.end(); ++it)
            it->anonymous_array_level += current_anonymous_array_level;
        current_anonymous_array_level = 0;
    }

    return *this;
}

void PathInDataBuilder::popBack()
{
    parts.pop_back();
}

void PathInDataBuilder::popBack(size_t n)
{
    assert(n <= parts.size());
    parts.resize(parts.size() - n);
}

}
