#include <DataTypes/Serializations/PathInData.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Common/SipHash.h>

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
{
    buildPath(parts_);
    buildParts(parts_);
}

PathInData::PathInData(const PathInData & other)
    : path(other.path)
{
    buildParts(other.getParts());
}

PathInData & PathInData::operator=(const PathInData & other)
{
    if (this != &other)
    {
        path = other.path;
        buildParts(other.parts);
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

void PathInData::buildPath(const Parts & other_parts)
{
    if (other_parts.empty())
        return;

    path.clear();
    auto it = other_parts.begin();
    path += it->key;
    ++it;
    for (; it != other_parts.end(); ++it)
    {
        path += ".";
        path += it->key;
    }
}

void PathInData::buildParts(const Parts & other_parts)
{
    if (other_parts.empty())
        return;

    parts.clear();
    parts.reserve(other_parts.size());
    const char * begin = path.data();
    for (const auto & part : other_parts)
    {
        has_nested |= part.is_nested;
        parts.emplace_back(std::string_view{begin, part.key.length()}, part.is_nested, part.anonymous_array_level);
        begin += part.key.length() + 1;
    }
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
