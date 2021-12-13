#include <DataTypes/Serializations/DataPath.h>
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

Path::Path(std::string_view path_)
    : path(path_)
    , num_parts(1 + std::count(path.begin(), path.end(), '.'))
{
}

void Path::append(const Path & other)
{
    if (!other.path.empty())
    {
        path.reserve(path.size() + other.path.size() + 1);
        if (!path.empty())
            path += ".";
        path += other.path;
    }

    is_nested |= other.is_nested << num_parts;
    num_parts += other.num_parts;
}

Strings Path::getParts() const
{
    Strings parts;
    parts.reserve(num_parts);
    boost::algorithm::split(parts, path, boost::is_any_of("."));
    return parts;
}

void Path::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(path, out);
    writeVarUInt(num_parts, out);
    writeVarUInt(is_nested.to_ullong(), out);
}

void Path::readBinary(ReadBuffer & in)
{
    readStringBinary(path, in);
    readVarUInt(num_parts, in);

    UInt64 bits;
    readVarUInt(bits, in);
    is_nested = {bits};
}

Path Path::getNext(const Path & current_path, const Path & key, bool make_nested)
{
    Path next(current_path);
    next.append(key);

    if (!next.empty())
    {
        size_t nested_index = 0;
        if (!current_path.empty())
            nested_index = current_path.num_parts - 1;
        next.is_nested.set(nested_index, make_nested);
    }

    return next;
}

size_t Path::Hash::operator()(const Path & value) const
{
    return std::hash<String>{}(value.path);
}

}
