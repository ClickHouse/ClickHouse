#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <sparsehash/dense_hash_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}


void NamesAndTypes::readText(ReadBuffer & buf)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    assertString("columns format version: 1\n", buf);
    size_t count;
    DB::readText(count, buf);
    assertString(" columns:\n", buf);
    resize(count);
    for (NameAndType & it : *this)
    {
        readBackQuotedStringWithSQLStyle(it.name, buf);
        assertChar(' ', buf);
        String type_name;
        readString(type_name, buf);
        it.type = data_type_factory.get(type_name);
        assertChar('\n', buf);
    }
}

void NamesAndTypes::writeText(WriteBuffer & buf) const
{
    writeString("columns format version: 1\n", buf);
    DB::writeText(size(), buf);
    writeString(" columns:\n", buf);
    for (const auto & it : *this)
    {
        writeBackQuotedString(it.name, buf);
        writeChar(' ', buf);
        writeString(it.type->getName(), buf);
        writeChar('\n', buf);
    }
}

String NamesAndTypes::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

NamesAndTypes NamesAndTypes::parse(const String & s)
{
    ReadBufferFromString in(s);
    NamesAndTypes res;
    res.readText(in);
    assertEOF(in);
    return res;
}

bool NamesAndTypes::isSubsetOf(const NamesAndTypes & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return std::unique(vector.begin(), vector.end()) == vector.begin() + rhs.size();
}

size_t NamesAndTypes::sizeOfDifference(const NamesAndTypes & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return (std::unique(vector.begin(), vector.end()) - vector.begin()) * 2 - size() - rhs.size();
}

Names NamesAndTypes::getNames() const
{
    Names res;
    res.reserve(size());
    for (const NameAndType & column : *this)
        res.push_back(column.name);
    return res;
}

NamesAndTypes NamesAndTypes::filter(const NameSet & names) const
{
    NamesAndTypes res;
    for (const NameAndType & column : *this)
        if (names.count(column.name))
            res.push_back(column);
    return res;
}

NamesAndTypes NamesAndTypes::filter(const Names & names) const
{
    return filter(NameSet(names.begin(), names.end()));
}

NamesAndTypes NamesAndTypes::addTypes(const Names & names) const
{
    /// NOTE It's better to make a map in `IStorage` than to create it here every time again.
    google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> types;
    types.set_empty_key(StringRef());

    for (const NameAndType & column : *this)
        types[column.name] = &column.type;

    NamesAndTypes res;
    for (const String & name : names)
    {
        auto it = types.find(name);
        if (it == types.end())
            throw Exception("No column " + name, ErrorCodes::THERE_IS_NO_COLUMN);
        res.emplace_back(name, *it->second);
    }
    return res;
}

}
