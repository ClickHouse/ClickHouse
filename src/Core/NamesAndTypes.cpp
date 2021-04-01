#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

NameAndTypePair::NameAndTypePair(
    const String & name_in_storage_, const String & subcolumn_name_,
    const DataTypePtr & type_in_storage_, const DataTypePtr & subcolumn_type_)
    : name(name_in_storage_ + (subcolumn_name_.empty() ? "" : "." + subcolumn_name_))
    , type(subcolumn_type_)
    , type_in_storage(type_in_storage_)
    , subcolumn_delimiter_position(subcolumn_name_.empty() ? std::nullopt : std::make_optional(name_in_storage_.size()))
{
}

String NameAndTypePair::getNameInStorage() const
{
    if (!subcolumn_delimiter_position)
        return name;

    return name.substr(0, *subcolumn_delimiter_position);
}

String NameAndTypePair::getSubcolumnName() const
{
    if (!subcolumn_delimiter_position)
        return "";

    return name.substr(*subcolumn_delimiter_position + 1, name.size() - *subcolumn_delimiter_position);
}

void NamesAndTypesList::readText(ReadBuffer & buf)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    assertString("columns format version: 1\n", buf);
    size_t count;
    DB::readText(count, buf);
    assertString(" columns:\n", buf);

    String column_name;
    String type_name;
    for (size_t i = 0; i < count; ++i)
    {
        readBackQuotedStringWithSQLStyle(column_name, buf);
        assertChar(' ', buf);
        readString(type_name, buf);
        assertChar('\n', buf);

        emplace_back(column_name, data_type_factory.get(type_name));
    }

    assertEOF(buf);
}

void NamesAndTypesList::writeText(WriteBuffer & buf) const
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

String NamesAndTypesList::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

NamesAndTypesList NamesAndTypesList::parse(const String & s)
{
    ReadBufferFromString in(s);
    NamesAndTypesList res;
    res.readText(in);
    assertEOF(in);
    return res;
}

bool NamesAndTypesList::isSubsetOf(const NamesAndTypesList & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return std::unique(vector.begin(), vector.end()) == vector.begin() + rhs.size();
}

size_t NamesAndTypesList::sizeOfDifference(const NamesAndTypesList & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return (std::unique(vector.begin(), vector.end()) - vector.begin()) * 2 - size() - rhs.size();
}

void NamesAndTypesList::getDifference(const NamesAndTypesList & rhs, NamesAndTypesList & deleted, NamesAndTypesList & added) const
{
    NamesAndTypes lhs_vector(begin(), end());
    std::sort(lhs_vector.begin(), lhs_vector.end());
    NamesAndTypes rhs_vector(rhs.begin(), rhs.end());
    std::sort(rhs_vector.begin(), rhs_vector.end());

    std::set_difference(lhs_vector.begin(), lhs_vector.end(), rhs_vector.begin(), rhs_vector.end(),
        std::back_inserter(deleted));
    std::set_difference(rhs_vector.begin(), rhs_vector.end(), lhs_vector.begin(), lhs_vector.end(),
        std::back_inserter(added));
}

Names NamesAndTypesList::getNames() const
{
    Names res;
    res.reserve(size());
    for (const NameAndTypePair & column : *this)
        res.push_back(column.name);
    return res;
}

DataTypes NamesAndTypesList::getTypes() const
{
    DataTypes res;
    res.reserve(size());
    for (const NameAndTypePair & column : *this)
        res.push_back(column.type);
    return res;
}

NamesAndTypesList NamesAndTypesList::filter(const NameSet & names) const
{
    NamesAndTypesList res;
    for (const NameAndTypePair & column : *this)
    {
        if (names.count(column.name))
            res.push_back(column);
    }
    return res;
}

NamesAndTypesList NamesAndTypesList::filter(const Names & names) const
{
    return filter(NameSet(names.begin(), names.end()));
}

NamesAndTypesList NamesAndTypesList::addTypes(const Names & names) const
{
    std::unordered_map<std::string_view, const NameAndTypePair *> self_columns;

    for (const auto & column : *this)
        self_columns[column.name] = &column;

    NamesAndTypesList res;
    for (const String & name : names)
    {
        auto it = self_columns.find(name);
        if (it == self_columns.end())
            throw Exception("No column " + name, ErrorCodes::THERE_IS_NO_COLUMN);
        res.emplace_back(*it->second);
    }

    return res;
}

bool NamesAndTypesList::contains(const String & name) const
{
    for (const NameAndTypePair & column : *this)
    {
        if (column.name == name)
            return true;
    }
    return false;
}

std::optional<NameAndTypePair> NamesAndTypesList::tryGetByName(const std::string & name) const
{
    for (const NameAndTypePair & column : *this)
    {
        if (column.name == name)
            return column;
    }
    return {};
}
}
