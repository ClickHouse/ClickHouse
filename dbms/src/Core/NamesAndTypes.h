#pragma once

#include <string>
#include <vector>
#include <initializer_list>

#include <DataTypes/IDataType.h>
#include <Core/Names.h>


namespace DB
{

struct NameAndType
{
    String name;
    DataTypePtr type;

    NameAndType() {}
    NameAndType(const String & name_, const DataTypePtr & type_) : name(name_), type(type_) {}

    bool operator<(const NameAndType & rhs) const
    {
        return std::forward_as_tuple(name, type->getName()) < std::forward_as_tuple(rhs.name, rhs.type->getName());
    }

    bool operator==(const NameAndType & rhs) const
    {
        return name == rhs.name && type->equals(*rhs.type);
    }
};

class NamesAndTypes : public std::vector<NameAndType>
{
public:
    using std::vector<NameAndType>::vector;

    void readText(ReadBuffer & buf);
    void writeText(WriteBuffer & buf) const;

    String toString() const;
    static NamesAndTypes parse(const String & s);

    /// All `rhs` elements must be different.
    bool isSubsetOf(const NamesAndTypes & rhs) const;

    /// Hamming distance between sets
    ///  (in other words, the added and deleted columns are counted once, the columns that changed the type - twice).
    size_t sizeOfDifference(const NamesAndTypes & rhs) const;

    Names getNames() const;

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypes filter(const NameSet & names) const;

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypes filter(const Names & names) const;

    /// Unlike `filter`, returns columns in the order in which they go in `names`.
    NamesAndTypes addTypes(const Names & names) const;
};

}
