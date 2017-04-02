#pragma once

#include <map>
#include <list>
#include <string>
#include <set>

#include <DataTypes/IDataType.h>
#include <Core/Names.h>


namespace DB
{

struct NameAndTypePair
{
    String name;
    DataTypePtr type;

    NameAndTypePair() {}
    NameAndTypePair(const String & name_, const DataTypePtr & type_) : name(name_), type(type_) {}

    bool operator<(const NameAndTypePair & rhs) const
    {
        return std::forward_as_tuple(name, type->getName()) < std::forward_as_tuple(rhs.name, rhs.type->getName());
    }

    bool operator==(const NameAndTypePair & rhs) const
    {
        return name == rhs.name && type->getName() == rhs.type->getName();
    }
};

using NamesAndTypes = std::vector<NameAndTypePair>;

class NamesAndTypesList : public std::list<NameAndTypePair>
{
public:
    using std::list<NameAndTypePair>::list;

    void readText(ReadBuffer & buf);
    void writeText(WriteBuffer & buf) const;

    String toString() const;
    static NamesAndTypesList parse(const String & s);

    /// Все элементы rhs должны быть различны.
    bool isSubsetOf(const NamesAndTypesList & rhs) const;

    /// Расстояние Хемминга между множествами
    ///  (иными словами, добавленные и удаленные столбцы считаются один раз; столбцы, изменившие тип, - дважды).
    size_t sizeOfDifference(const NamesAndTypesList & rhs) const;

    Names getNames() const;

    /// Оставить только столбцы, имена которых есть в names. В names могут быть лишние столбцы.
    NamesAndTypesList filter(const NameSet & names) const;

    /// Оставить только столбцы, имена которых есть в names. В names могут быть лишние столбцы.
    NamesAndTypesList filter(const Names & names) const;

    /// В отличие от filter, возвращает столбцы в том порядке, в котором они идут в names.
    NamesAndTypesList addTypes(const Names & names) const;
};

using NamesAndTypesListPtr = std::shared_ptr<NamesAndTypesList>;

}
