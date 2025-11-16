#pragma once

#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

#include <initializer_list>
#include <list>
#include <optional>
#include <string>


namespace DB
{

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

class ReadBuffer;
class WriteBuffer;

struct NameAndTypePair
{
public:
    NameAndTypePair() = default;
    NameAndTypePair(const String & name_, const DataTypePtr & type_)
        : name(name_), type(type_), type_in_storage(type_) {}

    NameAndTypePair(const String & name_in_storage_, const String & subcolumn_name_,
        const DataTypePtr & type_in_storage_, const DataTypePtr & subcolumn_type_);

    String getNameInStorage() const;
    String getSubcolumnName() const;

    bool isSubcolumn() const { return subcolumn_delimiter_position != std::nullopt; }
    const DataTypePtr & getTypeInStorage() const { return type_in_storage; }

    bool operator<(const NameAndTypePair & rhs) const;
    bool operator==(const NameAndTypePair & rhs) const;

    String dump() const;

    /// Can be used to convert "t.a.b.c" from meaning "column `t` in storage, subcolumn `a.b.c` inside it"
    /// to meaning "column `t.a.b` in storage, subcolumn `c` inside it".
    void setDelimiterAndTypeInStorage(const String & name_in_storage_, DataTypePtr type_in_storage_);

    String name;
    DataTypePtr type;

private:
    DataTypePtr type_in_storage;
    std::optional<size_t> subcolumn_delimiter_position;
};

/// This needed to use structured bindings for NameAndTypePair
/// const auto & [name, type] = name_and_type
template <int I>
const std::tuple_element_t<I, NameAndTypePair> & get(const NameAndTypePair & name_and_type)
{
    if constexpr (I == 0)
        return name_and_type.name;
    else if constexpr (I == 1)
        return name_and_type.type;
}

/// auto & [name, type] = name_and_type
template <int I>
std::tuple_element_t<I, NameAndTypePair> & get(NameAndTypePair & name_and_type)
{
    if constexpr (I == 0)
        return name_and_type.name;
    else if constexpr (I == 1)
        return name_and_type.type;
}

using NamesAndTypes = std::vector<NameAndTypePair>;

class NamesAndTypesList : public std::list<NameAndTypePair>
{
public:
    NamesAndTypesList() = default;

    NamesAndTypesList(std::initializer_list<NameAndTypePair> init) : std::list<NameAndTypePair>(init) {}

    template <typename Iterator>
    NamesAndTypesList(Iterator begin, Iterator end) : std::list<NameAndTypePair>(begin, end) {}

    void readText(ReadBuffer & buf, bool check_eof = true);
    void writeText(WriteBuffer & buf) const;

    String toString() const;
    static NamesAndTypesList parse(const String & s);

    /// All `rhs` elements must be different.
    bool isSubsetOf(const NamesAndTypesList & rhs) const;

    /// Hamming distance between sets
    ///  (in other words, the added and deleted columns are counted once, the columns that changed the type - twice).
    size_t sizeOfDifference(const NamesAndTypesList & rhs) const;

    /// If an element changes type, it is present both in deleted (with the old type) and in added (with the new type).
    void getDifference(const NamesAndTypesList & rhs, NamesAndTypesList & deleted, NamesAndTypesList & added) const;

    Names getNames() const;
    NameSet getNameSet() const;
    DataTypes getTypes() const;

    /// Creates a mapping from name to the type
    std::unordered_map<std::string, DataTypePtr> getNameToTypeMap() const;

    /// Remove columns which names are not in the `names`.
    void filterColumns(const NameSet & names);

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypesList filter(const NameSet & names) const;

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypesList filter(const Names & names) const;

    /// Leave only the columns whose names are not in the `names`.
    NamesAndTypesList eraseNames(const NameSet & names) const;

    /// Unlike `filter`, returns columns in the order in which they go in `names`.
    NamesAndTypesList addTypes(const Names & names) const;

    /// Check if `name` is one of the column names
    bool contains(const String & name) const;
    bool containsCaseInsensitive(const String & name) const;

    /// Try to get column by name, returns empty optional if column not found
    std::optional<NameAndTypePair> tryGetByName(const std::string & name) const;

    /// Try to get column position by name, returns number of columns if column isn't found
    size_t getPosByName(const std::string & name) const noexcept;

    String toNamesAndTypesDescription() const;
    /// Same as NamesAndTypesList::readText, but includes `type_in_storage`.
    void readTextWithNamesInStorage(ReadBuffer & buf);
    /// Same as NamesAndTypesList::writeText, but includes `type_in_storage`.
    void writeTextWithNamesInStorage(WriteBuffer & buf) const;
};

using NamesAndTypesLists = std::vector<NamesAndTypesList>;

}

namespace std
{
    template <> struct tuple_size<DB::NameAndTypePair> : std::integral_constant<size_t, 2> {};
    template <> struct tuple_element<0, DB::NameAndTypePair> { using type = String; };
    template <> struct tuple_element<1, DB::NameAndTypePair> { using type = DB::DataTypePtr; };
}
