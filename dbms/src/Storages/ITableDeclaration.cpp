#include <Storages/ITableDeclaration.h>
#include <Common/Exception.h>

#include <boost/range/join.hpp>
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

#include <unordered_set>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int TYPE_MISMATCH;
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


void ITableDeclaration::setColumns(ColumnsDescription columns_)
{
    if (columns_.ordinary.empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    columns = std::move(columns_);
}


bool ITableDeclaration::hasColumn(const String & column_name) const
{
    return getColumns().hasPhysical(column_name); /// By default, we assume that there are no virtual columns in the storage.
}

NameAndTypePair ITableDeclaration::getColumn(const String & column_name) const
{
    return getColumns().getPhysical(column_name); /// By default, we assume that there are no virtual columns in the storage.
}


Block ITableDeclaration::getSampleBlock() const
{
    Block res;

    for (const auto & col : boost::join(getColumns().ordinary, getColumns().materialized))
        res.insert({ col.type->createColumn(), col.type, col.name });

    return res;
}


Block ITableDeclaration::getSampleBlockNonMaterialized() const
{
    Block res;

    for (const auto & col : getColumns().ordinary)
        res.insert({ col.type->createColumn(), col.type, col.name });

    return res;
}


Block ITableDeclaration::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    for (const auto & name : column_names)
    {
        auto col = getColumn(name);
        res.insert({ col.type->createColumn(), col.type, name });
    }

    return res;
}


static std::string listOfColumns(const NamesAndTypesList & available_columns)
{
    std::stringstream s;
    for (auto it = available_columns.begin(); it != available_columns.end(); ++it)
    {
        if (it != available_columns.begin())
            s << ", ";
        s << it->name;
    }
    return s.str();
}


using NamesAndTypesMap = GOOGLE_NAMESPACE::dense_hash_map<StringRef, const IDataType *, StringRefHash>;

static NamesAndTypesMap & getColumnsMapImpl(NamesAndTypesMap & res) { return res; }

template <typename Arg, typename... Args>
static NamesAndTypesMap & getColumnsMapImpl(NamesAndTypesMap & res, const Arg & arg, const Args &... args)
{
    static_assert(std::is_same_v<Arg, NamesAndTypesList>, "getColumnsMap requires arguments of type NamesAndTypesList");

    for (const auto & column : arg)
        res.insert({column.name, column.type.get()});

    return getColumnsMapImpl(res, args...);
}

template <typename... Args>
static NamesAndTypesMap getColumnsMap(const Args &... args)
{
    NamesAndTypesMap res;
    res.set_empty_key(StringRef());

    return getColumnsMapImpl(res, args...);
}


void ITableDeclaration::check(const Names & column_names) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
            ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const auto columns_map = getColumnsMap(available_columns);

    using UniqueStrings = GOOGLE_NAMESPACE::dense_hash_set<StringRef, StringRefHash>;
    UniqueStrings unique_names;
    unique_names.set_empty_key(StringRef());

    for (const auto & name : column_names)
    {
        if (columns_map.end() == columns_map.find(name))
            throw Exception("There is no column with name " + name + " in table. There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once",
                ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}


void ITableDeclaration::check(const NamesAndTypesList & provided_columns) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    using UniqueStrings = GOOGLE_NAMESPACE::dense_hash_set<StringRef, StringRefHash>;
    UniqueStrings unique_names;
    unique_names.set_empty_key(StringRef());

    for (const NameAndTypePair & column : provided_columns)
    {
        NamesAndTypesMap::const_iterator it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception("There is no column with name " + column.name + ". There are columns: "
                + listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception("Type mismatch for column " + column.name + ". Column has type "
                + it->second->getName() + ", got type " + column.type->getName(), ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(column.name))
            throw Exception("Column " + column.name + " queried more than once",
                ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(column.name);
    }
}


void ITableDeclaration::check(const NamesAndTypesList & provided_columns, const Names & column_names) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto available_columns_map = getColumnsMap(available_columns);
    const NamesAndTypesMap & provided_columns_map = getColumnsMap(provided_columns);

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
            ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    using UniqueStrings = GOOGLE_NAMESPACE::dense_hash_set<StringRef, StringRefHash>;
    UniqueStrings unique_names;
    unique_names.set_empty_key(StringRef());

    for (const String & name : column_names)
    {
        NamesAndTypesMap::const_iterator it = provided_columns_map.find(name);
        if (provided_columns_map.end() == it)
            continue;

        NamesAndTypesMap::const_iterator jt = available_columns_map.find(name);
        if (available_columns_map.end() == jt)
            throw Exception("There is no column with name " + name + ". There are columns: "
                + listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->equals(*jt->second))
            throw Exception("Type mismatch for column " + name + ". Column has type "
                + jt->second->getName() + ", got type " + it->second->getName(), ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once",
                ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}


void ITableDeclaration::check(const Block & block, bool need_all) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    using NameSet = std::unordered_set<String>;
    NameSet names_in_block;

    block.checkNumberOfRows();

    for (const auto & column : block)
    {
        if (names_in_block.count(column.name))
            throw Exception("Duplicate column " + column.name + " in block",
                            ErrorCodes::DUPLICATE_COLUMN);

        names_in_block.insert(column.name);

        NamesAndTypesMap::const_iterator it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception("There is no column with name " + column.name + ". There are columns: "
                + listOfColumns(available_columns), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception("Type mismatch for column " + column.name + ". Column has type "
                + it->second->getName() + ", got type " + column.type->getName(), ErrorCodes::TYPE_MISMATCH);
    }

    if (need_all && names_in_block.size() < columns_map.size())
    {
        for (NamesAndTypesList::const_iterator it = available_columns.begin(); it != available_columns.end(); ++it)
        {
            if (!names_in_block.count(it->name))
                throw Exception("Expected column " + it->name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
}


ITableDeclaration::ITableDeclaration(ColumnsDescription columns_)
{
    setColumns(std::move(columns_));
}

}
