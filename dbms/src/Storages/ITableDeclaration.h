#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Storages/ColumnDefault.h>

#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>


namespace DB
{

class Context;

/** Description of the table.
  * Do not thread safe. See IStorage::lockStructure ().
  */
class ITableDeclaration
{
public:
    /** The name of the table.
      */
    virtual std::string getTableName() const = 0;

    /** Get a list of names and table column types, only non-virtual.
      */
    NamesAndTypesList getColumnsList() const;
    const NamesAndTypesList & getColumnsListNonMaterialized() const { return getColumnsListImpl(); }

    /** Get a list of column table names, only non-virtual.
      */
    virtual Names getColumnNamesList() const;

    /** Get a description of the real (non-virtual) column by its name.
      */
    virtual NameAndTypePair getRealColumn(const String & column_name) const;

    /** Is there a real (non-virtual) column with that name.
      */
    virtual bool hasRealColumn(const String & column_name) const;

    NameAndTypePair getMaterializedColumn(const String & column_name) const;
    bool hasMaterializedColumn(const String & column_name) const;

    /** Get a description of any column by its name.
      */
    virtual NameAndTypePair getColumn(const String & column_name) const;

    /** Is there a column with that name.
      */
    virtual bool hasColumn(const String & column_name) const;

    const DataTypePtr getDataTypeByName(const String & column_name) const;

    /** The same, but in the form of a block-sample.
      */
    Block getSampleBlock() const;
    Block getSampleBlockNonMaterialized() const;
    Block getSampleBlockForColumns(const Names & column_names) const;

    /** Verify that all the requested names are in the table and are set correctly.
      * (the list of names is not empty and the names do not repeat)
      */
    void check(const Names & column_names) const;

    /** Check that all the requested names are in the table and have the correct types.
      */
    void check(const NamesAndTypesList & columns) const;

    /** Check that all names from the intersection of `names` and `columns` are in the table and have the same types.
      */
    void check(const NamesAndTypesList & columns, const Names & column_names) const;

    /** Check that the data block for the record contains all the columns of the table with the correct types,
      *  contains only the columns of the table, and all the columns are different.
      * If need_all, still checks that all the columns of the table are in the block.
      */
    void check(const Block & block, bool need_all = false) const;


    virtual ~ITableDeclaration() = default;

    ITableDeclaration() = default;
    ITableDeclaration(
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults);

    NamesAndTypesList columns;
    NamesAndTypesList materialized_columns{};
    NamesAndTypesList alias_columns{};
    ColumnDefaults column_defaults{};

private:
    virtual const NamesAndTypesList & getColumnsListImpl() const
    {
        return columns;
    }

    using ColumnsListRange = boost::range::joined_range<const NamesAndTypesList, const NamesAndTypesList>;
    /// Returns a lazily joined range of table's ordinary and materialized columns, without unnecessary copying
    ColumnsListRange getColumnsListRange() const;
};

}
