#pragma once

#include <Storages/ColumnsDescription.h>


namespace DB
{

/** Description of the table.
  * Is not thread safe. See IStorage::lockStructure ().
  */
class ITableDeclaration
{
public:
    virtual const ColumnsDescription & getColumns() const { return columns; }
    virtual void setColumns(ColumnsDescription columns_);

    /// NOTE: These methods should include virtual columns, but should NOT include ALIAS columns
    /// (they are treated separately).
    virtual NameAndTypePair getColumn(const String & column_name) const;
    virtual bool hasColumn(const String & column_name) const;

    Block getSampleBlock() const;
    Block getSampleBlockNonMaterialized() const;

    /// Including virtual and alias columns.
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


    ITableDeclaration() = default;
    explicit ITableDeclaration(ColumnsDescription columns_);
    virtual ~ITableDeclaration() = default;

private:
    ColumnsDescription columns;
};

}
