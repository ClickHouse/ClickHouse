#pragma once

#include <vector>
#include <list>
#include <map>
#include <initializer_list>

#include <Core/BlockInfo.h>
#include <Core/NamesAndTypes.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>



namespace DB
{

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitary position, to change order of columns.
  */

class Context;

class Block
{
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::map<String, size_t>;

    Container data;
    IndexByName index_by_name;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(const ColumnsWithTypeAndName & data_);

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName & elem);
    void insert(size_t position, ColumnWithTypeAndName && elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName & elem);
    void insert(ColumnWithTypeAndName && elem);
    /// insert the column to the end, if there is no column with that name yet
    void insertUnique(const ColumnWithTypeAndName & elem);
    void insertUnique(ColumnWithTypeAndName && elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the column with the specified name
    void erase(const String & name);

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName & getByPosition(size_t position) { return data[position]; }
    const ColumnWithTypeAndName & getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName & safeGetByPosition(size_t position);
    const ColumnWithTypeAndName & safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName & getByName(const std::string & name);
    const ColumnWithTypeAndName & getByName(const std::string & name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    bool has(const std::string & name) const;

    size_t getPositionByName(const std::string & name) const;

    const ColumnsWithTypeAndName & getColumnsWithTypeAndName() const;
    NamesAndTypesList getNamesAndTypesList() const;
    Names getNames() const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows() const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    size_t allocatedBytes() const;

    operator bool() const { return !data.empty(); }
    bool operator!() const { return data.empty(); }

    /** Get a list of column names separated by commas. */
    std::string dumpNames() const;

     /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dumpStructure() const;

    /** Get the same block, but empty. */
    Block cloneEmpty() const;

    Columns getColumns() const;
    void setColumns(const Columns & columns);
    Block cloneWithColumns(const Columns & columns) const;
    Block cloneWithoutColumns() const;

    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutateColumns() const;

    /** Replace columns in a block */
    void setColumns(MutableColumns && columns);
    Block cloneWithColumns(MutableColumns && columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    Block sortColumns() const;

    void clear();
    void swap(Block & other) noexcept;

    /** Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void updateHash(SipHash & hash) const;

private:
    void eraseImpl(size_t position);
    void initializeIndexByName();
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;


/// Compare number of columns, data types, column types, column names, and values of constant columns.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Throw exception when blocks are different.
void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, const std::string & context_description);

/// Calculate difference in structure of blocks and write description into output strings. NOTE It doesn't compare values of constant columns.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);


/** Additional data to the blocks. They are only needed for a query
  * DESCRIBE TABLE with Distributed tables.
  */
struct BlockExtraInfo
{
    BlockExtraInfo() {}
    operator bool() const { return is_valid; }
    bool operator!() const { return !is_valid; }

    std::string host;
    std::string resolved_address;
    std::string user;
    UInt16 port = 0;

    bool is_valid = false;
};

}
