#pragma once

#include <Core/BlockInfo.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <initializer_list>
#include <vector>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/UnorderedMapWithMemoryTracking.h>


class SipHash;

namespace DB
{

class ISerialization;
class SerializationInfoByName;
using SerializationPtr = std::shared_ptr<const ISerialization>;
using Serializations = VectorWithMemoryTracking<SerializationPtr>;

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */

class Block
{
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = UnorderedMapWithMemoryTracking<String, size_t, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal>;

    Container data;
    IndexByName index_by_name;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(const ColumnsWithTypeAndName & data_); /// NOLINT
    Block(ColumnsWithTypeAndName && data_); /// NOLINT

    /// insert the column at the specified position
    void insert(size_t position, ColumnWithTypeAndName elem);
    /// insert the column to the end
    void insert(ColumnWithTypeAndName elem);
    /// insert the column to the end, if there is no column with that name yet
    void insertUnique(ColumnWithTypeAndName elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the columns at the specified positions
    void erase(const std::set<size_t> & positions);
    /// remove the column with the specified name
    void erase(const String & name);

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName & getByPosition(size_t position) { return data[position]; }
    [[nodiscard]] const ColumnWithTypeAndName & getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName & safeGetByPosition(size_t position);
    [[nodiscard]] const ColumnWithTypeAndName & safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName* findByName(const std::string & name, bool case_insensitive = false)
    {
        return const_cast<ColumnWithTypeAndName *>(
            const_cast<const Block *>(this)->findByName(name, case_insensitive));
    }

    [[nodiscard]] const ColumnWithTypeAndName * findByName(std::string_view name, bool case_insensitive = false) const;

    [[nodiscard]] const ColumnWithTypeAndName * findByName(const std::string & name, bool case_insensitive = false) const;
    [[nodiscard]] std::optional<ColumnWithTypeAndName> findSubcolumnByName(const std::string & name) const;
    [[nodiscard]] std::optional<ColumnWithTypeAndName> findColumnOrSubcolumnByName(const std::string & name) const;

    ColumnWithTypeAndName & getByName(const std::string & name, bool case_insensitive = false)
    {
        return const_cast<ColumnWithTypeAndName &>(
            const_cast<const Block *>(this)->getByName(name, case_insensitive));
    }

    [[nodiscard]] const ColumnWithTypeAndName & getByName(const std::string & name, bool case_insensitive = false) const;
    [[nodiscard]] ColumnWithTypeAndName getSubcolumnByName(const std::string & name) const;
    [[nodiscard]] ColumnWithTypeAndName getColumnOrSubcolumnByName(const std::string & name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    [[nodiscard]] Container::const_iterator begin() const { return data.begin(); }
    [[nodiscard]] Container::const_iterator end() const { return data.end(); }
    [[nodiscard]] Container::const_iterator cbegin() const { return data.cbegin(); }
    [[nodiscard]] Container::const_iterator cend() const { return data.cend(); }

    [[nodiscard]] bool has(const std::string & name, bool case_insensitive = false) const;

    [[nodiscard]] size_t getPositionByName(const std::string & name, bool case_insensitive = false) const;
    [[nodiscard]] std::optional<size_t> findPositionByName(std::string_view name, bool case_insensitive = false) const;

    [[nodiscard]] const ColumnsWithTypeAndName & getColumnsWithTypeAndName() const;
    [[nodiscard]] NamesAndTypesList getNamesAndTypesList() const;
    [[nodiscard]] NamesAndTypes getNamesAndTypes() const;
    [[nodiscard]] Names getNames() const;
    [[nodiscard]] NameSet getNameSet() const;
    [[nodiscard]] DataTypes getDataTypes() const;
    [[nodiscard]] Names getDataTypeNames() const;

    /// Hash table match `column name -> position in the block`.

    [[nodiscard]] const IndexByName & getIndexByName() const { return index_by_name; }

    [[nodiscard]] Serializations getSerializations() const;
    [[nodiscard]] Serializations getSerializations(const SerializationInfoByName & hints) const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    [[nodiscard]] size_t rows() const;

    [[nodiscard]] size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows(bool allow_null_columns = false) const;

    /// Approximate number of bytes in memory - for profiling and limits.
    [[nodiscard]] size_t bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    [[nodiscard]] size_t allocatedBytes() const;

    [[nodiscard]] bool empty() const { return !columns(); }

    /** Get a list of column names separated by commas. */
    [[nodiscard]] std::string dumpNames() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    [[nodiscard]] std::string dumpStructure() const;

    /** List of column names and positions from index */
    [[nodiscard]] std::string dumpIndex() const;

    /** Get the same block, but empty. */
    [[nodiscard]] Block cloneEmpty() const;

    [[nodiscard]] Columns getColumns() const;
    void setColumns(const Columns & columns);
    void setColumn(size_t position, ColumnWithTypeAndName column);
    [[nodiscard]] Block cloneWithColumns(const Columns & columns) const;
    [[nodiscard]] Block cloneWithoutColumns() const;
    [[nodiscard]] Block cloneWithCutColumns(size_t start, size_t length) const;

    /** Get empty columns with the same types as in block. */
    [[nodiscard]] MutableColumns cloneEmptyColumns() const;

    /** Get empty columns with the same types as in block and given serializations. */
    [[nodiscard]] MutableColumns cloneEmptyColumns(const Serializations & serializations) const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutateColumns();

    Columns detachColumns();

    /** Replace columns in a block */
    void setColumns(MutableColumns && columns);
    Block cloneWithColumns(MutableColumns && columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    [[nodiscard]] Block sortColumns() const;

    /** See IColumn::shrinkToFit() */
    [[nodiscard]] Block shrinkToFit() const;

    [[nodiscard]] Block compress() const;

    [[nodiscard]] Block decompress() const;

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
    void reserve(size_t count);

    /// This is needed to allow function execution over data.
    /// It is safe because functions does not change column names, so index is unaffected.
    /// It is temporary.
    friend class ExpressionActions;
    friend class ActionsDAG;
};

/// Compare number of columns, data types, column types, column names, and values of constant columns.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Throw exception when blocks are different.
void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, std::string_view context_description);

void assertBlocksHaveEqualStructureAllowReplicated(const Block & lhs, const Block & rhs, std::string_view context_description);

/// Actual header is compatible to desired if block have equal structure except constants.
/// It is allowed when column from actual header is constant, but in desired is not.
/// If both columns are constant, it is checked that they have the same value.
bool isCompatibleHeader(const Block & actual, const Block & desired);
void assertCompatibleHeader(const Block & actual, const Block & desired, std::string_view context_description);

/// Calculate difference in structure of blocks and write description into output strings. NOTE It doesn't compare values of constant columns.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);

void removeSpecialColumnRepresentations(Block & block);

/// Converts columns-constants and special representations (like sparse or replicated) to full columns ("materializes" them).
Block materializeBlock(const Block & block, bool remove_special_column_representations = true);
void materializeBlockInplace(Block & block, bool remove_special_column_representations = true);

Block concatenateBlocks(const Blocks & blocks);

/// If the block has no columns, adds a dummy column with given number of rows.
/// Without it, things like ExpressionActions can't tell many rows to output.
/// Name of the new column is randomly generated and returned, so you can remove the column later.
/// Returns empty string if the block is already not empty.
String addDummyColumnWithRowCount(Block & block, size_t num_rows);

}
