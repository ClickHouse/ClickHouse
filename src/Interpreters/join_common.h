#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

struct ColumnWithTypeAndName;
class TableJoin;
class IColumn;

using ColumnRawPtrs = std::vector<const IColumn *>;
using ColumnRawPtrMap = std::unordered_map<String, const IColumn *>;
using UInt8ColumnDataPtr = const ColumnUInt8::Container *;

namespace JoinCommon
{

/// Store boolean column handling constant value without materializing
/// Behaves similar to std::variant<bool, ColumnPtr>, but provides more convenient specialized interface
class JoinMask
{
public:
    explicit JoinMask(bool value)
        : column(nullptr)
        , const_value(value)
    {}

    explicit JoinMask(ColumnPtr col)
        : column(col)
        , const_value(false)
    {}

    bool isConstant() { return !column; }

    /// Return data if mask is not constant
    UInt8ColumnDataPtr getData()
    {
        if (column)
            return &assert_cast<const ColumnUInt8 &>(*column).getData();
        return nullptr;
    }

    inline bool isRowFiltered(size_t row) const
    {
        if (column)
            return !assert_cast<const ColumnUInt8 &>(*column).getData()[row];
        return !const_value;
    }

private:
    ColumnPtr column;
    /// Used if column is null
    bool const_value;
};


bool isNullable(const DataTypePtr & type);
bool canBecomeNullable(const DataTypePtr & type);
DataTypePtr convertTypeToNullable(const DataTypePtr & type);
void convertColumnToNullable(ColumnWithTypeAndName & column);
void convertColumnsToNullable(Block & block, size_t starting_pos = 0);
void convertColumnsToNullable(MutableColumns & mutable_columns, size_t starting_pos = 0);
void removeColumnNullability(ColumnWithTypeAndName & column);
void changeColumnRepresentation(const ColumnPtr & src_column, ColumnPtr & dst_column);
ColumnPtr emptyNotNullableClone(const ColumnPtr & column);
ColumnPtr materializeColumn(const Block & block, const String & name);
Columns materializeColumns(const Block & block, const Names & names);
ColumnRawPtrs materializeColumnsInplace(Block & block, const Names & names);
ColumnRawPtrMap materializeColumnsInplaceMap(Block & block, const Names & names);
ColumnRawPtrs getRawPointers(const Columns & columns);
void convertToFullColumnsInplace(Block & block);
void convertToFullColumnsInplace(Block & block, const Names & names, bool change_type = true);
void restoreLowCardinalityInplace(Block & block, const Names & lowcard_keys);

ColumnRawPtrs extractKeysForJoin(const Block & block_keys, const Names & key_names_right);

/// Throw an exception if join condition column is not UIint8
void checkTypesOfMasks(const Block & block_left, const String & condition_name_left,
                       const Block & block_right, const String & condition_name_right);

/// Throw an exception if blocks have different types of key columns . Compare up to Nullability.
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left,
                      const Block & block_right, const Names & key_names_right);

/// Check both keys and conditions
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const String & condition_name_left,
                      const Block & block_right, const Names & key_names_right, const String & condition_name_right);

void createMissedColumns(Block & block);
void joinTotals(Block left_totals, Block right_totals, const TableJoin & table_join, Block & out_block);

void addDefaultValues(IColumn & column, const DataTypePtr & type, size_t count);

bool typesEqualUpToNullability(DataTypePtr left_type, DataTypePtr right_type);

/// Return mask array of type ColumnUInt8 for specified column. Source should have type UInt8 or Nullable(UInt8).
JoinMask getColumnAsMask(const Block & block, const String & column_name);

/// Split key and other columns by keys name list
void splitAdditionalColumns(const Names & key_names, const Block & sample_block, Block & block_keys, Block & block_others);

void changeLowCardinalityInplace(ColumnWithTypeAndName & column);

}

/// Creates result from right table data in RIGHT and FULL JOIN when keys are not present in left table.
class NotJoinedBlocks final
{
public:
    using LeftToRightKeyRemap = std::unordered_map<String, String>;

    /// Returns non joined columns from right part of join
    class RightColumnsFiller
    {
    public:
        /// Create empty block for right part
        virtual Block getEmptyBlock() = 0;
        /// Fill columns from right part of join with not joined rows
        virtual size_t fillColumns(MutableColumns & columns_right) = 0;

        virtual ~RightColumnsFiller() = default;
    };

    NotJoinedBlocks(std::unique_ptr<RightColumnsFiller> filler_,
              const Block & result_sample_block_,
              size_t left_columns_count,
              const LeftToRightKeyRemap & left_to_right_key_remap);

    Block read();

private:
    void extractColumnChanges(size_t right_pos, size_t result_pos);
    void correctLowcardAndNullability(Block & block);
    void addLeftColumns(Block & block, size_t rows_added) const;
    void addRightColumns(Block & block, MutableColumns & columns_right) const;
    void copySameKeys(Block & block) const;

    std::unique_ptr<RightColumnsFiller> filler;

    /// Right block saved in Join
    Block saved_block_sample;

    /// Output of join
    Block result_sample_block;

    /// Indices of columns in result_sample_block that should be generated
    std::vector<size_t> column_indices_left;
    /// Indices of columns that come from the right-side table: right_pos -> result_pos
    std::unordered_map<size_t, size_t> column_indices_right;

    std::unordered_map<size_t, size_t> same_result_keys;

    /// Which right columns (saved in parent) need Nullability/LowCardinality change
    ///  before placing them in result block
    std::vector<std::pair<size_t, bool>> right_nullability_changes;
    std::vector<std::pair<size_t, bool>> right_lowcard_changes;

    void setRightIndex(size_t right_pos, size_t result_position);
};

}
