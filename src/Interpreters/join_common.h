#pragma once

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

struct ColumnWithTypeAndName;
class TableJoin;
struct JoinInfo;
class IColumn;
using ColumnRawPtrs = std::vector<const IColumn *>;
using NameToTypeMap = std::unordered_map<String, DataTypePtr>;

namespace JoinCommon
{

void convertColumnToNullable(ColumnWithTypeAndName & column, bool low_card_nullability = false);
void convertColumnsToNullable(Block & block, size_t starting_pos = 0);
void removeColumnNullability(ColumnWithTypeAndName & column);
void changeColumnRepresentation(const ColumnPtr & src_column, ColumnPtr & dst_column);
ColumnPtr emptyNotNullableClone(const ColumnPtr & column);
Columns materializeColumns(const Block & block, const Names & names);
ColumnRawPtrs materializeColumnsInplace(Block & block, const Names & names);
ColumnRawPtrs getRawPointers(const Columns & columns);
void removeLowCardinalityInplace(Block & block);
void removeLowCardinalityInplace(Block & block, const Names & names, bool change_type = true);
void restoreLowCardinalityInplace(Block & block);

ColumnRawPtrs extractKeysForJoin(const Block & block_keys, const Names & key_names_right);

/// Throw an exception if blocks have different types of key columns. Compare up to Nullability.
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right);

void createMissedColumns(Block & block);
void joinTotals(const Block & totals, const Block & columns_to_add, const JoinInfo & table_join, Block & block);

void addDefaultValues(IColumn & column, const DataTypePtr & type, size_t count);

bool typesEqualUpToNullability(DataTypePtr left_type, DataTypePtr right_type);


/// Calculate converting actions, rename key columns in required
/// For `USING` join we will convert key columns inplace and affect into types in the result table
/// For `JOIN ON` we will create new columns with converted keys to join by.
ActionsDAGPtr applyKeyConvertToTable(
    const ColumnsWithTypeAndName & cols_src, const NameToTypeMap & type_mapping, bool replace_columns, Names & names_to_rename);

void splitAdditionalColumns(const Names & key_names_right, const Block & sample_block, Block & block_keys, Block & block_others);

Block getRequiredRightKeys(const Names & left_keys, const Names & right_keys,
                           const NameSet & required_keys, const Block & right_table_keys, std::vector<String> & keys_sources);

}

/// Creates result from right table data in RIGHT and FULL JOIN when keys are not present in left table.
class NotJoined
{
public:
    NotJoined(const JoinInfo & join_info, const Block & saved_block_sample_, const Block & right_sample_block,
              const Block & result_sample_block_);

    void correctLowcardAndNullability(MutableColumns & columns_right);
    void addLeftColumns(Block & block, size_t rows_added) const;
    void addRightColumns(Block & block, MutableColumns & columns_right) const;
    void copySameKeys(Block & block) const;

protected:
    Block saved_block_sample;
    Block result_sample_block;

    ~NotJoined() = default;

private:
    /// Indices of columns in result_sample_block that should be generated
    std::vector<size_t> column_indices_left;
    /// Indices of columns that come from the right-side table: right_pos -> result_pos
    std::unordered_map<size_t, size_t> column_indices_right;
    ///
    std::unordered_map<size_t, size_t> same_result_keys;
    /// Which right columns (saved in parent) need nullability change before placing them in result block
    std::vector<size_t> right_nullability_adds;
    std::vector<size_t> right_nullability_removes;
    /// Which right columns (saved in parent) need LowCardinality change before placing them in result block
    std::vector<std::pair<size_t, ColumnPtr>> right_lowcard_changes;

    void setRightIndex(size_t right_pos, size_t result_position);
    void extractColumnChanges(size_t right_pos, size_t result_pos);
};

}
