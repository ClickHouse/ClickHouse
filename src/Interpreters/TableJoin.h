#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/join_common.h>
#include <Interpreters/asof.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/SizeLimits.h>
#include <DataTypes/getLeastSupertype.h>
#include <Storages/IStorage_fwd.h>

#include <utility>
#include <memory>


namespace DB
{

class Context;
class ASTSelectQuery;
struct DatabaseAndTableWithAlias;
class Block;
class DictionaryReader;

struct ColumnWithTypeAndName;
using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;

struct Settings;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class TableJoin
{

public:
    using NameToTypeMap = std::unordered_map<String, DataTypePtr>;

private:
    /** Query of the form `SELECT expr(x) AS k FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k`
      * The join is made by column k.
      * During the JOIN,
      *  - in the "right" table, it will be available by alias `k`, since `Project` action for the subquery was executed.
      *  - in the "left" table, it will be accessible by the name `expr(x)`, since `Project` action has not been executed yet.
      * You must remember both of these options.
      *
      * Query of the form `SELECT ... from t1 ANY LEFT JOIN (SELECT ... from t2) ON expr(t1 columns) = expr(t2 columns)`
      *     to the subquery will be added expression `expr(t2 columns)`.
      * It's possible to use name `expr(t2 columns)`.
      */

    friend class TreeRewriter;

    const SizeLimits size_limits;
    const size_t default_max_bytes = 0;
    const bool join_use_nulls = false;
    const size_t max_joined_block_rows = 0;
    JoinAlgorithm join_algorithm = JoinAlgorithm::AUTO;
    const size_t partial_merge_join_rows_in_right_blocks = 0;
    const size_t partial_merge_join_left_table_buffer_bytes = 0;
    const size_t max_files_to_merge = 0;
    const String temporary_files_codec = "LZ4";

    Names key_names_left;
    Names key_names_right; /// Duplicating names are qualified.

    ASTs key_asts_left;
    ASTs key_asts_right;
    ASTTableJoin table_join;

    ASOF::Inequality asof_inequality = ASOF::Inequality::GreaterOrEquals;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    NamesAndTypesList columns_from_joined_table;
    /// Columns will be added to block by JOIN.
    /// It's a subset of columns_from_joined_table with corrected Nullability and type (if inplace type conversion is required)
    NamesAndTypesList columns_added_by_join;

    /// Target type to convert key columns before join
    NameToTypeMap left_type_map;
    NameToTypeMap right_type_map;

    ActionsDAGPtr left_converting_actions;
    ActionsDAGPtr right_converting_actions;

    /// Name -> original name. Names are the same as in columns_from_joined_table list.
    std::unordered_map<String, String> original_names;
    /// Original name -> name. Only renamed columns.
    std::unordered_map<String, String> renames;

    VolumePtr tmp_volume;

    Names requiredJoinedNames() const;

    /// Create converting actions and change key column names if required
    ActionsDAGPtr applyKeyConvertToTable(
        const ColumnsWithTypeAndName & cols_src, const NameToTypeMap & type_mapping, Names & names_to_rename) const;

public:
    TableJoin() = default;
    TableJoin(const Settings &, VolumePtr tmp_volume);

    /// for StorageJoin
    TableJoin(SizeLimits limits, bool use_nulls, ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness,
              const Names & key_names_right_)
        : size_limits(limits)
        , default_max_bytes(0)
        , join_use_nulls(use_nulls)
        , join_algorithm(JoinAlgorithm::HASH)
        , key_names_right(key_names_right_)
    {
        table_join.kind = kind;
        table_join.strictness = strictness;
    }

    StoragePtr joined_storage;
    std::shared_ptr<DictionaryReader> dictionary_reader;

    ASTTableJoin::Kind kind() const { return table_join.kind; }
    ASTTableJoin::Strictness strictness() const { return table_join.strictness; }
    bool sameStrictnessAndKind(ASTTableJoin::Strictness, ASTTableJoin::Kind) const;
    const SizeLimits & sizeLimits() const { return size_limits; }
    VolumePtr getTemporaryVolume() { return tmp_volume; }
    bool allowMergeJoin() const;
    bool allowDictJoin(const String & dict_key, const Block & sample_block, Names &, NamesAndTypesList &) const;
    bool preferMergeJoin() const { return join_algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE; }
    bool forceMergeJoin() const { return join_algorithm == JoinAlgorithm::PARTIAL_MERGE; }
    bool forceHashJoin() const
    {
        /// HashJoin always used for DictJoin
        return dictionary_reader || join_algorithm == JoinAlgorithm::HASH;
    }

    bool forceNullableRight() const { return join_use_nulls && isLeftOrFull(table_join.kind); }
    bool forceNullableLeft() const { return join_use_nulls && isRightOrFull(table_join.kind); }
    size_t defaultMaxBytes() const { return default_max_bytes; }
    size_t maxJoinedBlockRows() const { return max_joined_block_rows; }
    size_t maxRowsInRightBlock() const { return partial_merge_join_rows_in_right_blocks; }
    size_t maxBytesInLeftBuffer() const { return partial_merge_join_left_table_buffer_bytes; }
    size_t maxFilesToMerge() const { return max_files_to_merge; }
    const String & temporaryFilesCodec() const { return temporary_files_codec; }
    bool needStreamWithNonJoinedRows() const;

    void resetCollected();
    void addUsingKey(const ASTPtr & ast);
    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    bool hasUsing() const { return table_join.using_expression_list != nullptr; }
    bool hasOn() const { return table_join.on_expression != nullptr; }

    NamesWithAliases getNamesWithAliases(const NameSet & required_columns) const;
    NamesWithAliases getRequiredColumns(const Block & sample, const Names & action_required_columns) const;

    void deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix);
    size_t rightKeyInclusion(const String & name) const;
    NameSet requiredRightKeys() const;

    bool leftBecomeNullable(const DataTypePtr & column_type) const;
    bool rightBecomeNullable(const DataTypePtr & column_type) const;
    void addJoinedColumn(const NameAndTypePair & joined_column);

    void addJoinedColumnsAndCorrectTypes(NamesAndTypesList & names_and_types, bool correct_nullability = true) const;
    void addJoinedColumnsAndCorrectTypes(ColumnsWithTypeAndName & columns, bool correct_nullability = true) const;

    /// Calculates common supertypes for corresponding join key columns.
    bool inferJoinKeyCommonType(const NamesAndTypesList & left, const NamesAndTypesList & right);

    /// Calculate converting actions, rename key columns in required
    /// For `USING` join we will convert key columns inplace and affect into types in the result table
    /// For `JOIN ON` we will create new columns with converted keys to join by.
    bool applyJoinKeyConvert(const ColumnsWithTypeAndName & left_sample_columns, const ColumnsWithTypeAndName & right_sample_columns);

    bool needConvert() const { return !left_type_map.empty(); }

    /// Key columns should be converted before join.
    ActionsDAGPtr leftConvertingActions() const { return left_converting_actions; }
    ActionsDAGPtr rightConvertingActions() const { return right_converting_actions; }

    void setAsofInequality(ASOF::Inequality inequality) { asof_inequality = inequality; }
    ASOF::Inequality getAsofInequality() { return asof_inequality; }

    ASTPtr leftKeysList() const;
    ASTPtr rightKeysList() const; /// For ON syntax only

    const Names & keyNamesLeft() const { return key_names_left; }
    const Names & keyNamesRight() const { return key_names_right; }
    const NamesAndTypesList & columnsFromJoinedTable() const { return columns_from_joined_table; }
    Names columnsAddedByJoin() const
    {
        Names res;
        for (const auto & col : columns_added_by_join)
            res.push_back(col.name);
        return res;
    }

    /// StorageJoin overrides key names (cause of different names qualification)
    void setRightKeys(const Names & keys) { key_names_right = keys; }

    /// Split key and other columns by keys name list
    void splitAdditionalColumns(const Block & sample_block, Block & block_keys, Block & block_others) const;
    Block getRequiredRightKeys(const Block & right_table_keys, std::vector<String> & keys_sources) const;

    String renamedRightColumnName(const String & name) const;
};

}
