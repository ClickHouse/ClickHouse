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

struct JoinInfo
{
    JoinInfo() = default;

    JoinInfo(const ASTTableJoin & table_join_ast, const Settings & settings);

    JoinInfo(SizeLimits limits, bool use_nulls, ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_)
        : kind(kind_), strictness(strictness_), join_use_nulls(use_nulls), join_algorithm(JoinAlgorithm::HASH), size_limits(limits)
    {
    }

    /// for StorageJoin
    JoinInfo(
        SizeLimits limits, bool use_nulls, ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_, const Names & key_names_right_)
        : kind(kind_)
        , strictness(strictness_)
        , key_names_right(key_names_right_)
        , join_use_nulls(use_nulls)
        , join_algorithm(JoinAlgorithm::HASH)
        , size_limits(limits)
    {
    }

    enum class MatchExpressionType
    {
        JoinUsing,
        JoinOn
    };

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    MatchExpressionType match_expression;

    Names key_names_left;
    Names key_names_right;

    NameSet required_right_keys;

    const bool join_use_nulls = false;
    JoinAlgorithm join_algorithm = JoinAlgorithm::AUTO;
    ASOF::Inequality asof_inequality = ASOF::Inequality::GreaterOrEquals;

    /// Settings
    const size_t max_joined_block_rows = 0;
    const bool partial_merge_join_optimizations = false;
    const size_t partial_merge_join_rows_in_right_blocks = 0;
    const size_t partial_merge_join_left_table_buffer_bytes = 0;
    const size_t max_files_to_merge = 0;
    SizeLimits size_limits;

    bool forceNullableRight() const { return join_use_nulls && isLeftOrFull(kind); }
    bool forceNullableLeft() const { return join_use_nulls && isRightOrFull(kind); }
    bool preferMergeJoin() const { return join_algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE; }
    bool forceMergeJoin() const { return join_algorithm == JoinAlgorithm::PARTIAL_MERGE; }
    bool forceHashJoin() const { return join_algorithm == JoinAlgorithm::HASH; }

    bool hasUsing() const { return match_expression == JoinInfo::MatchExpressionType::JoinUsing; }
    bool hasOn() const { return match_expression == JoinInfo::MatchExpressionType::JoinOn; }
};

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

    JoinInfo join_info;

    Names key_names_left;
    Names key_names_right; /// Duplicating names are qualified.

    ASTs key_asts_left;
    ASTs key_asts_right;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    NamesAndTypesList columns_from_joined_table;
    /// Columns will be added to block by JOIN.
    /// It's a subset of columns_from_joined_table with corrected Nullability and type (if inplace type conversion is required)
    NamesAndTypesList columns_added_by_join;

    /// Target type to convert key columns before join
    NameToTypeMap left_type_map;
    NameToTypeMap right_type_map;

    /// Name -> original name. Names are the same as in columns_from_joined_table list.
    std::unordered_map<String, String> original_names;
    /// Original name -> name. Only renamed columns.
    std::unordered_map<String, String> renames;

    const String temporary_files_codec = "LZ4";
    VolumePtr tmp_volume;

    Names requiredJoinedNames() const;

public:
    TableJoin() = default;
    TableJoin(const ASTTableJoin & table_join_ast, const Settings & settings, VolumePtr tmp_volume_);

    StoragePtr joined_storage;
    std::shared_ptr<DictionaryReader> dictionary_reader;

    bool sameStrictnessAndKind(ASTTableJoin::Strictness, ASTTableJoin::Kind) const;
    std::pair<VolumePtr, String> getTemporaryVolume() const { return std::make_pair(tmp_volume, temporary_files_codec) ; }
    bool allowDictJoin(const String & dict_key, const Block & sample_block, Names &, NamesAndTypesList &) const;
    bool needStreamWithNonJoinedRows() const;

    void addUsingKey(const ASTPtr & ast);
    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    bool hasOn() const { return join_info.hasOn(); }

    NamesWithAliases getNamesWithAliases(const NameSet & required_columns) const;
    NamesWithAliases getRequiredColumns(const Block & sample, const Names & action_required_columns) const;

    void deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix);
    size_t rightKeyInclusion(const String & name) const;
    NameSet requiredRightKeys() const;

    void addJoinedColumn(const NameAndTypePair & joined_column);

    void addJoinedColumnsAndCorrectTypes(NamesAndTypesList & names_and_types, bool correct_nullability = true) const;
    void addJoinedColumnsAndCorrectTypes(ColumnsWithTypeAndName & columns, bool correct_nullability = true) const;

    /// Calculates common supertypes for corresponding join key columns.
    bool inferJoinKeyCommonType(const NamesAndTypesList & left, const NamesAndTypesList & right);

    bool applyJoinKeyConvert(const ColumnsWithTypeAndName & left_sample_columns,
                                        const ColumnsWithTypeAndName & right_sample_columns,
                                        ActionsDAGPtr & left_converting_actions,
                                        ActionsDAGPtr & right_converting_actions);

    JoinInfo getJoinInfo() const;

    void setAsofInequality(ASOF::Inequality inequality) { join_info.asof_inequality = inequality; }

    ASTPtr leftKeysList() const;
    ASTPtr rightKeysList() const; /// For ON syntax only

    const Names & keyNamesLeft() const { return key_names_left; }
    const NamesAndTypesList & columnsFromJoinedTable() const { return columns_from_joined_table; }
    Names columnsAddedByJoin() const
    {
        Names res;
        for (const auto & col : columns_added_by_join)
            res.push_back(col.name);
        return res;
    }
};

}
