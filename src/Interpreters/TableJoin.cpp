#include <Interpreters/TableJoin.h>

#include <Common/Exception.h>
#include <base/types.h>
#include <Common/StringUtils.h>
#include <Interpreters/ActionsDAG.h>

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>

#include <Dictionaries/DictionaryStructure.h>

#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>

#include <Common/logger_useful.h>
#include <algorithm>
#include <string>
#include <type_traits>
#include <vector>

#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_join_right_table_sorting;
    extern const SettingsUInt64 cross_join_min_bytes_to_compress;
    extern const SettingsUInt64 cross_join_min_rows_to_compress;
    extern const SettingsUInt64 default_max_bytes_in_join;
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsUInt64 join_on_disk_max_files_to_merge;
    extern const SettingsUInt64 join_output_by_rowlist_perkey_rows_threshold;
    extern const SettingsOverflowMode join_overflow_mode;
    extern const SettingsUInt64 join_to_sort_maximum_table_rows;
    extern const SettingsUInt64 join_to_sort_minimum_perkey_rows;
    extern const SettingsBool join_use_nulls;
    extern const SettingsUInt64 max_bytes_in_join;
    extern const SettingsUInt64 max_joined_block_size_rows;
    extern const SettingsUInt64 max_memory_usage;
    extern const SettingsUInt64 max_rows_in_join;
    extern const SettingsUInt64 partial_merge_join_left_table_buffer_bytes;
    extern const SettingsUInt64 partial_merge_join_rows_in_right_blocks;
    extern const SettingsString temporary_files_codec;
}

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{

std::string formatTypeMap(const TableJoin::NameToTypeMap & target, const TableJoin::NameToTypeMap & source)
{
    std::vector<std::string> text;
    for (const auto & [k, v] : target)
    {
        auto src_type_it = source.find(k);
        std::string src_type_name = src_type_it != source.end() ? src_type_it->second->getName() : "";
        text.push_back(fmt::format("{} : {} -> {}", k, src_type_name, v->getName()));
    }
    return fmt::format("{}", fmt::join(text, ", "));
}

}

namespace
{

struct BothSidesTag {};
struct LeftSideTag {};
struct RightSideTag {};

template <typename SideTag = BothSidesTag, typename OnExpr, typename Func>
bool forAllKeys(OnExpr & expressions, Func callback)
{
    static_assert(std::is_same_v<SideTag, BothSidesTag> ||
                  std::is_same_v<SideTag, LeftSideTag> ||
                  std::is_same_v<SideTag, RightSideTag>);

    for (auto & expr : expressions)
    {
        if constexpr (std::is_same_v<SideTag, BothSidesTag>)
            assert(expr.key_names_left.size() == expr.key_names_right.size());

        size_t sz = !std::is_same_v<SideTag, RightSideTag> ? expr.key_names_left.size() : expr.key_names_right.size();
        for (size_t i = 0; i < sz; ++i)
        {
            bool cont;
            if constexpr (std::is_same_v<SideTag, BothSidesTag>)
                cont = callback(expr.key_names_left[i], expr.key_names_right[i]);
            if constexpr (std::is_same_v<SideTag, LeftSideTag>)
                cont = callback(expr.key_names_left[i]);
            if constexpr (std::is_same_v<SideTag, RightSideTag>)
                cont = callback(expr.key_names_right[i]);

            if (!cont)
                return false;
        }
    }
    return true;
}

}

TableJoin::TableJoin(const Settings & settings, VolumePtr tmp_volume_, TemporaryDataOnDiskScopePtr tmp_data_)
    : size_limits(SizeLimits{settings[Setting::max_rows_in_join], settings[Setting::max_bytes_in_join], settings[Setting::join_overflow_mode]})
    , default_max_bytes(settings[Setting::default_max_bytes_in_join])
    , join_use_nulls(settings[Setting::join_use_nulls])
    , cross_join_min_rows_to_compress(settings[Setting::cross_join_min_rows_to_compress])
    , cross_join_min_bytes_to_compress(settings[Setting::cross_join_min_bytes_to_compress])
    , max_joined_block_rows(settings[Setting::max_joined_block_size_rows])
    , join_algorithm(settings[Setting::join_algorithm])
    , partial_merge_join_rows_in_right_blocks(settings[Setting::partial_merge_join_rows_in_right_blocks])
    , partial_merge_join_left_table_buffer_bytes(settings[Setting::partial_merge_join_left_table_buffer_bytes])
    , max_files_to_merge(settings[Setting::join_on_disk_max_files_to_merge])
    , temporary_files_codec(settings[Setting::temporary_files_codec])
    , output_by_rowlist_perkey_rows_threshold(settings[Setting::join_output_by_rowlist_perkey_rows_threshold])
    , sort_right_minimum_perkey_rows(settings[Setting::join_to_sort_minimum_perkey_rows])
    , sort_right_maximum_table_rows(settings[Setting::join_to_sort_maximum_table_rows])
    , allow_join_sorting(settings[Setting::allow_experimental_join_right_table_sorting])
    , max_memory_usage(settings[Setting::max_memory_usage])
    , tmp_volume(tmp_volume_)
    , tmp_data(tmp_data_)
{
}

void TableJoin::resetKeys()
{
    clauses.clear();

    key_asts_left.clear();
    key_asts_right.clear();
    left_type_map.clear();
    right_type_map.clear();
}

void TableJoin::resetCollected()
{
    clauses.clear();
    columns_from_joined_table.clear();
    columns_added_by_join.clear();
    original_names.clear();
    renames.clear();
    left_type_map.clear();
    right_type_map.clear();
}

void TableJoin::addUsingKey(const ASTPtr & ast)
{
    /** For USING key and right key AST are the same.
      * Example:
      * SELECT ... FROM t1 JOIN t2 USING (key)
      * Both key_asts_left and key_asts_right will reference the same ASTIdentifer `key`
      */
    addKey(ast->getColumnName(), renamedRightColumnName(ast->getAliasOrColumnName()), ast, ast);
}

void TableJoin::addDisjunct()
{
    clauses.emplace_back();

    if (getStorageJoin() && clauses.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin with ORs is not supported");
}

void TableJoin::addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast, bool null_safe_comparison)
{
    addKey(left_table_ast->getColumnName(), right_table_ast->getAliasOrColumnName(), left_table_ast, right_table_ast, null_safe_comparison);
    right_key_aliases[right_table_ast->getColumnName()] = right_table_ast->getAliasOrColumnName();
}

/// @return how many times right key appears in ON section.
size_t TableJoin::rightKeyInclusion(const String & name) const
{
    if (hasUsing())
        return 0;

    size_t count = 0;
    for (const auto & clause : clauses)
        count += std::count(clause.key_names_right.begin(), clause.key_names_right.end(), name);
    return count;
}

void TableJoin::deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix)
{
    NameSet joined_columns;
    NamesAndTypesList dedup_columns;

    for (auto & column : columns_from_joined_table)
    {
        if (joined_columns.contains(column.name))
            continue;

        joined_columns.insert(column.name);

        dedup_columns.push_back(column);
        auto & inserted = dedup_columns.back();

        /// Also qualify unusual column names - that does not look like identifiers.

        if (left_table_columns.contains(column.name) || !isValidIdentifierBegin(column.name.at(0)))
            inserted.name = right_table_prefix + column.name;

        original_names[inserted.name] = column.name;
        if (inserted.name != column.name)
            renames[column.name] = inserted.name;
    }

    columns_from_joined_table.swap(dedup_columns);
}

String TableJoin::getOriginalName(const String & column_name) const
{
    auto it = original_names.find(column_name);
    if (it != original_names.end())
        return it->second;
    return column_name;
}

NamesWithAliases TableJoin::getNamesWithAliases(const NameSet & required_columns) const
{
    NamesWithAliases out;
    out.reserve(required_columns.size());
    for (const auto & name : required_columns)
    {
        auto original_name = getOriginalName(name);
        out.emplace_back(original_name, name);
    }
    return out;
}

ASTPtr TableJoin::leftKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    keys_list->children = key_asts_left;

    for (const auto & clause : clauses)
    {
        if (clause.on_filter_condition_left)
            keys_list->children.push_back(clause.on_filter_condition_left);
    }
    return keys_list;
}

ASTPtr TableJoin::rightKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();

    if (hasOn())
        keys_list->children = key_asts_right;

    for (const auto & clause : clauses)
    {
        if (clause.on_filter_condition_right)
            keys_list->children.push_back(clause.on_filter_condition_right);
    }
    return keys_list;
}

Names TableJoin::requiredJoinedNames() const
{
    Names key_names_right = getAllNames(JoinTableSide::Right);
    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name);

    /*
     * In case of `SELECT count() FROM ... JOIN .. ON NULL` required columns set for right table is empty.
     * But we have to get at least one column from right table to know the number of rows.
     */
    if (required_columns_set.empty() && !columns_from_joined_table.empty())
        return {columns_from_joined_table.begin()->name};

    return Names(required_columns_set.begin(), required_columns_set.end());
}

NameSet TableJoin::requiredRightKeys() const
{
    NameSet required;
    forAllKeys<RightSideTag>(clauses, [this, &required](const auto & name)
    {
        auto rename = renamedRightColumnName(name);
        for (const auto & column : columns_added_by_join)
            if (rename == column.name)
                required.insert(name);
        return true;
    });
    return required;
}

NamesWithAliases TableJoin::getRequiredColumns(const Block & sample, const Names & action_required_columns) const
{
    NameSet required_columns(action_required_columns.begin(), action_required_columns.end());

    for (auto & column : requiredJoinedNames())
        if (!sample.has(column))
            required_columns.insert(column);

    return getNamesWithAliases(required_columns);
}

Block TableJoin::getRequiredRightKeys(const Block & right_table_keys, std::vector<String> & keys_sources) const
{
    NameSet required_keys = requiredRightKeys();
    Block required_right_keys;
    if (required_keys.empty())
        return required_right_keys;

    forAllKeys(clauses, [&](const auto & left_key_name, const auto & right_key_name)
    {
        if (required_keys.contains(right_key_name) && !required_right_keys.has(right_key_name))
        {
            const auto & right_key = right_table_keys.getByName(right_key_name);
            required_right_keys.insert(right_key);
            keys_sources.push_back(left_key_name);
        }
        return true;
    });
    return required_right_keys;
}

bool TableJoin::leftBecomeNullable(const DataTypePtr & column_type) const
{
    return forceNullableLeft() && JoinCommon::canBecomeNullable(column_type);
}

bool TableJoin::rightBecomeNullable(const DataTypePtr & column_type) const
{
    return forceNullableRight() && JoinCommon::canBecomeNullable(column_type);
}

void TableJoin::addJoinedColumn(const NameAndTypePair & joined_column)
{
    columns_added_by_join.emplace_back(joined_column);
}

NamesAndTypesList TableJoin::correctedColumnsAddedByJoin() const
{
    NamesAndTypesList result;
    for (const auto & col : columns_added_by_join)
    {
        DataTypePtr type = col.type;
        if (hasUsing())
        {
            if (auto it = right_type_map.find(col.name); it != right_type_map.end())
                type = it->second;
        }

        if (rightBecomeNullable(type))
            type = JoinCommon::convertTypeToNullable(type);
        result.emplace_back(col.name, type);
    }

    return result;
}

void TableJoin::addJoinedColumnsAndCorrectTypes(NamesAndTypesList & left_columns, bool correct_nullability)
{
    addJoinedColumnsAndCorrectTypesImpl(left_columns, correct_nullability);
}

void TableJoin::addJoinedColumnsAndCorrectTypes(ColumnsWithTypeAndName & left_columns, bool correct_nullability)
{
    addJoinedColumnsAndCorrectTypesImpl(left_columns, correct_nullability);
}

template <typename TColumns>
void TableJoin::addJoinedColumnsAndCorrectTypesImpl(TColumns & left_columns, bool correct_nullability)
{
    static_assert(std::is_same_v<typename TColumns::value_type, ColumnWithTypeAndName> ||
                  std::is_same_v<typename TColumns::value_type, NameAndTypePair>);

    constexpr bool has_column = std::is_same_v<typename TColumns::value_type, ColumnWithTypeAndName>;
    for (auto & col : left_columns)
    {
        if (hasUsing())
        {
            /*
             * Join with `USING` semantic allows to have columns with changed types in result table.
             * But `JOIN ON` should preserve types from original table.
             * So we need to know changed types in result tables before further analysis (e.g. analyzeAggregation)
             * For `JOIN ON expr1 == expr2` we will infer common type later in makeTableJoin,
             *   when part of plan built and types of expression will be known.
             */
            bool require_strict_keys_match = isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE);
            inferJoinKeyCommonType(left_columns, columns_from_joined_table, !isSpecialStorage(), require_strict_keys_match);

            if (auto it = left_type_map.find(col.name); it != left_type_map.end())
            {
                col.type = it->second;
                if constexpr (has_column)
                    col.column = nullptr;
            }
        }

        if (correct_nullability && leftBecomeNullable(col.type))
        {
            col.type = JoinCommon::convertTypeToNullable(col.type);
            if constexpr (has_column)
                col.column = nullptr;
        }
    }

    for (const auto & col : correctedColumnsAddedByJoin())
        if constexpr (has_column)
            left_columns.emplace_back(nullptr, col.type, col.name);
        else
            left_columns.emplace_back(col.name, col.type);
}

bool TableJoin::sameStrictnessAndKind(JoinStrictness strictness_, JoinKind kind_) const
{
    if (strictness_ == strictness() && kind_ == kind())
        return true;

    /// Compatibility: old ANY INNER == new SEMI LEFT
    if (strictness_ == JoinStrictness::Semi && isLeft(kind_) &&
        strictness() == JoinStrictness::RightAny && isInner(kind()))
        return true;
    if (strictness() == JoinStrictness::Semi && isLeft(kind()) &&
        strictness_ == JoinStrictness::RightAny && isInner(kind_))
        return true;

    return false;
}

bool TableJoin::oneDisjunct() const
{
    return clauses.size() == 1;
}

bool TableJoin::needStreamWithNonJoinedRows() const
{
    if (strictness() == JoinStrictness::Asof ||
        strictness() == JoinStrictness::Semi)
        return false;
    return isRightOrFull(kind());
}

static void renameIfNeeded(String & name, const NameToNameMap & renames)
{
    if (const auto it = renames.find(name); it != renames.end())
        name = it->second;
}

static void makeColumnNameUnique(const ColumnsWithTypeAndName & source_columns, String & name)
{
    for (const auto & source_col : source_columns)
    {
        if (source_col.name != name)
            continue;

        /// Duplicate found, slow path
        NameSet names;
        for (const auto & col : source_columns)
            names.insert(col.name);

        String base_name = name;
        for (size_t i = 0; ; ++i)
        {
            name = base_name + "_" + toString(i);
            if (!names.contains(name))
                return;
        }
    }
}

static std::optional<ActionsDAG> createWrapWithTupleActions(
    const ColumnsWithTypeAndName & source_columns,
    std::unordered_set<std::string_view> && column_names_to_wrap,
    NameToNameMap & new_names)
{
    if (column_names_to_wrap.empty())
        return {};

    ActionsDAG actions_dag(source_columns);

    FunctionOverloadResolverPtr func_builder = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());

    for (const auto * input_node : actions_dag.getInputs())
    {
        const auto & column_name = input_node->result_name;
        auto it = column_names_to_wrap.find(column_name);
        if (it == column_names_to_wrap.end())
            continue;
        column_names_to_wrap.erase(it);

        String node_name = "__wrapNullsafe(" + column_name + ")";
        makeColumnNameUnique(source_columns, node_name);

        const auto & dst_node = actions_dag.addFunction(func_builder, {input_node}, node_name);
        new_names[column_name] = dst_node.result_name;
        actions_dag.addOrReplaceInOutputs(dst_node);
    }

    if (!column_names_to_wrap.empty())
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Can't find columns {} in input columns [{}]",
                        fmt::join(column_names_to_wrap, ", "), Block(source_columns).dumpNames());

    return actions_dag;
}

/// Wrap only those keys that are nullable on both sides
std::pair<NameSet, NameSet> TableJoin::getKeysForNullSafeComparion(const ColumnsWithTypeAndName & left_sample_columns, const ColumnsWithTypeAndName & right_sample_columns)
{
    std::unordered_map<String, size_t> left_idx;
    for (size_t i = 0; i < left_sample_columns.size(); ++i)
        left_idx[left_sample_columns[i].name] = i;

    std::unordered_map<String, size_t> right_idx;
    for (size_t i = 0; i < right_sample_columns.size(); ++i)
        right_idx[right_sample_columns[i].name] = i;

    NameSet left_keys_to_wrap;
    NameSet right_keys_to_wrap;

    for (const auto & clause : clauses)
    {
        for (size_t i : clause.nullsafe_compare_key_indexes)
        {
            const auto & left_key = clause.key_names_left[i];
            const auto & right_key = clause.key_names_right[i];
            auto lit = left_idx.find(left_key);
            if (lit == left_idx.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find key {} in left columns [{}]",
                                left_key, Block(left_sample_columns).dumpNames());
            auto rit = right_idx.find(right_key);
            if (rit == right_idx.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find key {} in right columns [{}]",
                                right_key, Block(right_sample_columns).dumpNames());

            if (!left_sample_columns[lit->second].type->isNullable() || !right_sample_columns[rit->second].type->isNullable())
                continue;

            left_keys_to_wrap.insert(left_key);
            right_keys_to_wrap.insert(right_key);
        }
    }

    return {left_keys_to_wrap, right_keys_to_wrap};
}

static void mergeDags(std::optional<ActionsDAG> & result_dag, std::optional<ActionsDAG> && new_dag)
{
    if (!new_dag)
        return;
    if (result_dag)
        result_dag->mergeInplace(std::move(*new_dag));
    else
        result_dag = std::move(new_dag);
}

std::pair<std::optional<ActionsDAG>, std::optional<ActionsDAG>>
TableJoin::createConvertingActions(
    const ColumnsWithTypeAndName & left_sample_columns,
    const ColumnsWithTypeAndName & right_sample_columns)
{
    std::optional<ActionsDAG> left_dag;
    std::optional<ActionsDAG> right_dag;
    /** If the types are not equal, we need to convert them to a common type.
      * Example:
      *   SELECT * FROM t1 JOIN t2 ON t1.a = t2.b
      * Assume that t1.a is UInt16 and t2.b is Int8. The supertype for them is Int32.
      * The query will be semantically transformed to:
      *   SELECT * FROM t1 JOIN t2 ON CAST(t1.a AS 'Int32') = CAST(t2.b AS 'Int32')
      * As a result, the user will get the original columns `a` and `b` without `CAST`.
      *
      */
    NameToNameMap left_column_rename;
    NameToNameMap right_column_rename;

    /// FullSortingMerge join algorithm doesn't support joining keys with different types (e.g. String and Nullable(String))
    bool require_strict_keys_match = isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE);
    inferJoinKeyCommonType(left_sample_columns, right_sample_columns, !isSpecialStorage(), require_strict_keys_match);
    if (!left_type_map.empty() || !right_type_map.empty())
    {
        left_dag = applyKeyConvertToTable(left_sample_columns, left_type_map, JoinTableSide::Left, left_column_rename);
        right_dag = applyKeyConvertToTable(right_sample_columns, right_type_map, JoinTableSide::Right, right_column_rename);
    }

    /**
      * Similarly, when we have a null-safe comparison (a IS NOT DISTICT FROM b),
      * we need to wrap keys with a non-nullable type.
      * The type `tuple` can be used for this purpose,
      * because value tuple(NULL) is not NULL itself (moreover it has type Tuple(Nullable(T) which is not Nullable).
      * Thus, join algorithm will match keys with values tuple(NULL).
      * Example:
      *   SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.b
      * This will be semantically transformed to:
      *   SELECT * FROM t1 JOIN t2 ON tuple(t1.a) == tuple(t2.b)
      */
    auto [left_keys_nullsafe_comparison, right_keys_nullsafe_comparison] = getKeysForNullSafeComparion(
        left_dag ? left_dag->getResultColumns() : left_sample_columns,
        right_dag ? right_dag->getResultColumns() : right_sample_columns);
    if (!left_keys_nullsafe_comparison.empty() || !right_keys_nullsafe_comparison.empty())
    {
        auto new_left_dag = applyNullsafeWrapper(
            left_dag ? left_dag->getResultColumns() : left_sample_columns,
            left_keys_nullsafe_comparison, JoinTableSide::Left, left_column_rename);
        mergeDags(left_dag, std::move(new_left_dag));

        auto new_right_dag = applyNullsafeWrapper(
            right_dag ? right_dag->getResultColumns() : right_sample_columns,
            right_keys_nullsafe_comparison, JoinTableSide::Right, right_column_rename);
        mergeDags(right_dag, std::move(new_right_dag));
    }

    if (forceNullableLeft())
    {
        auto new_left_dag = applyJoinUseNullsConversion(
            left_dag ? left_dag->getResultColumns() : left_sample_columns,
            left_column_rename);
        mergeDags(left_dag, std::move(new_left_dag));
    }

    if (forceNullableRight())
    {
        auto new_right_dag = applyJoinUseNullsConversion(
            right_dag ? right_dag->getResultColumns() : right_sample_columns,
            right_column_rename);
        mergeDags(right_dag, std::move(new_right_dag));
    }

    return {std::move(left_dag), std::move(right_dag)};
}

template <typename LeftNamesAndTypes, typename RightNamesAndTypes>
void TableJoin::inferJoinKeyCommonType(const LeftNamesAndTypes & left, const RightNamesAndTypes & right, bool allow_right, bool require_strict_keys_match)
{
    if (!left_type_map.empty() || !right_type_map.empty())
        return;

    NameToTypeMap left_types;
    for (const auto & col : left)
        left_types[col.name] = col.type;

    NameToTypeMap right_types;
    for (const auto & col : right)
        right_types[renamedRightColumnName(col.name)] = col.type;

    if (strictness() == JoinStrictness::Asof)
    {
        if (clauses.size() != 1)
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join over multiple keys is not supported");
    }

    forAllKeys(clauses, [&](const auto & left_key_name, const auto & right_key_name)
    {
        auto ltypeit = left_types.find(left_key_name);
        auto rtypeit = right_types.find(right_key_name);
        if (ltypeit == left_types.end() || rtypeit == right_types.end())
        {
            /// Name mismatch, give up
            left_type_map.clear();
            right_type_map.clear();
            return false;
        }
        const auto & ltype = ltypeit->second;
        const auto & rtype = rtypeit->second;

        bool type_equals = require_strict_keys_match ? ltype->equals(*rtype) : JoinCommon::typesEqualUpToNullability(ltype, rtype);
        if (type_equals)
            return true;

        DataTypePtr common_type;
        try
        {
            /// TODO(vdimir): use getMostSubtype if possible
            common_type = DB::getLeastSupertype(DataTypes{ltype, rtype});
        }
        catch (DB::Exception & ex)
        {
            throw DB::Exception(ErrorCodes::TYPE_MISMATCH,
                "Can't infer common type for joined columns: {}: {} at left, {}: {} at right. {}",
                left_key_name, ltype->getName(),
                right_key_name, rtype->getName(),
                ex.message());
        }

        if (!allow_right && !common_type->equals(*rtype))
        {
            throw DB::Exception(ErrorCodes::TYPE_MISMATCH,
                "Can't change type for right table: {}: {} -> {}.",
                right_key_name, rtype->getName(), common_type->getName());
        }
        left_type_map[left_key_name] = right_type_map[right_key_name] = common_type;

        return true;
    });

    if (!left_type_map.empty() || !right_type_map.empty())
    {
        LOG_TRACE(
            getLogger("TableJoin"),
            "Infer supertype for joined columns. Left: [{}], Right: [{}]",
            formatTypeMap(left_type_map, left_types),
            formatTypeMap(right_type_map, right_types));
    }
}

static std::optional<ActionsDAG> changeKeyTypes(const ColumnsWithTypeAndName & cols_src,
                                    const TableJoin::NameToTypeMap & type_mapping,
                                    bool add_new_cols,
                                    NameToNameMap & key_column_rename)
{
    ColumnsWithTypeAndName cols_dst = cols_src;
    bool has_some_to_do = false;
    for (auto & col : cols_dst)
    {
        if (auto it = type_mapping.find(col.name); it != type_mapping.end() && col.type != it->second)
        {
            col.type = it->second;
            col.column = nullptr;
            has_some_to_do = true;
        }
    }
    if (!has_some_to_do)
        return {};

    return ActionsDAG::makeConvertingActions(
        /* source= */ cols_src,
        /* result= */ cols_dst,
        /* mode= */ ActionsDAG::MatchColumnsMode::Name,
        /* ignore_constant_values= */ true,
        /* add_casted_columns= */ add_new_cols,
        /* new_names= */ &key_column_rename);
}

static std::optional<ActionsDAG> changeTypesToNullable(
    const ColumnsWithTypeAndName & cols_src,
    const NameSet & exception_cols)
{
    ColumnsWithTypeAndName cols_dst = cols_src;
    bool has_some_to_do = false;
    for (auto & col : cols_dst)
    {
        if (exception_cols.contains(col.name))
            continue;
        col.type = JoinCommon::convertTypeToNullable(col.type);
        col.column = nullptr;
        has_some_to_do = true;
    }

    if (!has_some_to_do)
        return {};

    return ActionsDAG::makeConvertingActions(
        /* source= */ cols_src,
        /* result= */ cols_dst,
        /* mode= */ ActionsDAG::MatchColumnsMode::Name,
        /* ignore_constant_values= */ true,
        /* add_casted_columns= */ false,
        /* new_names= */ nullptr);
}

std::optional<ActionsDAG> TableJoin::applyKeyConvertToTable(
    const ColumnsWithTypeAndName & cols_src,
    const NameToTypeMap & type_mapping,
    JoinTableSide table_side,
    NameToNameMap & key_column_rename)
{
    if (type_mapping.empty())
        return {};

    /// Create DAG to convert key columns
    auto convert_dag = changeKeyTypes(cols_src, type_mapping, !hasUsing(), key_column_rename);
    applyRename(table_side, key_column_rename);
    return convert_dag;
}

std::optional<ActionsDAG> TableJoin::applyNullsafeWrapper(
    const ColumnsWithTypeAndName & cols_src,
    const NameSet & columns_for_nullsafe_comparison,
    JoinTableSide table_side,
    NameToNameMap & key_column_rename)
{
    if (columns_for_nullsafe_comparison.empty())
        return {};

    std::unordered_set<std::string_view> column_names_to_wrap;
    for (const auto & name : columns_for_nullsafe_comparison)
    {
        /// Take into account column renaming for type conversion
        /// if we changed key `a == b` to `_CAST(a, 'UInt64') = b` we need to wrap `tuple(_CAST(a, 'UInt64')) = tuple(b)`
        if (auto it = key_column_rename.find(name); it != key_column_rename.end())
            column_names_to_wrap.insert(it->second);
        else
            column_names_to_wrap.insert(name);
    }

    /// Create DAG to wrap keys with tuple for null-safe comparison
    auto null_safe_wrap_dag = createWrapWithTupleActions(cols_src, std::move(column_names_to_wrap), key_column_rename);
    for (auto & clause : clauses)
    {
        for (size_t i : clause.nullsafe_compare_key_indexes)
        {
            if (table_side == JoinTableSide::Left)
                renameIfNeeded(clause.key_names_left[i], key_column_rename);
            else
                renameIfNeeded(clause.key_names_right[i], key_column_rename);
        }
    }

    return null_safe_wrap_dag;
}

std::optional<ActionsDAG> TableJoin::applyJoinUseNullsConversion(
    const ColumnsWithTypeAndName & cols_src,
    const NameToNameMap & key_column_rename)
{
    /// Do not need to make nullable temporary columns that would be used only as join keys, but is not visible to user
    NameSet exclude_columns;
    for (const auto & it : key_column_rename)
        exclude_columns.insert(it.second);

    /// Create DAG to make columns nullable if needed
    return changeTypesToNullable(cols_src, exclude_columns);
}

void TableJoin::setStorageJoin(std::shared_ptr<const IKeyValueEntity> storage)
{
    right_kv_storage = storage;
}

void TableJoin::setStorageJoin(std::shared_ptr<StorageJoin> storage)
{
    right_storage_join = storage;
}

void TableJoin::setRightStorageName(const std::string & storage_name)
{
    right_storage_name = storage_name;
}

const std::string & TableJoin::getRightStorageName() const
{
    return right_storage_name;
}

String TableJoin::renamedRightColumnName(const String & name) const
{
    if (const auto it = renames.find(name); it != renames.end())
        return it->second;
    return name;
}

String TableJoin::renamedRightColumnNameWithAlias(const String & name) const
{
    auto renamed = renamedRightColumnName(name);
    if (const auto it = right_key_aliases.find(renamed); it != right_key_aliases.end())
        return it->second;
    return renamed;
}

void TableJoin::setRename(const String & from, const String & to)
{
    renames[from] = to;
}

void TableJoin::addKey(const String & left_name, const String & right_name,
                       const ASTPtr & left_ast, const ASTPtr & right_ast,
                       bool null_safe_comparison)
{
    clauses.back().addKey(left_name, right_name, null_safe_comparison);

    key_asts_left.emplace_back(left_ast);
    key_asts_right.emplace_back(right_ast ? right_ast : left_ast);
}

static void addJoinConditionWithAnd(ASTPtr & current_cond, const ASTPtr & new_cond)
{
    if (current_cond == nullptr)
        /// no conditions, set new one
        current_cond = new_cond;
    else if (const auto * func = current_cond->as<ASTFunction>(); func && func->name == "and")
        /// already have `and` in condition, just add new argument
        func->arguments->children.push_back(new_cond);
    else
        /// already have some conditions, unite it with `and`
        current_cond = makeASTFunction("and", current_cond, new_cond);
}

void TableJoin::addJoinCondition(const ASTPtr & ast, bool is_left)
{
    auto & cond_ast = is_left ? clauses.back().on_filter_condition_left : clauses.back().on_filter_condition_right;
    LOG_TRACE(getLogger("TableJoin"), "Adding join condition for {} table: {} -> {}",
              (is_left ? "left" : "right"), ast ? queryToString(ast) : "NULL", cond_ast ? queryToString(cond_ast) : "NULL");
    addJoinConditionWithAnd(cond_ast, ast);
}

std::unordered_map<String, String> TableJoin::leftToRightKeyRemap() const
{
    std::unordered_map<String, String> left_to_right_key_remap;
    if (hasUsing())
    {
        const auto & required_right_keys = requiredRightKeys();
        forAllKeys(clauses, [&](const auto & left_key_name, const auto & right_key_name)
        {
            if (!required_right_keys.contains(right_key_name))
                left_to_right_key_remap[left_key_name] = right_key_name;
            return true;
        });
    }
    return left_to_right_key_remap;
}

Names TableJoin::getAllNames(JoinTableSide side) const
{
    Names res;
    auto func = [&res](const auto & name) { res.emplace_back(name); return true; };
    if (side == JoinTableSide::Left)
        forAllKeys<LeftSideTag>(clauses, func);
    else
        forAllKeys<RightSideTag>(clauses, func);
    return res;
}

void TableJoin::applyRename(JoinTableSide side, const NameToNameMap & name_map)
{
    auto rename_callback = [&name_map](auto & key_name)
    {
        renameIfNeeded(key_name, name_map);
        return true;
    };
    if (side == JoinTableSide::Left)
        forAllKeys<LeftSideTag>(clauses, rename_callback);
    else
        forAllKeys<RightSideTag>(clauses, rename_callback);
}

void TableJoin::assertHasOneOnExpr() const
{
    if (!oneDisjunct())
    {
        std::vector<String> text;
        for (const auto & onexpr : clauses)
            text.push_back(onexpr.formatDebug());
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Expected to have only one join clause, got {}: [{}], query: '{}'",
                            clauses.size(), fmt::join(text, " | "), queryToString(table_join));
    }
}

void TableJoin::resetToCross()
{
    this->resetKeys();
    this->table_join.kind = JoinKind::Cross;
}

bool TableJoin::allowParallelHashJoin() const
{
    if (std::find(join_algorithm.begin(), join_algorithm.end(), JoinAlgorithm::PARALLEL_HASH) == join_algorithm.end())
        return false;
    if (!right_storage_name.empty())
        return false;
    if (table_join.kind != JoinKind::Left && table_join.kind != JoinKind::Inner)
        return false;
    if (table_join.strictness == JoinStrictness::Asof)
        return false;
    if (isSpecialStorage() || !oneDisjunct())
        return false;
    return true;
}

ActionsDAG TableJoin::createJoinedBlockActions(ContextPtr context) const
{
    ASTPtr expression_list = rightKeysList();
    auto syntax_result = TreeRewriter(context).analyze(expression_list, columnsFromJoinedTable());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActionsDAG(true, false);
}

size_t TableJoin::getMaxMemoryUsage() const
{
    return max_memory_usage;
}


}
