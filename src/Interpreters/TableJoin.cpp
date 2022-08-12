#include <Interpreters/TableJoin.h>

#include <Common/Exception.h>
#include <base/types.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/ActionsDAG.h>

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeNullable.h>

#include <Dictionaries/DictionaryStructure.h>

#include <Interpreters/ExternalDictionariesLoader.h>

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


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

TableJoin::TableJoin(const Settings & settings, VolumePtr tmp_volume_)
    : size_limits(SizeLimits{settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode})
    , default_max_bytes(settings.default_max_bytes_in_join)
    , join_use_nulls(settings.join_use_nulls)
    , max_joined_block_rows(settings.max_joined_block_size_rows)
    , join_algorithm(settings.join_algorithm)
    , partial_merge_join_rows_in_right_blocks(settings.partial_merge_join_rows_in_right_blocks)
    , partial_merge_join_left_table_buffer_bytes(settings.partial_merge_join_left_table_buffer_bytes)
    , max_files_to_merge(settings.join_on_disk_max_files_to_merge)
    , temporary_files_codec(settings.temporary_files_codec)
    , tmp_volume(tmp_volume_)
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
    addKey(ast->getColumnName(), renamedRightColumnName(ast->getAliasOrColumnName()), ast);
}

void TableJoin::addDisjunct()
{
    clauses.emplace_back();

    if (getStorageJoin() && clauses.size() > 1)
        throw Exception("StorageJoin with ORs is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void TableJoin::addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast)
{
    addKey(left_table_ast->getColumnName(), right_table_ast->getAliasOrColumnName(), left_table_ast, right_table_ast);
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
            inferJoinKeyCommonType(left_columns, columns_from_joined_table, !isSpecialStorage(), isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE));

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

std::pair<ActionsDAGPtr, ActionsDAGPtr>
TableJoin::createConvertingActions(
    const ColumnsWithTypeAndName & left_sample_columns,
    const ColumnsWithTypeAndName & right_sample_columns)
{
    inferJoinKeyCommonType(left_sample_columns, right_sample_columns, !isSpecialStorage(), isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE));

    NameToNameMap left_key_column_rename;
    NameToNameMap right_key_column_rename;
    auto left_converting_actions = applyKeyConvertToTable(
        left_sample_columns, left_type_map, left_key_column_rename, forceNullableLeft());
    auto right_converting_actions = applyKeyConvertToTable(
        right_sample_columns, right_type_map, right_key_column_rename, forceNullableRight());

    {
        auto log_actions = [](const String & side, const ActionsDAGPtr & dag)
        {
            if (dag)
            {
                /// Just debug message
                std::vector<std::string> input_cols;
                for (const auto & col : dag->getRequiredColumns())
                    input_cols.push_back(col.name + ": " + col.type->getName());

                std::vector<std::string> output_cols;
                for (const auto & col : dag->getResultColumns())
                    output_cols.push_back(col.name + ": " + col.type->getName());

                LOG_DEBUG(&Poco::Logger::get("TableJoin"), "{} JOIN converting actions: [{}] -> [{}]",
                    side, fmt::join(input_cols, ", "), fmt::join(output_cols, ", "));
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("TableJoin"), "{} JOIN converting actions: empty", side);
                return;
            }
            auto format_cols = [](const auto & cols) -> std::string
            {
                std::vector<std::string> str_cols;
                str_cols.reserve(cols.size());
                for (const auto & col : cols)
                    str_cols.push_back(fmt::format("'{}': {}", col.name, col.type->getName()));
                return fmt::format("[{}]", fmt::join(str_cols, ", "));
            };
            LOG_DEBUG(&Poco::Logger::get("TableJoin"), "{} JOIN converting actions: {} -> {}",
                side, format_cols(dag->getRequiredColumns()), format_cols(dag->getResultColumns()));
        };
        log_actions("Left", left_converting_actions);
        log_actions("Right", right_converting_actions);
    }

    forAllKeys(clauses, [&](auto & left_key, auto & right_key)
    {
        renameIfNeeded(left_key, left_key_column_rename);
        renameIfNeeded(right_key, right_key_column_rename);
        return true;
    });

    return {left_converting_actions, right_converting_actions};
}

template <typename LeftNamesAndTypes, typename RightNamesAndTypes>
void TableJoin::inferJoinKeyCommonType(const LeftNamesAndTypes & left, const RightNamesAndTypes & right, bool allow_right, bool strict)
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
            throw DB::Exception("ASOF join over multiple keys is not supported", ErrorCodes::NOT_IMPLEMENTED);

        auto asof_key_type = right_types.find(clauses.back().key_names_right.back());
        if (asof_key_type != right_types.end() && asof_key_type->second->isNullable())
            throw DB::Exception("ASOF join over right table Nullable column is not implemented", ErrorCodes::NOT_IMPLEMENTED);
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

        bool type_equals = strict ? ltype->equals(*rtype) : JoinCommon::typesEqualUpToNullability(ltype, rtype);
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
            &Poco::Logger::get("TableJoin"),
            "Infer supertype for joined columns. Left: [{}], Right: [{}]",
            formatTypeMap(left_type_map, left_types),
            formatTypeMap(right_type_map, right_types));
    }
}

static ActionsDAGPtr changeKeyTypes(const ColumnsWithTypeAndName & cols_src,
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
        return nullptr;

    return ActionsDAG::makeConvertingActions(
        /* source= */ cols_src,
        /* result= */ cols_dst,
        /* mode= */ ActionsDAG::MatchColumnsMode::Name,
        /* ignore_constant_values= */ true,
        /* add_casted_columns= */ add_new_cols,
        /* new_names= */ &key_column_rename);
}

static ActionsDAGPtr changeTypesToNullable(
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
        return nullptr;

    return ActionsDAG::makeConvertingActions(
        /* source= */ cols_src,
        /* result= */ cols_dst,
        /* mode= */ ActionsDAG::MatchColumnsMode::Name,
        /* ignore_constant_values= */ true,
        /* add_casted_columns= */ false,
        /* new_names= */ nullptr);
}

ActionsDAGPtr TableJoin::applyKeyConvertToTable(
    const ColumnsWithTypeAndName & cols_src,
    const NameToTypeMap & type_mapping,
    NameToNameMap & key_column_rename,
    bool make_nullable) const
{
    /// Create DAG to convert key columns
    ActionsDAGPtr dag_stage1 = changeKeyTypes(cols_src, type_mapping, !hasUsing(), key_column_rename);

    /// Create DAG to make columns nullable if needed
    if (make_nullable)
    {
        /// Do not need to make nullable temporary columns that would be used only as join keys, but is not visible to user
        NameSet cols_not_nullable;
        for (const auto & t : key_column_rename)
            cols_not_nullable.insert(t.second);

        ColumnsWithTypeAndName input_cols = dag_stage1 ? dag_stage1->getResultColumns() : cols_src;
        ActionsDAGPtr dag_stage2 = changeTypesToNullable(input_cols, cols_not_nullable);

        /// Merge dags if we got two ones
        if (dag_stage1)
            return ActionsDAG::merge(std::move(*dag_stage1), std::move(*dag_stage2));
        else
            return dag_stage2;
    }
    return dag_stage1;
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

void TableJoin::addKey(const String & left_name, const String & right_name, const ASTPtr & left_ast, const ASTPtr & right_ast)
{
    clauses.back().key_names_left.emplace_back(left_name);
    key_asts_left.emplace_back(left_ast);

    clauses.back().key_names_right.emplace_back(right_name);
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
    LOG_TRACE(&Poco::Logger::get("TableJoin"), "Adding join condition for {} table: {} -> {}",
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
    if (!right_storage_name.empty() || !join_algorithm.isSet(JoinAlgorithm::PARALLEL_HASH))
        return false;
    if (table_join.kind != JoinKind::Left && table_join.kind != JoinKind::Inner)
        return false;
    if (table_join.strictness == JoinStrictness::Asof)
        return false;
    if (isSpecialStorage() || !oneDisjunct())
        return false;
    return true;
}

}
