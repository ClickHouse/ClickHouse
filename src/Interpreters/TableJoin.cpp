#include <Interpreters/TableJoin.h>


#include <Common/StringUtils/StringUtils.h>

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeNullable.h>

#include <Dictionaries/DictionaryStructure.h>

#include <Interpreters/DictionaryReader.h>
#include <Interpreters/ExternalDictionariesLoader.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>

#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
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

TableJoin::TableJoin(const Settings & settings, VolumePtr tmp_volume_)
    : size_limits(SizeLimits{settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode})
    , default_max_bytes(settings.default_max_bytes_in_join)
    , join_use_nulls(settings.join_use_nulls)
    , max_joined_block_rows(settings.max_joined_block_size_rows)
    , join_algorithm(settings.join_algorithm)
    , partial_merge_join_optimizations(settings.partial_merge_join_optimizations)
    , partial_merge_join_rows_in_right_blocks(settings.partial_merge_join_rows_in_right_blocks)
    , partial_merge_join_left_table_buffer_bytes(settings.partial_merge_join_left_table_buffer_bytes)
    , max_files_to_merge(settings.join_on_disk_max_files_to_merge)
    , temporary_files_codec(settings.temporary_files_codec)
    , tmp_volume(tmp_volume_)
{
}

void TableJoin::resetCollected()
{
    key_names_left.clear();
    key_names_right.clear();
    key_asts_left.clear();
    key_asts_right.clear();
    columns_from_joined_table.clear();
    columns_added_by_join.clear();
    original_names.clear();
    renames.clear();
    left_type_map.clear();
    right_type_map.clear();
}

void TableJoin::addUsingKey(const ASTPtr & ast)
{
    key_names_left.push_back(ast->getColumnName());
    key_names_right.push_back(ast->getAliasOrColumnName());

    key_asts_left.push_back(ast);
    key_asts_right.push_back(ast);

    auto & right_key = key_names_right.back();
    if (renames.count(right_key))
        right_key = renames[right_key];
}

void TableJoin::addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast)
{
    key_names_left.push_back(left_table_ast->getColumnName());
    key_names_right.push_back(right_table_ast->getAliasOrColumnName());

    key_asts_left.push_back(left_table_ast);
    key_asts_right.push_back(right_table_ast);
}

/// @return how many times right key appears in ON section.
size_t TableJoin::rightKeyInclusion(const String & name) const
{
    if (hasUsing())
        return 0;

    size_t count = 0;
    for (const auto & key_name : key_names_right)
        if (name == key_name)
            ++count;
    return count;
}

void TableJoin::deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix)
{
    NameSet joined_columns;
    NamesAndTypesList dedup_columns;

    for (auto & column : columns_from_joined_table)
    {
        if (joined_columns.count(column.name))
            continue;

        joined_columns.insert(column.name);

        dedup_columns.push_back(column);
        auto & inserted = dedup_columns.back();

        /// Also qualify unusual column names - that does not look like identifiers.

        if (left_table_columns.count(column.name) || !isValidIdentifierBegin(column.name.at(0)))
            inserted.name = right_table_prefix + column.name;

        original_names[inserted.name] = column.name;
        if (inserted.name != column.name)
            renames[column.name] = inserted.name;
    }

    columns_from_joined_table.swap(dedup_columns);
}

NamesWithAliases TableJoin::getNamesWithAliases(const NameSet & required_columns) const
{
    NamesWithAliases out;
    for (const auto & column : required_columns)
    {
        auto it = original_names.find(column);
        if (it != original_names.end())
            out.emplace_back(it->second, it->first); /// {original_name, name}
    }
    return out;
}

ASTPtr TableJoin::leftKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    keys_list->children = key_asts_left;
    if (ASTPtr extra_cond = joinConditionColumn(JoinTableSide::Left))
        keys_list->children.push_back(extra_cond);
    return keys_list;
}

ASTPtr TableJoin::rightKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    if (hasOn())
        keys_list->children = key_asts_right;
    if (ASTPtr extra_cond = joinConditionColumn(JoinTableSide::Right))
        keys_list->children.push_back(extra_cond);
    return keys_list;
}

Names TableJoin::requiredJoinedNames() const
{
    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name);

    return Names(required_columns_set.begin(), required_columns_set.end());
}

NameSet TableJoin::requiredRightKeys() const
{
    NameSet required;
    for (const auto & name : key_names_right)
    {
        auto rename = renamedRightColumnName(name);
        for (const auto & column : columns_added_by_join)
            if (rename == column.name)
                required.insert(name);
    }
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
    const Names & left_keys = keyNamesLeft();
    const Names & right_keys = keyNamesRight();
    NameSet required_keys = requiredRightKeys();
    Block required_right_keys;

    for (size_t i = 0; i < right_keys.size(); ++i)
    {
        const String & right_key_name = right_keys[i];

        if (required_keys.count(right_key_name) && !required_right_keys.has(right_key_name))
        {
            const auto & right_key = right_table_keys.getByName(right_key_name);
            required_right_keys.insert(right_key);
            keys_sources.push_back(left_keys[i]);
        }
    }

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
            inferJoinKeyCommonType(left_columns, columns_from_joined_table, !isSpecialStorage());

            if (auto it = left_type_map.find(col.name); it != left_type_map.end())
                col.type = it->second;
        }

        if (correct_nullability && leftBecomeNullable(col.type))
            col.type = JoinCommon::convertTypeToNullable(col.type);
    }

    for (const auto & col : correctedColumnsAddedByJoin())
        left_columns.emplace_back(col.name, col.type);
}

bool TableJoin::sameStrictnessAndKind(ASTTableJoin::Strictness strictness_, ASTTableJoin::Kind kind_) const
{
    if (strictness_ == strictness() && kind_ == kind())
        return true;

    /// Compatibility: old ANY INNER == new SEMI LEFT
    if (strictness_ == ASTTableJoin::Strictness::Semi && isLeft(kind_) &&
        strictness() == ASTTableJoin::Strictness::RightAny && isInner(kind()))
        return true;
    if (strictness() == ASTTableJoin::Strictness::Semi && isLeft(kind()) &&
        strictness_ == ASTTableJoin::Strictness::RightAny && isInner(kind_))
        return true;

    return false;
}

bool TableJoin::allowMergeJoin() const
{
    bool is_any = (strictness() == ASTTableJoin::Strictness::Any);
    bool is_all = (strictness() == ASTTableJoin::Strictness::All);
    bool is_semi = (strictness() == ASTTableJoin::Strictness::Semi);

    bool all_join = is_all && (isInner(kind()) || isLeft(kind()) || isRight(kind()) || isFull(kind()));
    bool special_left = isLeft(kind()) && (is_any || is_semi);
    return all_join || special_left;
}

bool TableJoin::needStreamWithNonJoinedRows() const
{
    if (strictness() == ASTTableJoin::Strictness::Asof ||
        strictness() == ASTTableJoin::Strictness::Semi)
        return false;
    return isRightOrFull(kind());
}

static std::optional<String> getDictKeyName(const String & dict_name , ContextPtr context)
{
    auto dictionary = context->getExternalDictionariesLoader().getDictionary(dict_name, context);
    if (!dictionary)
        return {};

    if (const auto & structure = dictionary->getStructure(); structure.id)
        return structure.id->name;
    return {};
}

bool TableJoin::tryInitDictJoin(const Block & sample_block, ContextPtr context)
{
    using Strictness = ASTTableJoin::Strictness;

    bool allowed_inner = isInner(kind()) && strictness() == Strictness::All;
    bool allowed_left = isLeft(kind()) && (strictness() == Strictness::Any ||
                                           strictness() == Strictness::All ||
                                           strictness() == Strictness::Semi ||
                                           strictness() == Strictness::Anti);

    /// Support ALL INNER, [ANY | ALL | SEMI | ANTI] LEFT
    if (!allowed_inner && !allowed_left)
        return false;

    const Names & right_keys = keyNamesRight();
    if (right_keys.size() != 1)
        return false;

    /// TODO: support 'JOIN ... ON expr(dict_key) = table_key'
    auto it_key = original_names.find(right_keys[0]);
    if (it_key == original_names.end())
        return false;

    if (!right_storage_dictionary)
        return false;

    auto dict_name = right_storage_dictionary->getDictionaryName();

    auto dict_key = getDictKeyName(dict_name, context);
    if (!dict_key.has_value() || *dict_key != it_key->second)
        return false; /// JOIN key != Dictionary key

    Names src_names;
    NamesAndTypesList dst_columns;
    for (const auto & col : sample_block)
    {
        if (col.name == right_keys[0])
            continue; /// do not extract key column

        auto it = original_names.find(col.name);
        if (it != original_names.end())
        {
            String original = it->second;
            src_names.push_back(original);
            dst_columns.push_back({col.name, col.type});
        }
    }
    dictionary_reader = std::make_shared<DictionaryReader>(dict_name, src_names, dst_columns, context);

    return true;
}

std::pair<ActionsDAGPtr, ActionsDAGPtr>
TableJoin::createConvertingActions(const ColumnsWithTypeAndName & left_sample_columns, const ColumnsWithTypeAndName & right_sample_columns)
{
    inferJoinKeyCommonType(left_sample_columns, right_sample_columns, !isSpecialStorage());

    auto left_converting_actions = applyKeyConvertToTable(left_sample_columns, left_type_map, key_names_left);
    auto right_converting_actions = applyKeyConvertToTable(right_sample_columns, right_type_map, key_names_right);

    return {left_converting_actions, right_converting_actions};
}

template <typename LeftNamesAndTypes, typename RightNamesAndTypes>
bool TableJoin::inferJoinKeyCommonType(const LeftNamesAndTypes & left, const RightNamesAndTypes & right, bool allow_right)
{
    if (!left_type_map.empty() || !right_type_map.empty())
        return true;

    NameToTypeMap left_types;
    for (const auto & col : left)
        left_types[col.name] = col.type;

    NameToTypeMap right_types;
    for (const auto & col : right)
        right_types[renamedRightColumnName(col.name)] = col.type;

    for (size_t i = 0; i < key_names_left.size(); ++i)
    {
        auto ltype = left_types.find(key_names_left[i]);
        auto rtype = right_types.find(key_names_right[i]);
        if (ltype == left_types.end() || rtype == right_types.end())
        {
            /// Name mismatch, give up
            left_type_map.clear();
            right_type_map.clear();
            return false;
        }

        if (JoinCommon::typesEqualUpToNullability(ltype->second, rtype->second))
            continue;

        DataTypePtr common_type;
        try
        {
            /// TODO(vdimir): use getMostSubtype if possible
            common_type = DB::getLeastSupertype({ltype->second, rtype->second});
        }
        catch (DB::Exception & ex)
        {
            throw DB::Exception(ErrorCodes::TYPE_MISMATCH,
                "Can't infer common type for joined columns: {}: {} at left, {}: {} at right. {}",
                key_names_left[i], ltype->second->getName(),
                key_names_right[i], rtype->second->getName(),
                ex.message());
        }

        if (!allow_right && !common_type->equals(*rtype->second))
        {
            throw DB::Exception(ErrorCodes::TYPE_MISMATCH,
                "Can't change type for right table: {}: {} -> {}.",
                key_names_right[i], rtype->second->getName(), common_type->getName());
        }
        left_type_map[key_names_left[i]] = right_type_map[key_names_right[i]] = common_type;
    }

    if (!left_type_map.empty() || !right_type_map.empty())
    {
        LOG_TRACE(
            &Poco::Logger::get("TableJoin"),
            "Infer supertype for joined columns. Left: [{}], Right: [{}]",
            formatTypeMap(left_type_map, left_types),
            formatTypeMap(right_type_map, right_types));
    }

    return !left_type_map.empty();
}

ActionsDAGPtr TableJoin::applyKeyConvertToTable(
    const ColumnsWithTypeAndName & cols_src, const NameToTypeMap & type_mapping, Names & names_to_rename) const
{
    bool has_some_to_do = false;

    ColumnsWithTypeAndName cols_dst = cols_src;
    for (auto & col : cols_dst)
    {
        if (auto it = type_mapping.find(col.name); it != type_mapping.end())
        {
            has_some_to_do = true;
            col.type = it->second;
            col.column = nullptr;
        }
    }
    if (!has_some_to_do)
        return nullptr;

    NameToNameMap key_column_rename;
    /// Returns converting actions for tables that need to be performed before join
    auto dag = ActionsDAG::makeConvertingActions(
        cols_src, cols_dst, ActionsDAG::MatchColumnsMode::Name, true, !hasUsing(), &key_column_rename);

    for (auto & name : names_to_rename)
    {
        const auto it = key_column_rename.find(name);
        if (it != key_column_rename.end())
            name = it->second;
    }
    return dag;
}

void TableJoin::setStorageJoin(std::shared_ptr<StorageJoin> storage)
{
    if (right_storage_dictionary)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "StorageJoin and Dictionary join are mutually exclusive");
    right_storage_join = storage;
}

void TableJoin::setStorageJoin(std::shared_ptr<StorageDictionary> storage)
{
    if (right_storage_join)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "StorageJoin and Dictionary join are mutually exclusive");
    right_storage_dictionary = storage;
}

String TableJoin::renamedRightColumnName(const String & name) const
{
    if (const auto it = renames.find(name); it != renames.end())
        return it->second;
    return name;
}

void TableJoin::addJoinCondition(const ASTPtr & ast, bool is_left)
{
    LOG_TRACE(&Poco::Logger::get("TableJoin"), "Add join condition for {} table: {}", (is_left ? "left" : "right"), queryToString(ast));

    if (is_left)
        on_filter_condition_asts_left.push_back(ast);
    else
        on_filter_condition_asts_right.push_back(ast);
}

std::unordered_map<String, String> TableJoin::leftToRightKeyRemap() const
{
    std::unordered_map<String, String> left_to_right_key_remap;
    if (hasUsing())
    {
        const auto & required_right_keys = requiredRightKeys();
        for (size_t i = 0; i < key_names_left.size(); ++i)
        {
            const String & left_key_name = key_names_left[i];
            const String & right_key_name = key_names_right[i];

            if (!required_right_keys.contains(right_key_name))
                left_to_right_key_remap[left_key_name] = right_key_name;
        }
    }
    return left_to_right_key_remap;
}

/// Returns all conditions related to one table joined with 'and' function
static ASTPtr buildJoinConditionColumn(const ASTs & on_filter_condition_asts)
{
    if (on_filter_condition_asts.empty())
        return nullptr;

    if (on_filter_condition_asts.size() == 1)
        return on_filter_condition_asts[0];

    auto function = std::make_shared<ASTFunction>();
    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    function->arguments->children = on_filter_condition_asts;
    return function;
}

ASTPtr TableJoin::joinConditionColumn(JoinTableSide side) const
{
    if (side == JoinTableSide::Left)
        return buildJoinConditionColumn(on_filter_condition_asts_left);
    return buildJoinConditionColumn(on_filter_condition_asts_right);
}

std::pair<String, String> TableJoin::joinConditionColumnNames() const
{
    std::pair<String, String> res;
    if (auto cond_ast = joinConditionColumn(JoinTableSide::Left))
        res.first = cond_ast->getColumnName();
    if (auto cond_ast = joinConditionColumn(JoinTableSide::Right))
        res.second = cond_ast->getColumnName();
    return res;
}

}
