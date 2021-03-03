#include <Interpreters/TableJoin.h>

#include <Parsers/ASTExpressionList.h>

#include <Core/Settings.h>
#include <Core/Block.h>

#include <Common/StringUtils/StringUtils.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/materializeBlock.h>


namespace DB
{

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
    if (settings.partial_merge_join)
        join_algorithm = JoinAlgorithm::PREFER_PARTIAL_MERGE;
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

NameSet TableJoin::getQualifiedColumnsSet() const
{
    NameSet out;
    for (const auto & names : original_names)
        out.insert(names.first);
    return out;
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
    return keys_list;
}

ASTPtr TableJoin::rightKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    if (hasOn())
        keys_list->children = key_asts_right;
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
        for (const auto & column : columns_added_by_join)
            if (name == column.name)
                required.insert(name);
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

void TableJoin::splitAdditionalColumns(const Block & sample_block, Block & block_keys, Block & block_others) const
{
    block_others = materializeBlock(sample_block);

    for (const String & column_name : key_names_right)
    {
        /// Extract right keys with correct keys order. There could be the same key names.
        if (!block_keys.has(column_name))
        {
            auto & col = block_others.getByName(column_name);
            block_keys.insert(col);
            block_others.erase(column_name);
        }
    }
}

Block TableJoin::getRequiredRightKeys(const Block & right_table_keys, std::vector<String> & keys_sources) const
{
    const Names & left_keys = keyNamesLeft();
    const Names & right_keys = keyNamesRight();
    NameSet required_keys(requiredRightKeys().begin(), requiredRightKeys().end());
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
    return forceNullableLeft() && column_type->canBeInsideNullable();
}

bool TableJoin::rightBecomeNullable(const DataTypePtr & column_type) const
{
    return forceNullableRight() && column_type->canBeInsideNullable();
}

void TableJoin::addJoinedColumn(const NameAndTypePair & joined_column)
{
    if (rightBecomeNullable(joined_column.type))
        columns_added_by_join.emplace_back(NameAndTypePair(joined_column.name, makeNullable(joined_column.type)));
    else
        columns_added_by_join.push_back(joined_column);
}

void TableJoin::addJoinedColumnsAndCorrectNullability(Block & sample_block) const
{
    for (auto & col : sample_block)
    {
        /// Materialize column.
        /// Column is not empty if it is constant, but after Join all constants will be materialized.
        /// So, we need remove constants from header.
        if (col.column)
            col.column = nullptr;

        if (leftBecomeNullable(col.type))
            col.type = makeNullable(col.type);
    }

    for (const auto & col : columns_added_by_join)
    {
        auto res_type = col.type;

        if (rightBecomeNullable(res_type))
            res_type = makeNullable(res_type);

        sample_block.insert(ColumnWithTypeAndName(nullptr, res_type, col.name));
    }
}

bool TableJoin::sameJoin(const TableJoin * x, const TableJoin * y)
{
    if (!x && !y)
        return true;
    if (!x || !y)
        return false;

    return x->table_join.kind == y->table_join.kind
        && x->table_join.strictness == y->table_join.strictness
        && x->key_names_left == y->key_names_left
        && x->key_names_right == y->key_names_right
        && x->columns_added_by_join == y->columns_added_by_join;
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

bool TableJoin::allowDictJoin(const String & dict_key, const Block & sample_block, Names & src_names, NamesAndTypesList & dst_columns) const
{
    /// Support ALL INNER, [ANY | ALL | SEMI | ANTI] LEFT
    if (!isLeft(kind()) && !(isInner(kind()) && strictness() == ASTTableJoin::Strictness::All))
        return false;

    const Names & right_keys = keyNamesRight();
    if (right_keys.size() != 1)
        return false;

    /// TODO: support 'JOIN ... ON expr(dict_key) = table_key'
    auto it_key = original_names.find(right_keys[0]);
    if (it_key == original_names.end())
        return false;

    if (dict_key != it_key->second)
        return false; /// JOIN key != Dictionary key

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

    return true;
}

}
