#include <Interpreters/TableJoin.h>

#include <Parsers/ASTExpressionList.h>

#include <Core/Settings.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <Common/StringUtils/StringUtils.h>

#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

TableJoin::TableJoin(const ASTTableJoin & table_join_ast, const Settings & settings, VolumePtr tmp_volume_)
    : join_info(table_join_ast, settings)
    , temporary_files_codec(settings.temporary_files_codec)
    , tmp_volume(tmp_volume_)
{
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
    if (!hasOn())
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


void TableJoin::addJoinedColumn(const NameAndTypePair & joined_column)
{
    DataTypePtr type = joined_column.type;

    if (!hasOn())
    {
        if (auto it = right_type_map.find(joined_column.name); it != right_type_map.end())
            type = it->second;
    }

    if (join_info.forceNullableRight() && type->canBeInsideNullable())
        type = makeNullable(joined_column.type);

    columns_added_by_join.emplace_back(joined_column.name, type);
}

void TableJoin::addJoinedColumnsAndCorrectTypes(NamesAndTypesList & names_and_types, bool correct_nullability) const
{
    ColumnsWithTypeAndName columns;
    for (auto & pair : names_and_types)
        columns.emplace_back(nullptr, std::move(pair.type), std::move(pair.name));
    names_and_types.clear();

    addJoinedColumnsAndCorrectTypes(columns, correct_nullability);

    for (auto & col : columns)
        names_and_types.emplace_back(std::move(col.name), std::move(col.type));
}

void TableJoin::addJoinedColumnsAndCorrectTypes(ColumnsWithTypeAndName & columns, bool correct_nullability) const
{
    for (auto & col : columns)
    {
        if (!hasOn())
        {
            if (auto it = left_type_map.find(col.name); it != left_type_map.end())
                col.type = it->second;
        }
        if (correct_nullability && join_info.forceNullableLeft() && col.type->canBeInsideNullable())
        {
            /// No need to nullify constants
            bool is_column_const = col.column && isColumnConst(*col.column);
            if (!is_column_const)
                col.type = makeNullable(col.type);
        }
    }

    /// Types in columns_added_by_join already converted and set nullable if needed
    for (const auto & col : columns_added_by_join)
        columns.emplace_back(nullptr, col.type, col.name);
}


bool TableJoin::needStreamWithNonJoinedRows() const
{
    if (join_info.strictness == ASTTableJoin::Strictness::Asof ||
        join_info.strictness == ASTTableJoin::Strictness::Semi)
        return false;
    return isRightOrFull(join_info.kind);
}

bool TableJoin::allowDictJoin(const String & dict_key, const Block & sample_block, Names & src_names, NamesAndTypesList & dst_columns) const
{
    /// Support ALL INNER, [ANY | ALL | SEMI | ANTI] LEFT
    if (!isLeft(join_info.kind) && !(isInner(join_info.kind) && join_info.strictness == ASTTableJoin::Strictness::All))
        return false;

    if (key_names_right.size() != 1)
        return false;

    /// TODO: support 'JOIN ... ON expr(dict_key) = table_key'
    auto it_key = original_names.find(key_names_right[0]);
    if (it_key == original_names.end())
        return false;

    if (dict_key != it_key->second)
        return false; /// JOIN key != Dictionary key

    for (const auto & col : sample_block)
    {
        if (col.name == key_names_right[0])
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

bool TableJoin::applyJoinKeyConvert(const ColumnsWithTypeAndName & left_sample_columns,
                                    const ColumnsWithTypeAndName & right_sample_columns,
                                    ActionsDAGPtr & left_converting_actions,
                                    ActionsDAGPtr & right_converting_actions)
{
    bool need_convert = !left_type_map.empty();
    if (hasOn() && !need_convert)
    {
        /// For `USING` we already inferred common type an syntax analyzer stage
        NamesAndTypesList left_list;
        NamesAndTypesList right_list;
        for (const auto & col : left_sample_columns)
            left_list.emplace_back(col.name, col.type);
        for (const auto & col : right_sample_columns)
            right_list.emplace_back(col.name, col.type);

        need_convert = inferJoinKeyCommonType(left_list, right_list);
    }

    if (need_convert)
    {
        left_converting_actions = JoinCommon::applyKeyConvertToTable(left_sample_columns, left_type_map, hasOn(), key_names_left);
        right_converting_actions = JoinCommon::applyKeyConvertToTable(right_sample_columns, right_type_map, hasOn(), key_names_right);
    }

    return need_convert;
}

bool TableJoin::inferJoinKeyCommonType(const NamesAndTypesList & left, const NamesAndTypesList & right)
{
    std::unordered_map<String, DataTypePtr> left_types;
    for (const auto & col : left)
    {
        left_types[col.name] = col.type;
    }

    std::unordered_map<String, DataTypePtr> right_types;
    for (const auto & col : right)
    {
        if (auto it = renames.find(col.name); it != renames.end())
            right_types[it->second] = col.type;
        else
            right_types[col.name] = col.type;
    }

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

        DataTypePtr supertype;
        try
        {
            supertype = DB::getLeastSupertype({ltype->second, rtype->second});
        }
        catch (DB::Exception & ex)
        {
            throw Exception(
                "Type mismatch of columns to JOIN by: " +
                    key_names_left[i] + ": " + ltype->second->getName() + " at left, " +
                    key_names_right[i] + ": " + rtype->second->getName() + " at right. " +
                    "Can't get supertype: " + ex.message(),
                ErrorCodes::TYPE_MISMATCH);
        }
        left_type_map[key_names_left[i]] = right_type_map[key_names_right[i]] = supertype;
    }

    return !left_type_map.empty();
}

JoinInfo TableJoin::getJoinInfo() const
{
    JoinInfo res = join_info;
    res.key_names_right = key_names_right;
    res.key_names_left = key_names_left;
    res.required_right_keys = requiredRightKeys();
    return res;
}



JoinInfo::JoinInfo(const ASTTableJoin & table_join_ast, const Settings & settings)
    : join_use_nulls(settings.join_use_nulls)
    , join_algorithm(settings.join_algorithm)
    , max_joined_block_rows(settings.max_joined_block_size_rows)
    , partial_merge_join_optimizations(settings.partial_merge_join_optimizations)
    , partial_merge_join_rows_in_right_blocks(settings.partial_merge_join_rows_in_right_blocks)
    , partial_merge_join_left_table_buffer_bytes(settings.partial_merge_join_left_table_buffer_bytes)
    , max_files_to_merge(settings.join_on_disk_max_files_to_merge)
    , size_limits(SizeLimits{settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode})

{
    kind = table_join_ast.kind;
    strictness = table_join_ast.strictness;
    if (table_join_ast.using_expression_list)
        match_expression = JoinInfo::MatchExpressionType::JoinUsing;
    if (table_join_ast.on_expression)
        match_expression = JoinInfo::MatchExpressionType::JoinOn;


    if (!size_limits.hasLimits())
        size_limits.max_bytes = settings.default_max_bytes_in_join;
}

}
