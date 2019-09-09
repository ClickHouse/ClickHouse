#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Join.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Core/Block.h>
#include <Storages/IStorage.h>

#include <DataTypes/DataTypeNullable.h>


namespace DB
{

void AnalyzedJoin::addUsingKey(const ASTPtr & ast)
{
    key_names_left.push_back(ast->getColumnName());
    key_names_right.push_back(ast->getAliasOrColumnName());

    key_asts_left.push_back(ast);
    key_asts_right.push_back(ast);

    auto & right_key = key_names_right.back();
    if (renames.count(right_key))
        right_key = renames[right_key];
}

void AnalyzedJoin::addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast)
{
    key_names_left.push_back(left_table_ast->getColumnName());
    key_names_right.push_back(right_table_ast->getAliasOrColumnName());

    key_asts_left.push_back(left_table_ast);
    key_asts_right.push_back(right_table_ast);
}

/// @return how many times right key appears in ON section.
size_t AnalyzedJoin::rightKeyInclusion(const String & name) const
{
    if (hasUsing())
        return 0;

    size_t count = 0;
    for (const auto & key_name : key_names_right)
        if (name == key_name)
            ++count;
    return count;
}

void AnalyzedJoin::deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix)
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

        if (left_table_columns.count(column.name))
            inserted.name = right_table_prefix + column.name;

        original_names[inserted.name] = column.name;
        if (inserted.name != column.name)
            renames[column.name] = inserted.name;
    }

    columns_from_joined_table.swap(dedup_columns);
}

NameSet AnalyzedJoin::getQualifiedColumnsSet() const
{
    NameSet out;
    for (const auto & names : original_names)
        out.insert(names.first);
    return out;
}

NameSet AnalyzedJoin::getOriginalColumnsSet() const
{
    NameSet out;
    for (const auto & names : original_names)
        out.insert(names.second);
    return out;
}

NamesWithAliases AnalyzedJoin::getNamesWithAliases(const NameSet & required_columns) const
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

ASTPtr AnalyzedJoin::leftKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    keys_list->children = key_asts_left;
    return keys_list;
}

ASTPtr AnalyzedJoin::rightKeysList() const
{
    ASTPtr keys_list = std::make_shared<ASTExpressionList>();
    if (hasOn())
        keys_list->children = key_asts_right;
    return keys_list;
}

Names AnalyzedJoin::requiredJoinedNames() const
{
    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name);

    return Names(required_columns_set.begin(), required_columns_set.end());
}

NamesWithAliases AnalyzedJoin::getRequiredColumns(const Block & sample, const Names & action_required_columns) const
{
    NameSet required_columns(action_required_columns.begin(), action_required_columns.end());

    for (auto & column : requiredJoinedNames())
        if (!sample.has(column))
            required_columns.insert(column);

    return getNamesWithAliases(required_columns);
}

void AnalyzedJoin::addJoinedColumn(const NameAndTypePair & joined_column)
{
    if (join_use_nulls && isLeftOrFull(table_join.kind))
    {
        auto type = joined_column.type->canBeInsideNullable() ? makeNullable(joined_column.type) : joined_column.type;
        columns_added_by_join.emplace_back(NameAndTypePair(joined_column.name, std::move(type)));
    }
    else
        columns_added_by_join.push_back(joined_column);
}

void AnalyzedJoin::addJoinedColumnsAndCorrectNullability(Block & sample_block) const
{
    bool right_or_full_join = isRightOrFull(table_join.kind);
    bool left_or_full_join = isLeftOrFull(table_join.kind);

    for (auto & col : sample_block)
    {
        /// Materialize column.
        /// Column is not empty if it is constant, but after Join all constants will be materialized.
        /// So, we need remove constants from header.
        if (col.column)
            col.column = nullptr;

        bool make_nullable = join_use_nulls && right_or_full_join;

        if (make_nullable && col.type->canBeInsideNullable())
            col.type = makeNullable(col.type);
    }

    for (const auto & col : columns_added_by_join)
    {
        auto res_type = col.type;

        bool make_nullable = join_use_nulls && left_or_full_join;

        if (!make_nullable)
        {
            /// Keys from right table are usually not stored in Join, but copied from the left one.
            /// So, if left key is nullable, let's make right key nullable too.
            /// Note: for some join types it's not needed and, probably, may be removed.
            /// Note: changing this code, take into account the implementation in Join.cpp.
            auto it = std::find(key_names_right.begin(), key_names_right.end(), col.name);
            if (it != key_names_right.end())
            {
                auto pos = it - key_names_right.begin();
                const auto & left_key_name = key_names_left[pos];
                make_nullable = sample_block.getByName(left_key_name).type->isNullable();
            }
        }

        if (make_nullable && res_type->canBeInsideNullable())
            res_type = makeNullable(res_type);

        sample_block.insert(ColumnWithTypeAndName(nullptr, res_type, col.name));
    }
}

bool AnalyzedJoin::sameJoin(const AnalyzedJoin * x, const AnalyzedJoin * y)
{
    if (!x && !y)
        return true;
    if (!x || !y)
        return false;

    return x->table_join.kind == y->table_join.kind
        && x->table_join.strictness == y->table_join.strictness
        && x->key_names_left == y->key_names_left
        && x->key_names_right == y->key_names_right
        && x->columns_added_by_join == y->columns_added_by_join
        && x->hash_join == y->hash_join;
}

BlockInputStreamPtr AnalyzedJoin::createStreamWithNonJoinedDataIfFullOrRightJoin(const Block & source_header, UInt64 max_block_size) const
{
    if (isRightOrFull(table_join.kind))
        return hash_join->createStreamWithNonJoinedRows(source_header, *this, max_block_size);
    return {};
}

JoinPtr AnalyzedJoin::makeHashJoin(const Block & sample_block, const SizeLimits & size_limits_for_join) const
{
    auto join = std::make_shared<Join>(key_names_right, join_use_nulls, size_limits_for_join, table_join.kind, table_join.strictness);
    join->setSampleBlock(sample_block);
    return join;
}

void AnalyzedJoin::joinBlock(Block & block) const
{
    hash_join->joinBlock(block, *this);
}

void AnalyzedJoin::joinTotals(Block & block) const
{
    hash_join->joinTotals(block);
}

bool AnalyzedJoin::hasTotals() const
{
    return hash_join->hasTotals();
}

NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context).getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        const auto table_function = table_expression.table_function;
        auto query_context = const_cast<Context *>(&context.getQueryContext());
        const auto & function_storage = query_context->executeTableFunction(table_function);
        names_and_type_list = function_storage->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }
    else if (table_expression.database_and_table_name)
    {
        DatabaseAndTableWithAlias database_table(table_expression.database_and_table_name);
        const auto & table = context.getTable(database_table.database, database_table.table);
        names_and_type_list = table->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }

    return names_and_type_list;
}

}
