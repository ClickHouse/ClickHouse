#include <Storages/KVStorageUtils.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Functions/IFunction.h>


namespace DB
{

using FieldSet = std::set<Field>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct MultiColumnKeySet
{
    explicit MultiColumnKeySet(size_t columns)
        : key_values(columns), explicit_empty(columns)
    {
    }

    size_t columns() const { return key_values.size(); }

    size_t filledColumns() const
    {
        size_t filled{0};
        for (size_t i = 0; i < columns(); ++i)
            if (!key_values[i].empty() || explicit_empty[i])
                ++filled;
        return filled;
    }

    // For each column in this, if the column is empty, move rhs's corresponding column to this.
    // Otherwise, make the column the intersection between this and rhs.
    // If this column or rhs's column is explicit_empty, make this column explicit empty.
    void intersectOrAdd(MultiColumnKeySet& rhs)
    {
        assert(rhs.columns() == columns());
        for (size_t i = 0; i < columns(); ++i)
        {
            if (explicit_empty[i] || rhs.explicit_empty[i])
            {
                explicit_empty[i] = true;
                continue;
            }

            if (rhs.key_values[i].empty())
                continue;

            auto & key_value = key_values[i];
            if (key_value.empty())
                key_value = std::move(rhs.key_values[i]);
            else if (!rhs.key_values[i].empty())
            {
                if (key_value.size() > rhs.key_values[i].size())
                    std::swap(key_value, rhs.key_values[i]);
                for (auto it = key_value.begin(); it != key_value.end();)
                {
                    if (!rhs.key_values[i].contains(*it))
                        it = key_value.erase(it);
                    else
                        ++it;
                }
                if (key_value.empty())
                    explicit_empty[i] = true;
            }
        }
    }

    void addField(size_t column, FieldSet& field_set)
    {
        for (const auto & field : field_set)
            key_values[column].insert(field);
        explicit_empty[column] = false;
    }

    std::vector<FieldSet> key_values;
    // Identifies if the key is explicitly set empty by WHERE clause, like 'WHERE key = 1 AND key in (4, 5)'.
    std::vector<bool> explicit_empty;
};

KeyIterator::KeyIterator(FieldVectorsPtr keys_, size_t begin, size_t keys_to_process)
    : keys{keys_}
    , key_value_indices(keys->size())
    , keys_remaining(keys_to_process)
{
    std::vector<size_t> size_products(columns(), 1);
    for (int32_t i = static_cast<int32_t>(keys->size()) - 2; i >= 0; --i)
        size_products[i] = size_products[i + 1] * keys->at(i + 1).size();
    const auto total_keys = size_products[0] * keys->at(0).size();
    if (keys_remaining == 0)
        keys_remaining = total_keys - begin;
    assert(begin + keys_remaining <= total_keys);
    for (size_t i = 0; i < columns(); ++i)
    {
        key_value_indices[i] = begin / size_products[i];
        begin -= key_value_indices[i] * size_products[i];
    }
}

void KeyIterator::advance()
{
    assert(!atEnd());
    --keys_remaining;
    for (size_t i = columns() - 1; ; --i)
    {
        ++key_value_indices[i];
        if (key_value_indices[i] < keys->at(i).size())
            return;
        if (i == 0)
            return;
        key_value_indices[i] = 0;
    }
}

namespace
{
// returns keys may be filter by condition
bool traverseASTFilter(
    const std::string & primary_key,const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSetsPtr & prepared_sets, const ContextPtr & context, FieldVectorPtr & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, primary_key_type, child, prepared_sets, context, res))
                return true;
        return false;
    }
    else if (function->name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto & child : function->arguments->children)
            if (!traverseASTFilter(primary_key, primary_key_type, child, prepared_sets, context, res))
                return false;
        return true;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        std::shared_ptr<IAST> value;

        if (args.children.size() != 2)
            return false;

        if (function->name == "in")
        {
            if (!prepared_sets)
                return false;

            ident = args.children.at(0)->as<ASTIdentifier>();
            if (!ident)
                return false;

            if (ident->name() != primary_key)
                return false;
            value = args.children.at(1);

            PreparedSets::Hash set_key = value->getTreeHash(/*ignore_aliases=*/ true);
            FutureSetPtr future_set;

            if ((value->as<ASTSubquery>() || value->as<ASTIdentifier>()))
                future_set = prepared_sets->findSubquery(set_key);
            else
                future_set = prepared_sets->findTuple(set_key, {primary_key_type});

            if (!future_set)
                return false;

            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set)
                return false;

            if (!set->hasExplicitSetElements())
                return false;

            set->checkColumnsNumber(1);
            const auto & set_column = *set->getSetElements()[0];

            if (set_column.getDataType() != primary_key_type->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }
        else
        {
            if ((ident = args.children.at(0)->as<ASTIdentifier>()))
                value = args.children.at(1);
            else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
                value = args.children.at(0);
            else
                return false;

            if (ident->name() != primary_key)
                return false;

            const auto node = evaluateConstantExpressionAsLiteral(value, context);
            /// function->name == "equals"
            if (const auto * literal = node->as<ASTLiteral>())
            {
                auto converted_field = convertFieldToType(literal->value, *primary_key_type);
                if (!converted_field.isNull())
                    res->push_back(converted_field);
                return true;
            }
        }
    }
    return false;
}

bool traverseDAGFilter(
    const std::unordered_map<std::string, size_t>& primary_key_pos,
    const DataTypes & primary_key_types,
    const ActionsDAG::Node * elem,
    const ContextPtr & context, MultiColumnKeySet & res)
{
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilter(primary_key_pos, primary_key_types, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();
    const auto primary_key_columns = primary_key_pos.size();

    if (func_name == "and")
    {
        bool found{false};
        for (const auto * child : elem->children)
        {
            MultiColumnKeySet partial_res(primary_key_columns);
            if (!traverseDAGFilter(primary_key_pos, primary_key_types, child, context, partial_res))
                continue;
            found = true;
            res.intersectOrAdd(partial_res);
        }
        return found;
    }
    else if (func_name == "or")
    {
        // TODO: Support combining multiple predicates with all key columns specified connected by OR.
        // Like 'WHERE (k1 IN (1,2,3) AND k2 = 5) OR (k1 = 6 AND k2 IN (2, 7))'.
        // An efficient method to deduplicate between predicates is needed.
        for (const auto * child : elem->children)
        {
            MultiColumnKeySet partial_res(primary_key_columns);
            if (!traverseDAGFilter(primary_key_pos, primary_key_types, child, context, partial_res))
                return false;

            if (partial_res.filledColumns() != 1)
                return false;

            std::optional<size_t> res_filled_column = std::nullopt;
            for (size_t i = 0; i < primary_key_columns; ++i)
                if (!res.key_values[i].empty() || res.explicit_empty[i])
                {
                    if (res_filled_column.has_value())
                        return false;
                    res_filled_column = i;
                }

            if (!res_filled_column.has_value())
            {
                res = std::move(partial_res);
                continue;
            }
            const auto filled_column = res_filled_column.value();
            if (partial_res.key_values[filled_column].empty() && !partial_res.explicit_empty[filled_column])
                return false;
            if (!partial_res.key_values[filled_column].empty())
                res.addField(filled_column, partial_res.key_values[filled_column]);
        }
        return true;
    }
    else if (func_name == "equals" || func_name == "in")
    {
        if (elem->children.size() != 2)
            return false;

        if (func_name == "in")
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (!primary_key_pos.contains(key->result_name))
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const IColumn * value_col = value->column.get();
            if (const auto * col_const = typeid_cast<const ColumnConst *>(value_col))
                value_col = &col_const->getDataColumn();

            const auto * col_set = typeid_cast<const ColumnSet *>(value_col);
            if (!col_set)
                return false;

            auto future_set = col_set->getData();
            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set)
                return false;

            if (!set->hasExplicitSetElements())
                return false;

            set->checkColumnsNumber(1);
            const auto & set_column = *set->getSetElements()[0];

            const auto pos = primary_key_pos.at(key->result_name);
            if (set_column.getDataType() != primary_key_types[pos]->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res.key_values[pos].insert(set_column[row]);
            return true;
        }
        else
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (!primary_key_pos.contains(key->result_name))
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const auto pos = primary_key_pos.at(key->result_name);
            auto converted_field = convertFieldToType((*value->column)[0], *primary_key_types[pos]);
            if (!converted_field.isNull())
                res.key_values[pos].insert(converted_field);
            return true;
        }
    }
    return false;
}

}


std::pair<FieldVectorsPtr, bool> getFilterKeys(
    const Names & primary_key, const DataTypes & primary_key_types, const ActionDAGNodes & filter_nodes, const ContextPtr & context)
{
    if (filter_nodes.nodes.empty())
        return {nullptr, true};

    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    std::unordered_map<String, size_t> primary_key_pos;
    for (size_t i = 0; i < primary_key.size(); ++i)
        primary_key_pos[primary_key[i]] = i;

    MultiColumnKeySet res(primary_key.size());
    auto matched_keys = traverseDAGFilter(primary_key_pos, primary_key_types, predicate, context, res);
    if (!matched_keys)
        return {nullptr, true};

    for (size_t i = 0; i < primary_key.size(); ++i)
    {
        if (res.explicit_empty[i])
            return {nullptr, false};
        if (res.key_values[i].empty())
            return {nullptr, true};
    }
    auto key_values = std::make_shared<std::vector<FieldVector>>();
    key_values->reserve(primary_key.size());
    for (auto & key_value : res.key_values)
        key_values->emplace_back(key_value.begin(), key_value.end());
    return {key_values, false};
}

std::pair<FieldVectorPtr, bool> getFilterKeys(
    const String & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info, const ContextPtr & context)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
        return {{}, true};

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseASTFilter(primary_key, primary_key_type, select.where(), query_info.prepared_sets, context, res);
    return std::make_pair(res, !matched_keys);
}

std::vector<std::string> serializeKeysToRawString(
    KeyIterator& key_iterator,
    const DataTypes & key_column_types,
    size_t max_block_size)
{
    std::vector<std::string> result;
    while (!key_iterator.atEnd() && result.size() < max_block_size)
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        for (size_t i = 0; i < key_iterator.columns(); ++i)
            key_column_types[i]->getDefaultSerialization()->serializeBinary(key_iterator.keyValueAt(i), wb, {});
        wb.finalize();
        key_iterator.advance();
    }
    return result;
}

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it,
    FieldVector::const_iterator end,
    DataTypePtr key_column_type,
    size_t max_block_size)
{
    size_t num_keys = end - it;

    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it < end && (max_block_size == 0 || rows_processed < max_block_size))
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        key_column_type->getDefaultSerialization()->serializeBinary(*it, wb, {});
        wb.finalize();

        ++it;
        ++rows_processed;
    }
    return result;
}

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys)
{
    if (!keys.column)
        return {};

    size_t num_keys = keys.column->size();
    std::vector<std::string> result;
    result.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        Field field;
        keys.column->get(i, field);
        /// TODO(@vdimir): use serializeBinaryBulk
        keys.type->getDefaultSerialization()->serializeBinary(field, wb, {});
        wb.finalize();
    }
    return result;
}

/// In current implementation redis can have key with only one column.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key)
{
    if (primary_key.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one primary key is supported");
    return header.getPositionByName(primary_key[0]);
}

}
