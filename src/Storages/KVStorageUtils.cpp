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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
// returns keys may be filter by condition
bool traverseASTFilter(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSetsPtr & prepared_sets, const ContextPtr & context, FieldVectorPtr & res)
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
    if (function->name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto & child : function->arguments->children)
            if (!traverseASTFilter(primary_key, primary_key_type, child, prepared_sets, context, res))
                return false;
        return true;
    }
    if (function->name == "equals" || function->name == "in")
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

            PreparedSets::Hash set_key = value->getTreeHash(/*ignore_aliases=*/true);
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
    return false;
}

bool traverseDAGFilter(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const ActionsDAG::Node * elem, const ContextPtr & context, FieldVectorPtr & res)
{
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilter(primary_key, primary_key_type, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();

    if (func_name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto * child : elem->children)
            if (traverseDAGFilter(primary_key, primary_key_type, child, context, res))
                return true;
        return false;
    }
    if (func_name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto * child : elem->children)
            if (!traverseDAGFilter(primary_key, primary_key_type, child, context, res))
                return false;
        return true;
    }
    if (func_name == "equals" || func_name == "in")
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

            if (key->result_name != primary_key)
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

            if (set_column.getDataType() != primary_key_type->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }

        const auto * key = elem->children.at(0);
        while (key->type == ActionsDAG::ActionType::ALIAS)
            key = key->children.at(0);

        if (key->type != ActionsDAG::ActionType::INPUT)
            return false;

        if (key->result_name != primary_key)
            return false;

        const auto * value = elem->children.at(1);
        if (value->type != ActionsDAG::ActionType::COLUMN)
            return false;

        auto converted_field = convertFieldToType((*value->column)[0], *primary_key_type);
        if (!converted_field.isNull())
            res->push_back(converted_field);
        return true;
    }
    return false;
}
}

std::pair<FieldVectorPtr, bool> getFilterKeys(
    const String & primary_key, const DataTypePtr & primary_key_type, const std::optional<ActionsDAG> & filter_actions_dag, const ContextPtr & context)
{
    if (!filter_actions_dag)
        return {{}, true};

    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseDAGFilter(primary_key, primary_key_type, predicate, context, res);
    return std::make_pair(res, !matched_keys);
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

/// In current implementation rocks db/redis can have key with only one column.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key)
{
    if (primary_key.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one primary key is supported");
    return header.getPositionByName(primary_key[0]);
}

}
