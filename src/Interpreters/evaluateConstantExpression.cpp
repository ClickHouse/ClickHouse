#include <Interpreters/evaluateConstantExpression.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnTuple.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <unordered_map>


namespace DB
{
namespace Setting
{
    extern const SettingsBool normalize_function_names;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static EvaluateConstantExpressionResult getFieldAndDataTypeFromLiteral(ASTLiteral * literal)
{
    auto type = applyVisitor(FieldToDataType(), literal->value);
    /// In case of Array field nested fields can have different types.
    /// Example: Array [1, 2.3] will have 2 fields with types UInt64 and Float64
    /// when result type is Array(Float64).
    /// So, we need to convert this field to the result type.
    Field res = convertFieldToType(literal->value, *type);
    return {res, type};
}

std::optional<EvaluateConstantExpressionResult> evaluateConstantExpressionImpl(const ASTPtr & node, const ContextPtr & context, bool no_throw)
{
    if (ASTLiteral * literal = node->as<ASTLiteral>())
        return getFieldAndDataTypeFromLiteral(literal);

    NamesAndTypesList source_columns = {{ "_dummy", std::make_shared<DataTypeUInt8>() }};

    auto ast = node->clone();

    if (ast->as<ASTSubquery>() != nullptr)
    {
        /** For subqueries getColumnName if there are no alias will return __subquery_ + 'hash'.
          * If there is alias getColumnName for subquery will return alias.
          * In result block name of subquery after QueryAliasesVisitor pass will be _subquery1.
          * We specify alias for subquery, because we need to get column from result block.
          */
        ast->setAlias("constant_expression");
    }

    ReplaceQueryParameterVisitor param_visitor(context->getQueryParameters());
    param_visitor.visit(ast);

    /// Notice: function name normalization is disabled when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY
        && context->getSettingsRef()[Setting::normalize_function_names])
        FunctionNameNormalizer::visit(ast.get());

    auto syntax_result = TreeRewriter(context, no_throw).analyze(ast, source_columns);
    if (!syntax_result)
        return {};

    /// AST potentially could be transformed to literal during TreeRewriter analyze.
    /// For example if we have SQL user defined function that return literal AS subquery.
    if (ASTLiteral * literal = ast->as<ASTLiteral>())
        return getFieldAndDataTypeFromLiteral(literal);

    auto actions = ExpressionAnalyzer(ast, syntax_result, context).getConstActionsDAG();

    ColumnPtr result_column;
    DataTypePtr result_type;
    String result_name = ast->getColumnName();
    for (const auto & action_node : actions.getOutputs())
    {
        if ((action_node->result_name == result_name) && action_node->column)
        {
            result_column = action_node->column;
            result_type = action_node->result_type;
            break;
        }
    }

    if (!result_column)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Element of set in IN, VALUES, or LIMIT, or aggregate function parameter, or a table function argument "
                        "is not a constant expression (result column not found): {}", result_name);

    if (result_column->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Empty result column after evaluation "
                        "of constant expression for IN, VALUES, or LIMIT, or aggregate function parameter, or a table function argument");

    /// Expressions like rand() or now() are not constant
    if (!isColumnConst(*result_column))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Element of set in IN, VALUES, or LIMIT, or aggregate function parameter, or a table function argument "
                        "is not a constant expression (result column is not const): {}", result_name);

    return std::make_pair((*result_column)[0], result_type);
}

std::optional<EvaluateConstantExpressionResult> tryEvaluateConstantExpression(const ASTPtr & node, const ContextPtr & context)
{
    return evaluateConstantExpressionImpl(node, context, true);
}

EvaluateConstantExpressionResult evaluateConstantExpression(const ASTPtr & node, const ContextPtr & context)
{
    auto res = evaluateConstantExpressionImpl(node, context, false);
    if (!res)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "evaluateConstantExpression expected to return a result or throw an exception");
    return *res;
}

ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, const ContextPtr & context)
{
    /// If it's already a literal.
    if (node->as<ASTLiteral>())
        return node;
    return std::make_shared<ASTLiteral>(evaluateConstantExpression(node, context).first);
}

ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, const ContextPtr & context)
{
    if (const auto * id = node->as<ASTIdentifier>())
        return std::make_shared<ASTLiteral>(id->name());

    return evaluateConstantExpressionAsLiteral(node, context);
}

ASTPtr evaluateConstantExpressionForDatabaseName(const ASTPtr & node, const ContextPtr & context)
{
    ASTPtr res = evaluateConstantExpressionOrIdentifierAsLiteral(node, context);
    auto & literal = res->as<ASTLiteral &>();
    if (literal.value.safeGet<String>().empty())
    {
        String current_database = context->getCurrentDatabase();
        if (current_database.empty())
        {
            /// Table was created on older version of ClickHouse and CREATE contains not folded expression.
            /// Current database is not set yet during server startup, so we cannot evaluate it correctly.
            literal.value = context->getConfigRef().getString("default_database", "default");
        }
        else
            literal.value = current_database;
    }
    return res;
}


namespace
{
    using Conjunction = ColumnsWithTypeAndName;
    using Disjunction = std::vector<Conjunction>;

    Disjunction analyzeEquals(const ASTIdentifier * identifier, const Field & value, const ExpressionActionsPtr & expr)
    {
        if (!identifier || value.isNull())
        {
            return {};
        }

        for (const auto & name_and_type : expr->getRequiredColumnsWithTypes())
        {
            const auto & name = name_and_type.name;
            const auto & type = name_and_type.type;

            if (name == identifier->name())
            {
                ColumnWithTypeAndName column;
                Field converted = convertFieldToType(value, *type);
                if (converted.isNull())
                    return {};
                column.column = type->createColumnConst(1, converted);
                column.name = name;
                column.type = type;
                return {{std::move(column)}};
            }
        }

        return {};
    }

    Disjunction analyzeEquals(const ASTIdentifier * identifier, const ASTLiteral * literal, const ExpressionActionsPtr & expr)
    {
        if (!identifier || !literal)
        {
            return {};
        }

        return analyzeEquals(identifier, literal->value, expr);
    }

    Disjunction andDNF(const Disjunction & left, const Disjunction & right)
    {
        if (left.empty())
        {
            return right;
        }

        Disjunction result;

        for (const auto & conjunct1 : left)
        {
            for (const auto & conjunct2 : right)
            {
                Conjunction new_conjunct{conjunct1};
                new_conjunct.insert(new_conjunct.end(), conjunct2.begin(), conjunct2.end());
                result.emplace_back(new_conjunct);
            }
        }

        return result;
    }

    Disjunction analyzeFunction(const ASTFunction * fn, const ExpressionActionsPtr & expr, size_t & limit)
    {
        if (!fn || !limit)
        {
            return {};
        }

        // TODO: enumerate all possible function names!

        if (fn->name == "equals")
        {
            const auto * left = fn->arguments->children.front().get();
            const auto * right = fn->arguments->children.back().get();
            const auto * identifier = left->as<ASTIdentifier>() ? left->as<ASTIdentifier>() : right->as<ASTIdentifier>();
            const auto * literal = left->as<ASTLiteral>() ? left->as<ASTLiteral>() : right->as<ASTLiteral>();

            --limit;
            return analyzeEquals(identifier, literal, expr);
        }
        else if (fn->name == "in")
        {
            const auto * left = fn->arguments->children.front().get();
            const auto * right = fn->arguments->children.back().get();
            const auto * identifier = left->as<ASTIdentifier>();

            Disjunction result;

            auto add_dnf = [&](const auto & dnf)
            {
                if (dnf.size() > limit)
                {
                    result.clear();
                    return false;
                }

                result.insert(result.end(), dnf.begin(), dnf.end());
                limit -= dnf.size();
                return true;
            };

            if (const auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple")
            {
                const auto * tuple_elements = tuple_func->children.front()->as<ASTExpressionList>();
                for (const auto & child : tuple_elements->children)
                {
                    const auto * literal = child->as<ASTLiteral>();
                    const auto dnf = analyzeEquals(identifier, literal, expr);

                    if (dnf.empty())
                    {
                        return {};
                    }

                    if (!add_dnf(dnf))
                    {
                        return {};
                    }
                }
            }
            else if (const auto * tuple_literal = right->as<ASTLiteral>(); tuple_literal)
            {
                if (tuple_literal->value.getType() == Field::Types::Tuple)
                {
                    const auto & tuple = tuple_literal->value.safeGet<const Tuple &>();
                    for (const auto & child : tuple)
                    {
                        const auto dnf = analyzeEquals(identifier, child, expr);

                        if (dnf.empty())
                        {
                            return {};
                        }

                        if (!add_dnf(dnf))
                        {
                            return {};
                        }
                    }
                }
                else
                    return analyzeEquals(identifier, tuple_literal, expr);
            }
            else
            {
                return {};
            }

            return result;
        }
        else if (fn->name == "or")
        {
            const auto * args = fn->children.front()->as<ASTExpressionList>();

            if (!args)
            {
                return {};
            }

            Disjunction result;

            for (const auto & arg : args->children)
            {
                const auto dnf = analyzeFunction(arg->as<ASTFunction>(), expr, limit);

                if (dnf.empty())
                {
                    return {};
                }

                /// limit accounted in analyzeFunction()
                result.insert(result.end(), dnf.begin(), dnf.end());
            }

            return result;
        }
        else if (fn->name == "and")
        {
            const auto * args = fn->children.front()->as<ASTExpressionList>();

            if (!args)
            {
                return {};
            }

            Disjunction result;

            for (const auto & arg : args->children)
            {
                const auto dnf = analyzeFunction(arg->as<ASTFunction>(), expr, limit);

                if (dnf.empty())
                {
                    continue;
                }

                /// limit accounted in analyzeFunction()
                result = andDNF(result, dnf);
            }

            return result;
        }

        return {};
    }

    /// This is a map which stores constants for a single conjunction.
    /// It can contain execution results from different stanges.
    /// Example: for expression `(a + b) * c` and predicate `a = 1 and b = 2 and a + b = 3` the map will be
    /// a -> 1, b -> 2, a + b -> 3
    /// It is allowed to have a map with contradictive conditions, like for `a = 1 and b = 2 and a + b = 5`,
    /// but a map for predicate like `a = 1 and a = 2` cannot be built.
    using ConjunctionMap = ActionsDAG::IntermediateExecutionResult;
    using DisjunctionList = std::list<ConjunctionMap>;

    std::optional<ConjunctionMap> andConjunctions(const ConjunctionMap & lhs, const ConjunctionMap & rhs)
    {
        ConjunctionMap res;
        for (const auto & [node, column] : rhs)
        {
            auto it = lhs.find(node);
            /// If constants are different, the conjunction is invalid.
            if (it != lhs.end() && column.column->compareAt(0, 0, *it->second.column, 1))
                return {};

            if (it == lhs.end())
                res.emplace(node, column);
        }

        res.insert(lhs.begin(), lhs.end());
        return res;
    }

    DisjunctionList andDisjunctions(const DisjunctionList & lhs, const DisjunctionList & rhs)
    {
        DisjunctionList res;
        for (const auto & lhs_map : lhs)
            for (const auto & rhs_map : rhs)
                if (auto conj = andConjunctions(lhs_map, rhs_map))
                    res.emplace_back(std::move(*conj));

        return res;
    }

    DisjunctionList orDisjunctions(DisjunctionList && lhs, DisjunctionList && rhs)
    {
        lhs.splice(lhs.end(), std::move(rhs));
        return lhs;
    }

    const ActionsDAG::Node * findMatch(const ActionsDAG::Node * key, const MatchedTrees::Matches & matches)
    {
        auto it = matches.find(key);
        if (it == matches.end())
            return {};

        const auto & match = it->second;
        if (!match.node || match.monotonicity)
            return nullptr;

        return match.node;
    }

    ColumnPtr tryCastColumn(ColumnPtr col, const DataTypePtr & from_type, const DataTypePtr & to_type)
    {
        auto to_type_no_lc = recursiveRemoveLowCardinality(to_type);
        // std::cerr << ".. casting " << from_type->getName() << " -> " << to_type_no_lc->getName() << std::endl;
        if (!to_type_no_lc->canBeInsideNullable())
            return {};

        auto res = castColumnAccurateOrNull({col, from_type, std::string()}, makeNullable(to_type_no_lc));
        if (res->onlyNull())
            return nullptr;

        if (!typeid_cast<const ColumnNullable *>(res.get()))
            return nullptr;

        return res;
    }

    std::optional<ConjunctionMap::value_type> analyzeConstant(
        const ActionsDAG::Node * key,
        const ActionsDAG::Node * value,
        const MatchedTrees::Matches & matches)
    {
        if (value->type != ActionsDAG::ActionType::COLUMN)
            return {};

        if (const auto * col = typeid_cast<const ColumnConst *>(value->column.get()))
        {
            if (const auto * node = findMatch(key, matches))
            {
                ColumnPtr column = col->getPtr();
                if (!value->result_type->equals(*node->result_type))
                {
                    auto inner = tryCastColumn(col->getDataColumnPtr(), value->result_type, node->result_type);
                    if (!inner || inner->isNullAt(0))
                        return {};

                    auto innder_column = node->result_type->createColumn();
                    innder_column->insert((*inner)[0]);
                    column = ColumnConst::create(std::move(innder_column), 1);
                }

                return ConjunctionMap::value_type{node, {column, node->result_type, node->result_name}};
            }
        }

        return {};
    }

    std::optional<DisjunctionList> analyzeSet(
        const ActionsDAG::Node * key,
        const ActionsDAG::Node * value,
        const MatchedTrees::Matches & matches,
        const ContextPtr & context,
        size_t max_elements)
    {
        if (value->type != ActionsDAG::ActionType::COLUMN)
            return {};

        auto col = value->column;
        if (const auto * col_const = typeid_cast<const ColumnConst *>(col.get()))
            col = col_const->getDataColumnPtr();

        const auto * col_set = typeid_cast<const ColumnSet *>(col.get());
        if (!col_set || !col_set->getData())
            return {};

        auto * set_from_tuple = typeid_cast<FutureSetFromTuple *>(col_set->getData().get());
        if (!set_from_tuple)
            return {};

        SetPtr set = set_from_tuple->buildOrderedSetInplace(context);
        if (!set || !set->hasExplicitSetElements())
            return {};

        const auto * node = findMatch(key, matches);
        if (!node)
            return {};

        auto elements = set->getSetElements();
        auto types = set->getElementsTypes();

        ColumnPtr column;
        DataTypePtr type;
        if (elements.empty())
            return {};
        if (elements.size() == 1)
        {
            column = elements[0];
            type = types[0];
        }
        else
        {
            column = ColumnTuple::create(std::move(elements));
            type = std::make_shared<DataTypeTuple>(std::move(types));
        }

        if (column->size() > max_elements)
            return {};

        ColumnPtr casted_col;
        const NullMap * null_map = nullptr;

        if (!type->equals(*node->result_type))
        {
            casted_col = tryCastColumn(column, value->result_type, node->result_type);
            if (!casted_col)
                return {};
            const auto & col_nullable = assert_cast<const ColumnNullable &>(*casted_col);
            null_map = &col_nullable.getNullMapData();
            column = col_nullable.getNestedColumnPtr();
        }

        DisjunctionList res;
        if (node->result_type->isNullable() && set->hasNull())
        {
            auto col_null = node->result_type->createColumnConst(1, Field());
            res.push_back({ConjunctionMap{{node, {col_null, node->result_type, node->result_name}}}});
        }

        size_t num_rows = column->size();
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (null_map && (*null_map)[row])
                continue;

            auto innder_column = node->result_type->createColumn();
            innder_column->insert((*column)[row]);
            auto column_const = ColumnConst::create(std::move(innder_column), 1);

            res.push_back({ConjunctionMap{{node, {std::move(column_const), node->result_type, node->result_name}}}});
        }

        return res;
    }

    std::optional<DisjunctionList> analyze(const ActionsDAG::Node * node, const MatchedTrees::Matches & matches, const ContextPtr & context, size_t max_elements)
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            if (node->function_base->getName() == "equals")
            {
                const auto * lhs_node = node->children.at(0);
                const auto * rhs_node = node->children.at(1);
                if (auto val = analyzeConstant(lhs_node, rhs_node, matches))
                    return DisjunctionList{ConjunctionMap{std::move(*val)}};

                if (auto val = analyzeConstant(rhs_node, lhs_node, matches))
                    return DisjunctionList{ConjunctionMap{std::move(*val)}};
            }
            else if (node->function_base->getName() == "in")
            {
                const auto * lhs_node = node->children.at(0);
                const auto * rhs_node = node->children.at(1);

                return analyzeSet(lhs_node, rhs_node, matches, context, max_elements);
            }
            else if (node->function_base->getName() == "or")
            {
                DisjunctionList res;
                for (const auto * child : node->children)
                {
                    auto val = analyze(child, matches, context, max_elements);
                    if (!val)
                        return {};

                    if (val->size() + res.size() > max_elements)
                        return {};

                    res = orDisjunctions(std::move(res), std::move(*val));
                }

                return res;
            }
            else if (node->function_base->getName() == "and")
            {
                std::vector<DisjunctionList> lists;
                for (const auto * child : node->children)
                {
                    auto val = analyze(child, matches, context, max_elements);
                    if (!val)
                        continue;

                    lists.push_back(std::move(*val));
                }

                if (lists.empty())
                    return {};

                std::sort(lists.begin(), lists.end(),
                    [](const auto & lhs, const auto & rhs) { return lhs.size() < rhs.size(); });

                DisjunctionList res;
                bool first = true;
                for (auto & list : lists)
                {
                    if (first)
                    {
                        first = false;
                        res = std::move(list);
                        continue;
                    }

                    if (res.size() * list.size() > max_elements)
                        break;

                    res = andDisjunctions(res, list);
                }

                return res;
            }
        }
        else if (node->type == ActionsDAG::ActionType::COLUMN)
        {
            if (isColumnConst(*node->column) && node->result_type->canBeUsedInBooleanContext())
            {
                if (!node->column->getBool(0))
                    return DisjunctionList{};
            }
        }

        return {};
    }

    std::optional<ColumnsWithTypeAndName> evaluateConjunction(
        const ActionsDAG::NodeRawConstPtrs & target_expr,
        ConjunctionMap && conjunction)
    {
        auto columns = ActionsDAG::evaluatePartialResult(conjunction, target_expr, /* input_rows_count= */ 1, /* throw_on_error= */ false);
        for (const auto & column : columns)
            if (!column.column)
                return {};

        return columns;
    }
}

std::optional<ConstantVariants> evaluateExpressionOverConstantCondition(
    const ActionsDAG::Node * predicate,
    const ActionsDAG::NodeRawConstPtrs & expr,
    const ContextPtr & context,
    size_t max_elements)
{
    auto inverted_dag = KeyCondition::cloneASTWithInversionPushDown({predicate}, context);
    auto matches = matchTrees(expr, inverted_dag, false);

    auto predicates = analyze(inverted_dag.getOutputs().at(0), matches, context, max_elements);

    if (!predicates)
        return {};

    ConstantVariants res;
    for (auto & conjunction : *predicates)
    {
        auto vals = evaluateConjunction(expr, std::move(conjunction));
        if (!vals)
            return {};

        res.push_back(std::move(*vals));
    }

    return res;
}

std::optional<Blocks> evaluateExpressionOverConstantCondition(const ASTPtr & node, const ExpressionActionsPtr & target_expr, size_t & limit)
{
    Blocks result;

    if (const auto * fn = node->as<ASTFunction>())
    {
        const auto dnf = analyzeFunction(fn, target_expr, limit);

        if (dnf.empty() || !limit)
        {
            return {};
        }

        auto has_required_columns = [&target_expr](const Block & block) -> bool
        {
            for (const auto & name : target_expr->getRequiredColumns())
            {
                bool has_column = false;
                for (const auto & column_name : block.getNames())
                {
                    if (column_name == name)
                    {
                        has_column = true;
                        break;
                    }
                }

                if (!has_column)
                    return false;
            }

            return true;
        };

        for (const auto & conjunct : dnf)
        {
            Block block;
            bool always_false = false;

            for (const auto & elem : conjunct)
            {
                if (!block.has(elem.name))
                {
                    block.insert(elem);
                }
                else
                {
                    /// Conjunction of condition on column equality to distinct values can never be satisfied.

                    const ColumnWithTypeAndName & prev = block.getByName(elem.name);

                    if (isColumnConst(*prev.column) && isColumnConst(*elem.column))
                    {
                        Field prev_value = assert_cast<const ColumnConst &>(*prev.column).getField();
                        Field curr_value = assert_cast<const ColumnConst &>(*elem.column).getField();

                        always_false = prev_value != curr_value;
                        if (always_false)
                            break;
                    }
                }
            }

            if (always_false)
                continue;

            // Block should contain all required columns from `target_expr`
            if (!has_required_columns(block))
            {
                return {};
            }

            target_expr->execute(block);

            if (block.rows() == 1)
            {
                result.push_back(block);
            }
            else if (block.rows() == 0)
            {
                // filter out cases like "WHERE a = 1 AND a = 2"
                continue;
            }
            else
            {
                // FIXME: shouldn't happen
                return {};
            }
        }
    }
    else if (const auto * literal = node->as<ASTLiteral>())
    {
        // Check if it's always true or false.
        if (literal->value.getType() == Field::Types::UInt64 && literal->value.safeGet<UInt64>() == 0)
            return {result};
        else
            return {};
    }

    return {result};
}

}
