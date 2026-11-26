#include <Analyzer/Passes/AliasMapPushdownPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Columns/ColumnConst.h>
#include <Columns/validateColumnType.h>
#include <Common/SipHash.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Settings.h>
#include <Core/ShortCircuitFunctionEvaluation.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/IStorage.h>

#include <fmt/format.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_early_constant_folding;
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool join_use_nulls;
    extern const SettingsBool optimize_map_alias_pushdown;
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
}

namespace
{

constexpr size_t MAX_ALIAS_MAP_PUSHDOWN_DEPTH = 16;
constexpr size_t MAX_PUSHDOWN_STEPS = 64;
constexpr size_t MAX_CONSTANT_ARRAY_UNROLL = 64;

enum class MapAccessKind : uint8_t
{
    ArrayElement,
    ContainsKey,
};

struct FoldConfig
{
    bool fold_constants = true;
    bool allow_nondet_folding = true;
};

struct TableColKey
{
    const TableNode * table = nullptr;
    String col_name;

    bool operator==(const TableColKey & other) const = default;
};

struct TableColKeyHash
{
    size_t operator()(const TableColKey & key) const
    {
        SipHash hash;
        hash.update(key.table);
        hash.update(key.col_name.size());
        hash.update(key.col_name);
        return hash.get64();
    }
};

struct PushdownContext
{
    const ContextPtr & query_context;
    const TableNode * root_table_node = nullptr;
    FoldConfig fold_cfg;
    bool short_circuit_enabled = true;
    bool applied_rule = false;
    size_t steps = 0;
};

bool isExprDeterministic(const QueryTreeNodePtr & node)
{
    return !queryTreeNodeContainsNonDeterministicFunction(node);
}

QueryTreeNodePtr preserveNonDeterminismIfConstant(
    QueryTreeNodePtr node,
    bool conds_deterministic,
    const QueryTreeNodePtr & non_det_source)
{
    if (conds_deterministic || !non_det_source || !node)
        return node;
    if (const auto * const_node = node->as<ConstantNode>(); const_node && const_node->isDeterministic())
    {
        return std::make_shared<ConstantNode>(
            ConstantValue{const_node->getColumn(), const_node->getResultType()},
            non_det_source,
            false);
    }
    return node;
}

QueryTreeNodePtr makeConstUInt8(
    bool value,
    bool is_deterministic = true,
    const QueryTreeNodePtr & source_expression = nullptr)
{
    if (!is_deterministic && source_expression)
    {
        auto dt = std::make_shared<DataTypeUInt8>();
        auto col = dt->createColumnConst(1, Field(static_cast<UInt64>(value ? 1 : 0)));
        const auto * col_const = typeid_cast<const ColumnConst *>(col.get());
        return std::make_shared<ConstantNode>(ConstantValue{col_const->getPtr(), std::move(dt)}, source_expression, false);
    }
    return std::make_shared<ConstantNode>(Field(static_cast<UInt64>(value ? 1 : 0)), std::make_shared<DataTypeUInt8>());
}

QueryTreeNodePtr makeConstBoolLike(
    bool value,
    const DataTypePtr & target_type,
    bool is_deterministic,
    const QueryTreeNodePtr & source_expression,
    const ContextPtr & context)
{
    QueryTreeNodePtr res = makeConstUInt8(value, is_deterministic, source_expression);
    if (!target_type || res->getResultType()->equals(*target_type))
        return res;

    auto col = target_type->createColumnConst(1, Field(static_cast<UInt64>(value ? 1 : 0)));
    if (const auto * col_const = col ? typeid_cast<const ColumnConst *>(col.get()) : nullptr;
        col_const && !col_const->getDataColumn().isDummy() && columnMatchesType(*col, *target_type))
    {
        return std::make_shared<ConstantNode>(
            ConstantValue{col_const->getPtr(), target_type},
            is_deterministic ? nullptr : source_expression,
            is_deterministic);
    }

    return buildCastFunction(std::move(res), target_type, context, true);
}

QueryTreeNodePtr makeMapDefaultConstant(
    const DataTypePtr & value_type,
    bool is_deterministic = true,
    const QueryTreeNodePtr & source_expression = nullptr)
{
    QueryTreeNodePtr def_node = std::make_shared<ConstantNode>(value_type->getDefault(), value_type);
    return preserveNonDeterminismIfConstant(std::move(def_node), is_deterministic, source_expression);
}

bool isDefaultConstantForType(const QueryTreeNodePtr & node, const DataTypePtr & value_type)
{
    const auto * const_node = node ? node->as<ConstantNode>() : nullptr;
    if (!const_node)
        return false;
    return const_node->getResultType()->equals(*value_type) && const_node->getValue() == value_type->getDefault();
}

bool referencesLambdaParam(const QueryTreeNodePtr & node, const IQueryTreeNode * lambda_args_ptr, const String & param_name)
{
    if (!node || isQueryOrUnionNode(node))
        return false;

    if (const auto * col_node = node->as<ColumnNode>())
    {
        auto src = col_node->getColumnSourceOrNull();
        if (src && src.get() == lambda_args_ptr && col_node->getColumnName() == param_name)
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && referencesLambdaParam(child, lambda_args_ptr, param_name))
            return true;
    }
    return false;
}

bool isLambdaParamNode(const QueryTreeNodePtr & node, const IQueryTreeNode * lambda_args_ptr, const String & param_name)
{
    const auto * col_node = node ? node->as<ColumnNode>() : nullptr;
    if (!col_node)
        return false;
    auto src = col_node->getColumnSourceOrNull();
    return src && src.get() == lambda_args_ptr && col_node->getColumnName() == param_name;
}

bool hasDisallowedRewriteBodyNodes(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;
    if (isQueryOrUnionNode(node))
        return true;

    if (const auto * fn = node->as<FunctionNode>())
    {
        if (!fn->isResolved() || !fn->isOrdinaryCall() || fn->getFunctionName() == "arrayJoin")
            return true;
        if (auto func = fn->getFunction(); func && func->isStateful())
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && hasDisallowedRewriteBodyNodes(child))
            return true;
    }
    return false;
}

std::optional<bool> tryEvaluateConstantInTuple(
    const ConstantNode & lhs,
    const ConstantNode & rhs,
    bool is_not_in,
    const FoldConfig & fold_cfg)
{
    if (!fold_cfg.fold_constants || lhs.isNull() || rhs.isNull())
        return std::nullopt;
    if ((!lhs.isDeterministic() || !rhs.isDeterministic()) && !fold_cfg.allow_nondet_folding)
        return std::nullopt;

    const DataTypePtr lhs_base = removeLowCardinality(lhs.getResultType());
    if (!isString(lhs_base) && (!isInteger(lhs_base) || isBool(lhs_base)))
        return std::nullopt;

    const Field & lhs_val = lhs.getValue();
    const Field & rhs_val = rhs.getValue();

    bool found = false;
    if (rhs_val.getType() == Field::Types::Tuple)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeLowCardinality(rhs.getResultType()).get());
        const auto & tuple = rhs_val.safeGet<Tuple>();
        if (!tuple_type || tuple_type->getElements().size() != tuple.size())
            return std::nullopt;

        const auto & elem_types = tuple_type->getElements();
        for (size_t i = 0; i < tuple.size(); ++i)
        {
            if (tuple[i].isNull())
                return std::nullopt;
            const DataTypePtr elem_base = removeLowCardinality(elem_types[i]);
            if (!lhs_base->equals(*elem_base))
                return std::nullopt;
            if (lhs_val == tuple[i])
            {
                found = true;
                break;
            }
        }
    }
    else
    {
        const DataTypePtr rhs_base = removeLowCardinality(rhs.getResultType());
        if (!lhs_base->equals(*rhs_base))
            return std::nullopt;
        found = (lhs_val == rhs_val);
    }

    return is_not_in ? !found : found;
}

QueryTreeNodePtr tryFoldResolvedFunction(const QueryTreeNodePtr & node, const FoldConfig & fold_cfg)
{
    if (!fold_cfg.fold_constants)
        return node;

    const auto * fn = node ? node->as<FunctionNode>() : nullptr;
    if (!fn || !fn->isResolved() || !fn->isOrdinaryCall())
        return node;

    const String & name = fn->getFunctionName();
    const auto & args = fn->getArguments().getNodes();

    if (name == "in" || name == "notIn")
    {
        if (args.size() == 2 && !isNullableOrLowCardinalityNullable(fn->getResultType()))
        {
            const auto * lhs_const = args[0]->as<ConstantNode>();
            const auto * rhs_const = args[1]->as<ConstantNode>();
            if (lhs_const && rhs_const)
            {
                if (auto res = tryEvaluateConstantInTuple(*lhs_const, *rhs_const, name == "notIn", fold_cfg))
                {
                    const bool is_det = lhs_const->isDeterministic() && rhs_const->isDeterministic();
                    return makeConstUInt8(*res, is_det, node);
                }
            }
        }
        return node;
    }

    if (isNameOfInFunction(name))
        return node;

    auto func_base = fn->getFunction();
    if (!func_base || func_base->isStateful() || !func_base->isSuitableForConstantFolding()
        || !func_base->isDeterministicInScopeOfQuery())
        return node;

    if (!fold_cfg.allow_nondet_folding && !func_base->isDeterministic())
        return node;

    auto arg_columns = fn->getArgumentColumns();
    for (const auto & arg : arg_columns)
    {
        if (!arg.column || !isColumnConst(*arg.column))
            return node;
    }

    bool all_deterministic = true;
    for (const auto & arg : args)
    {
        if (!isExprDeterministic(arg))
        {
            all_deterministic = false;
            if (!fold_cfg.allow_nondet_folding)
                return node;
        }
    }

    auto result_type = func_base->getResultType();
    auto executable_func = func_base->prepare(arg_columns);
    auto col = executable_func->execute(arg_columns, result_type, 1, /*dry_run=*/ true);
    if (col && col->empty() && isColumnConst(*col))
        col = col->cloneResized(1);

    const auto * col_const = col ? typeid_cast<const ColumnConst *>(col.get()) : nullptr;
    if (!col_const || col_const->getDataColumn().isDummy())
        return node;
    if (!columnMatchesType(*col, *result_type))
        return node;
    if (col->byteSize() >= 1_MiB)
        return node;

    const bool is_deterministic = all_deterministic && func_base->isDeterministic();

    return std::make_shared<ConstantNode>(
        ConstantValue{col_const->getPtr(), std::move(result_type)},
        node,
        is_deterministic);
}

QueryTreeNodePtr castResultIfNeeded(
    QueryTreeNodePtr expr,
    const DataTypePtr & target_type,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    if (!expr || !target_type || expr->getResultType()->equals(*target_type))
        return expr;
    return tryFoldResolvedFunction(buildCastFunction(std::move(expr), target_type, context, true), fold_cfg);
}

QueryTreeNodePtr convertConstKeyToTargetType(
    const QueryTreeNodePtr & key_expr,
    const DataTypePtr & target_key_type,
    const ContextPtr & context)
{
    if (!key_expr || !target_key_type)
        return nullptr;
    if (key_expr->getResultType()->equals(*target_key_type))
        return key_expr->clone();

    const auto * key_const = key_expr->as<ConstantNode>();
    if (!key_const || key_const->isNull())
        return nullptr;

    DataTypePtr target_unwrapped = removeLowCardinality(target_key_type);
    Field converted = tryConvertFieldToTypeExact(
        key_const->getValue(), *target_unwrapped, removeLowCardinality(key_const->getResultType()).get());
    if (converted.isNull())
        return nullptr;

    auto col = target_key_type->createColumnConst(1, converted);
    const auto * col_const = col ? typeid_cast<const ColumnConst *>(col.get()) : nullptr;
    if (!col_const || !columnMatchesType(*col, *target_key_type))
    {
        QueryTreeNodePtr scalar = std::make_shared<ConstantNode>(std::move(converted), target_unwrapped);
        if (!target_key_type->equals(*target_unwrapped))
            scalar = buildCastFunction(std::move(scalar), target_key_type, context, true);
        return preserveNonDeterminismIfConstant(std::move(scalar), key_const->isDeterministic(), key_expr);
    }

    return std::make_shared<ConstantNode>(
        ConstantValue{col_const->getPtr(), target_key_type},
        key_const->isDeterministic() ? nullptr : key_expr,
        key_const->isDeterministic());
}

QueryTreeNodePtr convertConditionToNonNullableUInt8(
    QueryTreeNodePtr cond,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    if (!cond || !isNullableOrLowCardinalityNullable(cond->getResultType()))
        return cond;

    return tryFoldResolvedFunction(
        createResolvedFunction(context, "ifNull", {std::move(cond), makeConstUInt8(false)}),
        fold_cfg);
}

QueryTreeNodePtr coerceSingleKeptLogicalOperand(
    QueryTreeNodePtr operand,
    const DataTypePtr & result_type,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    if (isBool(operand->getResultType()))
        return castResultIfNeeded(std::move(operand), result_type, context, fold_cfg);

    QueryTreeNodePtr zero = std::make_shared<ConstantNode>(operand->getResultType()->getDefault(), operand->getResultType());
    QueryTreeNodePtr as_bool_uint8 = tryFoldResolvedFunction(
        createResolvedFunction(context, "notEquals", {std::move(operand), std::move(zero)}),
        fold_cfg);
    return castResultIfNeeded(std::move(as_bool_uint8), result_type, context, fold_cfg);
}

QueryTreeNodePtr substituteAndFoldExpr(
    const QueryTreeNodePtr & node,
    const IQueryTreeNode * k_source,
    const String & k_name,
    const QueryTreeNodePtr & k_replacement,
    const IQueryTreeNode * v_source,
    const String & v_name,
    const QueryTreeNodePtr & v_replacement,
    const ContextPtr & context,
    const FoldConfig & fold_cfg);

/// Unrolls `arrayExists` / `arrayAll` over a small constant array only when every element folds to a constant boolean.
QueryTreeNodePtr tryFoldConstantArrayQuantifier(
    const FunctionNode & fn,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    if (!fold_cfg.fold_constants)
        return nullptr;

    const String & name = fn.getFunctionName();
    if ((name != "arrayExists" && name != "arrayAll") || isNullableOrLowCardinalityNullable(fn.getResultType()))
        return nullptr;

    const auto & args = fn.getArguments().getNodes();
    if (args.size() != 2)
        return nullptr;

    const auto * lambda = args[0]->as<LambdaNode>();
    const auto * arr_const = args[1]->as<ConstantNode>();
    if (!lambda || lambda->getArguments().getNames().size() != 1 || !lambda->getExpression()
        || hasDisallowedRewriteBodyNodes(lambda->getExpression())
        || !arr_const || arr_const->isNull() || !isArray(arr_const->getResultType()))
        return nullptr;

    if (!arr_const->isDeterministic() && !fold_cfg.allow_nondet_folding)
        return nullptr;

    const Array & arr = arr_const->getValue().safeGet<Array>();
    if (arr.size() > MAX_CONSTANT_ARRAY_UNROLL)
        return nullptr;

    const DataTypePtr & result_type = fn.getResultType();
    bool all_det = arr_const->isDeterministic();
    QueryTreeNodePtr non_det_src = all_det ? nullptr : args[1];

    if (arr.empty())
        return makeConstBoolLike(name == "arrayAll", result_type, all_det, non_det_src, context);

    const DataTypePtr elem_type = assert_cast<const DataTypeArray &>(*arr_const->getResultType()).getNestedType();
    const auto * lambda_args_ptr = &lambda->getArguments();
    const String & param_name = lambda->getArguments().getNames()[0];

    bool any_true = false;
    bool all_true = true;

    /// Evaluate all elements so runtime exceptions on later elements are never swallowed.
    for (const Field & elem : arr)
    {
        auto elem_col = elem_type->createColumnConst(1, elem);
        const auto * elem_col_const = typeid_cast<const ColumnConst *>(elem_col.get());
        QueryTreeNodePtr elem_node = std::make_shared<ConstantNode>(
            ConstantValue{elem_col_const->getPtr(), elem_type},
            args[1],
            arr_const->isDeterministic());

        QueryTreeNodePtr body = substituteAndFoldExpr(
            lambda->getExpression(),
            lambda_args_ptr,
            param_name,
            elem_node,
            nullptr,
            {},
            nullptr,
            context,
            fold_cfg);
        body = convertConditionToNonNullableUInt8(std::move(body), context, fold_cfg);

        if (!isExprDeterministic(body))
        {
            all_det = false;
            if (!non_det_src)
                non_det_src = body;
        }

        auto b = tryExtractConstantFromConditionNode(body);
        if (!b || (!all_det && !fold_cfg.allow_nondet_folding))
            return nullptr;

        any_true |= *b;
        all_true &= *b;
    }

    const bool res = (name == "arrayExists") ? any_true : all_true;
    return makeConstBoolLike(res, result_type, all_det, non_det_src, context);
}

void reresolveAndCastFunctionArgsIfNeeded(
    FunctionNode & fn,
    const VectorWithMemoryTracking<DataTypePtr> & orig_arg_types,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    const String & fn_name = fn.getFunctionName();
    if (!isNameOfInFunction(fn_name) && FunctionFactory::instance().has(fn_name))
    {
        resolveOrdinaryFunctionNodeByName(fn, fn_name, context);
        return;
    }

    auto & args = fn.getArguments().getNodes();
    for (size_t i = 0; i < args.size() && i < orig_arg_types.size(); ++i)
    {
        if (args[i] && orig_arg_types[i] && !args[i]->getResultType()->equals(*orig_arg_types[i]))
            args[i] = castResultIfNeeded(args[i], orig_arg_types[i], context, fold_cfg);
    }
}

QueryTreeNodePtr substituteAndFoldExpr(
    const QueryTreeNodePtr & node,
    const IQueryTreeNode * k_source,
    const String & k_name,
    const QueryTreeNodePtr & k_replacement,
    const IQueryTreeNode * v_source,
    const String & v_name,
    const QueryTreeNodePtr & v_replacement,
    const ContextPtr & context,
    const FoldConfig & fold_cfg)
{
    if (!node || isQueryOrUnionNode(node))
        return node ? node->clone() : nullptr;

    if (const auto * col_node = node->as<ColumnNode>())
    {
        auto src = col_node->getColumnSourceOrNull();
        auto make_cast_if_needed = [&](QueryTreeNodePtr rep)
        {
            return castResultIfNeeded(std::move(rep), col_node->getColumnType(), context, fold_cfg);
        };
        if (k_replacement && src && src.get() == k_source && col_node->getColumnName() == k_name)
            return make_cast_if_needed(k_replacement->clone());
        if (v_replacement && src && src.get() == v_source && col_node->getColumnName() == v_name)
            return make_cast_if_needed(v_replacement->clone());
        return node->clone();
    }

    QueryTreeNodePtr cloned = node->clone();
    if (auto * inner_lambda = cloned->as<LambdaNode>())
    {
        if (inner_lambda->getExpression())
        {
            const auto orig_type = inner_lambda->getExpression()->getResultType();
            QueryTreeNodePtr new_expr = substituteAndFoldExpr(
                inner_lambda->getExpression(), k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
            new_expr = castResultIfNeeded(std::move(new_expr), orig_type, context, fold_cfg);
            inner_lambda->getExpression() = std::move(new_expr);
        }
        return cloned;
    }

    if (auto * fn = cloned->as<FunctionNode>())
    {
        auto & args = fn->getArguments().getNodes();
        const String & fn_name = fn->getFunctionName();
        const DataTypePtr orig_result_type = fn->getResultType();

        VectorWithMemoryTracking<DataTypePtr> orig_arg_types;
        orig_arg_types.reserve(args.size());
        for (const auto & arg : args)
            orig_arg_types.push_back(arg ? arg->getResultType() : nullptr);

        auto can_collapse_cond = [&](const QueryTreeNodePtr & cond) -> std::optional<bool>
        {
            if (!fold_cfg.fold_constants)
                return std::nullopt;
            if (!isExprDeterministic(cond) && !fold_cfg.allow_nondet_folding)
                return std::nullopt;
            return tryExtractConstantFromConditionNode(cond);
        };

        /// Top-down evaluation of conditionals so statically dead branches are never evaluated.
        if (fn->isResolved() && fn->isOrdinaryCall() && fn_name == "if" && args.size() == 3)
        {
            args[0] = substituteAndFoldExpr(args[0], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
            if (auto b = can_collapse_cond(args[0]))
            {
                const bool cond_det = isExprDeterministic(args[0]);
                QueryTreeNodePtr chosen = substituteAndFoldExpr(
                    *b ? args[1] : args[2], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
                return preserveNonDeterminismIfConstant(
                    castResultIfNeeded(std::move(chosen), orig_result_type, context, fold_cfg),
                    cond_det,
                    args[0]);
            }
            args[1] = substituteAndFoldExpr(args[1], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
            args[2] = substituteAndFoldExpr(args[2], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
            reresolveAndCastFunctionArgsIfNeeded(*fn, orig_arg_types, context, fold_cfg);
            return castResultIfNeeded(tryFoldResolvedFunction(cloned, fold_cfg), orig_result_type, context, fold_cfg);
        }

        if (fn->isResolved() && fn->isOrdinaryCall() && fn_name == "multiIf" && args.size() >= 3 && (args.size() % 2 == 1))
        {
            QueryTreeNodes kept_args;
            kept_args.reserve(args.size());
            bool all_conds_det = true;
            QueryTreeNodePtr non_det_src;

            for (size_t i = 0; i + 1 < args.size(); i += 2)
            {
                QueryTreeNodePtr cond_i = substituteAndFoldExpr(
                    args[i], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
                if (!isExprDeterministic(cond_i))
                {
                    all_conds_det = false;
                    if (!non_det_src)
                        non_det_src = cond_i;
                }

                auto b = can_collapse_cond(cond_i);
                if (b && !*b)
                    continue;

                QueryTreeNodePtr val_i = substituteAndFoldExpr(
                    args[i + 1], k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);

                if (b && *b)
                {
                    if (kept_args.empty())
                        return preserveNonDeterminismIfConstant(
                            castResultIfNeeded(std::move(val_i), orig_result_type, context, fold_cfg),
                            all_conds_det,
                            non_det_src);
                    kept_args.push_back(std::move(val_i));
                    const String rebuilt_name = (kept_args.size() == 3) ? "if" : "multiIf";
                    QueryTreeNodePtr rebuilt = createResolvedFunction(context, rebuilt_name, std::move(kept_args));
                    return preserveNonDeterminismIfConstant(
                        castResultIfNeeded(tryFoldResolvedFunction(std::move(rebuilt), fold_cfg), orig_result_type, context, fold_cfg),
                        all_conds_det,
                        non_det_src);
                }

                kept_args.push_back(std::move(cond_i));
                kept_args.push_back(std::move(val_i));
            }

            QueryTreeNodePtr else_val = substituteAndFoldExpr(
                args.back(), k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
            if (kept_args.empty())
                return preserveNonDeterminismIfConstant(
                    castResultIfNeeded(std::move(else_val), orig_result_type, context, fold_cfg),
                    all_conds_det,
                    non_det_src);

            kept_args.push_back(std::move(else_val));
            if (kept_args.size() != args.size())
            {
                const String rebuilt_name = (kept_args.size() == 3) ? "if" : "multiIf";
                QueryTreeNodePtr rebuilt = createResolvedFunction(context, rebuilt_name, std::move(kept_args));
                return preserveNonDeterminismIfConstant(
                    castResultIfNeeded(tryFoldResolvedFunction(std::move(rebuilt), fold_cfg), orig_result_type, context, fold_cfg),
                    all_conds_det,
                    non_det_src);
            }

            args = std::move(kept_args);
            reresolveAndCastFunctionArgsIfNeeded(*fn, orig_arg_types, context, fold_cfg);
            return preserveNonDeterminismIfConstant(
                castResultIfNeeded(tryFoldResolvedFunction(cloned, fold_cfg), orig_result_type, context, fold_cfg),
                all_conds_det,
                non_det_src);
        }

        if (fn->isResolved() && fn->isOrdinaryCall()
            && (fn_name == "and" || fn_name == "or")
            && !isNullableOrLowCardinalityNullable(orig_result_type))
        {
            const bool is_and = (fn_name == "and");
            QueryTreeNodes kept;
            kept.reserve(args.size());
            bool all_dropped_det = true;
            QueryTreeNodePtr non_det_src;

            for (const auto & arg : args)
            {
                QueryTreeNodePtr arg_sub = substituteAndFoldExpr(
                    arg, k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
                if (auto b = can_collapse_cond(arg_sub))
                {
                    const bool arg_det = isExprDeterministic(arg_sub);
                    all_dropped_det &= arg_det;
                    if (!arg_det && !non_det_src)
                        non_det_src = arg_sub;
                    if ((is_and && !*b) || (!is_and && *b))
                        return makeConstBoolLike(*b, orig_result_type, all_dropped_det, non_det_src ? non_det_src : arg_sub, context);
                    continue;
                }
                kept.push_back(std::move(arg_sub));
            }

            if (kept.empty())
                return makeConstBoolLike(is_and, orig_result_type, all_dropped_det, non_det_src, context);
            if (kept.size() == 1)
                return preserveNonDeterminismIfConstant(
                    coerceSingleKeptLogicalOperand(std::move(kept[0]), orig_result_type, context, fold_cfg),
                    all_dropped_det,
                    non_det_src);
            if (kept.size() != args.size())
                return preserveNonDeterminismIfConstant(
                    tryFoldResolvedFunction(createResolvedFunction(context, fn_name, std::move(kept)), fold_cfg),
                    all_dropped_det,
                    non_det_src);

            args = std::move(kept);
            reresolveAndCastFunctionArgsIfNeeded(*fn, orig_arg_types, context, fold_cfg);
            return tryFoldResolvedFunction(cloned, fold_cfg);
        }

        bool has_lambda_arg = false;
        for (auto & arg : args)
        {
            if (arg && arg->getNodeType() == QueryTreeNodeType::LAMBDA)
                has_lambda_arg = true;
            arg = substituteAndFoldExpr(
                arg, k_source, k_name, k_replacement, v_source, v_name, v_replacement, context, fold_cfg);
        }

        if (fn->isResolved() && fn->isOrdinaryCall())
        {
            if (has_lambda_arg)
            {
                if (auto folded_quantifier = tryFoldConstantArrayQuantifier(*fn, context, fold_cfg))
                    return folded_quantifier;
                return cloned;
            }

            reresolveAndCastFunctionArgsIfNeeded(*fn, orig_arg_types, context, fold_cfg);
            return tryFoldResolvedFunction(cloned, fold_cfg);
        }
    }

    return cloned;
}

QueryTreeNodePtr localFoldOnly(const QueryTreeNodePtr & expr, const ContextPtr & context, const FoldConfig & fold_cfg)
{
    return substituteAndFoldExpr(expr, nullptr, {}, nullptr, nullptr, {}, nullptr, context, fold_cfg);
}

std::optional<bool> tryCollapseCondition(const QueryTreeNodePtr & cond, const FoldConfig & fold_cfg)
{
    if (!fold_cfg.fold_constants || !cond)
        return std::nullopt;
    if (!isExprDeterministic(cond) && !fold_cfg.allow_nondet_folding)
        return std::nullopt;
    return tryExtractConstantFromConditionNode(cond);
}

DataTypePtr getMapValueTypeOrNull(const QueryTreeNodePtr & map_expr)
{
    if (!map_expr)
        return nullptr;
    auto type = map_expr->getResultType();
    if (!type || !isMap(type))
        return nullptr;
    return assert_cast<const DataTypeMap &>(*type).getValueType();
}

DataTypePtr getMapKeyTypeOrNull(const QueryTreeNodePtr & map_expr)
{
    if (!map_expr)
        return nullptr;
    auto type = map_expr->getResultType();
    if (!type || !isMap(type))
        return nullptr;
    return assert_cast<const DataTypeMap &>(*type).getKeyType();
}

QueryTreeNodePtr buildBaseMapAccess(
    const QueryTreeNodePtr & map_expr,
    const QueryTreeNodePtr & key_expr,
    MapAccessKind kind,
    const PushdownContext & ctx)
{
    auto map_key_type = getMapKeyTypeOrNull(map_expr);
    auto map_val_type = getMapValueTypeOrNull(map_expr);
    if (!map_key_type || !map_val_type || isNothingOrNullableNothing(map_key_type) || isNothingOrNullableNothing(map_val_type))
        return nullptr;

    QueryTreeNodePtr key_arg = convertConstKeyToTargetType(key_expr, map_key_type, ctx.query_context);
    if (!key_arg)
        return nullptr;

    const String fn_name = (kind == MapAccessKind::ArrayElement) ? "arrayElement" : "mapContainsKey";
    QueryTreeNodePtr res = createResolvedFunction(ctx.query_context, fn_name, {map_expr->clone(), std::move(key_arg)});
    if (map_expr->getNodeType() == QueryTreeNodeType::CONSTANT)
        res = tryFoldResolvedFunction(std::move(res), ctx.fold_cfg);
    return res;
}

QueryTreeNodePtr tryMatchPrefixExtract(
    const QueryTreeNodePtr & out_k,
    const IQueryTreeNode * k_source,
    const String & k_name,
    const QueryTreeNodePtr & inner_map,
    const QueryTreeNodePtr & key_expr,
    String & out_prefix)
{
    if (!out_k || !key_expr)
        return nullptr;

    /// FixedString prefix slicing pads trailing NUL bytes; restrict to variable-length String keys.
    if (!isString(removeLowCardinality(out_k->getResultType())) || !isString(removeLowCardinality(key_expr->getResultType())))
        return nullptr;

    const auto * sub_fn = out_k->as<FunctionNode>();
    if (!sub_fn || !sub_fn->isResolved() || !sub_fn->isOrdinaryCall())
        return nullptr;

    const String & sub_name = sub_fn->getFunctionName();
    if (sub_name != "substring" && sub_name != "substr" && sub_name != "substringUTF8")
        return nullptr;

    const auto & sub_args = sub_fn->getArguments().getNodes();
    if (sub_args.size() != 2 || !isLambdaParamNode(sub_args[0], k_source, k_name))
        return nullptr;
    if (!isString(removeLowCardinality(sub_args[0]->getResultType())))
        return nullptr;

    const auto * offset_const = sub_args[1]->as<ConstantNode>();
    if (!offset_const || offset_const->isNull())
        return nullptr;

    Int64 offset_val = 0;
    const auto & offset_field = offset_const->getValue();
    if (offset_field.getType() == Field::Types::UInt64)
        offset_val = static_cast<Int64>(offset_field.safeGet<UInt64>());
    else if (offset_field.getType() == Field::Types::Int64)
        offset_val = offset_field.safeGet<Int64>();
    else
        return nullptr;

    const auto * filter_fn = inner_map ? inner_map->as<FunctionNode>() : nullptr;
    if (!filter_fn || !filter_fn->isResolved() || !filter_fn->isOrdinaryCall() || filter_fn->getFunctionName() != "mapFilter")
        return nullptr;

    const auto & filter_args = filter_fn->getArguments().getNodes();
    if (filter_args.size() != 2)
        return nullptr;

    const auto * filter_lambda = filter_args[0]->as<LambdaNode>();
    if (!filter_lambda || filter_lambda->getArguments().getNames().size() != 2 || !filter_lambda->getExpression())
        return nullptr;

    const auto * sw_fn = filter_lambda->getExpression()->as<FunctionNode>();
    if (!sw_fn || !sw_fn->isResolved() || !sw_fn->isOrdinaryCall() || sw_fn->getFunctionName() != "startsWith")
        return nullptr;

    const auto & sw_args = sw_fn->getArguments().getNodes();
    if (sw_args.size() != 2)
        return nullptr;

    const auto * f_lambda_args = &filter_lambda->getArguments();
    const String & fk_name = filter_lambda->getArguments().getNames()[0];
    if (!isLambdaParamNode(sw_args[0], f_lambda_args, fk_name))
        return nullptr;
    if (!isString(removeLowCardinality(sw_args[0]->getResultType())))
        return nullptr;

    const auto * prefix_const = sw_args[1]->as<ConstantNode>();
    if (!prefix_const || prefix_const->isNull() || prefix_const->getValue().getType() != Field::Types::String)
        return nullptr;

    const String & prefix = prefix_const->getValue().safeGet<String>();
    if (prefix.empty() || offset_val != static_cast<Int64>(prefix.size() + 1))
        return nullptr;

    for (unsigned char ch : prefix)
    {
        if (ch >= 0x80)
            return nullptr;
    }

    out_prefix = prefix;
    return filter_args[1];
}

QueryTreeNodePtr tryPushMapAccess(
    const QueryTreeNodePtr & map_expr,
    const QueryTreeNodePtr & key_expr,
    MapAccessKind kind,
    PushdownContext & ctx,
    size_t depth)
{
    if (!map_expr || !key_expr || depth >= MAX_ALIAS_MAP_PUSHDOWN_DEPTH || ++ctx.steps > MAX_PUSHDOWN_STEPS)
        return nullptr;

    auto map_key_type = getMapKeyTypeOrNull(map_expr);
    auto value_type = getMapValueTypeOrNull(map_expr);
    if (!map_key_type || !value_type)
        return nullptr;

    if (isNothingOrNullableNothing(map_key_type) || isNothingOrNullableNothing(value_type))
    {
        ctx.applied_rule = true;
        return (kind == MapAccessKind::ContainsKey) ? makeConstUInt8(false) : nullptr;
    }

    /// Rule 1: Nested ALIAS ColumnNode on the same TableNode.
    if (const auto * col_node = map_expr->as<ColumnNode>())
    {
        if (col_node->hasExpression())
        {
            auto src = col_node->getColumnSourceOrNull();
            if (src && src.get() == ctx.root_table_node && !hasDisallowedRewriteBodyNodes(col_node->getExpression()))
            {
                ctx.applied_rule = true;
                return tryPushMapAccess(col_node->getExpression(), key_expr, kind, ctx, depth + 1);
            }
        }
        return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
    }

    const auto * fn = map_expr->as<FunctionNode>();
    if (!fn || !fn->isResolved() || !fn->isOrdinaryCall())
        return buildBaseMapAccess(map_expr, key_expr, kind, ctx);

    const String & fn_name = fn->getFunctionName();
    const auto & args = fn->getArguments().getNodes();

    /// Unwrap _CAST only for empty maps or when both key and value types match exactly.
    /// Otherwise casting `default(inner_V)` into `outer_V` can corrupt defaults or throw.
    if (fn_name == "_CAST" && args.size() == 2 && isMap(args[0]->getResultType()))
    {
        if (const auto * inner_fn = args[0]->as<FunctionNode>();
            inner_fn && inner_fn->isResolved() && inner_fn->isOrdinaryCall()
            && inner_fn->getFunctionName() == "map" && inner_fn->getArguments().getNodes().empty())
        {
            ctx.applied_rule = true;
            return (kind == MapAccessKind::ContainsKey) ? makeConstUInt8(false) : makeMapDefaultConstant(value_type);
        }

        if (const auto * inner_const = args[0]->as<ConstantNode>();
            inner_const && !inner_const->isNull() && inner_const->getValue().getType() == Field::Types::Map
            && inner_const->getValue().safeGet<Map>().empty())
        {
            ctx.applied_rule = true;
            return (kind == MapAccessKind::ContainsKey) ? makeConstUInt8(false) : makeMapDefaultConstant(value_type);
        }

        auto inner_key_type = getMapKeyTypeOrNull(args[0]);
        auto inner_val_type = getMapValueTypeOrNull(args[0]);
        if (!inner_key_type || !inner_val_type || !inner_key_type->equals(*map_key_type))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
        if (kind == MapAccessKind::ArrayElement && !inner_val_type->equals(*value_type))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);

        return tryPushMapAccess(args[0], key_expr, kind, ctx, depth + 1);
    }

    /// Rule 2: Literal `map(k1, v1, k2, v2, ...)`
    if (fn_name == "map" && (args.size() % 2 == 0))
    {
        ctx.applied_rule = true;
        if (args.empty())
            return (kind == MapAccessKind::ContainsKey) ? makeConstUInt8(false) : makeMapDefaultConstant(value_type);

        QueryTreeNodePtr key_arg = convertConstKeyToTargetType(key_expr, map_key_type, ctx.query_context);
        if (!key_arg)
            return nullptr;

        if (kind == MapAccessKind::ContainsKey)
        {
            QueryTreeNodes eq_nodes;
            eq_nodes.reserve(args.size() / 2);
            for (size_t i = 0; i < args.size(); i += 2)
            {
                if (!args[i]->getResultType()->equals(*map_key_type))
                    return nullptr;
                eq_nodes.push_back(createResolvedFunction(ctx.query_context, "equals", {key_arg->clone(), args[i]->clone()}));
            }

            QueryTreeNodePtr result = (eq_nodes.size() == 1)
                ? eq_nodes[0]
                : createResolvedFunction(ctx.query_context, "or", std::move(eq_nodes));
            return convertConditionToNonNullableUInt8(
                localFoldOnly(std::move(result), ctx.query_context, ctx.fold_cfg),
                ctx.query_context,
                ctx.fold_cfg);
        }

        QueryTreeNodes multi_if_args;
        multi_if_args.reserve(args.size() + 1);
        for (size_t i = 0; i < args.size(); i += 2)
        {
            if (!args[i]->getResultType()->equals(*map_key_type))
                return nullptr;
            QueryTreeNodePtr v_i = castResultIfNeeded(args[i + 1]->clone(), value_type, ctx.query_context, ctx.fold_cfg);
            multi_if_args.push_back(createResolvedFunction(ctx.query_context, "equals", {key_arg->clone(), args[i]->clone()}));
            multi_if_args.push_back(std::move(v_i));
        }
        multi_if_args.push_back(makeMapDefaultConstant(value_type));

        const String cond_fn = (multi_if_args.size() == 3) ? "if" : "multiIf";
        QueryTreeNodePtr result = createResolvedFunction(ctx.query_context, cond_fn, std::move(multi_if_args));
        return localFoldOnly(std::move(result), ctx.query_context, ctx.fold_cfg);
    }

    /// Rule 3: `mapFilter((k, v) -> pred, inner_map)`
    if (fn_name == "mapFilter" && args.size() == 2)
    {
        const auto * lambda = args[0]->as<LambdaNode>();
        if (lambda && lambda->getArguments().getNames().size() == 2 && lambda->getExpression())
        {
            const auto * lambda_args_ptr = &lambda->getArguments();
            const String & k_name = lambda->getArguments().getNames()[0];
            const String & v_name = lambda->getArguments().getNames()[1];
            const auto & pred = lambda->getExpression();
            const auto & inner_map = args[1];

            QueryTreeNodePtr key_arg = convertConstKeyToTargetType(key_expr, map_key_type, ctx.query_context);
            if (!key_arg)
                return nullptr;

            const bool uses_v = referencesLambdaParam(pred, lambda_args_ptr, v_name);
            if (uses_v && !ctx.short_circuit_enabled)
                return nullptr;

            ctx.applied_rule = true;

            if (!uses_v)
            {
                QueryTreeNodePtr pred_k = substituteAndFoldExpr(
                    pred, lambda_args_ptr, k_name, key_arg, nullptr, {}, nullptr, ctx.query_context, ctx.fold_cfg);
                pred_k = convertConditionToNonNullableUInt8(std::move(pred_k), ctx.query_context, ctx.fold_cfg);
                const bool pred_det = isExprDeterministic(pred_k);

                if (auto b = tryCollapseCondition(pred_k, ctx.fold_cfg))
                {
                    if (!*b)
                    {
                        if (kind == MapAccessKind::ContainsKey)
                            return makeConstUInt8(false, pred_det, pred_k);
                        return makeMapDefaultConstant(value_type, pred_det, pred_k);
                    }
                    return preserveNonDeterminismIfConstant(
                        tryPushMapAccess(inner_map, key_arg, kind, ctx, depth + 1), pred_det, pred_k);
                }

                QueryTreeNodePtr inner_res = tryPushMapAccess(inner_map, key_arg, kind, ctx, depth + 1);
                if (!inner_res)
                    return nullptr;

                if (kind == MapAccessKind::ContainsKey)
                    return localFoldOnly(
                        createResolvedFunction(ctx.query_context, "and", {pred_k, inner_res}),
                        ctx.query_context,
                        ctx.fold_cfg);

                inner_res = castResultIfNeeded(std::move(inner_res), value_type, ctx.query_context, ctx.fold_cfg);
                return localFoldOnly(
                    createResolvedFunction(ctx.query_context, "if", {pred_k, inner_res, makeMapDefaultConstant(value_type)}),
                    ctx.query_context,
                    ctx.fold_cfg);
            }

            QueryTreeNodePtr inner_has = tryPushMapAccess(inner_map, key_arg, MapAccessKind::ContainsKey, ctx, depth + 1);
            if (!inner_has)
                return nullptr;
            if (auto has_const = tryCollapseCondition(inner_has, ctx.fold_cfg); has_const && !*has_const)
            {
                const bool has_det = isExprDeterministic(inner_has);
                return (kind == MapAccessKind::ContainsKey)
                    ? makeConstUInt8(false, has_det, inner_has)
                    : makeMapDefaultConstant(value_type, has_det, inner_has);
            }

            QueryTreeNodePtr inner_val = tryPushMapAccess(inner_map, key_arg, MapAccessKind::ArrayElement, ctx, depth + 1);
            if (!inner_val)
                return nullptr;
            inner_val = castResultIfNeeded(std::move(inner_val), value_type, ctx.query_context, ctx.fold_cfg);

            QueryTreeNodePtr pred_kv = substituteAndFoldExpr(
                pred, lambda_args_ptr, k_name, key_arg, lambda_args_ptr, v_name, inner_val, ctx.query_context, ctx.fold_cfg);
            pred_kv = convertConditionToNonNullableUInt8(std::move(pred_kv), ctx.query_context, ctx.fold_cfg);

            QueryTreeNodePtr keep_cond = localFoldOnly(
                createResolvedFunction(ctx.query_context, "and", {inner_has, pred_kv}),
                ctx.query_context,
                ctx.fold_cfg);

            if (auto b = tryCollapseCondition(keep_cond, ctx.fold_cfg))
            {
                const bool cond_det = isExprDeterministic(keep_cond);
                if (!*b)
                    return (kind == MapAccessKind::ContainsKey)
                        ? makeConstUInt8(false, cond_det, keep_cond)
                        : makeMapDefaultConstant(value_type, cond_det, keep_cond);
                return (kind == MapAccessKind::ContainsKey)
                    ? makeConstUInt8(true, cond_det, keep_cond)
                    : preserveNonDeterminismIfConstant(std::move(inner_val), cond_det, keep_cond);
            }

            if (kind == MapAccessKind::ContainsKey)
                return keep_cond;

            return localFoldOnly(
                createResolvedFunction(ctx.query_context, "if", {keep_cond, inner_val, makeMapDefaultConstant(value_type)}),
                ctx.query_context,
                ctx.fold_cfg);
        }
    }

    /// Rule 4: `mapApply((k, v) -> (out_k, out_v), inner_map)`
    if (fn_name == "mapApply" && args.size() == 2)
    {
        const auto * lambda = args[0]->as<LambdaNode>();
        const auto * tuple_fn = (lambda && lambda->getArguments().getNames().size() == 2 && lambda->getExpression())
            ? lambda->getExpression()->as<FunctionNode>()
            : nullptr;

        if (tuple_fn && tuple_fn->isResolved() && tuple_fn->isOrdinaryCall()
            && tuple_fn->getFunctionName() == "tuple" && tuple_fn->getArguments().getNodes().size() == 2)
        {
            const auto * lambda_args_ptr = &lambda->getArguments();
            const String & k_name = lambda->getArguments().getNames()[0];
            const String & v_name = lambda->getArguments().getNames()[1];
            const auto & out_k = tuple_fn->getArguments().getNodes()[0];
            const auto & out_v = tuple_fn->getArguments().getNodes()[1];
            const auto & inner_map = args[1];

            QueryTreeNodePtr key_arg = convertConstKeyToTargetType(key_expr, map_key_type, ctx.query_context);
            if (!key_arg)
                return nullptr;

            QueryTreeNodePtr effective_map = inner_map;
            QueryTreeNodePtr effective_key = key_arg;
            bool matched_shape = false;

            if (isLambdaParamNode(out_k, lambda_args_ptr, k_name))
            {
                auto inner_key_type = getMapKeyTypeOrNull(inner_map);
                if (inner_key_type && inner_key_type->equals(*map_key_type))
                    matched_shape = true;
            }
            else
            {
                String prefix;
                if (QueryTreeNodePtr base_map = tryMatchPrefixExtract(out_k, lambda_args_ptr, k_name, inner_map, key_arg, prefix))
                {
                    if (!referencesLambdaParam(out_v, lambda_args_ptr, k_name))
                    {
                        auto prefix_node = std::make_shared<ConstantNode>(Field(prefix), std::make_shared<DataTypeString>());
                        effective_key = localFoldOnly(
                            createResolvedFunction(ctx.query_context, "concat", {std::move(prefix_node), key_arg->clone()}),
                            ctx.query_context,
                            ctx.fold_cfg);
                        effective_map = std::move(base_map);
                        matched_shape = true;
                    }
                }
            }

            if (matched_shape)
            {
                const bool uses_v = referencesLambdaParam(out_v, lambda_args_ptr, v_name);
                if (kind == MapAccessKind::ArrayElement && uses_v && !ctx.short_circuit_enabled)
                    return nullptr;

                ctx.applied_rule = true;
                if (kind == MapAccessKind::ContainsKey)
                    return tryPushMapAccess(effective_map, effective_key, MapAccessKind::ContainsKey, ctx, depth + 1);

                QueryTreeNodePtr out_v_k = substituteAndFoldExpr(
                    out_v, lambda_args_ptr, k_name, effective_key, nullptr, {}, nullptr, ctx.query_context, ctx.fold_cfg);

                if (isLambdaParamNode(out_v_k, lambda_args_ptr, v_name))
                {
                    auto eff_val_type = getMapValueTypeOrNull(effective_map);
                    if (!eff_val_type || !eff_val_type->equals(*value_type))
                        return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
                    return tryPushMapAccess(effective_map, effective_key, MapAccessKind::ArrayElement, ctx, depth + 1);
                }

                if (isDefaultConstantForType(out_v_k, value_type))
                    return out_v_k;

                QueryTreeNodePtr inner_has = tryPushMapAccess(effective_map, effective_key, MapAccessKind::ContainsKey, ctx, depth + 1);
                if (!inner_has)
                    return nullptr;
                if (auto has_const = tryCollapseCondition(inner_has, ctx.fold_cfg); has_const && !*has_const)
                    return makeMapDefaultConstant(value_type, isExprDeterministic(inner_has), inner_has);

                QueryTreeNodePtr out_v_final = out_v_k;
                if (referencesLambdaParam(out_v_k, lambda_args_ptr, v_name))
                {
                    QueryTreeNodePtr inner_val = tryPushMapAccess(
                        effective_map, effective_key, MapAccessKind::ArrayElement, ctx, depth + 1);
                    if (!inner_val)
                        return nullptr;
                    out_v_final = substituteAndFoldExpr(
                        out_v_k, nullptr, {}, nullptr, lambda_args_ptr, v_name, inner_val, ctx.query_context, ctx.fold_cfg);
                }

                out_v_final = castResultIfNeeded(std::move(out_v_final), value_type, ctx.query_context, ctx.fold_cfg);
                return localFoldOnly(
                    createResolvedFunction(ctx.query_context, "if", {inner_has, out_v_final, makeMapDefaultConstant(value_type)}),
                    ctx.query_context,
                    ctx.fold_cfg);
            }
        }
    }

    /// Rule 5: `mapUpdate(left_map, right_map)` -> right overrides left on matching key.
    if (fn_name == "mapUpdate" && args.size() == 2)
    {
        const auto & left_map = args[0];
        const auto & right_map = args[1];

        auto left_key_type = getMapKeyTypeOrNull(left_map);
        auto right_key_type = getMapKeyTypeOrNull(right_map);
        auto left_val_type = getMapValueTypeOrNull(left_map);
        auto right_val_type = getMapValueTypeOrNull(right_map);
        if (!left_key_type || !right_key_type || !left_key_type->equals(*map_key_type) || !right_key_type->equals(*map_key_type))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
        if (kind == MapAccessKind::ArrayElement
            && (!left_val_type || !right_val_type || !left_val_type->equals(*value_type) || !right_val_type->equals(*value_type)))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);

        ctx.applied_rule = true;
        QueryTreeNodePtr right_has = tryPushMapAccess(right_map, key_expr, MapAccessKind::ContainsKey, ctx, depth + 1);
        if (!right_has)
            return nullptr;
        auto right_has_const = tryCollapseCondition(right_has, ctx.fold_cfg);
        const bool right_has_det = isExprDeterministic(right_has);

        if (kind == MapAccessKind::ContainsKey)
        {
            if (right_has_const && *right_has_const)
                return makeConstUInt8(true, right_has_det, right_has);

            QueryTreeNodePtr left_has = tryPushMapAccess(left_map, key_expr, MapAccessKind::ContainsKey, ctx, depth + 1);
            if (!left_has)
                return nullptr;
            if (right_has_const && !*right_has_const)
                return preserveNonDeterminismIfConstant(std::move(left_has), right_has_det, right_has);
            if (auto left_has_const = tryCollapseCondition(left_has, ctx.fold_cfg); left_has_const && !*left_has_const)
                return preserveNonDeterminismIfConstant(std::move(right_has), isExprDeterministic(left_has), left_has);
            return localFoldOnly(createResolvedFunction(ctx.query_context, "or", {right_has, left_has}), ctx.query_context, ctx.fold_cfg);
        }

        if (right_has_const && *right_has_const)
            return preserveNonDeterminismIfConstant(
                tryPushMapAccess(right_map, key_expr, MapAccessKind::ArrayElement, ctx, depth + 1),
                right_has_det,
                right_has);
        if (right_has_const && !*right_has_const)
            return preserveNonDeterminismIfConstant(
                tryPushMapAccess(left_map, key_expr, MapAccessKind::ArrayElement, ctx, depth + 1),
                right_has_det,
                right_has);

        QueryTreeNodePtr left_val = tryPushMapAccess(left_map, key_expr, MapAccessKind::ArrayElement, ctx, depth + 1);
        QueryTreeNodePtr right_val = tryPushMapAccess(right_map, key_expr, MapAccessKind::ArrayElement, ctx, depth + 1);
        if (!left_val || !right_val)
            return nullptr;

        if (!value_type->isNullable() && !isNullableOrLowCardinalityNullable(value_type)
            && isDefaultConstantForType(left_val, value_type))
            return preserveNonDeterminismIfConstant(std::move(right_val), isExprDeterministic(left_val), left_val);

        return localFoldOnly(
            createResolvedFunction(ctx.query_context, "if", {right_has, right_val, left_val}),
            ctx.query_context,
            ctx.fold_cfg);
    }

    /// Rule 6: `if` / `multiIf` returning Map (require matching branch key/value types).
    if (fn_name == "if" && args.size() == 3)
    {
        auto true_k = getMapKeyTypeOrNull(args[1]);
        auto false_k = getMapKeyTypeOrNull(args[2]);
        auto true_v = getMapValueTypeOrNull(args[1]);
        auto false_v = getMapValueTypeOrNull(args[2]);
        if (!true_k || !false_k || !true_k->equals(*map_key_type) || !false_k->equals(*map_key_type))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
        if (kind == MapAccessKind::ArrayElement
            && (!true_v || !false_v || !true_v->equals(*value_type) || !false_v->equals(*value_type)))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);

        ctx.applied_rule = true;
        if (auto cond_const = tryCollapseCondition(args[0], ctx.fold_cfg))
            return preserveNonDeterminismIfConstant(
                tryPushMapAccess(*cond_const ? args[1] : args[2], key_expr, kind, ctx, depth + 1),
                isExprDeterministic(args[0]),
                args[0]);

        QueryTreeNodePtr true_res = tryPushMapAccess(args[1], key_expr, kind, ctx, depth + 1);
        QueryTreeNodePtr false_res = tryPushMapAccess(args[2], key_expr, kind, ctx, depth + 1);
        if (!true_res || !false_res)
            return nullptr;

        return localFoldOnly(
            createResolvedFunction(ctx.query_context, "if", {args[0]->clone(), true_res, false_res}),
            ctx.query_context,
            ctx.fold_cfg);
    }

    if (fn_name == "multiIf" && args.size() >= 3 && (args.size() % 2 == 1))
    {
        for (size_t i = 1; i < args.size(); i += (i + 1 < args.size() ? 2 : 1))
        {
            auto bk = getMapKeyTypeOrNull(args[i]);
            auto bv = getMapValueTypeOrNull(args[i]);
            if (!bk || !bk->equals(*map_key_type))
                return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
            if (kind == MapAccessKind::ArrayElement && (!bv || !bv->equals(*value_type)))
                return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
            if (i + 1 == args.size())
                break;
        }
        auto else_k = getMapKeyTypeOrNull(args.back());
        auto else_v = getMapValueTypeOrNull(args.back());
        if (!else_k || !else_k->equals(*map_key_type)
            || (kind == MapAccessKind::ArrayElement && (!else_v || !else_v->equals(*value_type))))
            return buildBaseMapAccess(map_expr, key_expr, kind, ctx);

        ctx.applied_rule = true;
        QueryTreeNodes new_args;
        new_args.reserve(args.size());
        bool all_conds_det = true;
        QueryTreeNodePtr non_det_src;

        for (size_t i = 0; i + 1 < args.size(); i += 2)
        {
            if (!isExprDeterministic(args[i]))
            {
                all_conds_det = false;
                if (!non_det_src)
                    non_det_src = args[i];
            }
            auto b = tryCollapseCondition(args[i], ctx.fold_cfg);
            if (b && !*b)
                continue;
            if (b && *b)
            {
                QueryTreeNodePtr branch_res = tryPushMapAccess(args[i + 1], key_expr, kind, ctx, depth + 1);
                if (!branch_res)
                    return nullptr;
                if (new_args.empty())
                    return preserveNonDeterminismIfConstant(std::move(branch_res), all_conds_det, non_det_src);
                new_args.push_back(std::move(branch_res));
                return preserveNonDeterminismIfConstant(
                    localFoldOnly(
                        createResolvedFunction(ctx.query_context, (new_args.size() == 3) ? "if" : "multiIf", std::move(new_args)),
                        ctx.query_context,
                        ctx.fold_cfg),
                    all_conds_det,
                    non_det_src);
            }
            QueryTreeNodePtr branch_res = tryPushMapAccess(args[i + 1], key_expr, kind, ctx, depth + 1);
            if (!branch_res)
                return nullptr;
            new_args.push_back(args[i]->clone());
            new_args.push_back(std::move(branch_res));
        }

        QueryTreeNodePtr else_res = tryPushMapAccess(args.back(), key_expr, kind, ctx, depth + 1);
        if (!else_res)
            return nullptr;
        if (new_args.empty())
            return preserveNonDeterminismIfConstant(std::move(else_res), all_conds_det, non_det_src);
        new_args.push_back(std::move(else_res));
        return preserveNonDeterminismIfConstant(
            localFoldOnly(
                createResolvedFunction(ctx.query_context, (new_args.size() == 3) ? "if" : "multiIf", std::move(new_args)),
                ctx.query_context,
                ctx.fold_cfg),
            all_conds_det,
            non_det_src);
    }

    return buildBaseMapAccess(map_expr, key_expr, kind, ctx);
}

bool isConstArrayElementFirstArg(const IQueryTreeNode * parent, const IQueryTreeNode * child)
{
    const auto * fn = parent ? parent->as<FunctionNode>() : nullptr;
    if (!fn || !fn->isResolved() || !fn->isOrdinaryCall() || fn->getFunctionName() != "arrayElement")
        return false;
    const auto & args = fn->getArguments().getNodes();
    if (args.size() != 2 || args[0].get() != child)
        return false;
    const auto * key_const = args[1]->as<ConstantNode>();
    return key_const && !key_const->isNull();
}

void collectGroupByColumnRefs(
    const QueryTreeNodePtr & node,
    const IQueryTreeNode * parent,
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> & out)
{
    if (!node || isQueryOrUnionNode(node))
        return;

    if (const auto * col_node = node->as<ColumnNode>())
    {
        if (isConstArrayElementFirstArg(parent, col_node))
            return;

        auto src = col_node->getColumnSourceOrNull();
        if (const auto * table_node = src ? src->as<TableNode>() : nullptr)
            out.insert(TableColKey{table_node, col_node->getColumnName()});
        if (col_node->hasExpression())
            collectGroupByColumnRefs(col_node->getExpression(), nullptr, out);
        return;
    }

    const IQueryTreeNode * next_parent = (node->getNodeType() == QueryTreeNodeType::LIST) ? parent : node.get();
    for (const auto & child : node->getChildren())
    {
        if (child)
            collectGroupByColumnRefs(child, next_parent, out);
    }
}

void collectAllTableColumnRefs(
    const QueryTreeNodePtr & node,
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> & out)
{
    if (!node || isQueryOrUnionNode(node))
        return;

    if (const auto * col_node = node->as<ColumnNode>())
    {
        auto src = col_node->getColumnSourceOrNull();
        if (const auto * table_node = src ? src->as<TableNode>() : nullptr)
            out.insert(TableColKey{table_node, col_node->getColumnName()});
        if (col_node->hasExpression())
            collectAllTableColumnRefs(col_node->getExpression(), out);
        return;
    }

    for (const auto & child : node->getChildren())
    {
        if (child)
            collectAllTableColumnRefs(child, out);
    }
}

class AliasPushdownPrecheckVisitor : public InDepthQueryTreeVisitorWithContext<AliasPushdownPrecheckVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<AliasPushdownPrecheckVisitor>;
    using Base::Base;

    UnorderedSetWithMemoryTracking<const TableNode *> skip_tables;
    UnorderedSetWithMemoryTracking<const QueryNode *> skip_query_nodes;
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> forbidden_cols;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        return parent->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * qn = node->as<QueryNode>())
        {
            join_depth_stack.push_back(0);
            if (getSettings()[Setting::group_by_use_nulls]
                && (qn->isGroupByWithRollup() || qn->isGroupByWithCube() || qn->isGroupByWithGroupingSets()))
                skip_query_nodes.insert(qn);

            for (const auto & gb_node : qn->getGroupBy().getNodes())
                collectGroupByColumnRefs(gb_node, nullptr, forbidden_cols);

            for (const auto & corr_node : qn->getCorrelatedColumns().getNodes())
                collectAllTableColumnRefs(corr_node, forbidden_cols);
        }
        else if (node->getNodeType() == QueryTreeNodeType::JOIN)
        {
            if (!join_depth_stack.empty())
                ++join_depth_stack.back();
        }
        else if (const auto * tn = node->as<TableNode>())
        {
            if (getSettings()[Setting::join_use_nulls] && !join_depth_stack.empty() && join_depth_stack.back() > 0)
                skip_tables.insert(tn);
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::JOIN)
        {
            if (!join_depth_stack.empty() && join_depth_stack.back() > 0)
                --join_depth_stack.back();
        }
        else if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            if (!join_depth_stack.empty())
                join_depth_stack.pop_back();
        }
    }

private:
    VectorWithMemoryTracking<size_t> join_depth_stack;
};

class AliasMapPushdownVisitor : public InDepthQueryTreeVisitorWithContext<AliasMapPushdownVisitor>
{
public:
    AliasMapPushdownVisitor(ContextPtr context_, AliasPushdownPrecheckVisitor precheck_)
        : InDepthQueryTreeVisitorWithContext(std::move(context_))
        , precheck(std::move(precheck_))
    {}

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        /// Keep table-function arguments untouched, same as other optimizer passes.
        return parent->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * qn = node->as<QueryNode>())
            query_node_stack.push_back(qn);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            if (!query_node_stack.empty())
                query_node_stack.pop_back();
            return;
        }

        const auto & settings = getSettings();
        if (!settings[Setting::optimize_map_alias_pushdown])
            return;

        if (!query_node_stack.empty() && precheck.skip_query_nodes.contains(query_node_stack.back()))
            return;

        auto * fn = node->as<FunctionNode>();
        if (!fn || !fn->isResolved() || !fn->isOrdinaryCall())
            return;

        std::optional<MapAccessKind> kind;
        const String & name = fn->getFunctionName();
        if (name == "arrayElement")
            kind = MapAccessKind::ArrayElement;
        else if (name == "has" || name == "mapContainsKey")
            kind = MapAccessKind::ContainsKey;
        else
            return;

        const auto & args = fn->getArguments().getNodes();
        if (args.size() != 2)
            return;

        const auto * alias_col = args[0]->as<ColumnNode>();
        if (!alias_col || !alias_col->hasExpression() || !isMap(alias_col->getColumnType()))
            return;

        auto column_source = alias_col->getColumnSourceOrNull();
        const auto * table_node = column_source ? column_source->as<TableNode>() : nullptr;
        if (!table_node || table_node->getStorage()->isRemote() || precheck.skip_tables.contains(table_node))
            return;

        if (isNullableOrLowCardinalityNullable(alias_col->getResultType()) || fn->isConvertedToNullable())
            return;

        /// Constant keys only: guarantees pushdown stays table-local, wraps cleanly for RBAC
        /// without tagging physical columns, and allows subcolumn / materialized-column passes.
        const auto * key_const = args[1]->as<ConstantNode>();
        if (!key_const
            || key_const->isNull()
            || isNullableOrLowCardinalityNullable(key_const->getResultType())
            || isNothingOrNullableNothing(key_const->getResultType()))
            return;

        if (precheck.forbidden_cols.contains(TableColKey{table_node, alias_col->getColumnName()}))
            return;

        UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> alias_deps;
        collectAllTableColumnRefs(alias_col->getExpression(), alias_deps);
        for (const auto & dep : alias_deps)
        {
            if (precheck.forbidden_cols.contains(dep))
                return;
        }

        if (hasDisallowedRewriteBodyNodes(alias_col->getExpression()))
            return;

        const String access_check_col = alias_col->getEffectiveAccessCheckColumnName();
        const FoldConfig fold_cfg{
            settings[Setting::enable_early_constant_folding],
            settings[Setting::enable_early_constant_folding] && !getContext()->isDistributed(),
        };
        const bool short_circuit_enabled
            = settings[Setting::short_circuit_function_evaluation] != ShortCircuitFunctionEvaluation::DISABLE;

        PushdownContext ctx{getContext(), table_node, fold_cfg, short_circuit_enabled, false, 0};
        QueryTreeNodePtr pushed_expr = tryPushMapAccess(alias_col->getExpression(), args[1], *kind, ctx, 0);
        if (!pushed_expr || !ctx.applied_rule)
            return;

        DataTypePtr original_result_type = fn->getResultType();
        if (isNothingOrNullableNothing(original_result_type))
            return;

        pushed_expr = castResultIfNeeded(std::move(pushed_expr), original_result_type, getContext(), fold_cfg);

        const auto expr_hash = pushed_expr->getTreeHash();
        const String synthetic_name = fmt::format("__alias_pushdown_{}_{:016x}_{:016x}", access_check_col, expr_hash.low64, expr_hash.high64);

        auto wrapped = std::make_shared<ColumnNode>(
            NameAndTypePair{synthetic_name, original_result_type},
            std::move(pushed_expr),
            column_source);
        wrapped->setPushdownMetadata(access_check_col, node->clone());
        if (fn->hasAlias())
            wrapped->setAlias(fn->getAlias());
        node = std::move(wrapped);
    }

private:
    AliasPushdownPrecheckVisitor precheck;
    VectorWithMemoryTracking<const QueryNode *> query_node_stack;
};

}

void AliasMapPushdownPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::optimize_map_alias_pushdown]
        || context->canUseParallelReplicasOnInitiator()
        || context->canUseOffsetParallelReplicas()
        || context->canUseParallelReplicasCustomKey())
        return;

    AliasPushdownPrecheckVisitor precheck(context);
    precheck.visit(query_tree_node);

    AliasMapPushdownVisitor visitor(std::move(context), std::move(precheck));
    visitor.visit(query_tree_node);
}

}
