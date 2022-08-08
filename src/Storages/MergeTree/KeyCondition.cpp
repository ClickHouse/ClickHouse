#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/misc.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/indexHint.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Storages/KeyDescription.h>

#include <cassert>
#include <stack>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
}


String Range::toString() const
{
    WriteBufferFromOwnString str;

    str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";
    str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
}


/// Example: for `Hello\_World% ...` string it returns `Hello_World`, and for `%test%` returns an empty string.
String extractFixedPrefixFromLikePattern(const String & like_pattern)
{
    String fixed_prefix;

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
                [[fallthrough]];
            case '_':
                return fixed_prefix;

            case '\\':
                ++pos;
                if (pos == end)
                    break;
                [[fallthrough]];
            default:
                fixed_prefix += *pos;
                break;
        }

        ++pos;
    }

    return fixed_prefix;
}


/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
static String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is max (255), then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == std::numeric_limits<UInt8>::max())
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}

static void appendColumnNameWithoutAlias(const ActionsDAG::Node & node, WriteBuffer & out, bool legacy = false)
{
    switch (node.type)
    {
        case (ActionsDAG::ActionType::INPUT): [[fallthrough]];
        case (ActionsDAG::ActionType::COLUMN):
            writeString(node.result_name, out);
            break;
        case (ActionsDAG::ActionType::ALIAS):
            appendColumnNameWithoutAlias(*node.children.front(), out, legacy);
            break;
        case (ActionsDAG::ActionType::ARRAY_JOIN):
            writeCString("arrayJoin(", out);
            appendColumnNameWithoutAlias(*node.children.front(), out, legacy);
            writeChar(')', out);
            break;
        case (ActionsDAG::ActionType::FUNCTION):
        {
            auto name = node.function_base->getName();
            if (legacy && name == "modulo")
                writeCString("moduleLegacy", out);
            else
                writeString(name, out);

            writeChar('(', out);
            bool first = true;
            for (const auto * arg : node.children)
            {
                if (!first)
                    writeCString(", ", out);
                first = false;

                appendColumnNameWithoutAlias(*arg, out, legacy);
            }
            writeChar(')', out);
        }
    }
}

static std::string getColumnNameWithoutAlias(const ActionsDAG::Node & node, bool legacy = false)
{
    WriteBufferFromOwnString out;
    appendColumnNameWithoutAlias(node, out, legacy);
    return std::move(out.str());
}

class KeyCondition::Tree
{
public:
    explicit Tree(const IAST * ast_) : ast(ast_) { assert(ast); }
    explicit Tree(const ActionsDAG::Node * dag_) : dag(dag_) { assert(dag); }

    std::string getColumnName() const
    {
        if (ast)
            return ast->getColumnNameWithoutAlias();
        else
            return getColumnNameWithoutAlias(*dag);
    }

    std::string getColumnNameLegacy() const
    {
        if (ast)
        {
            auto adjusted_ast = ast->clone();
            KeyDescription::moduloToModuloLegacyRecursive(adjusted_ast);
            return adjusted_ast->getColumnNameWithoutAlias();
        }
        else
            return getColumnNameWithoutAlias(*dag, true);
    }

    bool isFunction() const
    {
        if (ast)
            return typeid_cast<const ASTFunction *>(ast);
        else
            return dag->type == ActionsDAG::ActionType::FUNCTION;
    }

    bool isConstant() const
    {
        if (ast)
            return typeid_cast<const ASTLiteral *>(ast);
        else
            return dag->column && isColumnConst(*dag->column);
    }

    ColumnWithTypeAndName getConstant() const
    {
        if (!isConstant())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyCondition::Tree node is not a constant");

        ColumnWithTypeAndName res;

        if (ast)
        {
            const auto * literal = assert_cast<const ASTLiteral *>(ast);
            res.type = applyVisitor(FieldToDataType(), literal->value);
            res.column = res.type->createColumnConst(0, literal->value);

        }
        else
        {
            res.type = dag->result_type;
            res.column = dag->column;
        }

        return res;
    }

    bool tryGetConstant(const Block & block_with_constants, Field & out_value, DataTypePtr & out_type) const
    {
        if (ast)
        {
            // Constant expr should use alias names if any
            String column_name = ast->getColumnName();

            if (const auto * lit = ast->as<ASTLiteral>())
            {
                /// By default block_with_constants has only one column named "_dummy".
                /// If block contains only constants it's may not be preprocessed by
                //  ExpressionAnalyzer, so try to look up in the default column.
                if (!block_with_constants.has(column_name))
                    column_name = "_dummy";

                /// Simple literal
                out_value = lit->value;
                out_type = block_with_constants.getByName(column_name).type;

                /// If constant is not Null, we can assume it's type is not Nullable as well.
                if (!out_value.isNull())
                    out_type = removeNullable(out_type);

                return true;
            }
            else if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
            {
                /// An expression which is dependent on constants only
                const auto & expr_info = block_with_constants.getByName(column_name);
                out_value = (*expr_info.column)[0];
                out_type = expr_info.type;

                if (!out_value.isNull())
                    out_type = removeNullable(out_type);

                return true;
            }
        }
        else
        {
            if (dag->column && isColumnConst(*dag->column))
            {
                out_value = (*dag->column)[0];
                out_type = dag->result_type;

                if (!out_value.isNull())
                    out_type = removeNullable(out_type);

                return true;
            }
        }

        return false;
    }

    ConstSetPtr tryGetPreparedSet(
        const PreparedSets & sets,
        const std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> & indexes_mapping,
        const DataTypes & data_types) const
    {
        if (ast)
        {
            if (ast->as<ASTSubquery>() || ast->as<ASTTableIdentifier>())
                return prepared_sets->get(PreparedSetKey::forSubquery(*set));

            /// We have `PreparedSetKey::forLiteral` but it is useless here as we don't have enough information
            /// about types in left argument of the IN operator. Instead, we manually iterate through all the sets
            /// and find the one for the right arg based on the AST structure (getTreeHash), after that we check
            /// that the types it was prepared with are compatible with the types of the primary key.
            auto types_match = [&indexes_mapping, &data_types](const SetPtr & candidate_set)
            {
                assert(indexes_mapping.size() == data_types.size());

                for (size_t i = 0; i < indexes_mapping.size(); ++i)
                    if (!candidate_set->areTypesEqual(indexes_mapping[i].tuple_index, data_types[i]))
                        return false;

                return true;
            };

            for (const auto & set : prepared_sets->getByTreeHash(right_arg->getTreeHash()))
            {
                if (types_match(set))
                    return set;
            }
        }
        else
        {
            if (dag->column)
            {
                const IColumn * col = dag->column.get();
                if (const auto * col_const = typeid_cast<const ColumnConst *>(col))
                    col = &col_const->getDataColumn();

                if (const auto * col_set = typeid_cast<const ColumnSet *>(col))
                {
                    auto set = col_set->getData();
                    if (set->isCreated())
                        return set;
                }
            }
        }

        return nullptr;
    }

    FunctionTree asFunction() const;

protected:
    const IAST * ast = nullptr;
    const ActionsDAG::Node * dag = nullptr;
};

class KeyCondition::FunctionTree : public KeyCondition::Tree
{
public:
    std::string getFunctionName() const
    {
        if (ast)
            return assert_cast<const ASTFunction *>(ast)->name;
        else
            return dag->function_base->getName();
    }

    size_t numArguments() const
    {
        if (ast)
        {
            const auto * func = assert_cast<const ASTFunction *>(ast);
            return func->arguments ? func->arguments->children.size() : 0;
        }
        else
            return dag->children.size();
    }

    Tree getArgumentAt(size_t idx) const
    {
        if (ast)
            return Tree(assert_cast<const ASTFunction *>(ast)->arguments->children[idx].get());
        else
            return Tree(dag->children[idx]);
    }

private:
    using Tree::Tree;

    friend class Tree;
};


KeyCondition::FunctionTree KeyCondition::Tree::asFunction() const
{
    if (!isFunction())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyCondition::Tree node is not a function");

    if (ast)
        return KeyCondition::FunctionTree(ast);
    else
        return KeyCondition::FunctionTree(dag);
}


/// A dictionary containing actions to the corresponding functions to turn them into `RPNElement`
const KeyCondition::AtomMap KeyCondition::atom_map
{
    {
        "notEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "equals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "less",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, false);
            return true;
        }
    },
    {
        "greater",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, false);
            return true;
        }
    },
    {
        "lessOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, true);
            return true;
        }
    },
    {
        "greaterOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, true);
            return true;
        }
    },
    {
        "in",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "notIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "globalIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "globalNotIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "nullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "notNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "globalNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "globalNotNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "empty",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range("");
            return true;
        }
    },
    {
        "notEmpty",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range("");
            return true;
        }
    },
    {
        "like",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>());
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "startsWith",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = value.get<const String &>();
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "isNotNull",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IS_NOT_NULL;
            // isNotNull means (-Inf, +Inf), which is the default Range
            out.range = Range();
            return true;
        }
    },
    {
        "isNull",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IS_NULL;
            // isNull means +Inf (NULLS_LAST) or -Inf (NULLS_FIRST),
            // which is equivalent to not in Range (-Inf, +Inf)
            out.range = Range();
            return true;
        }
    }
};


static const std::map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"notEquals", "equals"},
        {"less", "greaterOrEquals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"lessOrEquals", "greater"},
        {"in", "notIn"},
        {"notIn", "in"},
        {"globalIn", "globalNotIn"},
        {"globalNotIn", "globalIn"},
        {"nullIn", "notNullIn"},
        {"notNullIn", "nullIn"},
        {"globalNullIn", "globalNotNullIn"},
        {"globalNullNotIn", "globalNullIn"},
        {"isNull", "isNotNull"},
        {"isNotNull", "isNull"},
        {"like", "notLike"},
        {"notLike", "like"},
        {"empty", "notEmpty"},
        {"notEmpty", "empty"},
};


bool isLogicalOperator(const String & func_name)
{
    return (func_name == "and" || func_name == "or" || func_name == "not" || func_name == "indexHint");
}

/// The node can be one of:
///   - Logical operator (AND, OR, NOT and indexHint() - logical NOOP)
///   - An "atom" (relational operator, constant, expression)
///   - A logical constant expression
///   - Any other function
ASTPtr cloneASTWithInversionPushDown(const ASTPtr node, const bool need_inversion = false)
{
    const ASTFunction * func = node->as<ASTFunction>();

    if (func && isLogicalOperator(func->name))
    {
        if (func->name == "not")
        {
            return cloneASTWithInversionPushDown(func->arguments->children.front(), !need_inversion);
        }

        const auto result_node = makeASTFunction(func->name);

        /// indexHint() is a special case - logical NOOP function
        if (result_node->name != "indexHint" && need_inversion)
        {
            result_node->name = (result_node->name == "and") ? "or" : "and";
        }

        if (func->arguments)
        {
            for (const auto & child : func->arguments->children)
            {
                result_node->arguments->children.push_back(cloneASTWithInversionPushDown(child, need_inversion));
            }
        }

        return result_node;
    }

    auto cloned_node = node->clone();

    if (func && inverse_relations.find(func->name) != inverse_relations.cend())
    {
        if (need_inversion)
        {
            cloned_node->as<ASTFunction>()->name = inverse_relations.at(func->name);
        }

        return cloned_node;
    }

    return need_inversion ? makeASTFunction("not", cloned_node) : cloned_node;
}

static const ActionsDAG::Node & cloneASTWithInversionPushDown(
    const ActionsDAG::Node & node,
    ActionsDAG & inverted_dag,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> to_inverted,
    const ContextPtr & context,
    const bool need_inversion)
{
    {
        auto it = to_inverted.find(&node);
        if (it != to_inverted.end())
            return *it->second;
    }

    const ActionsDAG::Node * res = nullptr;

    switch (node.type)
    {
        case (ActionsDAG::ActionType::INPUT):
        {
            /// Note: inputs order is not important here. Will match columns by names.
            res = &inverted_dag.addInput({node.column, node.result_type, node.result_name});
            break;
        }
        case (ActionsDAG::ActionType::COLUMN):
        {
            res = &inverted_dag.addColumn({node.column, node.result_type, node.result_name});
            break;
        }
        case (ActionsDAG::ActionType::ALIAS):
        {
            /// Ignore aliases
            const auto & alias = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, need_inversion);
            to_inverted[&node] = &alias;
            return alias;
        }
        case (ActionsDAG::ActionType::ARRAY_JOIN):
        {
            const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, false);
            res = &inverted_dag.addArrayJoin(arg, {});
            break;
        }
        case (ActionsDAG::ActionType::FUNCTION):
        {
            auto name = node.function_base->getName();
            if (name == "not")
            {
                const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, !need_inversion);
                to_inverted[&node] = &arg;
                return arg;
            }

            if (name == "materialize")
            {
                /// Ignore materialize
                const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, need_inversion);
                to_inverted[&node] = &arg;
                return arg;
            }

            if (name == "indexHint")
            {
                ActionsDAG::NodeRawConstPtrs children;
                if (const auto * adaptor = typeid_cast<const FunctionToOverloadResolverAdaptor *>(node.function_builder.get()))
                {
                    if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction()))
                    {
                        const auto & index_hint_dag = index_hint->getActions();
                        children = index_hint_dag->getIndex();

                        for (auto & arg : children)
                            arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, need_inversion);
                    }
                }

                const auto & func = inverted_dag.addFunction(node.function_builder, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            if (need_inversion && (name == "and" || name == "or"))
            {
                ActionsDAG::NodeRawConstPtrs children(node.children);

                for (auto & arg : children)
                    arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, need_inversion);

                FunctionOverloadResolverPtr function_builder;

                if (name == "and")
                    function_builder = FunctionFactory::instance().get("or", context);
                else if (name == "or")
                    function_builder = FunctionFactory::instance().get("and", context);

                assert(function_builder);

                /// We match columns by name, so it is important to fill name correctly.
                /// So, use empty string to make it automatically.
                const auto & func = inverted_dag.addFunction(function_builder, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            ActionsDAG::NodeRawConstPtrs children(node.children);

            for (auto & arg : children)
                arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, false);

            auto it = inverse_relations.find(name);
            if (it != inverse_relations.end())
            {
                const auto & func_name = need_inversion ? it->second : it->first;
                auto function_builder = FunctionFactory::instance().get(func_name, context);
                const auto & func = inverted_dag.addFunction(function_builder, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            res = &inverted_dag.addFunction(node.function_builder, children, "");
        }
    }

    if (need_inversion)
        res = &inverted_dag.addFunction(FunctionFactory::instance().get("not", context), {res}, "");

    to_inverted[&node] = res;
    return *res;
}

static ActionsDAGPtr cloneASTWithInversionPushDown(ActionsDAG::NodeRawConstPtrs nodes, const ContextPtr & context)
{
    auto res = std::make_shared<ActionsDAG>();

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> to_inverted;

    for (auto & node : nodes)
        node = &cloneASTWithInversionPushDown(*node, *res, to_inverted, context, false);

    if (nodes.size() > 1)
    {
        auto function_builder = FunctionFactory::instance().get("and", context);
        nodes = {&res->addFunction(function_builder, std::move(nodes), "")};
    }

    res->getIndex().swap(nodes);

    return res;
}


inline bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }


/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block KeyCondition::getBlockWithConstants(
    const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, ContextPtr context)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    const auto expr_for_constant_folding = ExpressionAnalyzer(query, syntax_analyzer_result, context).getConstActions();

    expr_for_constant_folding->execute(result);

    return result;
}

static NameSet getAllSubexpressionNames(const ExpressionActions & key_expr)
{
    NameSet names;
    for (const auto & action : key_expr.getActions())
        names.insert(action.node->result_name);

    return names;
}

KeyCondition::KeyCondition(
    const ASTPtr & query,
    TreeRewriterResultPtr syntax_analyzer_result,
    PreparedSetsPtr prepared_sets_,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , prepared_sets(prepared_sets_)
    , single_point(single_point_)
    , strict(strict_)
{
    for (size_t i = 0, size = key_column_names.size(); i < size; ++i)
    {
        const auto & name = key_column_names[i];
        if (!key_columns.contains(name))
            key_columns[name] = i;
    }

    /** Evaluation of expressions that depend only on constants.
      * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
      */
    Block block_with_constants = getBlockWithConstants(query, syntax_analyzer_result, context);

    for (const auto & [name, _] : syntax_analyzer_result->array_join_result_to_source)
        array_joined_columns.insert(name);

    const ASTSelectQuery & select = query->as<ASTSelectQuery &>();
    if (select.where() || select.prewhere())
    {
        ASTPtr filter_query;
        if (select.where() && select.prewhere())
            filter_query = makeASTFunction("and", select.where(), select.prewhere());
        else
            filter_query = select.where() ? select.where() : select.prewhere();

        /** When non-strictly monotonic functions are employed in functional index (e.g. ORDER BY toStartOfHour(dateTime)),
          * the use of NOT operator in predicate will result in the indexing algorithm leave out some data.
          * This is caused by rewriting in KeyCondition::tryParseAtomFromAST of relational operators to less strict
          * when parsing the AST into internal RPN representation.
          * To overcome the problem, before parsing the AST we transform it to its semantically equivalent form where all NOT's
          * are pushed down and applied (when possible) to leaf nodes.
          */
        auto ast = cloneASTWithInversionPushDown(filter_query);
        traverseAST(Tree(ast.get()), context, block_with_constants);
    }
    else
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
}

KeyCondition::KeyCondition(
    ActionDAGNodes dag_nodes,
    TreeRewriterResultPtr syntax_analyzer_result,
    PreparedSetsPtr prepared_sets_,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , prepared_sets(prepared_sets_)
    , single_point(single_point_)
    , strict(strict_)
{
    for (size_t i = 0, size = key_column_names.size(); i < size; ++i)
    {
        const auto & name = key_column_names[i];
        if (!key_columns.contains(name))
            key_columns[name] = i;
    }

    for (const auto & [name, _] : syntax_analyzer_result->array_join_result_to_source)
        array_joined_columns.insert(name);

    if (!dag_nodes.nodes.empty())
    {
        auto inverted_dag = cloneASTWithInversionPushDown(std::move(dag_nodes.nodes), context);

        // std::cerr << "========== inverted dag: " << inverted_dag->dumpDAG() << std::endl;

        Block empty;
        for (const auto * node : inverted_dag->getIndex())
            traverseAST(Tree(node), context, empty);
    }
    else
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
}

bool KeyCondition::addCondition(const String & column, const Range & range)
{
    if (!key_columns.contains(column))
        return false;
    rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, key_columns[column], range);
    rpn.emplace_back(RPNElement::FUNCTION_AND);
    return true;
}

/** Computes value of constant expression and its data type.
  * Returns false, if expression isn't constant.
  */
bool KeyCondition::getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    return Tree(expr.get()).tryGetConstant(block_with_constants, out_value, out_type);
}


static Field applyFunctionForField(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnsWithTypeAndName columns
    {
        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
    };

    auto col = func->execute(columns, func->getResultType(), 1);
    return (*col)[0];
}

/// The case when arguments may have types different than in the primary key.
static std::pair<Field, DataTypePtr> applyFunctionForFieldOfUnknownType(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnsWithTypeAndName arguments{{ arg_type->createColumnConst(1, arg_value), arg_type, "x" }};
    DataTypePtr return_type = func->getResultType();

    auto col = func->execute(arguments, return_type, 1);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}


/// Same as above but for binary operators
static std::pair<Field, DataTypePtr> applyBinaryFunctionForFieldOfUnknownType(
    const FunctionOverloadResolverPtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value,
    const DataTypePtr & arg_type2,
    const Field & arg_value2)
{
    ColumnsWithTypeAndName arguments{
        {arg_type->createColumnConst(1, arg_value), arg_type, "x"}, {arg_type2->createColumnConst(1, arg_value2), arg_type2, "y"}};

    FunctionBasePtr func_base = func->build(arguments);

    DataTypePtr return_type = func_base->getResultType();

    auto col = func_base->execute(arguments, return_type, 1);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}


static FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
    {
        if ((*columns)[i].name == result_name)
            result_idx = i;
    }

    if (result_idx == columns->size())
    {
        ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
        field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

void KeyCondition::traverseAST(const Tree & node, ContextPtr context, Block & block_with_constants)
{
    RPNElement element;

    if (node.isFunction())
    {
        auto func = node.asFunction();
        if (tryParseLogicalOperatorFromAST(func, element))
        {
            size_t num_args = func.numArguments();
            for (size_t i = 0; i < num_args; ++i)
            {
                traverseAST(func.getArgumentAt(i), context, block_with_constants);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                  * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                  */
                if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
                    rpn.emplace_back(element);
            }

            return;
        }
    }

    if (!tryParseAtomFromAST(node, context, block_with_constants, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}

/** The key functional expression constraint may be inferred from a plain column in the expression.
  * For example, if the key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
  * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
  * condition also holds, so the index may be used to select only parts satisfying this condition.
  *
  * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
  * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
  * Instead, we can qualify only functions that do not transform the range (for example rounding),
  * which while not strictly monotonic, are monotonic everywhere on the input range.
  */
bool KeyCondition::transformConstantWithValidFunctions(
    const String & expr_name,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type,
    std::function<bool(IFunctionBase &, const IDataType &)> always_monotonic) const
{
    const auto & sample_block = key_expr->getSampleBlock();

    for (const auto & node : key_expr->getNodes())
    {
        auto it = key_columns.find(node.result_name);
        if (it != key_columns.end())
        {
            std::stack<const ActionsDAG::Node *> chain;

            const auto * cur_node = &node;
            bool is_valid_chain = true;

            while (is_valid_chain)
            {
                if (cur_node->result_name == expr_name)
                    break;

                chain.push(cur_node);

                if (cur_node->type == ActionsDAG::ActionType::FUNCTION && cur_node->children.size() <= 2)
                {
                    is_valid_chain = always_monotonic(*cur_node->function_base, *cur_node->result_type);

                    const ActionsDAG::Node * next_node = nullptr;
                    for (const auto * arg : cur_node->children)
                    {
                        if (arg->column && isColumnConst(*arg->column))
                            continue;

                        if (next_node)
                            is_valid_chain = false;

                        next_node = arg;
                    }

                    if (!next_node)
                        is_valid_chain = false;

                    cur_node = next_node;
                }
                else if (cur_node->type == ActionsDAG::ActionType::ALIAS)
                    cur_node = cur_node->children.front();
                else
                    is_valid_chain = false;
            }

            if (is_valid_chain)
            {
                auto const_type = cur_node->result_type;
                auto const_column = out_type->createColumnConst(1, out_value);
                auto const_value = (*castColumnAccurateOrNull({const_column, out_type, ""}, const_type))[0];

                if (const_value.isNull())
                    return false;

                while (!chain.empty())
                {
                    const auto * func = chain.top();
                    chain.pop();

                    if (func->type != ActionsDAG::ActionType::FUNCTION)
                        continue;

                    if (func->children.size() == 1)
                    {
                        std::tie(const_value, const_type)
                            = applyFunctionForFieldOfUnknownType(func->function_base, const_type, const_value);
                    }
                    else if (func->children.size() == 2)
                    {
                        const auto * left = func->children[0];
                        const auto * right = func->children[1];
                        if (left->column && isColumnConst(*left->column))
                        {
                            auto left_arg_type = left->result_type;
                            auto left_arg_value = (*left->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                func->function_builder, left_arg_type, left_arg_value, const_type, const_value);
                        }
                        else
                        {
                            auto right_arg_type = right->result_type;
                            auto right_arg_value = (*right->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                func->function_builder, const_type, const_value, right_arg_type, right_arg_value);
                        }
                    }
                }

                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                out_value = const_value;
                out_type = const_type;
                return true;
            }
        }
    }

    return false;
}

bool KeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const Tree & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_columns.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
        return false;

    if (out_value.isNull())
        return false;

    return transformConstantWithValidFunctions(
        expr_name, out_key_column_num, out_key_column_type, out_value, out_type, [](IFunctionBase & func, const IDataType & type)
        {
            if (!func.hasInformationAboutMonotonicity())
                return false;
            else
            {
                /// Range is irrelevant in this case.
                auto monotonicity = func.getMonotonicityForRange(type, Field(), Field());
                if (!monotonicity.is_always_monotonic)
                    return false;
            }
            return true;
        });
}

/// Looking for possible transformation of `column = constant` into `partition_expr = function(constant)`
bool KeyCondition::canConstantBeWrappedByFunctions(
    const Tree & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_columns.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
    {
        /// Let's check another one case.
        /// If our storage was created with moduloLegacy in partition key,
        /// We can assume that `modulo(...) = const` is the same as `moduloLegacy(...) = const`.
        /// Replace modulo to moduloLegacy in AST and check if we also have such a column.
        ///
        /// We do not check this in canConstantBeWrappedByMonotonicFunctions.
        /// The case `f(modulo(...))` for totally monotonic `f ` is considered to be rare.
        ///
        /// Note: for negative values, we can filter more partitions then needed.
        expr_name = node.getColumnNameLegacy();

        if (!key_subexpr_names.contains(expr_name))
            return false;
    }

    if (out_value.isNull())
        return false;

    return transformConstantWithValidFunctions(
        expr_name, out_key_column_num, out_key_column_type, out_value, out_type, [](IFunctionBase & func, const IDataType &)
        {
            return func.isDeterministic();
        });
}

bool KeyCondition::tryPrepareSetIndex(
    const FunctionTree & func,
    ContextPtr context,
    RPNElement & out,
    size_t & out_key_column_num)
{
    const auto & left_arg = func.getArgumentAt(0);

    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
    DataTypes data_types;

    auto get_key_tuple_position_mapping = [&](const Tree & node, size_t tuple_index)
    {
        MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
        index_mapping.tuple_index = tuple_index;
        DataTypePtr data_type;
        if (isKeyPossiblyWrappedByMonotonicFunctions(
                node, context, index_mapping.key_index, data_type, index_mapping.functions))
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            if (out_key_column_num < index_mapping.key_index)
                out_key_column_num = index_mapping.key_index;
        }
    };

    size_t left_args_count = 1;
    if (left_arg.isFunction())
    {
        /// Note: in case of ActionsDAG, tuple may be a constant.
        /// In this case, there is no keys in tuple. So, we don't have to check it.
        auto left_arg_tuple = left_arg.asFunction();
        if (left_arg_tuple.getFunctionName() == "tuple")
        {
            left_args_count = left_arg_tuple.numArguments();
            for (size_t i = 0; i < left_args_count; ++i)
                get_key_tuple_position_mapping(left_arg_tuple.getArgumentAt(i), i);
        }
        else
            get_key_tuple_position_mapping(left_arg, 0);
    }
    else
        get_key_tuple_position_mapping(left_arg, 0);

    if (indexes_mapping.empty())
        return false;

    const auto right_arg = func.getArgumentAt(1);

    auto prepared_set = right_arg.tryGetPreparedSet(prepared_sets, indexes_mapping, data_types);
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    prepared_set->checkColumnsNumber(left_args_count);
    for (size_t i = 0; i < indexes_mapping.size(); ++i)
        prepared_set->checkTypesEqual(indexes_mapping[i].tuple_index, data_types[i]);

    out.set_index = std::make_shared<MergeTreeSetIndex>(prepared_set->getSetElements(), std::move(indexes_mapping));

    return true;
}


/** Allow to use two argument function with constant argument to be analyzed as a single argument function.
  * In other words, it performs "currying" (binding of arguments).
  * This is needed, for example, to support correct analysis of `toDate(time, 'UTC')`.
  */
class FunctionWithOptionalConstArg : public IFunctionBase
{
public:
    enum Kind
    {
        NO_CONST = 0,
        LEFT_CONST,
        RIGHT_CONST,
    };

    explicit FunctionWithOptionalConstArg(const FunctionBasePtr & func_) : func(func_) {}
    FunctionWithOptionalConstArg(const FunctionBasePtr & func_, const ColumnWithTypeAndName & const_arg_, Kind kind_)
        : func(func_), const_arg(const_arg_), kind(kind_)
    {
    }

    String getName() const override { return func->getName(); }

    const DataTypes & getArgumentTypes() const override { return func->getArgumentTypes(); }

    const DataTypePtr & getResultType() const override { return func->getResultType(); }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const override { return func->prepare(arguments); }

    ColumnPtr
    execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const override
    {
        if (kind == Kind::LEFT_CONST)
        {
            ColumnsWithTypeAndName new_arguments;
            new_arguments.reserve(arguments.size() + 1);
            new_arguments.push_back(const_arg);
            for (const auto & arg : arguments)
                new_arguments.push_back(arg);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        else if (kind == Kind::RIGHT_CONST)
        {
            auto new_arguments = arguments;
            new_arguments.push_back(const_arg);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        else
            return func->prepare(arguments)->execute(arguments, result_type, input_rows_count, dry_run);
    }

    bool isDeterministic() const override { return func->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return func->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return func->hasInformationAboutMonotonicity(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override { return func->isSuitableForShortCircuitArgumentsExecution(arguments); }

    IFunctionBase::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return func->getMonotonicityForRange(type, left, right);
    }

    Kind getKind() const { return kind; }
    const ColumnWithTypeAndName & getConstArg() const { return const_arg; }

private:
    FunctionBasePtr func;
    ColumnWithTypeAndName const_arg;
    Kind kind = Kind::NO_CONST;
};


bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctions(
    const Tree & node,
    ContextPtr context,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<FunctionTree> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_key_column_num, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto function = *it;
        auto func_builder = FunctionFactory::instance().tryGet(function.getFunctionName(), context);
        if (!func_builder)
            return false;
        ColumnsWithTypeAndName arguments;
        ColumnWithTypeAndName const_arg;
        FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;
        if (function.numArguments() == 2)
        {
            if (function.getArgumentAt(0).isConstant())
            {
                const_arg = function.getArgumentAt(0).getConstant();
                arguments.push_back(const_arg);
                arguments.push_back({ nullptr, key_column_type, "" });
                kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
            }
            else if (function.getArgumentAt(1).isConstant())
            {
                arguments.push_back({ nullptr, key_column_type, "" });
                const_arg = function.getArgumentAt(1).getConstant();
                arguments.push_back(const_arg);
                kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
            }
        }
        else
            arguments.push_back({ nullptr, key_column_type, "" });
        auto func = func_builder->build(arguments);

        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        if (!func || (!single_point && !func->hasInformationAboutMonotonicity()))
            return false;

        key_column_type = func->getResultType();
        if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
            out_functions_chain.push_back(func);
        else
            out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(func, const_arg, kind));
    }

    out_key_res_column_type = key_column_type;

    return true;
}

bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const Tree & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    std::vector<FunctionTree> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();

    // Key columns should use canonical names for index analysis
    String name = node.getColumnName();

    if (array_joined_columns.contains(name))
        return false;

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(it->first).type;
        return true;
    }

    if (node.isFunction())
    {
        auto func = node.asFunction();

        size_t num_args = func.numArguments();
        if (num_args > 2 || num_args == 0)
            return false;

        out_functions_chain.push_back(func);
        bool ret = false;
        if (num_args == 2)
        {
            if (func.getArgumentAt(0).isConstant())
            {
                ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(func.getArgumentAt(1), out_key_column_num, out_key_column_type, out_functions_chain);
            }
            else if (func.getArgumentAt(1).isConstant())
            {
                ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(func.getArgumentAt(0), out_key_column_num, out_key_column_type, out_functions_chain);
            }
        }
        else
        {
            ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(func.getArgumentAt(0), out_key_column_num, out_key_column_type, out_functions_chain);
        }
        return ret;
    }

    return false;
}


static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const KeyCondition::Tree & node)
{
    try
    {
        src_value = convertFieldToType(src_value, *desired_type, src_type.get());
    }
    catch (...)
    {
        throw Exception("Key expression contains comparison between inconvertible types: " +
            desired_type->getName() + " and " + src_type->getName() +
            " inside " + node.getColumnName(),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


bool KeyCondition::tryParseAtomFromAST(const Tree & node, ContextPtr context, Block & block_with_constants, RPNElement & out)
{
    /** Functions < > = != <= >= in `notIn` isNull isNotNull, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  or constant expression - number.
      */
    Field const_value;
    DataTypePtr const_type;
    if (node.isFunction())
    {
        auto func = node.asFunction();
        size_t num_args = func.numArguments();

        DataTypePtr key_expr_type;    /// Type of expression containing key column
        size_t key_column_num = -1;   /// Number of a key column (inside key_column_names array)
        MonotonicFunctionsChain chain;
        std::string func_name = func.getFunctionName();

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (num_args == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(0), context, key_column_num, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        else if (num_args == 2)
        {
            size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            /// We don't look for inversed key transformations when strict is true, which is required for trivial count().
            /// Consider the following test case:
            ///
            /// create table test1(p DateTime, k int) engine MergeTree partition by toDate(p) order by k;
            /// insert into test1 values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);
            /// select count() from test1 where p > toDateTime('2020-09-01 10:00:00');
            ///
            /// toDate(DateTime) is always monotonic, but we cannot relax the predicates to be
            /// >= toDate(toDateTime('2020-09-01 10:00:00')), which returns 3 instead of the right count: 2.
            bool strict_condition = strict;

            /// If we use this key condition to prune partitions by single value, we cannot relax conditions for NOT.
            if (single_point
                && (func_name == "notLike" || func_name == "notIn" || func_name == "globalNotIn" || func_name == "notNullIn"
                    || func_name == "globalNotNullIn" || func_name == "notEquals" || func_name == "notEmpty"))
                strict_condition = true;

            if (functionIsInOrGlobalInOperator(func_name))
            {
                if (tryPrepareSetIndex(func, context, out, key_column_num))
                {
                    key_arg_pos = 0;
                    is_set_const = true;
                }
                else
                    return false;
            }
            else if (func.getArgumentAt(1).tryGetConstant(block_with_constants, const_value, const_type))
            {
                if (isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(0), context, key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 0;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else if (func.getArgumentAt(0).tryGetConstant(block_with_constants, const_value, const_type))
            {
                if (isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(1), context, key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 1;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 1;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);

            /// Replace <const> <sign> <data> on to <data> <-sign> <const>
            if (key_arg_pos == 1)
            {
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
                else if (func_name == "in" || func_name == "notIn" ||
                         func_name == "like" || func_name == "notLike" ||
                         func_name == "ilike" || func_name == "notIlike" ||
                         func_name == "startsWith")
                {
                    /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                    return false;
                }
            }

            key_expr_type = recursiveRemoveLowCardinality(key_expr_type);
            DataTypePtr key_expr_type_not_null;
            bool key_expr_type_is_nullable = false;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(key_expr_type.get()))
            {
                key_expr_type_is_nullable = true;
                key_expr_type_not_null = nullable_type->getNestedType();
            }
            else
                key_expr_type_not_null = key_expr_type;

            bool cast_not_needed = is_set_const /// Set args are already casted inside Set::createFromAST
                || ((isNativeInteger(key_expr_type_not_null) || isDateTime(key_expr_type_not_null))
                    && (isNativeInteger(const_type) || isDateTime(const_type))); /// Native integers and DateTime are accurately compared without cast.

            if (!cast_not_needed && !key_expr_type_not_null->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    const_value = convertFieldToType(const_value, *key_expr_type_not_null);
                    if (const_value.isNull())
                        return false;
                    // No need to set is_constant_transformed because we're doing exact conversion
                }
                else
                {
                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{key_expr_type_not_null, const_type});
                    if (!common_type)
                        return false;

                    if (!const_type->equals(*common_type))
                    {
                        castValueToType(common_type, const_value, const_type, node);

                        // Need to set is_constant_transformed unless we're doing exact conversion
                        if (!key_expr_type_not_null->equals(*common_type))
                            is_constant_transformed = true;
                    }
                    if (!key_expr_type_not_null->equals(*common_type))
                    {
                        auto common_type_maybe_nullable = (key_expr_type_is_nullable && !common_type->isNullable())
                            ? DataTypePtr(std::make_shared<DataTypeNullable>(common_type))
                            : common_type;
                        ColumnsWithTypeAndName arguments{
                            {nullptr, key_expr_type, ""},
                            {DataTypeString().createColumnConst(1, common_type_maybe_nullable->getName()), common_type_maybe_nullable, ""}};
                        FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();
                        auto func_cast = func_builder_cast->build(arguments);

                        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
                        if (!single_point && !func_cast->hasInformationAboutMonotonicity())
                            return false;
                        chain.push_back(func_cast);
                    }
                }
            }

            /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
            if (is_constant_transformed)
            {
                if (func_name == "less")
                    func_name = "lessOrEquals";
                else if (func_name == "greater")
                    func_name = "greaterOrEquals";
            }

        }
        else
            return false;

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        return atom_it->second(out, const_value);
    }
    else if (node.tryGetConstant(block_with_constants, const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}

bool KeyCondition::tryParseLogicalOperatorFromAST(const FunctionTree & func, RPNElement & out)
{
    /// Functions AND, OR, NOT.
    /// Also a special function `indexHint` - works as if instead of calling a function there are just parentheses
    /// (or, the same thing - calling the function `and` from one argument).

    if (func.getFunctionName() == "not")
    {
        if (func.numArguments() != 1)
            return false;

        out.function = RPNElement::FUNCTION_NOT;
    }
    else
    {
        if (func.getFunctionName() == "and" || func.getFunctionName() == "indexHint")
            out.function = RPNElement::FUNCTION_AND;
        else if (func.getFunctionName() == "or")
            out.function = RPNElement::FUNCTION_OR;
        else
            return false;
    }

    return true;
}

String KeyCondition::toString() const
{
    String res;
    for (size_t i = 0; i < rpn.size(); ++i)
    {
        if (i)
            res += ", ";
        res += rpn[i].toString();
    }
    return res;
}

KeyCondition::Description KeyCondition::getDescription() const
{
    /// This code may seem to be too difficult.
    /// Here we want to convert RPN back to tree, and also simplify some logical expressions like `and(x, true) -> x`.
    Description description;

    /// That's a binary tree. Explicit.
    /// Build and optimize it simultaneously.
    struct Node
    {
        enum class Type
        {
            /// Leaf, which is RPNElement.
            Leaf,
            /// Leafs, which are logical constants.
            True,
            False,
            /// Binary operators.
            And,
            Or,
        };

        Type type{};

        /// Only for Leaf
        const RPNElement * element = nullptr;
        /// This means that logical NOT is applied to leaf.
        bool negate = false;

        std::unique_ptr<Node> left = nullptr;
        std::unique_ptr<Node> right = nullptr;
    };

    /// The algorithm is the same as in KeyCondition::checkInHyperrectangle
    /// We build a pair of trees on stack. For checking if key condition may be true, and if it may be false.
    /// We need only `can_be_true` in result.
    struct Frame
    {
        std::unique_ptr<Node> can_be_true;
        std::unique_ptr<Node> can_be_false;
    };

    /// Combine two subtrees using logical operator.
    auto combine = [](std::unique_ptr<Node> left, std::unique_ptr<Node> right, Node::Type type)
    {
        /// Simplify operators with for one constant condition.

        if (type == Node::Type::And)
        {
            /// false AND right
            if (left->type == Node::Type::False)
                return left;

            /// left AND false
            if (right->type == Node::Type::False)
                return right;

            /// true AND right
            if (left->type == Node::Type::True)
                return right;

            /// left AND true
            if (right->type == Node::Type::True)
                return left;
        }

        if (type == Node::Type::Or)
        {
            /// false OR right
            if (left->type == Node::Type::False)
                return right;

            /// left OR false
            if (right->type == Node::Type::False)
                return left;

            /// true OR right
            if (left->type == Node::Type::True)
                return left;

            /// left OR true
            if (right->type == Node::Type::True)
                return right;
        }

        return std::make_unique<Node>(Node{
                .type = type,
                .left = std::move(left),
                .right = std::move(right)
            });
    };

    std::vector<Frame> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::True});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::True});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (
               element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::Leaf, .element = &element, .negate = false});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::Leaf, .element = &element, .negate = true});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            std::swap(rpn_stack.back().can_be_true, rpn_stack.back().can_be_false);
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());
            auto arg1 = std::move(rpn_stack.back());

            rpn_stack.pop_back();

            assert(!rpn_stack.empty());
            auto arg2 = std::move(rpn_stack.back());

            Frame frame;
            frame.can_be_true = combine(std::move(arg1.can_be_true), std::move(arg2.can_be_true), Node::Type::And);
            frame.can_be_false = combine(std::move(arg1.can_be_false), std::move(arg2.can_be_false), Node::Type::Or);

            rpn_stack.back() = std::move(frame);
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());
            auto arg1 = std::move(rpn_stack.back());

            rpn_stack.pop_back();

            assert(!rpn_stack.empty());
            auto arg2 = std::move(rpn_stack.back());

            Frame frame;
            frame.can_be_true = combine(std::move(arg1.can_be_true), std::move(arg2.can_be_true), Node::Type::Or);
            frame.can_be_false = combine(std::move(arg1.can_be_false), std::move(arg2.can_be_false), Node::Type::And);

            rpn_stack.back() = std::move(frame);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::False});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::True});

            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::True});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::False});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::checkInRange", ErrorCodes::LOGICAL_ERROR);

    std::vector<std::string_view> key_names(key_columns.size());
    std::vector<bool> is_key_used(key_columns.size(), false);

    for (const auto & key : key_columns)
        key_names[key.second] = key.first;

    WriteBufferFromOwnString buf;

    std::function<void(const Node *)> describe;
    describe = [&describe, &key_names, &is_key_used, &buf](const Node * node)
    {
        switch (node->type)
        {
            case Node::Type::Leaf:
            {
                is_key_used[node->element->key_column] = true;

                /// Note: for condition with double negation, like `not(x not in set)`,
                /// we can replace it to `x in set` here.
                /// But I won't do it, because `cloneASTWithInversionPushDown` already push down `not`.
                /// So, this seem to be impossible for `can_be_true` tree.
                if (node->negate)
                    buf << "not(";
                buf << node->element->toString(key_names[node->element->key_column], true);
                if (node->negate)
                    buf << ")";
                break;
            }
            case Node::Type::True:
                buf << "true";
                break;
            case Node::Type::False:
                buf << "false";
                break;
            case Node::Type::And:
                buf << "and(";
                describe(node->left.get());
                buf << ", ";
                describe(node->right.get());
                buf << ")";
                break;
            case Node::Type::Or:
                buf << "or(";
                describe(node->left.get());
                buf << ", ";
                describe(node->right.get());
                buf << ")";
                break;
        }
    };

    describe(rpn_stack.front().can_be_true.get());
    description.condition = std::move(buf.str());

    for (size_t i = 0; i < key_names.size(); ++i)
        if (is_key_used[i])
            description.used_keys.emplace_back(key_names[i]);

    return description;
}

/** Index is the value of key every `index_granularity` rows.
  * This value is called a "mark". That is, the index consists of marks.
  *
  * The key is the tuple.
  * The data is sorted by key in the sense of lexicographic order over tuples.
  *
  * A pair of marks specifies a segment with respect to the order over the tuples.
  * Denote it like this: [ x1 y1 z1 .. x2 y2 z2 ],
  *  where x1 y1 z1 - tuple - value of key in left border of segment;
  *        x2 y2 z2 - tuple - value of key in right boundary of segment.
  * In this section there are data between these marks.
  *
  * Or, the last mark specifies the range open on the right: [ a b c .. + inf )
  *
  * The set of all possible tuples can be considered as an n-dimensional space, where n is the size of the tuple.
  * A range of tuples specifies some subset of this space.
  *
  * Hyperrectangles will be the subrange of an n-dimensional space that is a direct product of one-dimensional ranges.
  * In this case, the one-dimensional range can be:
  * a point, a segment, an open interval, a half-open interval;
  * unlimited on the left, unlimited on the right ...
  *
  * The range of tuples can always be represented as a combination (union) of hyperrectangles.
  * For example, the range [ x1 y1 .. x2 y2 ] given x1 != x2 is equal to the union of the following three hyperrectangles:
  * [x1]       x [y1 .. +inf)
  * (x1 .. x2) x (-inf .. +inf)
  * [x2]       x (-inf .. y2]
  *
  * Or, for example, the range [ x1 y1 .. +inf ] is equal to the union of the following two hyperrectangles:
  * [x1]         x [y1 .. +inf)
  * (x1 .. +inf) x (-inf .. +inf)
  * It's easy to see that this is a special case of the variant above.
  *
  * This is important because it is easy for us to check the feasibility of the condition over the hyperrectangle,
  *  and therefore, feasibility of condition on the range of tuples will be checked by feasibility of condition
  *  over at least one hyperrectangle from which this range consists.
  */

template <typename F>
static BoolMask forAnyHyperrectangle(
    size_t key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    bool left_bounded,
    bool right_bounded,
    std::vector<Range> & hyperrectangle,
    size_t prefix_size,
    BoolMask initial_mask,
    F && callback)
{
    if (!left_bounded && !right_bounded)
        return callback(hyperrectangle);

    if (left_bounded && right_bounded)
    {
        /// Let's go through the matching elements of the key.
        while (prefix_size < key_size)
        {
            if (left_keys[prefix_size] == right_keys[prefix_size])
            {
                /// Point ranges.
                hyperrectangle[prefix_size] = Range(left_keys[prefix_size]);
                ++prefix_size;
            }
            else
                break;
        }
    }

    if (prefix_size == key_size)
        return callback(hyperrectangle);

    if (prefix_size + 1 == key_size)
    {
        if (left_bounded && right_bounded)
            hyperrectangle[prefix_size] = Range(left_keys[prefix_size], true, right_keys[prefix_size], true);
        else if (left_bounded)
            hyperrectangle[prefix_size] = Range::createLeftBounded(left_keys[prefix_size], true);
        else if (right_bounded)
            hyperrectangle[prefix_size] = Range::createRightBounded(right_keys[prefix_size], true);

        return callback(hyperrectangle);
    }

    /// (x1 .. x2) x (-inf .. +inf)

    if (left_bounded && right_bounded)
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size], false, right_keys[prefix_size], false);
    else if (left_bounded)
        hyperrectangle[prefix_size] = Range::createLeftBounded(left_keys[prefix_size], false);
    else if (right_bounded)
        hyperrectangle[prefix_size] = Range::createRightBounded(right_keys[prefix_size], false);

    for (size_t i = prefix_size + 1; i < key_size; ++i)
        hyperrectangle[i] = Range();


    BoolMask result = initial_mask;
    result = result | callback(hyperrectangle);

    /// There are several early-exit conditions (like the one below) hereinafter.
    /// They are important; in particular, if initial_mask == BoolMask::consider_only_can_be_true
    /// (which happens when this routine is called from KeyCondition::mayBeTrueXXX),
    /// they provide significant speedup, which may be observed on merge_tree_huge_pk performance test.
    if (result.isComplete())
        return result;

    /// [x1]       x [y1 .. +inf)

    if (left_bounded)
    {
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size]);
        result = result | forAnyHyperrectangle(key_size, left_keys, right_keys, true, false, hyperrectangle, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
    }

    /// [x2]       x (-inf .. y2]

    if (right_bounded)
    {
        hyperrectangle[prefix_size] = Range(right_keys[prefix_size]);
        result = result | forAnyHyperrectangle(key_size, left_keys, right_keys, false, true, hyperrectangle, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
    }

    return result;
}


BoolMask KeyCondition::checkInRange(
    size_t used_key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    const DataTypes & data_types,
    BoolMask initial_mask) const
{
    std::vector<Range> key_ranges(used_key_size, Range());

    // std::cerr << "Checking for: [";
    // for (size_t i = 0; i != used_key_size; ++i)
    //     std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), left_keys[i]);
    // std::cerr << " ... ";

    // for (size_t i = 0; i != used_key_size; ++i)
    //     std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), right_keys[i]);
    // std::cerr << "]\n";

    return forAnyHyperrectangle(used_key_size, left_keys, right_keys, true, true, key_ranges, 0, initial_mask,
        [&] (const std::vector<Range> & key_ranges_hyperrectangle)
    {
        auto res = checkInHyperrectangle(key_ranges_hyperrectangle, data_types);

        // std::cerr << "Hyperrectangle: ";
        // for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
        //     std::cerr << (i != 0 ? " x " : "") << key_ranges[i].toString();
        // std::cerr << ": " << res.can_be_true << "\n";

        return res;
    });
}

std::optional<Range> KeyCondition::applyMonotonicFunctionsChainToRange(
    Range key_range,
    const MonotonicFunctionsChain & functions,
    DataTypePtr current_type,
    bool single_point)
{
    for (const auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        IFunction::Monotonicity monotonicity = single_point
            ? IFunction::Monotonicity{true}
            : func->getMonotonicityForRange(*current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// If we apply function to open interval, we can get empty intervals in result.
        /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
        /// To avoid this we make range left and right included.
        /// Any function that treats NULL specially is not monotonic.
        /// Thus we can safely use isNull() as an -Inf/+Inf indicator here.
        if (!key_range.left.isNull())
        {
            key_range.left = applyFunction(func, current_type, key_range.left);
            key_range.left_included = true;
        }

        if (!key_range.right.isNull())
        {
            key_range.right = applyFunction(func, current_type, key_range.right);
            key_range.right_included = true;
        }

        current_type = func->getResultType();

        if (!monotonicity.is_positive)
            key_range.invert();
    }
    return key_range;
}

// Returns whether the condition is one continuous range of the primary key,
// where every field is matched by range or a single element set.
// This allows to use a more efficient lookup with no extra reads.
bool KeyCondition::matchesExactContinuousRange() const
{
    // Not implemented yet.
    if (hasMonotonicFunctionsChain())
        return false;

    enum Constraint
    {
        POINT,
        RANGE,
        UNKNOWN,
    };

    std::vector<Constraint> column_constraints(key_columns.size(), Constraint::UNKNOWN);

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::Function::FUNCTION_AND)
        {
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_IN_SET && element.set_index && element.set_index->size() == 1)
        {
            column_constraints[element.key_column] = Constraint::POINT;
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_IN_RANGE)
        {
            if (element.range.left == element.range.right)
            {
                column_constraints[element.key_column] = Constraint::POINT;
            }
            if (column_constraints[element.key_column] != Constraint::POINT)
            {
                column_constraints[element.key_column] = Constraint::RANGE;
            }
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_UNKNOWN)
        {
            continue;
        }

        return false;
    }

    auto min_constraint = column_constraints[0];

    if (min_constraint > Constraint::RANGE)
    {
        return false;
    }

    for (size_t i = 1; i < key_columns.size(); ++i)
    {
        if (column_constraints[i] < min_constraint)
        {
            return false;
        }

        if (column_constraints[i] == Constraint::RANGE && min_constraint == Constraint::RANGE)
        {
            return false;
        }

        min_constraint = column_constraints[i];
    }

    return true;
}

BoolMask KeyCondition::checkInHyperrectangle(
    const std::vector<Range> & hyperrectangle,
    const DataTypes & data_types) const
{
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            const Range * key_range = &hyperrectangle[element.key_column];

            /// The case when the column is wrapped in a chain of possibly monotonic functions.
            Range transformed_range;
            if (!element.monotonic_functions_chain.empty())
            {
                std::optional<Range> new_range = applyMonotonicFunctionsChainToRange(
                    *key_range,
                    element.monotonic_functions_chain,
                    data_types[element.key_column],
                    single_point
                );

                if (!new_range)
                {
                    rpn_stack.emplace_back(true, true);
                    continue;
                }
                transformed_range = *new_range;
                key_range = &transformed_range;
            }

            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL)
        {
            const Range * key_range = &hyperrectangle[element.key_column];

            /// No need to apply monotonic functions as nulls are kept.
            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == RPNElement::FUNCTION_IS_NULL)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (!element.set_index)
                throw Exception("Set for IN is not created yet", ErrorCodes::LOGICAL_ERROR);

            rpn_stack.emplace_back(element.set_index->checkInRange(hyperrectangle, data_types, single_point));
            if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::checkInRange", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0];
}


bool KeyCondition::mayBeTrueInRange(
    size_t used_key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    const DataTypes & data_types) const
{
    return checkInRange(used_key_size, left_keys, right_keys, data_types, BoolMask::consider_only_can_be_true).can_be_true;
}

String KeyCondition::RPNElement::toString() const { return toString("column " + std::to_string(key_column), false); }
String KeyCondition::RPNElement::toString(std::string_view column_name, bool print_constants) const
{
    auto print_wrapped_column = [this, &column_name, print_constants](WriteBuffer & buf)
    {
        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
        {
            buf << (*it)->getName() << "(";
            if (print_constants)
            {
                if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
                {
                    if (func->getKind() == FunctionWithOptionalConstArg::Kind::LEFT_CONST)
                        buf << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]) << ", ";
                }
            }
        }

        buf << column_name;

        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
        {
            if (print_constants)
            {
                if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
                {
                    if (func->getKind() == FunctionWithOptionalConstArg::Kind::RIGHT_CONST)
                        buf << ", " << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]);
                }
            }
            buf << ")";
        }
    };

    WriteBufferFromOwnString buf;
    switch (function)
    {
        case FUNCTION_AND:
            return "and";
        case FUNCTION_OR:
            return "or";
        case FUNCTION_NOT:
            return "not";
        case FUNCTION_UNKNOWN:
            return "unknown";
        case FUNCTION_NOT_IN_SET:
        case FUNCTION_IN_SET:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_IN_SET ? " in " : " notIn ");
            if (!set_index)
                buf << "unknown size set";
            else
                buf << set_index->size() << "-element set";
            buf << ")";
            return buf.str();
        }
        case FUNCTION_IN_RANGE:
        case FUNCTION_NOT_IN_RANGE:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString();
            buf << ")";
            return buf.str();
        }
        case FUNCTION_IS_NULL:
        case FUNCTION_IS_NOT_NULL:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_IS_NULL ? " isNull" : " isNotNull");
            buf << ")";
            return buf.str();
        }
        case ALWAYS_FALSE:
            return "false";
        case ALWAYS_TRUE:
            return "true";
    }

    __builtin_unreachable();
}


bool KeyCondition::alwaysUnknownOrTrue() const
{
    return unknownOrAlwaysTrue(false);
}
bool KeyCondition::anyUnknownOrAlwaysTrue() const
{
    return unknownOrAlwaysTrue(true);
}
bool KeyCondition::unknownOrAlwaysTrue(bool unknown_any) const
{
    std::vector<UInt8> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            /// If unknown_any is true, return instantly,
            /// to avoid processing it with FUNCTION_AND, and change the outcome.
            if (unknown_any)
                return true;
            /// Otherwise, it may be AND'ed via FUNCTION_AND
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::unknownOrAlwaysTrue", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0];
}


size_t KeyCondition::getMaxKeyColumn() const
{
    size_t res = 0;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (element.key_column > res)
                res = element.key_column;
        }
    }
    return res;
}

bool KeyCondition::hasMonotonicFunctionsChain() const
{
    for (const auto & element : rpn)
        if (!element.monotonic_functions_chain.empty()
            || (element.set_index && element.set_index->hasMonotonicFunctionsChain()))
            return true;
    return false;
}

}
