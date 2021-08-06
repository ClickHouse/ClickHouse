#include <Interpreters/evaluateConstantExpression.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/typeid_cast.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


std::pair<Field, std::shared_ptr<const IDataType>> evaluateConstantExpression(const ASTPtr & node, ContextPtr context)
{
    NamesAndTypesList source_columns = {{ "_dummy", std::make_shared<DataTypeUInt8>() }};
    auto ast = node->clone();
    ReplaceQueryParameterVisitor param_visitor(context->getQueryParameters());
    param_visitor.visit(ast);

    if (context->getSettingsRef().normalize_function_names)
        FunctionNameNormalizer().visit(ast.get());

    String name = ast->getColumnName(context->getSettingsRef());
    auto syntax_result = TreeRewriter(context).analyze(ast, source_columns);
    ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(ast, syntax_result, context).getConstActions();

    /// There must be at least one column in the block so that it knows the number of rows.
    Block block_with_constants{{ ColumnConst::create(ColumnUInt8::create(1, 0), 1), std::make_shared<DataTypeUInt8>(), "_dummy" }};

    expr_for_constant_folding->execute(block_with_constants);

    if (!block_with_constants || block_with_constants.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of constant expression for IN, VALUES or LIMIT or aggregate function parameter",
                        ErrorCodes::LOGICAL_ERROR);

    if (!block_with_constants.has(name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Element of set in IN, VALUES or LIMIT or aggregate function parameter is not a constant expression (result column not found): {}", name);

    const ColumnWithTypeAndName & result = block_with_constants.getByName(name);
    const IColumn & result_column = *result.column;

    /// Expressions like rand() or now() are not constant
    if (!isColumnConst(result_column))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Element of set in IN, VALUES or LIMIT or aggregate function parameter is not a constant expression (result column is not const): {}", name);

    return std::make_pair(result_column[0], result.type);
}


ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, ContextPtr context)
{
    /// If it's already a literal.
    if (node->as<ASTLiteral>())
        return node;
    return std::make_shared<ASTLiteral>(evaluateConstantExpression(node, context).first);
}

ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, ContextPtr context)
{
    if (const auto * id = node->as<ASTIdentifier>())
        return std::make_shared<ASTLiteral>(id->name());

    return evaluateConstantExpressionAsLiteral(node, context);
}

ASTPtr evaluateConstantExpressionForDatabaseName(const ASTPtr & node, ContextPtr context)
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

std::tuple<bool, ASTPtr> evaluateDatabaseNameForMergeEngine(const ASTPtr & node, ContextPtr context)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "REGEXP")
    {
        if (func->arguments->children.size() != 1)
            throw Exception("Arguments for REGEXP in Merge ENGINE should be 1", ErrorCodes::BAD_ARGUMENTS);

        auto * literal = func->arguments->children[0]->as<ASTLiteral>();
        if (!literal || literal->value.safeGet<String>().empty())
            throw Exception("Argument for REGEXP in Merge ENGINE should be a non empty String Literal", ErrorCodes::BAD_ARGUMENTS);

        return std::tuple{true, func->arguments->children[0]};
    }

    auto ast = evaluateConstantExpressionForDatabaseName(node, context);
    return std::tuple{false, ast};
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

            auto add_dnf = [&](const auto &dnf)
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
                    const auto & tuple = tuple_literal->value.get<const Tuple &>();
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
            Block block(conjunct);

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
        if (literal->value.getType() == Field::Types::UInt64 && literal->value.get<UInt64>() == 0)
            return {result};
        else
            return {};
    }

    return {result};
}

}
