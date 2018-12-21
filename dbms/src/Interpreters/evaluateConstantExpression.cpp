#include <Interpreters/evaluateConstantExpression.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


std::pair<Field, std::shared_ptr<const IDataType>> evaluateConstantExpression(const ASTPtr & node, const Context & context)
{
    NamesAndTypesList source_columns = {{ "_dummy", std::make_shared<DataTypeUInt8>() }};
    auto ast = node->clone();
    auto syntax_result = SyntaxAnalyzer(context, {}).analyze(ast, source_columns);
    ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(ast, syntax_result, context).getConstActions();

    /// There must be at least one column in the block so that it knows the number of rows.
    Block block_with_constants{{ ColumnConst::create(ColumnUInt8::create(1, 0), 1), std::make_shared<DataTypeUInt8>(), "_dummy" }};

    expr_for_constant_folding->execute(block_with_constants);

    if (!block_with_constants || block_with_constants.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of constant expression for IN or VALUES", ErrorCodes::LOGICAL_ERROR);

    String name = node->getColumnName();

    if (!block_with_constants.has(name))
        throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

    const ColumnWithTypeAndName & result = block_with_constants.getByName(name);
    const IColumn & result_column = *result.column;

    if (!result_column.isColumnConst())
        throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

    return std::make_pair(result_column[0], result.type);
}


ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, const Context & context)
{
    /// Branch with string in query.
    if (typeid_cast<const ASTLiteral *>(node.get()))
        return node;

    /// Branch with TableFunction in query.
    if (auto table_func_ptr = typeid_cast<ASTFunction *>(node.get()))
        if (TableFunctionFactory::instance().isTableFunctionName(table_func_ptr->name))
            return node;

    return std::make_shared<ASTLiteral>(evaluateConstantExpression(node, context).first);
}

ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, const Context & context)
{
    if (auto id = typeid_cast<const ASTIdentifier *>(node.get()))
        return std::make_shared<ASTLiteral>(id->name);

    return evaluateConstantExpressionAsLiteral(node, context);
}

namespace
{
    using Conjunction = ColumnsWithTypeAndName;
    using Disjunction = std::vector<Conjunction>;

    Disjunction analyzeEquals(const ASTIdentifier * identifier, const ASTLiteral * literal, const ExpressionActionsPtr & expr)
    {
        if (!identifier || !literal)
        {
            return {};
        }

        for (const auto & name_and_type : expr->getRequiredColumnsWithTypes())
        {
            const auto & name = name_and_type.name;
            const auto & type = name_and_type.type;

            if (name == identifier->name)
            {
                ColumnWithTypeAndName column;
                // FIXME: what to do if field is not convertable?
                column.column = type->createColumnConst(1, convertFieldToType(literal->value, *type));
                column.name = name;
                column.type = type;
                return {{std::move(column)}};
            }
        }

        return {};
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

    Disjunction analyzeFunction(const ASTFunction * fn, const ExpressionActionsPtr & expr)
    {
        if (!fn)
        {
            return {};
        }

        // TODO: enumerate all possible function names!

        if (fn->name == "equals")
        {
            const auto * left = fn->arguments->children.front().get();
            const auto * right = fn->arguments->children.back().get();
            const auto * identifier = typeid_cast<const ASTIdentifier *>(left) ? typeid_cast<const ASTIdentifier *>(left)
                                                                               : typeid_cast<const ASTIdentifier *>(right);
            const auto * literal = typeid_cast<const ASTLiteral *>(left) ? typeid_cast<const ASTLiteral *>(left)
                                                                         : typeid_cast<const ASTLiteral *>(right);

            return analyzeEquals(identifier, literal, expr);
        }
        else if (fn->name == "in")
        {
            const auto * left = fn->arguments->children.front().get();
            const auto * right = fn->arguments->children.back().get();
            const auto * identifier = typeid_cast<const ASTIdentifier *>(left);
            const auto * inner_fn = typeid_cast<const ASTFunction *>(right);

            if (!inner_fn)
            {
                return {};
            }

            const auto * tuple = typeid_cast<const ASTExpressionList *>(inner_fn->children.front().get());

            if (!tuple)
            {
                return {};
            }

            Disjunction result;

            for (const auto & child : tuple->children)
            {
                const auto * literal = typeid_cast<const ASTLiteral *>(child.get());
                const auto dnf = analyzeEquals(identifier, literal, expr);

                if (dnf.empty())
                {
                    return {};
                }

                result.insert(result.end(), dnf.begin(), dnf.end());
            }

            return result;
        }
        else if (fn->name == "or")
        {
            const auto * args = typeid_cast<const ASTExpressionList *>(fn->children.front().get());

            if (!args)
            {
                return {};
            }

            Disjunction result;

            for (const auto & arg : args->children)
            {
                const auto dnf = analyzeFunction(typeid_cast<const ASTFunction *>(arg.get()), expr);

                if (dnf.empty())
                {
                    return {};
                }

                result.insert(result.end(), dnf.begin(), dnf.end());
            }

            return result;
        }
        else if (fn->name == "and")
        {
            const auto * args = typeid_cast<const ASTExpressionList *>(fn->children.front().get());

            if (!args)
            {
                return {};
            }

            Disjunction result;

            for (const auto & arg : args->children)
            {
                const auto dnf = analyzeFunction(typeid_cast<const ASTFunction *>(arg.get()), expr);

                if (dnf.empty())
                {
                    continue;
                }

                result = andDNF(result, dnf);
            }

            return result;
        }

        return {};
    }
}

std::optional<Blocks> evaluateExpressionOverConstantCondition(const ASTPtr & node, const ExpressionActionsPtr & target_expr)
{
    Blocks result;

    // TODO: `node` may be always-false literal.

    if (const auto fn = typeid_cast<const ASTFunction *>(node.get()))
    {
        const auto dnf = analyzeFunction(fn, target_expr);

        if (dnf.empty())
        {
            return {};
        }

        auto hasRequiredColumns = [&target_expr](const Block & block) -> bool
        {
            for (const auto & name : target_expr->getRequiredColumns())
            {
                bool hasColumn = false;
                for (const auto & column_name : block.getNames())
                {
                    if (column_name == name)
                    {
                        hasColumn = true;
                        break;
                    }
                }

                if (!hasColumn)
                    return false;
            }

            return true;
        };

        for (const auto & conjunct : dnf)
        {
            Block block(conjunct);

            // Block should contain all required columns from `target_expr`
            if (!hasRequiredColumns(block))
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

    return {result};
}

}
