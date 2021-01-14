#pragma once

#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/KeyCondition.h>


namespace DB
{
class Context;

/// Builds reverse polish notation
template <typename RPNElement>
class RPNBuilder
{
public:
    using RPN = std::vector<RPNElement>;
    using AtomFromASTFunc = std::function<
            bool(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out)>;

    RPNBuilder(const SelectQueryInfo & query_info, const Context & context_, const AtomFromASTFunc & atomFromAST_)
        : context(context_), atomFromAST(atomFromAST_)
    {
        /** Evaluation of expressions that depend only on constants.
          * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
          */
        block_with_constants = KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context);

        /// Transform WHERE section to Reverse Polish notation
        const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query_info.query);
        if (select.where())
        {
            traverseAST(select.where());

            if (select.prewhere())
            {
                traverseAST(select.prewhere());
                rpn.emplace_back(RPNElement::FUNCTION_AND);
            }
        }
        else if (select.prewhere())
        {
            traverseAST(select.prewhere());
        }
        else
        {
            rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        }
    }

    RPN && extractRPN() { return std::move(rpn); }

private:
    void traverseAST(const ASTPtr & node)
    {
        RPNElement element;

        if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
        {
            if (operatorFromAST(func, element))
            {
                auto & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;
                for (size_t i = 0, size = args.size(); i < size; ++i)
                {
                    traverseAST(args[i]);

                    /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                      * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                      */
                    if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
                        rpn.emplace_back(std::move(element));
                }

                return;
            }
        }

        if (!atomFromAST(node, context, block_with_constants, element))
        {
            element.function = RPNElement::FUNCTION_UNKNOWN;
        }

        rpn.emplace_back(std::move(element));
    }

    bool operatorFromAST(const ASTFunction * func, RPNElement & out)
    {
        /// Functions AND, OR, NOT.
        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (func->name == "not")
        {
            if (args.size() != 1)
                return false;

            out.function = RPNElement::FUNCTION_NOT;
        }
        else
        {
            if (func->name == "and")
                out.function = RPNElement::FUNCTION_AND;
            else if (func->name == "or")
                out.function = RPNElement::FUNCTION_OR;
            else
                return false;
        }

        return true;
    }

    const Context & context;
    const AtomFromASTFunc & atomFromAST;
    Block block_with_constants;
    RPN rpn;
};


};
