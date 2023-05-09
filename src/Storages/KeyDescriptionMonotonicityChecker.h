#pragma once

#include <Functions/IFunction.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Field.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

/*
 * Code adapted from Storages/MergeTree/KeyCondition.
 *
 * Algorithm outline:
 * 1. Navigate through partition key AST and extract RPN functions as well as the partition key column
 * 2. Loops through RPN functions and builds a list of Functions/IFunction
 * 3. Loops through IFunction list, grabs monotonicity info on column Range and computes intermediate Range for next iterations.
 * */
class KeyDescriptionMonotonicityChecker
{
public:
    static IFunction::Monotonicity getMonotonicityInfo(const KeyDescription & key_description, const Range & range, ContextPtr context);

private:
    static std::vector<RPNBuilderFunctionTreeNode> buildRPNFunctionList(
        const KeyDescription & key_description,
        DataTypePtr & key_expr_type,
        RPNBuilderTreeContext & rpn_context
    );

    static std::vector<FunctionBasePtr> buildFunctionList(
        const std::vector<RPNBuilderFunctionTreeNode> & rpn_function_list,
        DataTypePtr key_column_type,
        ContextPtr context
    );
};

}
