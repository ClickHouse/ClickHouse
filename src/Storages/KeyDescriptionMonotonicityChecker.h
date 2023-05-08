#pragma once

#include <Functions/IFunction.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Field.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

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
