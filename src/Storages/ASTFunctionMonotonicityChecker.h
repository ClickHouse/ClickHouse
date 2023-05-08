#pragma once

#include <Functions/IFunction.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Field.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

class ASTFunctionMonotonicityChecker
{
public:
    static IFunction::Monotonicity getMonotonicityInfo(const KeyDescription & key_description, const Range & range, ContextPtr context);

private:
    static std::vector<RPNBuilderFunctionTreeNode> buildFunctionList(const KeyDescription & key_description, DataTypePtr & key_expr_type, ContextPtr context);
};

}
