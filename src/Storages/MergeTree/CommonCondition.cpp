#include <Parsers/ASTFunction.h>

#include "IO/ReadBuffer.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/IAST_fwd.h"

#include <Storages/MergeTree/CommonCondition.h>
#include <Storages/MergeTree/KeyCondition.h>

#include "Storages/SelectQueryInfo.h"
#include "base/logger_useful.h"

namespace DB::Condition::Common {

CommonCondition::CommonCondition(const SelectQueryInfo & query_info,
                                 ContextPtr context) {
    buildRPN(query_info, context);
    index_is_useful = matchAllRPNS();
}

bool CommonCondition::alwaysUnknownOrTrue() const {
    return !index_is_useful;
}

float CommonCondition::getComparisonDistance() const {
    return expression->distance;
}

std::vector<float> CommonCondition::getTargetVector() const {
    return expression->target;
}

String CommonCondition::getColumnName() const {
    return expression->column_name;
}

String CommonCondition::getMetric() const {
    return expression->metric_name;
}

size_t CommonCondition::getSpaceDim() const {
    return expression->target.size();
}

void CommonCondition::buildRPN(const SelectQueryInfo & query, ContextPtr context) {

    block_with_constants = KeyCondition::getBlockWithConstants(query.query, query.syntax_analyzer_result, context);

    const auto & select = query.query->as<ASTSelectQuery &>();

    if (select.prewhere()) {
        traverseAST(select.prewhere(), rpn_prewhere_clause);
    }

    if (select.where()) {
        traverseAST(select.where(), rpn_where_clause); 
    }

    std::reverse(rpn_prewhere_clause.begin(), rpn_prewhere_clause.end());
    std::reverse(rpn_where_clause.begin(), rpn_where_clause.end());
}

void CommonCondition::traverseAST(const ASTPtr & node, RPN & rpn) {
    if (const auto * func = node->as<ASTFunction>()) {
        const ASTs & args = func->arguments->children;

        for (const auto& arg : args) {
            traverseAST(arg, rpn);  
        }
    }

    RPNElement element;

    if(!traverseAtomAST(node, element)) {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element)); 
}

 bool CommonCondition::traverseAtomAST(const ASTPtr & node, RPNElement & out) {
     // Firstly check if we have constants behind the node
    {
        Field const_value;
        DataTypePtr const_type;


        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
        {
            /// Check constant type (use Float64 because all Fields implementation contains Float64 (for Float32 too))
            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = RPNElement::FUNCTION_FLOAT_LITERAL;
                out.float_literal.emplace(const_value.get<Float32>());
                out.func_name = "Float literal";

                return true;
            }
        }
    }

     if (const auto * function = node->as<ASTFunction>())
    {
        // Set the name
        out.func_name = function->name;

        // TODO: Add support for LpDistance
        if (function->name == "L1Distance" || 
            function->name == "L2Distance" ||
            function->name == "LinfDistance" || 
            function->name == "cosineDistance" ||
            function->name == "dotProduct") 
        {
            out.function = RPNElement::FUNCTION_DISTANCE;
        }
        else if (function->name == "tuple") 
        {
            out.function = RPNElement::FUNCTION_TUPLE;
        } 
        else if (function->name == "less" || 
                 function->name == "greater" ||
                 function->name == "lessOrEquals" ||
                 function->name == "greaterOrEquals") 
        {
            out.function = RPNElement::FUNCTION_COMPARISON;
        } 
        else 
        {
            return false;
        }

        return true;
    }
    // Match identifier 
    else if (const auto * identifier = node->as<ASTIdentifier>()) 
    {
        out.function = RPNElement::FUNCTION_IDENTIFIER;
        out.identifier.emplace(identifier->name());
        out.func_name = "column identifier";

        return true;
    } 

    return false;
 }

 namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

bool CommonCondition::matchAllRPNS() {
    ANNExpression expr_prewhere;
    ANNExpression expr_where;
    bool prewhere_is_valid = matchRPNWhere(rpn_prewhere_clause, expr_prewhere);
    bool where_is_valid = matchRPNWhere(rpn_where_clause, expr_where);

    // Unxpected situation
    if (prewhere_is_valid && where_is_valid) {
        throw Exception(
            "Expected to have only `where` or only `prewhere` querry part", DB::ErrorCodes::LOGICAL_ERROR);
    }

    expression = std::move(where_is_valid ? expr_where : expr_prewhere);
    return true;
 }

bool CommonCondition::matchRPNWhere(RPN & rpn, ANNExpression & expr) {
    const size_t minimal_elemets_count = 6;// At least 6 AST nodes in querry
    if (rpn.size() < minimal_elemets_count) {
        return false;
    }

    auto iter = rpn.begin();

    // Query starts from operator less
    if (iter->function != RPNElement::FUNCTION_COMPARISON) {
        return false;
    }

    ++iter;
    bool less_literal_found = false;

    // After less operator can be the second child - literal for comparison
    if (iter->function == RPNElement::FUNCTION_FLOAT_LITERAL) {
        expr.distance = getFloatLiteralOrPanic(iter);
        less_literal_found = true;
        ++iter;
    }

    // Expected Distance fucntion after less function
    if (iter->function != RPNElement::FUNCTION_DISTANCE) {
        return false;
    }
    expr.metric_name = iter->func_name;

    ++iter;
    bool identifier_found = false;

    // Can be identifier after DistanceFunction. Also it can be later
    if (iter->function == RPNElement::FUNCTION_IDENTIFIER) {
        expr.column_name = getIdentifierOrPanic(iter);
        identifier_found = true;
        ++iter;
    }

    // Expected tuple construction after DistanceFuction
    if (iter->function != RPNElement::FUNCTION_TUPLE) {
        return false;
    }

    ++iter;

    // Getting query's rest part, there should be floats and maybe identifier
    while(iter != rpn.end()) {
        if(iter->function == RPNElement::FUNCTION_FLOAT_LITERAL) 
        {
            expr.target.emplace_back(getFloatLiteralOrPanic(iter));
        } 
        else if(iter->function == RPNElement::FUNCTION_IDENTIFIER)
        {
            if(identifier_found) {
                return false;
            }
            expr.column_name = getIdentifierOrPanic(iter);
            identifier_found = true;
        } 
        else 
        {
            return false;
        }

        ++iter;
    }

    // Final checks of correctness
    if (!identifier_found || expr.target.empty()) {
        return false;
    }

    // If we hadn't found float literal for less operator, than
    // it was the last element in RPN
    if (!less_literal_found) {
        // In this case needs at least 2 float literals
        if (expr.target.size() >= 2) {
            expr.distance = expr.target.back();
            expr.target.pop_back();
            less_literal_found = true;
        } else {
            return false;
        }
    }

    // Querry is ok
    return true;
}

String CommonCondition::getIdentifierOrPanic(RPN::iterator& iter) {
    String identifier;
    try {
        identifier = std::move(iter->identifier.value());
    } catch(...) {
        CommonCondition::panicIfWrongBuiltRPN();
    }
    return identifier;
}

float CommonCondition::getFloatLiteralOrPanic(RPN::iterator& iter) {
    float literal = 0.0;
    try {
        literal = iter->float_literal.value();
    } catch(...) {
        CommonCondition::panicIfWrongBuiltRPN();
    }
    return literal;
}

void CommonCondition::panicIfWrongBuiltRPN() {
    throw Exception(
                "Wrong parsed AST in buildRPN\n", DB::ErrorCodes::LOGICAL_ERROR);
}

}
