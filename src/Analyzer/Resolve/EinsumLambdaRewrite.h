#pragma once

#include <Analyzer/FunctionNode.h>

#include <functional>

namespace DB
{

/** Rewrites the lambda form of `einsum` to the form with subscripts, before the arguments are resolved:
  *
  *   einsum((i, j, k) -> a[i][k] * b[k][j])  ->  einsum('ik,kj->ij', a, b)
  *   einsum((i) -> a[i][i])                  ->  einsum('ii->i', a)
  *   einsum((i, j) -> 0.299 * img[i, j, 1] + 0.587 * img[i, j, 2] + 0.114 * img[i, j, 3])
  *       ->  einsum('ij,ij,ij->ij', (x1, x2, x3) -> 0.299 * x1 + 0.587 * x2 + 0.114 * x3, img[][][1], img[][][2], img[][][3])
  *
  * The arguments of the lambda are the indices, and they can be used only as array subscripts.
  * Every chain of subscripts with indices is an element of a tensor: the array is the tensor,
  * and the subscripts that do not depend on the indices are applied to the array beforehand.
  * An index that occurs in two or more factors of a product is a summation index,
  * the other indices are the dimensions of the result in the order of the arguments of the lambda.
  * If the expression is a product of elements of tensors, it is calculated by `einsum` directly,
  * otherwise it becomes a lambda function that combines the elements.
  *
  * The arguments of `einsum` after the lambda are optional: each of them must be an array used in the expression.
  *
  * `is_scoped_function` returns the lambda function bound to a name of a function in the scope of the query, or nullptr,
  * like `WITH (x, y) -> x + y AS multiply`: then a call of `multiply` in the expression is not a product.
  * `get_called_lambda` returns the lambda function that a call of the name refers to: like `is_scoped_function`,
  * or a SQL user defined function. The names in such lambda functions are not used for the generated parameters,
  * because their expressions are resolved where they are called.
  */
using ScopedFunctionLookup = std::function<QueryTreeNodePtr(const String &)>;
QueryTreeNodePtr rewriteEinsumLambda(
    const FunctionNode & function_node, const ScopedFunctionLookup & is_scoped_function, const ScopedFunctionLookup & get_called_lambda);

}
