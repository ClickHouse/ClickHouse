#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replace predicate having Date/DateTime converters with their preimages to improve performance.
 *  Given a Date column c, toYear(c) = 2023 -> c >= '2023-01-01' AND c < '2024-01-01'
 *  Or if c is a DateTime column, toYear(c) = 2023 -> c >= '2023-01-01 00:00:00' AND c < '2024-01-01 00:00:00'.
 *  The similar optimization also applies to other converters.
 */
class OptimizeDateOrDateTimeConverterWithPreimagePass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeDateOrDateTimeConverterWithPreimagePass"; }

    String getDescription() override { return "Replace predicate having Date/DateTime converters with their preimages"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
