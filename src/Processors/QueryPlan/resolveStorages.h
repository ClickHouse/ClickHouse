#pragma once

#include <Analyzer/TableExpressionModifiers.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

class Identifier;
class TableNode;

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const std::optional<TableExpressionModifiers> & table_expression_modifiers, const ContextPtr & context);
Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context);

}
