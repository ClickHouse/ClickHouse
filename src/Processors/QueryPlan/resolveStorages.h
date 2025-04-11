#pragma once

#include <Interpreters/Context_fwd.h>
namespace DB
{

class Identifier;
class TableNode;

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context);
Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context);

}
