#pragma once
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/**
 * Create a DiskPtr from disk AST function like disk(<disk_configuration>),
 * add it to DiskSelector by a unique (but always the same for given configuration) disk name
 * and return this name.
 */
std::string getOrCreateDiskFromDiskAST(const ASTPtr & disk_function, ContextPtr context, bool attach);

}
