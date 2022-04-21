#pragma once

#include <memory>

namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class BackupRenamingConfig;
using BackupRenamingConfigPtr = std::shared_ptr<const BackupRenamingConfig>;

/// Changes names in AST according to the renaming settings.
ASTPtr renameInCreateQuery(const ASTPtr & ast, const BackupRenamingConfigPtr & renaming_config, const ContextPtr & context);
}
