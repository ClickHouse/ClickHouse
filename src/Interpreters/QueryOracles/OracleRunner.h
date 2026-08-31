#pragma once

#include <Interpreters/Context_fwd.h>

#include <string>

namespace DB
{

class OracleFixture;

/// The single mismatch-reporting path for all oracles. It:
///   * preserves the fixture (if one is in play) so the broken tables survive for triage;
///   * increments the ASTFuzzerOracleMismatches ProfileEvent;
///   * annotates the message with the active non-default settings (for reproduction);
///   * throws AST_FUZZER_ORACLE_MISMATCH.
/// `message` is the already-formatted oracle-specific text. This replaces the per-oracle inline
/// `throw Exception(AST_FUZZER_ORACLE_MISMATCH, ...)` plus the outer settings-annotation handler.
[[noreturn]] void raiseOracleMismatch(
    const std::string & message, const ContextPtr & context, OracleFixture * fixture = nullptr);

}
