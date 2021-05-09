#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Aliases.h>

#include <z3++.h>

namespace DB
{

class TreeSMTSolver
{
public:
    enum class STRICTNESS
    {
        REAL_LINEAR,  /// all types are uninterpreted or real, can use only linear arithmetic (+,-,*const)
        MIXED_LINEAR,  /// all types are uninterpreted or real or int, can use only linear arithmetic (+,-,*/const)
        POLYNOMIAL,  /// all types are uninterpreted or real or int, can use only arithmetic (+,-,*,/)
        FULL,   /// support all (real, int, strings)
    };

    TreeSMTSolver(
        STRICTNESS strictness,
        const NamesAndTypesList & source_columns);

    void addConstraint(const ASTPtr & ast);

    //TODO: add timeout
    /// New identifiers will be added to context
    bool alwaysTrue(const ASTPtr & ast);
    bool alwaysFalse(const ASTPtr & ast);

    /// Find min/max for expression.
    /// Primary key columns prefered.
    ASTPtr minimize(const ASTPtr & ast);
    ASTPtr maximize(const ASTPtr & ast);
private:
    const z3::expr & getOrCreateColumn(const String & name);
    const z3::expr & getColumn(const String & name) const;

    z3::expr transformToLogicExpression(const ASTPtr & ast);
    std::optional<z3::expr> transformToLogicExpressionImpl(const ASTPtr & ast);

    STRICTNESS strictness;
    std::unordered_map<String, z3::expr> name_to_column;
    std::unique_ptr<z3::context> context;
    std::vector<z3::expr> constraints;

    // TODO: std::unique_ptr<z3::solver> solver; + check assumptions
};

}
