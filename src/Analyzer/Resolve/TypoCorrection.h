#pragma once

#include <base/types.h>
#include <unordered_set>
#include <vector>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class Identifier;
struct IdentifierResolveScope;
struct AnalysisTableExpressionData;

struct TypoCorrection
{
    static void collectCompoundExpressionValidIdentifiers(
        const Identifier & unresolved_identifier,
        const DataTypePtr & compound_expression_type,
        const Identifier & valid_identifier_prefix,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectTableExpressionValidIdentifiers(
        const Identifier & unresolved_identifier,
        const QueryTreeNodePtr & table_expression,
        const AnalysisTableExpressionData & table_expression_data,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeValidIdentifiers(
        const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeWithParentScopesValidIdentifiers(
        const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static std::vector<String> collectIdentifierTypoHints(
        const Identifier & unresolved_identifier,
        const std::unordered_set<Identifier> & valid_identifiers);
};

}
