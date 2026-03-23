#include <Parsers/ASTFromJSON.h>
#include <Parsers/IAST.h>

/// Include ALL AST types for the factory.
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCollation.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifierTypePair.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTWithElement.h>

#include <Common/Exception.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <unordered_map>
#include <functional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using ASTCreator = std::function<ASTPtr()>;

/// Registry: maps JSON type name -> factory function that creates an empty AST node.
/// After creation, readJSON is called on the node to populate it.
const std::unordered_map<String, ASTCreator> & getASTFactory()
{
    static const std::unordered_map<String, ASTCreator> factory =
    {
        /// Expression types
        {"ExpressionList", [] { return make_intrusive<ASTExpressionList>(); }},
        {"Literal", [] { return make_intrusive<ASTLiteral>(Field{}); }},
        {"Identifier", [] { return make_intrusive<ASTIdentifier>(""); }},
        {"TableIdentifier", [] { return make_intrusive<ASTTableIdentifier>(""); }},
        {"Function", [] { return make_intrusive<ASTFunction>(); }},
        {"Subquery", [] { return make_intrusive<ASTSubquery>(); }},
        {"QueryParameter", [] { return make_intrusive<ASTQueryParameter>("", ""); }},
        {"Asterisk", [] { return make_intrusive<ASTAsterisk>(); }},
        {"QualifiedAsterisk", [] { return make_intrusive<ASTQualifiedAsterisk>(); }},

        /// Query types
        {"SelectQuery", [] { return make_intrusive<ASTSelectQuery>(); }},
        {"SelectWithUnionQuery", [] { return make_intrusive<ASTSelectWithUnionQuery>(); }},
        {"SelectIntersectExceptQuery", [] { return make_intrusive<ASTSelectIntersectExceptQuery>(); }},
        {"ProjectionSelectQuery", [] { return make_intrusive<ASTProjectionSelectQuery>(); }},
        {"SetQuery", [] { return make_intrusive<ASTSetQuery>(); }},
        {"ExplainQuery", [] { return make_intrusive<ASTExplainQuery>(ASTExplainQuery::ParsedAST); }},
        {"WithElement", [] { return make_intrusive<ASTWithElement>(); }},

        /// Table/Join types
        {"TablesInSelectQuery", [] { return make_intrusive<ASTTablesInSelectQuery>(); }},
        {"TablesInSelectQueryElement", [] { return make_intrusive<ASTTablesInSelectQueryElement>(); }},
        {"TableExpression", [] { return make_intrusive<ASTTableExpression>(); }},
        {"TableJoin", [] { return make_intrusive<ASTTableJoin>(); }},
        {"ArrayJoin", [] { return make_intrusive<ASTArrayJoin>(); }},
        {"OrderByElement", [] { return make_intrusive<ASTOrderByElement>(); }},
        {"StorageOrderByElement", [] { return make_intrusive<ASTStorageOrderByElement>(); }},
        {"WindowDefinition", [] { return make_intrusive<ASTWindowDefinition>(); }},
        {"WindowListElement", [] { return make_intrusive<ASTWindowListElement>(); }},
        {"SampleRatio", [] { return make_intrusive<ASTSampleRatio>(); }},
        {"InterpolateElement", [] { return make_intrusive<ASTInterpolateElement>(); }},
        {"Collation", [] { return make_intrusive<ASTCollation>(); }},

        /// Column matcher/transformer types
        {"ColumnsRegexpMatcher", [] { return make_intrusive<ASTColumnsRegexpMatcher>(); }},
        {"ColumnsListMatcher", [] { return make_intrusive<ASTColumnsListMatcher>(); }},
        {"QualifiedColumnsRegexpMatcher", [] { return make_intrusive<ASTQualifiedColumnsRegexpMatcher>(); }},
        {"QualifiedColumnsListMatcher", [] { return make_intrusive<ASTQualifiedColumnsListMatcher>(); }},
        {"ColumnsTransformerList", [] { return make_intrusive<ASTColumnsTransformerList>(); }},
        {"ColumnsApplyTransformer", [] { return make_intrusive<ASTColumnsApplyTransformer>(); }},
        {"ColumnsExceptTransformer", [] { return make_intrusive<ASTColumnsExceptTransformer>(); }},
        {"ColumnsReplaceTransformer", [] { return make_intrusive<ASTColumnsReplaceTransformer>(); }},
        {"ColumnsReplaceTransformerReplacement", [] { return make_intrusive<ASTColumnsReplaceTransformer::Replacement>(); }},

        /// DDL types
        {"ColumnDeclaration", [] { return make_intrusive<ASTColumnDeclaration>(); }},
        {"IndexDeclaration", [] { return make_intrusive<ASTIndexDeclaration>(); }},
        {"ConstraintDeclaration", [] { return make_intrusive<ASTConstraintDeclaration>(); }},
        {"ProjectionDeclaration", [] { return make_intrusive<ASTProjectionDeclaration>(); }},
        {"StatisticsDeclaration", [] { return make_intrusive<ASTStatisticsDeclaration>(); }},
        {"Columns definition", [] { return make_intrusive<ASTColumns>(); }},
        {"Storage", [] { return make_intrusive<ASTStorage>(); }},
        {"CreateQuery", [] { return make_intrusive<ASTCreateQuery>(); }},
        {"DropQuery", [] { return make_intrusive<ASTDropQuery>(); }},
        {"InsertQuery", [] { return make_intrusive<ASTInsertQuery>(); }},
        {"AlterCommand", [] { return make_intrusive<ASTAlterCommand>(); }},
        {"AlterQuery", [] { return make_intrusive<ASTAlterQuery>(); }},
        {"RenameQuery", [] { return make_intrusive<ASTRenameQuery>(); }},
        {"NameTypePair", [] { return make_intrusive<ASTNameTypePair>(); }},
        {"DataType", [] { return make_intrusive<ASTDataType>(); }},
        {"FunctionWithKeyValueArguments", [] { return make_intrusive<ASTFunctionWithKeyValueArguments>(); }},
        {"Pair", [] { return make_intrusive<ASTPair>(); }},
        {"TTLElement", [] { return make_intrusive<ASTTTLElement>(); }},
        {"Partition", [] { return make_intrusive<ASTPartition>(); }},

        /// Dictionary types
        {"DictionaryRange", [] { return make_intrusive<ASTDictionaryRange>(); }},
        {"DictionaryLifetime", [] { return make_intrusive<ASTDictionaryLifetime>(); }},
        {"DictionaryLayout", [] { return make_intrusive<ASTDictionaryLayout>(); }},
        {"DictionarySettings", [] { return make_intrusive<ASTDictionarySettings>(); }},
        {"Dictionary", [] { return make_intrusive<ASTDictionary>(); }},
        {"DictionaryAttributeDeclaration", [] { return make_intrusive<ASTDictionaryAttributeDeclaration>(); }},

        /// System/Show/Control types
        {"SystemQuery", [] { return make_intrusive<ASTSystemQuery>(); }},
        {"ShowTablesQuery", [] { return make_intrusive<ASTShowTablesQuery>(); }},
        {"ShowColumnsQuery", [] { return make_intrusive<ASTShowColumnsQuery>(); }},
        {"KillQueryQuery", [] { return make_intrusive<ASTKillQueryQuery>(); }},
        {"OptimizeQuery", [] { return make_intrusive<ASTOptimizeQuery>(); }},
        {"DeleteQuery", [] { return make_intrusive<ASTDeleteQuery>(); }},
        {"UpdateQuery", [] { return make_intrusive<ASTUpdateQuery>(); }},
        {"UseQuery", [] { return make_intrusive<ASTUseQuery>(); }},
        {"TransactionControl", [] { return make_intrusive<ASTTransactionControl>(); }},
        {"CheckTableQuery", [] { return make_intrusive<ASTCheckTableQuery>(); }},
        {"CheckAllTablesQuery", [] { return make_intrusive<ASTCheckAllTablesQuery>(); }},

        /// Misc types
        {"BackupQuery", [] { return make_intrusive<ASTBackupQuery>(); }},
        {"ViewTargets", [] { return make_intrusive<ASTViewTargets>(); }},
        {"SQLSecurity", [] { return make_intrusive<ASTSQLSecurity>(); }},
        {"RefreshStrategy", [] { return make_intrusive<ASTRefreshStrategy>(); }},
        {"TimeInterval", [] { return make_intrusive<ASTTimeInterval>(); }},
        {"Assignment", [] { return make_intrusive<ASTAssignment>(); }},
        {"TableOverride", [] { return make_intrusive<ASTTableOverride>(); }},
        {"TableOverrideList", [] { return make_intrusive<ASTTableOverrideList>(); }},
        {"ForeignKeyDeclaration", [] { return make_intrusive<ASTForeignKeyDeclaration>(); }},
        {"IdentifierTypePair", [] { return make_intrusive<ASTIdentifierTypePair>(); }},
    };

    return factory;
}

} /// namespace


ASTPtr IAST::createFromJSON(const String & json)
{
    Poco::JSON::Parser parser;
    auto result = parser.parse(json);
    auto obj = result.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a JSON object for AST deserialization");
    return createFromJSON(*obj);
}


ASTPtr IAST::createFromJSON(const Poco::JSON::Object & json)
{
    if (!json.has("type"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON object missing 'type' field for AST deserialization");

    String type = json.getValue<String>("type");

    const auto & factory = getASTFactory();
    auto it = factory.find(type);
    if (it == factory.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AST node type in JSON: '{}'", type);

    /// Create empty instance.
    ASTPtr node = it->second();

    /// Populate from JSON via virtual dispatch.
    node->readJSON(json);

    return node;
}


/// Wrapper functions for backward compatibility.
ASTPtr deserializeASTFromJSON(const String & json)
{
    return IAST::createFromJSON(json);
}

ASTPtr deserializeASTFromJSON(const Poco::JSON::Object & json)
{
    return IAST::createFromJSON(json);
}

}
