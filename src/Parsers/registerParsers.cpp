#include <Parsers/registerParsers.h>

#include <Parsers/Access/ParserCheckGrantQuery.h>
#include <Parsers/Access/ParserCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ParserCreateTokenQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserExecuteAsQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/Access/ParserMoveAccessEntityQuery.h>
#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAlterNamedCollectionQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/ParserHypotheticalObjectQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOnCluster.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserParallelWithQuery.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserRegistry.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserUndropQuery.h>
#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserWithElement.h>


namespace DB
{

void registerParsers()
{
    auto & registry = ParserRegistry::instance();

    static ParserQuery dummy_subquery_parser(/*end_*/ nullptr);

    registry.registerParser<ParserAlterNamedCollectionQuery>();
    registry.registerParser<ParserAlterQuery>();
    registry.registerParser<ParserCheckGrantQuery>();
    registry.registerParser<ParserCheckQuery>();
    registry.registerParser<ParserColumnsTransformers>();
    registry.registerParser<ParserCreateFunctionQuery>();
    registry.registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<ParserCreateHandlerQuery>(/*end_*/ nullptr); });
    registry.registerParser<ParserCreateMaskingPolicy>();
    registry.registerParser<ParserCreateQuery>();
    registry.registerParser<ParserCreateQuotaQuery>();
    registry.registerParser<ParserCreateRoleQuery>();
    registry.registerParser<ParserCreateRowPolicyQuery>();
    registry.registerParser<ParserCreateSettingsProfileQuery>();
    registry.registerParser<ParserCreateTokenQuery>();
    registry.registerParser<ParserCreateUserQuery>();
    registry.registerParser<ParserDeleteQuery>();
    registry.registerParser<ParserDescribeTableQuery>();
    registry.registerParser<ParserDropQuery>();
    registry.registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<ParserExecuteAsQuery>(dummy_subquery_parser); });
    registry.registerParser<ParserExplainQuery>();
    registry.registerParser<ParserExpression>();
    registry.registerParser<ParserGrantQuery>();
    registry.registerParser<ParserHypotheticalObjectQuery>();
    registry.registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<ParserInsertQuery>(/*end_*/ nullptr, /*allow_settings_after_format_in_insert_*/ false); });
    registry.registerParser<ParserKillQueryQuery>();
    registry.registerParser<ParserMoveAccessEntityQuery>();
    registry.registerParser<ParserOnCluster>();
    registry.registerParser<ParserOptimizeQuery>();
    registry.registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<ParserParallelWithQuery>(dummy_subquery_parser, /*first_subquery_*/ nullptr); });
    registry.registerParser<ParserPipeOperators>();
    registry.registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<ParserQueryWithOutput>(/*end_*/ nullptr); });
    registry.registerParser<ParserRenameQuery>();
    registry.registerParser<ParserSelectQuery>();
    registry.registerParser<ParserSelectWithUnionQuery>();
    registry.registerParser<ParserSetQuery>();
    registry.registerParser<ParserSetRoleQuery>();
    registry.registerParser<ParserShowTablesQuery>();
    registry.registerParser<ParserSystemQuery>();
    registry.registerParser<ParserTablePropertiesQuery>();
    registry.registerParser<ParserTablesInSelectQuery>();
    registry.registerParser<ParserUndropQuery>();
    registry.registerParser<ParserUpdateQuery>();
    registry.registerParser<ParserUseQuery>();
    registry.registerParser<ParserWithElement>();
}

}
