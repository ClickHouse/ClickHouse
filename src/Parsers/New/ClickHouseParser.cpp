
// Generated from ClickHouseParser.g4 by ANTLR 4.7.2


#include "ClickHouseParserVisitor.h"

#include "ClickHouseParser.h"


using namespace antlrcpp;
using namespace DB;
using namespace antlr4;

ClickHouseParser::ClickHouseParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

ClickHouseParser::~ClickHouseParser() {
  delete _interpreter;
}

std::string ClickHouseParser::getGrammarFileName() const {
  return "ClickHouseParser.g4";
}

const std::vector<std::string>& ClickHouseParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& ClickHouseParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- QueryStmtContext ------------------------------------------------------------------

ClickHouseParser::QueryStmtContext::QueryStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::QueryContext* ClickHouseParser::QueryStmtContext::query() {
  return getRuleContext<ClickHouseParser::QueryContext>(0);
}

tree::TerminalNode* ClickHouseParser::QueryStmtContext::INTO() {
  return getToken(ClickHouseParser::INTO, 0);
}

tree::TerminalNode* ClickHouseParser::QueryStmtContext::OUTFILE() {
  return getToken(ClickHouseParser::OUTFILE, 0);
}

tree::TerminalNode* ClickHouseParser::QueryStmtContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::QueryStmtContext::FORMAT() {
  return getToken(ClickHouseParser::FORMAT, 0);
}

ClickHouseParser::IdentifierOrNullContext* ClickHouseParser::QueryStmtContext::identifierOrNull() {
  return getRuleContext<ClickHouseParser::IdentifierOrNullContext>(0);
}

tree::TerminalNode* ClickHouseParser::QueryStmtContext::SEMICOLON() {
  return getToken(ClickHouseParser::SEMICOLON, 0);
}

ClickHouseParser::InsertStmtContext* ClickHouseParser::QueryStmtContext::insertStmt() {
  return getRuleContext<ClickHouseParser::InsertStmtContext>(0);
}


size_t ClickHouseParser::QueryStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleQueryStmt;
}

antlrcpp::Any ClickHouseParser::QueryStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitQueryStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::QueryStmtContext* ClickHouseParser::queryStmt() {
  QueryStmtContext *_localctx = _tracker.createInstance<QueryStmtContext>(_ctx, getState());
  enterRule(_localctx, 0, ClickHouseParser::RuleQueryStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(232);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::ALTER:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DROP:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::KILL:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SET:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 1);
        setState(218);
        query();
        setState(222);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::INTO) {
          setState(219);
          match(ClickHouseParser::INTO);
          setState(220);
          match(ClickHouseParser::OUTFILE);
          setState(221);
          match(ClickHouseParser::STRING_LITERAL);
        }
        setState(226);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::FORMAT) {
          setState(224);
          match(ClickHouseParser::FORMAT);
          setState(225);
          identifierOrNull();
        }
        setState(229);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::SEMICOLON) {
          setState(228);
          match(ClickHouseParser::SEMICOLON);
        }
        break;
      }

      case ClickHouseParser::INSERT: {
        enterOuterAlt(_localctx, 2);
        setState(231);
        insertStmt();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryContext ------------------------------------------------------------------

ClickHouseParser::QueryContext::QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::AlterStmtContext* ClickHouseParser::QueryContext::alterStmt() {
  return getRuleContext<ClickHouseParser::AlterStmtContext>(0);
}

ClickHouseParser::AttachStmtContext* ClickHouseParser::QueryContext::attachStmt() {
  return getRuleContext<ClickHouseParser::AttachStmtContext>(0);
}

ClickHouseParser::CheckStmtContext* ClickHouseParser::QueryContext::checkStmt() {
  return getRuleContext<ClickHouseParser::CheckStmtContext>(0);
}

ClickHouseParser::CreateStmtContext* ClickHouseParser::QueryContext::createStmt() {
  return getRuleContext<ClickHouseParser::CreateStmtContext>(0);
}

ClickHouseParser::DescribeStmtContext* ClickHouseParser::QueryContext::describeStmt() {
  return getRuleContext<ClickHouseParser::DescribeStmtContext>(0);
}

ClickHouseParser::DropStmtContext* ClickHouseParser::QueryContext::dropStmt() {
  return getRuleContext<ClickHouseParser::DropStmtContext>(0);
}

ClickHouseParser::ExistsStmtContext* ClickHouseParser::QueryContext::existsStmt() {
  return getRuleContext<ClickHouseParser::ExistsStmtContext>(0);
}

ClickHouseParser::ExplainStmtContext* ClickHouseParser::QueryContext::explainStmt() {
  return getRuleContext<ClickHouseParser::ExplainStmtContext>(0);
}

ClickHouseParser::KillStmtContext* ClickHouseParser::QueryContext::killStmt() {
  return getRuleContext<ClickHouseParser::KillStmtContext>(0);
}

ClickHouseParser::OptimizeStmtContext* ClickHouseParser::QueryContext::optimizeStmt() {
  return getRuleContext<ClickHouseParser::OptimizeStmtContext>(0);
}

ClickHouseParser::RenameStmtContext* ClickHouseParser::QueryContext::renameStmt() {
  return getRuleContext<ClickHouseParser::RenameStmtContext>(0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::QueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

ClickHouseParser::SetStmtContext* ClickHouseParser::QueryContext::setStmt() {
  return getRuleContext<ClickHouseParser::SetStmtContext>(0);
}

ClickHouseParser::ShowStmtContext* ClickHouseParser::QueryContext::showStmt() {
  return getRuleContext<ClickHouseParser::ShowStmtContext>(0);
}

ClickHouseParser::SystemStmtContext* ClickHouseParser::QueryContext::systemStmt() {
  return getRuleContext<ClickHouseParser::SystemStmtContext>(0);
}

ClickHouseParser::TruncateStmtContext* ClickHouseParser::QueryContext::truncateStmt() {
  return getRuleContext<ClickHouseParser::TruncateStmtContext>(0);
}

ClickHouseParser::UseStmtContext* ClickHouseParser::QueryContext::useStmt() {
  return getRuleContext<ClickHouseParser::UseStmtContext>(0);
}

ClickHouseParser::WatchStmtContext* ClickHouseParser::QueryContext::watchStmt() {
  return getRuleContext<ClickHouseParser::WatchStmtContext>(0);
}


size_t ClickHouseParser::QueryContext::getRuleIndex() const {
  return ClickHouseParser::RuleQuery;
}

antlrcpp::Any ClickHouseParser::QueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::QueryContext* ClickHouseParser::query() {
  QueryContext *_localctx = _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 2, ClickHouseParser::RuleQuery);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(252);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(234);
      alterStmt();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(235);
      attachStmt();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(236);
      checkStmt();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(237);
      createStmt();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(238);
      describeStmt();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(239);
      dropStmt();
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(240);
      existsStmt();
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(241);
      explainStmt();
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(242);
      killStmt();
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(243);
      optimizeStmt();
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(244);
      renameStmt();
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(245);
      selectUnionStmt();
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(246);
      setStmt();
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(247);
      showStmt();
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(248);
      systemStmt();
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(249);
      truncateStmt();
      break;
    }

    case 17: {
      enterOuterAlt(_localctx, 17);
      setState(250);
      useStmt();
      break;
    }

    case 18: {
      enterOuterAlt(_localctx, 18);
      setState(251);
      watchStmt();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AlterStmtContext ------------------------------------------------------------------

ClickHouseParser::AlterStmtContext::AlterStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::AlterStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleAlterStmt;
}

void ClickHouseParser::AlterStmtContext::copyFrom(AlterStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- AlterTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableStmtContext::ALTER() {
  return getToken(ClickHouseParser::ALTER, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AlterTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

std::vector<ClickHouseParser::AlterTableClauseContext *> ClickHouseParser::AlterTableStmtContext::alterTableClause() {
  return getRuleContexts<ClickHouseParser::AlterTableClauseContext>();
}

ClickHouseParser::AlterTableClauseContext* ClickHouseParser::AlterTableStmtContext::alterTableClause(size_t i) {
  return getRuleContext<ClickHouseParser::AlterTableClauseContext>(i);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::AlterTableStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::AlterTableStmtContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::AlterTableStmtContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::AlterTableStmtContext::AlterTableStmtContext(AlterStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AlterStmtContext* ClickHouseParser::alterStmt() {
  AlterStmtContext *_localctx = _tracker.createInstance<AlterStmtContext>(_ctx, getState());
  enterRule(_localctx, 4, ClickHouseParser::RuleAlterStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<AlterStmtContext *>(_tracker.createInstance<ClickHouseParser::AlterTableStmtContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(254);
    match(ClickHouseParser::ALTER);
    setState(255);
    match(ClickHouseParser::TABLE);
    setState(256);
    tableIdentifier();
    setState(258);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(257);
      clusterClause();
    }
    setState(260);
    alterTableClause();
    setState(265);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(261);
      match(ClickHouseParser::COMMA);
      setState(262);
      alterTableClause();
      setState(267);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AlterTableClauseContext ------------------------------------------------------------------

ClickHouseParser::AlterTableClauseContext::AlterTableClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::AlterTableClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleAlterTableClause;
}

void ClickHouseParser::AlterTableClauseContext::copyFrom(AlterTableClauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- AlterTableClauseReplaceContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseReplaceContext::REPLACE() {
  return getToken(ClickHouseParser::REPLACE, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseReplaceContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseReplaceContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AlterTableClauseReplaceContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseReplaceContext::AlterTableClauseReplaceContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseReplaceContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseReplace(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyOrderByContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyOrderByContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyOrderByContext::ORDER() {
  return getToken(ClickHouseParser::ORDER, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyOrderByContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::AlterTableClauseModifyOrderByContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::AlterTableClauseModifyOrderByContext::AlterTableClauseModifyOrderByContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyOrderByContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModifyOrderBy(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseUpdateContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseUpdateContext::UPDATE() {
  return getToken(ClickHouseParser::UPDATE, 0);
}

ClickHouseParser::AssignmentExprListContext* ClickHouseParser::AlterTableClauseUpdateContext::assignmentExprList() {
  return getRuleContext<ClickHouseParser::AssignmentExprListContext>(0);
}

ClickHouseParser::WhereClauseContext* ClickHouseParser::AlterTableClauseUpdateContext::whereClause() {
  return getRuleContext<ClickHouseParser::WhereClauseContext>(0);
}

ClickHouseParser::AlterTableClauseUpdateContext::AlterTableClauseUpdateContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseUpdateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseUpdate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseClearProjectionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearProjectionContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearProjectionContext::PROJECTION() {
  return getToken(ClickHouseParser::PROJECTION, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseClearProjectionContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearProjectionContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearProjectionContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearProjectionContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseClearProjectionContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseClearProjectionContext::AlterTableClauseClearProjectionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseClearProjectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseClearProjection(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyRemoveContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyRemoveContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyRemoveContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseModifyRemoveContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyRemoveContext::REMOVE() {
  return getToken(ClickHouseParser::REMOVE, 0);
}

ClickHouseParser::TableColumnPropertyTypeContext* ClickHouseParser::AlterTableClauseModifyRemoveContext::tableColumnPropertyType() {
  return getRuleContext<ClickHouseParser::TableColumnPropertyTypeContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyRemoveContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyRemoveContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseModifyRemoveContext::AlterTableClauseModifyRemoveContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyRemoveContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModifyRemove(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDeleteContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDeleteContext::DELETE() {
  return getToken(ClickHouseParser::DELETE, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDeleteContext::WHERE() {
  return getToken(ClickHouseParser::WHERE, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::AlterTableClauseDeleteContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::AlterTableClauseDeleteContext::AlterTableClauseDeleteContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDeleteContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDelete(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseCommentContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseCommentContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseCommentContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseCommentContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseCommentContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseCommentContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseCommentContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseCommentContext::AlterTableClauseCommentContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseCommentContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseComment(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDropColumnContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropColumnContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropColumnContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseDropColumnContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropColumnContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropColumnContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseDropColumnContext::AlterTableClauseDropColumnContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDropColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDropColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDetachContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDetachContext::DETACH() {
  return getToken(ClickHouseParser::DETACH, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseDetachContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseDetachContext::AlterTableClauseDetachContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDetachContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDetach(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseAddIndexContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::ADD() {
  return getToken(ClickHouseParser::ADD, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

ClickHouseParser::TableIndexDfntContext* ClickHouseParser::AlterTableClauseAddIndexContext::tableIndexDfnt() {
  return getRuleContext<ClickHouseParser::TableIndexDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddIndexContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseAddIndexContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseAddIndexContext::AlterTableClauseAddIndexContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseAddIndexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseAddIndex(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDropPartitionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropPartitionContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseDropPartitionContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseDropPartitionContext::AlterTableClauseDropPartitionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDropPartitionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDropPartition(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseMaterializeIndexContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeIndexContext::MATERIALIZE() {
  return getToken(ClickHouseParser::MATERIALIZE, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeIndexContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseMaterializeIndexContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeIndexContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeIndexContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeIndexContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseMaterializeIndexContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseMaterializeIndexContext::AlterTableClauseMaterializeIndexContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseMaterializeIndexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseMaterializeIndex(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseMaterializeProjectionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::MATERIALIZE() {
  return getToken(ClickHouseParser::MATERIALIZE, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::PROJECTION() {
  return getToken(ClickHouseParser::PROJECTION, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseMaterializeProjectionContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseMaterializeProjectionContext::AlterTableClauseMaterializeProjectionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseMaterializeProjectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseMaterializeProjection(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseMovePartitionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::MOVE() {
  return getToken(ClickHouseParser::MOVE, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseMovePartitionContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::DISK() {
  return getToken(ClickHouseParser::DISK, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::VOLUME() {
  return getToken(ClickHouseParser::VOLUME, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseMovePartitionContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AlterTableClauseMovePartitionContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseMovePartitionContext::AlterTableClauseMovePartitionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseMovePartitionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseMovePartition(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseRenameContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseRenameContext::RENAME() {
  return getToken(ClickHouseParser::RENAME, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRenameContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

std::vector<ClickHouseParser::NestedIdentifierContext *> ClickHouseParser::AlterTableClauseRenameContext::nestedIdentifier() {
  return getRuleContexts<ClickHouseParser::NestedIdentifierContext>();
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseRenameContext::nestedIdentifier(size_t i) {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRenameContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRenameContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRenameContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseRenameContext::AlterTableClauseRenameContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseRenameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseRename(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseFreezePartitionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseFreezePartitionContext::FREEZE() {
  return getToken(ClickHouseParser::FREEZE, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseFreezePartitionContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseFreezePartitionContext::AlterTableClauseFreezePartitionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseFreezePartitionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseFreezePartition(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseClearColumnContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearColumnContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearColumnContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseClearColumnContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearColumnContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearColumnContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearColumnContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseClearColumnContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseClearColumnContext::AlterTableClauseClearColumnContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseClearColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseClearColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::AlterTableClauseModifyContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseModifyContext::AlterTableClauseModifyContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModify(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseClearIndexContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearIndexContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearIndexContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseClearIndexContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearIndexContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearIndexContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearIndexContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseClearIndexContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseClearIndexContext::AlterTableClauseClearIndexContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseClearIndexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseClearIndex(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseRemoveTTLContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveTTLContext::REMOVE() {
  return getToken(ClickHouseParser::REMOVE, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveTTLContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

ClickHouseParser::AlterTableClauseRemoveTTLContext::AlterTableClauseRemoveTTLContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseRemoveTTLContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseRemoveTTL(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyCodecContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCodecContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCodecContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseModifyCodecContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::CodecExprContext* ClickHouseParser::AlterTableClauseModifyCodecContext::codecExpr() {
  return getRuleContext<ClickHouseParser::CodecExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCodecContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCodecContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseModifyCodecContext::AlterTableClauseModifyCodecContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyCodecContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModifyCodec(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseAttachContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseAttachContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseAttachContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAttachContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AlterTableClauseAttachContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseAttachContext::AlterTableClauseAttachContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseAttachContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseAttach(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDropProjectionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropProjectionContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropProjectionContext::PROJECTION() {
  return getToken(ClickHouseParser::PROJECTION, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseDropProjectionContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropProjectionContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropProjectionContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseDropProjectionContext::AlterTableClauseDropProjectionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDropProjectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDropProjection(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseDropIndexContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropIndexContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropIndexContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseDropIndexContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropIndexContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseDropIndexContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseDropIndexContext::AlterTableClauseDropIndexContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseDropIndexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseDropIndex(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyCommentContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseModifyCommentContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyCommentContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseModifyCommentContext::AlterTableClauseModifyCommentContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyCommentContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModifyComment(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseModifyTTLContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseModifyTTLContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

ClickHouseParser::TtlClauseContext* ClickHouseParser::AlterTableClauseModifyTTLContext::ttlClause() {
  return getRuleContext<ClickHouseParser::TtlClauseContext>(0);
}

ClickHouseParser::AlterTableClauseModifyTTLContext::AlterTableClauseModifyTTLContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseModifyTTLContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseModifyTTL(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseAddProjectionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::ADD() {
  return getToken(ClickHouseParser::ADD, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::PROJECTION() {
  return getToken(ClickHouseParser::PROJECTION, 0);
}

ClickHouseParser::TableProjectionDfntContext* ClickHouseParser::AlterTableClauseAddProjectionContext::tableProjectionDfnt() {
  return getRuleContext<ClickHouseParser::TableProjectionDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddProjectionContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseAddProjectionContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseAddProjectionContext::AlterTableClauseAddProjectionContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseAddProjectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseAddProjection(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseAddColumnContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::ADD() {
  return getToken(ClickHouseParser::ADD, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::AlterTableClauseAddColumnContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddColumnContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseAddColumnContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseAddColumnContext::AlterTableClauseAddColumnContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseAddColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseAddColumn(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AlterTableClauseContext* ClickHouseParser::alterTableClause() {
  AlterTableClauseContext *_localctx = _tracker.createInstance<AlterTableClauseContext>(_ctx, getState());
  enterRule(_localctx, 6, ClickHouseParser::RuleAlterTableClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(482);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAddColumnContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(268);
      match(ClickHouseParser::ADD);
      setState(269);
      match(ClickHouseParser::COLUMN);
      setState(273);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
      case 1: {
        setState(270);
        match(ClickHouseParser::IF);
        setState(271);
        match(ClickHouseParser::NOT);
        setState(272);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(275);
      tableColumnDfnt();
      setState(278);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AFTER) {
        setState(276);
        match(ClickHouseParser::AFTER);
        setState(277);
        nestedIdentifier();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAddIndexContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(280);
      match(ClickHouseParser::ADD);
      setState(281);
      match(ClickHouseParser::INDEX);
      setState(285);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
      case 1: {
        setState(282);
        match(ClickHouseParser::IF);
        setState(283);
        match(ClickHouseParser::NOT);
        setState(284);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(287);
      tableIndexDfnt();
      setState(290);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AFTER) {
        setState(288);
        match(ClickHouseParser::AFTER);
        setState(289);
        nestedIdentifier();
      }
      break;
    }

    case 3: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAddProjectionContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(292);
      match(ClickHouseParser::ADD);
      setState(293);
      match(ClickHouseParser::PROJECTION);
      setState(297);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
      case 1: {
        setState(294);
        match(ClickHouseParser::IF);
        setState(295);
        match(ClickHouseParser::NOT);
        setState(296);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(299);
      tableProjectionDfnt();
      setState(302);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AFTER) {
        setState(300);
        match(ClickHouseParser::AFTER);
        setState(301);
        nestedIdentifier();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAttachContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(304);
      match(ClickHouseParser::ATTACH);
      setState(305);
      partitionClause();
      setState(308);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM) {
        setState(306);
        match(ClickHouseParser::FROM);
        setState(307);
        tableIdentifier();
      }
      break;
    }

    case 5: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseClearColumnContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(310);
      match(ClickHouseParser::CLEAR);
      setState(311);
      match(ClickHouseParser::COLUMN);
      setState(314);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
      case 1: {
        setState(312);
        match(ClickHouseParser::IF);
        setState(313);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(316);
      nestedIdentifier();
      setState(319);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(317);
        match(ClickHouseParser::IN);
        setState(318);
        partitionClause();
      }
      break;
    }

    case 6: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseClearIndexContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(321);
      match(ClickHouseParser::CLEAR);
      setState(322);
      match(ClickHouseParser::INDEX);
      setState(325);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
      case 1: {
        setState(323);
        match(ClickHouseParser::IF);
        setState(324);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(327);
      nestedIdentifier();
      setState(330);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(328);
        match(ClickHouseParser::IN);
        setState(329);
        partitionClause();
      }
      break;
    }

    case 7: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseClearProjectionContext>(_localctx));
      enterOuterAlt(_localctx, 7);
      setState(332);
      match(ClickHouseParser::CLEAR);
      setState(333);
      match(ClickHouseParser::PROJECTION);
      setState(336);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
      case 1: {
        setState(334);
        match(ClickHouseParser::IF);
        setState(335);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(338);
      nestedIdentifier();
      setState(341);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(339);
        match(ClickHouseParser::IN);
        setState(340);
        partitionClause();
      }
      break;
    }

    case 8: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseCommentContext>(_localctx));
      enterOuterAlt(_localctx, 8);
      setState(343);
      match(ClickHouseParser::COMMENT);
      setState(344);
      match(ClickHouseParser::COLUMN);
      setState(347);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx)) {
      case 1: {
        setState(345);
        match(ClickHouseParser::IF);
        setState(346);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(349);
      nestedIdentifier();
      setState(350);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 9: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDeleteContext>(_localctx));
      enterOuterAlt(_localctx, 9);
      setState(352);
      match(ClickHouseParser::DELETE);
      setState(353);
      match(ClickHouseParser::WHERE);
      setState(354);
      columnExpr(0);
      break;
    }

    case 10: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDetachContext>(_localctx));
      enterOuterAlt(_localctx, 10);
      setState(355);
      match(ClickHouseParser::DETACH);
      setState(356);
      partitionClause();
      break;
    }

    case 11: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropColumnContext>(_localctx));
      enterOuterAlt(_localctx, 11);
      setState(357);
      match(ClickHouseParser::DROP);
      setState(358);
      match(ClickHouseParser::COLUMN);
      setState(361);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx)) {
      case 1: {
        setState(359);
        match(ClickHouseParser::IF);
        setState(360);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(363);
      nestedIdentifier();
      break;
    }

    case 12: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropIndexContext>(_localctx));
      enterOuterAlt(_localctx, 12);
      setState(364);
      match(ClickHouseParser::DROP);
      setState(365);
      match(ClickHouseParser::INDEX);
      setState(368);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx)) {
      case 1: {
        setState(366);
        match(ClickHouseParser::IF);
        setState(367);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(370);
      nestedIdentifier();
      break;
    }

    case 13: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropProjectionContext>(_localctx));
      enterOuterAlt(_localctx, 13);
      setState(371);
      match(ClickHouseParser::DROP);
      setState(372);
      match(ClickHouseParser::PROJECTION);
      setState(375);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx)) {
      case 1: {
        setState(373);
        match(ClickHouseParser::IF);
        setState(374);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(377);
      nestedIdentifier();
      break;
    }

    case 14: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropPartitionContext>(_localctx));
      enterOuterAlt(_localctx, 14);
      setState(378);
      match(ClickHouseParser::DROP);
      setState(379);
      partitionClause();
      break;
    }

    case 15: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseFreezePartitionContext>(_localctx));
      enterOuterAlt(_localctx, 15);
      setState(380);
      match(ClickHouseParser::FREEZE);
      setState(382);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::PARTITION) {
        setState(381);
        partitionClause();
      }
      break;
    }

    case 16: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseMaterializeIndexContext>(_localctx));
      enterOuterAlt(_localctx, 16);
      setState(384);
      match(ClickHouseParser::MATERIALIZE);
      setState(385);
      match(ClickHouseParser::INDEX);
      setState(388);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx)) {
      case 1: {
        setState(386);
        match(ClickHouseParser::IF);
        setState(387);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(390);
      nestedIdentifier();
      setState(393);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(391);
        match(ClickHouseParser::IN);
        setState(392);
        partitionClause();
      }
      break;
    }

    case 17: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseMaterializeProjectionContext>(_localctx));
      enterOuterAlt(_localctx, 17);
      setState(395);
      match(ClickHouseParser::MATERIALIZE);
      setState(396);
      match(ClickHouseParser::PROJECTION);
      setState(399);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
      case 1: {
        setState(397);
        match(ClickHouseParser::IF);
        setState(398);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(401);
      nestedIdentifier();
      setState(404);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(402);
        match(ClickHouseParser::IN);
        setState(403);
        partitionClause();
      }
      break;
    }

    case 18: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyCodecContext>(_localctx));
      enterOuterAlt(_localctx, 18);
      setState(406);
      match(ClickHouseParser::MODIFY);
      setState(407);
      match(ClickHouseParser::COLUMN);
      setState(410);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx)) {
      case 1: {
        setState(408);
        match(ClickHouseParser::IF);
        setState(409);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(412);
      nestedIdentifier();
      setState(413);
      codecExpr();
      break;
    }

    case 19: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyCommentContext>(_localctx));
      enterOuterAlt(_localctx, 19);
      setState(415);
      match(ClickHouseParser::MODIFY);
      setState(416);
      match(ClickHouseParser::COLUMN);
      setState(419);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
      case 1: {
        setState(417);
        match(ClickHouseParser::IF);
        setState(418);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(421);
      nestedIdentifier();
      setState(422);
      match(ClickHouseParser::COMMENT);
      setState(423);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 20: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyRemoveContext>(_localctx));
      enterOuterAlt(_localctx, 20);
      setState(425);
      match(ClickHouseParser::MODIFY);
      setState(426);
      match(ClickHouseParser::COLUMN);
      setState(429);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
      case 1: {
        setState(427);
        match(ClickHouseParser::IF);
        setState(428);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(431);
      nestedIdentifier();
      setState(432);
      match(ClickHouseParser::REMOVE);
      setState(433);
      tableColumnPropertyType();
      break;
    }

    case 21: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyContext>(_localctx));
      enterOuterAlt(_localctx, 21);
      setState(435);
      match(ClickHouseParser::MODIFY);
      setState(436);
      match(ClickHouseParser::COLUMN);
      setState(439);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
      case 1: {
        setState(437);
        match(ClickHouseParser::IF);
        setState(438);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(441);
      tableColumnDfnt();
      break;
    }

    case 22: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyOrderByContext>(_localctx));
      enterOuterAlt(_localctx, 22);
      setState(442);
      match(ClickHouseParser::MODIFY);
      setState(443);
      match(ClickHouseParser::ORDER);
      setState(444);
      match(ClickHouseParser::BY);
      setState(445);
      columnExpr(0);
      break;
    }

    case 23: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyTTLContext>(_localctx));
      enterOuterAlt(_localctx, 23);
      setState(446);
      match(ClickHouseParser::MODIFY);
      setState(447);
      ttlClause();
      break;
    }

    case 24: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseMovePartitionContext>(_localctx));
      enterOuterAlt(_localctx, 24);
      setState(448);
      match(ClickHouseParser::MOVE);
      setState(449);
      partitionClause();
      setState(459);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
      case 1: {
        setState(450);
        match(ClickHouseParser::TO);
        setState(451);
        match(ClickHouseParser::DISK);
        setState(452);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case 2: {
        setState(453);
        match(ClickHouseParser::TO);
        setState(454);
        match(ClickHouseParser::VOLUME);
        setState(455);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case 3: {
        setState(456);
        match(ClickHouseParser::TO);
        setState(457);
        match(ClickHouseParser::TABLE);
        setState(458);
        tableIdentifier();
        break;
      }

      }
      break;
    }

    case 25: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseRemoveTTLContext>(_localctx));
      enterOuterAlt(_localctx, 25);
      setState(461);
      match(ClickHouseParser::REMOVE);
      setState(462);
      match(ClickHouseParser::TTL);
      break;
    }

    case 26: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseRenameContext>(_localctx));
      enterOuterAlt(_localctx, 26);
      setState(463);
      match(ClickHouseParser::RENAME);
      setState(464);
      match(ClickHouseParser::COLUMN);
      setState(467);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
      case 1: {
        setState(465);
        match(ClickHouseParser::IF);
        setState(466);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(469);
      nestedIdentifier();
      setState(470);
      match(ClickHouseParser::TO);
      setState(471);
      nestedIdentifier();
      break;
    }

    case 27: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseReplaceContext>(_localctx));
      enterOuterAlt(_localctx, 27);
      setState(473);
      match(ClickHouseParser::REPLACE);
      setState(474);
      partitionClause();
      setState(475);
      match(ClickHouseParser::FROM);
      setState(476);
      tableIdentifier();
      break;
    }

    case 28: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseUpdateContext>(_localctx));
      enterOuterAlt(_localctx, 28);
      setState(478);
      match(ClickHouseParser::UPDATE);
      setState(479);
      assignmentExprList();
      setState(480);
      whereClause();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentExprListContext ------------------------------------------------------------------

ClickHouseParser::AssignmentExprListContext::AssignmentExprListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::AssignmentExprContext *> ClickHouseParser::AssignmentExprListContext::assignmentExpr() {
  return getRuleContexts<ClickHouseParser::AssignmentExprContext>();
}

ClickHouseParser::AssignmentExprContext* ClickHouseParser::AssignmentExprListContext::assignmentExpr(size_t i) {
  return getRuleContext<ClickHouseParser::AssignmentExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::AssignmentExprListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::AssignmentExprListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::AssignmentExprListContext::getRuleIndex() const {
  return ClickHouseParser::RuleAssignmentExprList;
}

antlrcpp::Any ClickHouseParser::AssignmentExprListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAssignmentExprList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::AssignmentExprListContext* ClickHouseParser::assignmentExprList() {
  AssignmentExprListContext *_localctx = _tracker.createInstance<AssignmentExprListContext>(_ctx, getState());
  enterRule(_localctx, 8, ClickHouseParser::RuleAssignmentExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(484);
    assignmentExpr();
    setState(489);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(485);
      match(ClickHouseParser::COMMA);
      setState(486);
      assignmentExpr();
      setState(491);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentExprContext ------------------------------------------------------------------

ClickHouseParser::AssignmentExprContext::AssignmentExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AssignmentExprContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AssignmentExprContext::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::AssignmentExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::AssignmentExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleAssignmentExpr;
}

antlrcpp::Any ClickHouseParser::AssignmentExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAssignmentExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::AssignmentExprContext* ClickHouseParser::assignmentExpr() {
  AssignmentExprContext *_localctx = _tracker.createInstance<AssignmentExprContext>(_ctx, getState());
  enterRule(_localctx, 10, ClickHouseParser::RuleAssignmentExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(492);
    nestedIdentifier();
    setState(493);
    match(ClickHouseParser::EQ_SINGLE);
    setState(494);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableColumnPropertyTypeContext ------------------------------------------------------------------

ClickHouseParser::TableColumnPropertyTypeContext::TableColumnPropertyTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::ALIAS() {
  return getToken(ClickHouseParser::ALIAS, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::CODEC() {
  return getToken(ClickHouseParser::CODEC, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::DEFAULT() {
  return getToken(ClickHouseParser::DEFAULT, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyTypeContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}


size_t ClickHouseParser::TableColumnPropertyTypeContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableColumnPropertyType;
}

antlrcpp::Any ClickHouseParser::TableColumnPropertyTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableColumnPropertyType(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableColumnPropertyTypeContext* ClickHouseParser::tableColumnPropertyType() {
  TableColumnPropertyTypeContext *_localctx = _tracker.createInstance<TableColumnPropertyTypeContext>(_ctx, getState());
  enterRule(_localctx, 12, ClickHouseParser::RuleTableColumnPropertyType);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(496);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::DEFAULT))) != 0) || _la == ClickHouseParser::MATERIALIZED || _la == ClickHouseParser::TTL)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PartitionClauseContext ------------------------------------------------------------------

ClickHouseParser::PartitionClauseContext::PartitionClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::PartitionClauseContext::PARTITION() {
  return getToken(ClickHouseParser::PARTITION, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::PartitionClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::PartitionClauseContext::ID() {
  return getToken(ClickHouseParser::ID, 0);
}

tree::TerminalNode* ClickHouseParser::PartitionClauseContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}


size_t ClickHouseParser::PartitionClauseContext::getRuleIndex() const {
  return ClickHouseParser::RulePartitionClause;
}

antlrcpp::Any ClickHouseParser::PartitionClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitPartitionClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::partitionClause() {
  PartitionClauseContext *_localctx = _tracker.createInstance<PartitionClauseContext>(_ctx, getState());
  enterRule(_localctx, 14, ClickHouseParser::RulePartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(503);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(498);
      match(ClickHouseParser::PARTITION);
      setState(499);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(500);
      match(ClickHouseParser::PARTITION);
      setState(501);
      match(ClickHouseParser::ID);
      setState(502);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AttachStmtContext ------------------------------------------------------------------

ClickHouseParser::AttachStmtContext::AttachStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::AttachStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleAttachStmt;
}

void ClickHouseParser::AttachStmtContext::copyFrom(AttachStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- AttachDictionaryStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AttachDictionaryStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::AttachDictionaryStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AttachDictionaryStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::AttachDictionaryStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::AttachDictionaryStmtContext::AttachDictionaryStmtContext(AttachStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AttachDictionaryStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAttachDictionaryStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AttachStmtContext* ClickHouseParser::attachStmt() {
  AttachStmtContext *_localctx = _tracker.createInstance<AttachStmtContext>(_ctx, getState());
  enterRule(_localctx, 16, ClickHouseParser::RuleAttachStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<AttachStmtContext *>(_tracker.createInstance<ClickHouseParser::AttachDictionaryStmtContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(505);
    match(ClickHouseParser::ATTACH);
    setState(506);
    match(ClickHouseParser::DICTIONARY);
    setState(507);
    tableIdentifier();
    setState(509);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(508);
      clusterClause();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CheckStmtContext ------------------------------------------------------------------

ClickHouseParser::CheckStmtContext::CheckStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::CheckStmtContext::CHECK() {
  return getToken(ClickHouseParser::CHECK, 0);
}

tree::TerminalNode* ClickHouseParser::CheckStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CheckStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::CheckStmtContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}


size_t ClickHouseParser::CheckStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleCheckStmt;
}

antlrcpp::Any ClickHouseParser::CheckStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCheckStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::CheckStmtContext* ClickHouseParser::checkStmt() {
  CheckStmtContext *_localctx = _tracker.createInstance<CheckStmtContext>(_ctx, getState());
  enterRule(_localctx, 18, ClickHouseParser::RuleCheckStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(511);
    match(ClickHouseParser::CHECK);
    setState(512);
    match(ClickHouseParser::TABLE);
    setState(513);
    tableIdentifier();
    setState(515);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(514);
      partitionClause();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CreateStmtContext ------------------------------------------------------------------

ClickHouseParser::CreateStmtContext::CreateStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::CreateStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleCreateStmt;
}

void ClickHouseParser::CreateStmtContext::copyFrom(CreateStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- CreateViewStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateViewStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SubqueryClauseContext* ClickHouseParser::CreateViewStmtContext::subqueryClause() {
  return getRuleContext<ClickHouseParser::SubqueryClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::OR() {
  return getToken(ClickHouseParser::OR, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::REPLACE() {
  return getToken(ClickHouseParser::REPLACE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::CreateViewStmtContext::uuidClause() {
  return getRuleContext<ClickHouseParser::UuidClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateViewStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::TableSchemaClauseContext* ClickHouseParser::CreateViewStmtContext::tableSchemaClause() {
  return getRuleContext<ClickHouseParser::TableSchemaClauseContext>(0);
}

ClickHouseParser::CreateViewStmtContext::CreateViewStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateViewStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateViewStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateDictionaryStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateDictionaryStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::DictionarySchemaClauseContext* ClickHouseParser::CreateDictionaryStmtContext::dictionarySchemaClause() {
  return getRuleContext<ClickHouseParser::DictionarySchemaClauseContext>(0);
}

ClickHouseParser::DictionaryEngineClauseContext* ClickHouseParser::CreateDictionaryStmtContext::dictionaryEngineClause() {
  return getRuleContext<ClickHouseParser::DictionaryEngineClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDictionaryStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::CreateDictionaryStmtContext::uuidClause() {
  return getRuleContext<ClickHouseParser::UuidClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateDictionaryStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::CreateDictionaryStmtContext::CreateDictionaryStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateDictionaryStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateDictionaryStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateDatabaseStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::CreateDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateDatabaseStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::EngineExprContext* ClickHouseParser::CreateDatabaseStmtContext::engineExpr() {
  return getRuleContext<ClickHouseParser::EngineExprContext>(0);
}

ClickHouseParser::CreateDatabaseStmtContext::CreateDatabaseStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateLiveViewStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::LIVE() {
  return getToken(ClickHouseParser::LIVE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateLiveViewStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SubqueryClauseContext* ClickHouseParser::CreateLiveViewStmtContext::subqueryClause() {
  return getRuleContext<ClickHouseParser::SubqueryClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::CreateLiveViewStmtContext::uuidClause() {
  return getRuleContext<ClickHouseParser::UuidClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateLiveViewStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::TIMEOUT() {
  return getToken(ClickHouseParser::TIMEOUT, 0);
}

ClickHouseParser::DestinationClauseContext* ClickHouseParser::CreateLiveViewStmtContext::destinationClause() {
  return getRuleContext<ClickHouseParser::DestinationClauseContext>(0);
}

ClickHouseParser::TableSchemaClauseContext* ClickHouseParser::CreateLiveViewStmtContext::tableSchemaClause() {
  return getRuleContext<ClickHouseParser::TableSchemaClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateLiveViewStmtContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}

ClickHouseParser::CreateLiveViewStmtContext::CreateLiveViewStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateLiveViewStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateLiveViewStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateMaterializedViewStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateMaterializedViewStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SubqueryClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::subqueryClause() {
  return getRuleContext<ClickHouseParser::SubqueryClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

ClickHouseParser::DestinationClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::destinationClause() {
  return getRuleContext<ClickHouseParser::DestinationClauseContext>(0);
}

ClickHouseParser::EngineClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::uuidClause() {
  return getRuleContext<ClickHouseParser::UuidClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::TableSchemaClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::tableSchemaClause() {
  return getRuleContext<ClickHouseParser::TableSchemaClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateMaterializedViewStmtContext::POPULATE() {
  return getToken(ClickHouseParser::POPULATE, 0);
}

ClickHouseParser::CreateMaterializedViewStmtContext::CreateMaterializedViewStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateMaterializedViewStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateMaterializedViewStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::CreateTableStmtContext::uuidClause() {
  return getRuleContext<ClickHouseParser::UuidClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::CreateTableStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::TableSchemaClauseContext* ClickHouseParser::CreateTableStmtContext::tableSchemaClause() {
  return getRuleContext<ClickHouseParser::TableSchemaClauseContext>(0);
}

ClickHouseParser::EngineClauseContext* ClickHouseParser::CreateTableStmtContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
}

ClickHouseParser::SubqueryClauseContext* ClickHouseParser::CreateTableStmtContext::subqueryClause() {
  return getRuleContext<ClickHouseParser::SubqueryClauseContext>(0);
}

ClickHouseParser::CreateTableStmtContext::CreateTableStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateTableStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::CreateStmtContext* ClickHouseParser::createStmt() {
  CreateStmtContext *_localctx = _tracker.createInstance<CreateStmtContext>(_ctx, getState());
  enterRule(_localctx, 20, ClickHouseParser::RuleCreateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(654);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 71, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(517);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(518);
      match(ClickHouseParser::DATABASE);
      setState(522);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
      case 1: {
        setState(519);
        match(ClickHouseParser::IF);
        setState(520);
        match(ClickHouseParser::NOT);
        setState(521);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(524);
      databaseIdentifier();
      setState(526);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(525);
        clusterClause();
      }
      setState(529);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(528);
        engineExpr();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateDictionaryStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(531);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(532);
      match(ClickHouseParser::DICTIONARY);
      setState(536);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
      case 1: {
        setState(533);
        match(ClickHouseParser::IF);
        setState(534);
        match(ClickHouseParser::NOT);
        setState(535);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(538);
      tableIdentifier();
      setState(540);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::UUID) {
        setState(539);
        uuidClause();
      }
      setState(543);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(542);
        clusterClause();
      }
      setState(545);
      dictionarySchemaClause();
      setState(546);
      dictionaryEngineClause();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateLiveViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(548);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(549);
      match(ClickHouseParser::LIVE);
      setState(550);
      match(ClickHouseParser::VIEW);
      setState(554);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
      case 1: {
        setState(551);
        match(ClickHouseParser::IF);
        setState(552);
        match(ClickHouseParser::NOT);
        setState(553);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(556);
      tableIdentifier();
      setState(558);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::UUID) {
        setState(557);
        uuidClause();
      }
      setState(561);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(560);
        clusterClause();
      }
      setState(568);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::WITH) {
        setState(563);
        match(ClickHouseParser::WITH);
        setState(564);
        match(ClickHouseParser::TIMEOUT);
        setState(566);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::DECIMAL_LITERAL) {
          setState(565);
          match(ClickHouseParser::DECIMAL_LITERAL);
        }
      }
      setState(571);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TO) {
        setState(570);
        destinationClause();
      }
      setState(574);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
      case 1: {
        setState(573);
        tableSchemaClause();
        break;
      }

      }
      setState(576);
      subqueryClause();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateMaterializedViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(578);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(579);
      match(ClickHouseParser::MATERIALIZED);
      setState(580);
      match(ClickHouseParser::VIEW);
      setState(584);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
      case 1: {
        setState(581);
        match(ClickHouseParser::IF);
        setState(582);
        match(ClickHouseParser::NOT);
        setState(583);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(586);
      tableIdentifier();
      setState(588);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::UUID) {
        setState(587);
        uuidClause();
      }
      setState(591);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(590);
        clusterClause();
      }
      setState(594);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS || _la == ClickHouseParser::LPAREN) {
        setState(593);
        tableSchemaClause();
      }
      setState(601);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::TO: {
          setState(596);
          destinationClause();
          break;
        }

        case ClickHouseParser::ENGINE: {
          setState(597);
          engineClause();
          setState(599);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::POPULATE) {
            setState(598);
            match(ClickHouseParser::POPULATE);
          }
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(603);
      subqueryClause();
      break;
    }

    case 5: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(605);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(607);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(606);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(609);
      match(ClickHouseParser::TABLE);
      setState(613);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 60, _ctx)) {
      case 1: {
        setState(610);
        match(ClickHouseParser::IF);
        setState(611);
        match(ClickHouseParser::NOT);
        setState(612);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(615);
      tableIdentifier();
      setState(617);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::UUID) {
        setState(616);
        uuidClause();
      }
      setState(620);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(619);
        clusterClause();
      }
      setState(623);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 63, _ctx)) {
      case 1: {
        setState(622);
        tableSchemaClause();
        break;
      }

      }
      setState(626);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(625);
        engineClause();
      }
      setState(629);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS) {
        setState(628);
        subqueryClause();
      }
      break;
    }

    case 6: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(631);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(634);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::OR) {
        setState(632);
        match(ClickHouseParser::OR);
        setState(633);
        match(ClickHouseParser::REPLACE);
      }
      setState(636);
      match(ClickHouseParser::VIEW);
      setState(640);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 67, _ctx)) {
      case 1: {
        setState(637);
        match(ClickHouseParser::IF);
        setState(638);
        match(ClickHouseParser::NOT);
        setState(639);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(642);
      tableIdentifier();
      setState(644);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::UUID) {
        setState(643);
        uuidClause();
      }
      setState(647);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(646);
        clusterClause();
      }
      setState(650);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 70, _ctx)) {
      case 1: {
        setState(649);
        tableSchemaClause();
        break;
      }

      }
      setState(652);
      subqueryClause();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionarySchemaClauseContext ------------------------------------------------------------------

ClickHouseParser::DictionarySchemaClauseContext::DictionarySchemaClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::DictionarySchemaClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::DictionaryAttrDfntContext *> ClickHouseParser::DictionarySchemaClauseContext::dictionaryAttrDfnt() {
  return getRuleContexts<ClickHouseParser::DictionaryAttrDfntContext>();
}

ClickHouseParser::DictionaryAttrDfntContext* ClickHouseParser::DictionarySchemaClauseContext::dictionaryAttrDfnt(size_t i) {
  return getRuleContext<ClickHouseParser::DictionaryAttrDfntContext>(i);
}

tree::TerminalNode* ClickHouseParser::DictionarySchemaClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionarySchemaClauseContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::DictionarySchemaClauseContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::DictionarySchemaClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionarySchemaClause;
}

antlrcpp::Any ClickHouseParser::DictionarySchemaClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionarySchemaClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionarySchemaClauseContext* ClickHouseParser::dictionarySchemaClause() {
  DictionarySchemaClauseContext *_localctx = _tracker.createInstance<DictionarySchemaClauseContext>(_ctx, getState());
  enterRule(_localctx, 22, ClickHouseParser::RuleDictionarySchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(656);
    match(ClickHouseParser::LPAREN);
    setState(657);
    dictionaryAttrDfnt();
    setState(662);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(658);
      match(ClickHouseParser::COMMA);
      setState(659);
      dictionaryAttrDfnt();
      setState(664);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(665);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionaryAttrDfntContext ------------------------------------------------------------------

ClickHouseParser::DictionaryAttrDfntContext::DictionaryAttrDfntContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::DictionaryAttrDfntContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::DictionaryAttrDfntContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionaryAttrDfntContext::DEFAULT() {
  return getTokens(ClickHouseParser::DEFAULT);
}

tree::TerminalNode* ClickHouseParser::DictionaryAttrDfntContext::DEFAULT(size_t i) {
  return getToken(ClickHouseParser::DEFAULT, i);
}

std::vector<ClickHouseParser::LiteralContext *> ClickHouseParser::DictionaryAttrDfntContext::literal() {
  return getRuleContexts<ClickHouseParser::LiteralContext>();
}

ClickHouseParser::LiteralContext* ClickHouseParser::DictionaryAttrDfntContext::literal(size_t i) {
  return getRuleContext<ClickHouseParser::LiteralContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionaryAttrDfntContext::EXPRESSION() {
  return getTokens(ClickHouseParser::EXPRESSION);
}

tree::TerminalNode* ClickHouseParser::DictionaryAttrDfntContext::EXPRESSION(size_t i) {
  return getToken(ClickHouseParser::EXPRESSION, i);
}

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::DictionaryAttrDfntContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::DictionaryAttrDfntContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionaryAttrDfntContext::HIERARCHICAL() {
  return getTokens(ClickHouseParser::HIERARCHICAL);
}

tree::TerminalNode* ClickHouseParser::DictionaryAttrDfntContext::HIERARCHICAL(size_t i) {
  return getToken(ClickHouseParser::HIERARCHICAL, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionaryAttrDfntContext::INJECTIVE() {
  return getTokens(ClickHouseParser::INJECTIVE);
}

tree::TerminalNode* ClickHouseParser::DictionaryAttrDfntContext::INJECTIVE(size_t i) {
  return getToken(ClickHouseParser::INJECTIVE, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DictionaryAttrDfntContext::IS_OBJECT_ID() {
  return getTokens(ClickHouseParser::IS_OBJECT_ID);
}

tree::TerminalNode* ClickHouseParser::DictionaryAttrDfntContext::IS_OBJECT_ID(size_t i) {
  return getToken(ClickHouseParser::IS_OBJECT_ID, i);
}


size_t ClickHouseParser::DictionaryAttrDfntContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionaryAttrDfnt;
}

antlrcpp::Any ClickHouseParser::DictionaryAttrDfntContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionaryAttrDfnt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionaryAttrDfntContext* ClickHouseParser::dictionaryAttrDfnt() {
  DictionaryAttrDfntContext *_localctx = _tracker.createInstance<DictionaryAttrDfntContext>(_ctx, getState());
  enterRule(_localctx, 24, ClickHouseParser::RuleDictionaryAttrDfnt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(667);
    identifier();
    setState(668);
    columnTypeExpr();
    setState(690);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(688);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 73, _ctx)) {
        case 1: {
          setState(669);

          if (!(!_localctx->attrs.count("default"))) throw FailedPredicateException(this, "!$attrs.count(\"default\")");
          setState(670);
          match(ClickHouseParser::DEFAULT);
          setState(671);
          literal();
          _localctx->attrs.insert("default");
          break;
        }

        case 2: {
          setState(674);

          if (!(!_localctx->attrs.count("expression"))) throw FailedPredicateException(this, "!$attrs.count(\"expression\")");
          setState(675);
          match(ClickHouseParser::EXPRESSION);
          setState(676);
          columnExpr(0);
          _localctx->attrs.insert("expression");
          break;
        }

        case 3: {
          setState(679);

          if (!(!_localctx->attrs.count("hierarchical"))) throw FailedPredicateException(this, "!$attrs.count(\"hierarchical\")");
          setState(680);
          match(ClickHouseParser::HIERARCHICAL);
          _localctx->attrs.insert("hierarchical");
          break;
        }

        case 4: {
          setState(682);

          if (!(!_localctx->attrs.count("injective"))) throw FailedPredicateException(this, "!$attrs.count(\"injective\")");
          setState(683);
          match(ClickHouseParser::INJECTIVE);
          _localctx->attrs.insert("injective");
          break;
        }

        case 5: {
          setState(685);

          if (!(!_localctx->attrs.count("is_object_id"))) throw FailedPredicateException(this, "!$attrs.count(\"is_object_id\")");
          setState(686);
          match(ClickHouseParser::IS_OBJECT_ID);
          _localctx->attrs.insert("is_object_id");
          break;
        }

        } 
      }
      setState(692);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionaryEngineClauseContext ------------------------------------------------------------------

ClickHouseParser::DictionaryEngineClauseContext::DictionaryEngineClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::DictionaryPrimaryKeyClauseContext* ClickHouseParser::DictionaryEngineClauseContext::dictionaryPrimaryKeyClause() {
  return getRuleContext<ClickHouseParser::DictionaryPrimaryKeyClauseContext>(0);
}

std::vector<ClickHouseParser::SourceClauseContext *> ClickHouseParser::DictionaryEngineClauseContext::sourceClause() {
  return getRuleContexts<ClickHouseParser::SourceClauseContext>();
}

ClickHouseParser::SourceClauseContext* ClickHouseParser::DictionaryEngineClauseContext::sourceClause(size_t i) {
  return getRuleContext<ClickHouseParser::SourceClauseContext>(i);
}

std::vector<ClickHouseParser::LifetimeClauseContext *> ClickHouseParser::DictionaryEngineClauseContext::lifetimeClause() {
  return getRuleContexts<ClickHouseParser::LifetimeClauseContext>();
}

ClickHouseParser::LifetimeClauseContext* ClickHouseParser::DictionaryEngineClauseContext::lifetimeClause(size_t i) {
  return getRuleContext<ClickHouseParser::LifetimeClauseContext>(i);
}

std::vector<ClickHouseParser::LayoutClauseContext *> ClickHouseParser::DictionaryEngineClauseContext::layoutClause() {
  return getRuleContexts<ClickHouseParser::LayoutClauseContext>();
}

ClickHouseParser::LayoutClauseContext* ClickHouseParser::DictionaryEngineClauseContext::layoutClause(size_t i) {
  return getRuleContext<ClickHouseParser::LayoutClauseContext>(i);
}

std::vector<ClickHouseParser::RangeClauseContext *> ClickHouseParser::DictionaryEngineClauseContext::rangeClause() {
  return getRuleContexts<ClickHouseParser::RangeClauseContext>();
}

ClickHouseParser::RangeClauseContext* ClickHouseParser::DictionaryEngineClauseContext::rangeClause(size_t i) {
  return getRuleContext<ClickHouseParser::RangeClauseContext>(i);
}

std::vector<ClickHouseParser::DictionarySettingsClauseContext *> ClickHouseParser::DictionaryEngineClauseContext::dictionarySettingsClause() {
  return getRuleContexts<ClickHouseParser::DictionarySettingsClauseContext>();
}

ClickHouseParser::DictionarySettingsClauseContext* ClickHouseParser::DictionaryEngineClauseContext::dictionarySettingsClause(size_t i) {
  return getRuleContext<ClickHouseParser::DictionarySettingsClauseContext>(i);
}


size_t ClickHouseParser::DictionaryEngineClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionaryEngineClause;
}

antlrcpp::Any ClickHouseParser::DictionaryEngineClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionaryEngineClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionaryEngineClauseContext* ClickHouseParser::dictionaryEngineClause() {
  DictionaryEngineClauseContext *_localctx = _tracker.createInstance<DictionaryEngineClauseContext>(_ctx, getState());
  enterRule(_localctx, 26, ClickHouseParser::RuleDictionaryEngineClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(694);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
    case 1: {
      setState(693);
      dictionaryPrimaryKeyClause();
      break;
    }

    }
    setState(718);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(716);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 76, _ctx)) {
        case 1: {
          setState(696);

          if (!(!_localctx->clauses.count("source"))) throw FailedPredicateException(this, "!$clauses.count(\"source\")");
          setState(697);
          sourceClause();
          _localctx->clauses.insert("source");
          break;
        }

        case 2: {
          setState(700);

          if (!(!_localctx->clauses.count("lifetime"))) throw FailedPredicateException(this, "!$clauses.count(\"lifetime\")");
          setState(701);
          lifetimeClause();
          _localctx->clauses.insert("lifetime");
          break;
        }

        case 3: {
          setState(704);

          if (!(!_localctx->clauses.count("layout"))) throw FailedPredicateException(this, "!$clauses.count(\"layout\")");
          setState(705);
          layoutClause();
          _localctx->clauses.insert("layout");
          break;
        }

        case 4: {
          setState(708);

          if (!(!_localctx->clauses.count("range"))) throw FailedPredicateException(this, "!$clauses.count(\"range\")");
          setState(709);
          rangeClause();
          _localctx->clauses.insert("range");
          break;
        }

        case 5: {
          setState(712);

          if (!(!_localctx->clauses.count("settings"))) throw FailedPredicateException(this, "!$clauses.count(\"settings\")");
          setState(713);
          dictionarySettingsClause();
          _localctx->clauses.insert("settings");
          break;
        }

        } 
      }
      setState(720);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionaryPrimaryKeyClauseContext ------------------------------------------------------------------

ClickHouseParser::DictionaryPrimaryKeyClauseContext::DictionaryPrimaryKeyClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::DictionaryPrimaryKeyClauseContext::PRIMARY() {
  return getToken(ClickHouseParser::PRIMARY, 0);
}

tree::TerminalNode* ClickHouseParser::DictionaryPrimaryKeyClauseContext::KEY() {
  return getToken(ClickHouseParser::KEY, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::DictionaryPrimaryKeyClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::DictionaryPrimaryKeyClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionaryPrimaryKeyClause;
}

antlrcpp::Any ClickHouseParser::DictionaryPrimaryKeyClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionaryPrimaryKeyClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionaryPrimaryKeyClauseContext* ClickHouseParser::dictionaryPrimaryKeyClause() {
  DictionaryPrimaryKeyClauseContext *_localctx = _tracker.createInstance<DictionaryPrimaryKeyClauseContext>(_ctx, getState());
  enterRule(_localctx, 28, ClickHouseParser::RuleDictionaryPrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(721);
    match(ClickHouseParser::PRIMARY);
    setState(722);
    match(ClickHouseParser::KEY);
    setState(723);
    columnExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionaryArgExprContext ------------------------------------------------------------------

ClickHouseParser::DictionaryArgExprContext::DictionaryArgExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::DictionaryArgExprContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::DictionaryArgExprContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

ClickHouseParser::LiteralContext* ClickHouseParser::DictionaryArgExprContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}

tree::TerminalNode* ClickHouseParser::DictionaryArgExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::DictionaryArgExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}


size_t ClickHouseParser::DictionaryArgExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionaryArgExpr;
}

antlrcpp::Any ClickHouseParser::DictionaryArgExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionaryArgExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionaryArgExprContext* ClickHouseParser::dictionaryArgExpr() {
  DictionaryArgExprContext *_localctx = _tracker.createInstance<DictionaryArgExprContext>(_ctx, getState());
  enterRule(_localctx, 30, ClickHouseParser::RuleDictionaryArgExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(725);
    identifier();
    setState(732);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::AST:
      case ClickHouseParser::ASYNC:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CLEAR:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::CODEC:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COLUMN:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CONSTRAINT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::CUBE:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DATABASES:
      case ClickHouseParser::DATE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELAY:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DICTIONARIES:
      case ClickHouseParser::DICTIONARY:
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EVENTS:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXPRESSION:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FREEZE:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HIERARCHICAL:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INJECTIVE:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::IS_OBJECT_ID:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::KILL:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LAYOUT:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIFETIME:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LIVE:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZE:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MAX:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MIN:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::MOVE:
      case ClickHouseParser::MUTATION:
      case ClickHouseParser::NO:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::POPULATE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RANGE:
      case ClickHouseParser::RELOAD:
      case ClickHouseParser::REMOVE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::REPLICATED:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::ROLLUP:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SENDS:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SOURCE:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::TEST:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMEOUT:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOP:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::UPDATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::UUID:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        setState(726);
        identifier();
        setState(729);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::LPAREN) {
          setState(727);
          match(ClickHouseParser::LPAREN);
          setState(728);
          match(ClickHouseParser::RPAREN);
        }
        break;
      }

      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::NULL_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::OCTAL_LITERAL:
      case ClickHouseParser::DECIMAL_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::STRING_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::DOT:
      case ClickHouseParser::PLUS: {
        setState(731);
        literal();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SourceClauseContext ------------------------------------------------------------------

ClickHouseParser::SourceClauseContext::SourceClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SourceClauseContext::SOURCE() {
  return getToken(ClickHouseParser::SOURCE, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SourceClauseContext::LPAREN() {
  return getTokens(ClickHouseParser::LPAREN);
}

tree::TerminalNode* ClickHouseParser::SourceClauseContext::LPAREN(size_t i) {
  return getToken(ClickHouseParser::LPAREN, i);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::SourceClauseContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SourceClauseContext::RPAREN() {
  return getTokens(ClickHouseParser::RPAREN);
}

tree::TerminalNode* ClickHouseParser::SourceClauseContext::RPAREN(size_t i) {
  return getToken(ClickHouseParser::RPAREN, i);
}

std::vector<ClickHouseParser::DictionaryArgExprContext *> ClickHouseParser::SourceClauseContext::dictionaryArgExpr() {
  return getRuleContexts<ClickHouseParser::DictionaryArgExprContext>();
}

ClickHouseParser::DictionaryArgExprContext* ClickHouseParser::SourceClauseContext::dictionaryArgExpr(size_t i) {
  return getRuleContext<ClickHouseParser::DictionaryArgExprContext>(i);
}


size_t ClickHouseParser::SourceClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSourceClause;
}

antlrcpp::Any ClickHouseParser::SourceClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSourceClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SourceClauseContext* ClickHouseParser::sourceClause() {
  SourceClauseContext *_localctx = _tracker.createInstance<SourceClauseContext>(_ctx, getState());
  enterRule(_localctx, 32, ClickHouseParser::RuleSourceClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(734);
    match(ClickHouseParser::SOURCE);
    setState(735);
    match(ClickHouseParser::LPAREN);
    setState(736);
    identifier();
    setState(737);
    match(ClickHouseParser::LPAREN);
    setState(741);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::AND)
      | (1ULL << ClickHouseParser::ANTI)
      | (1ULL << ClickHouseParser::ANY)
      | (1ULL << ClickHouseParser::ARRAY)
      | (1ULL << ClickHouseParser::AS)
      | (1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::ASOF)
      | (1ULL << ClickHouseParser::AST)
      | (1ULL << ClickHouseParser::ASYNC)
      | (1ULL << ClickHouseParser::ATTACH)
      | (1ULL << ClickHouseParser::BETWEEN)
      | (1ULL << ClickHouseParser::BOTH)
      | (1ULL << ClickHouseParser::BY)
      | (1ULL << ClickHouseParser::CASE)
      | (1ULL << ClickHouseParser::CAST)
      | (1ULL << ClickHouseParser::CHECK)
      | (1ULL << ClickHouseParser::CLEAR)
      | (1ULL << ClickHouseParser::CLUSTER)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::COLUMN)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::CONSTRAINT)
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::CUBE)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DATABASES)
      | (1ULL << ClickHouseParser::DATE)
      | (1ULL << ClickHouseParser::DAY)
      | (1ULL << ClickHouseParser::DEDUPLICATE)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::DELAY)
      | (1ULL << ClickHouseParser::DELETE)
      | (1ULL << ClickHouseParser::DESC)
      | (1ULL << ClickHouseParser::DESCENDING)
      | (1ULL << ClickHouseParser::DESCRIBE)
      | (1ULL << ClickHouseParser::DETACH)
      | (1ULL << ClickHouseParser::DICTIONARIES)
      | (1ULL << ClickHouseParser::DICTIONARY)
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EVENTS)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXPRESSION)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
      | (1ULL << (ClickHouseParser::FROM - 64))
      | (1ULL << (ClickHouseParser::FULL - 64))
      | (1ULL << (ClickHouseParser::FUNCTION - 64))
      | (1ULL << (ClickHouseParser::GLOBAL - 64))
      | (1ULL << (ClickHouseParser::GRANULARITY - 64))
      | (1ULL << (ClickHouseParser::GROUP - 64))
      | (1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
      | (1ULL << (ClickHouseParser::HOUR - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INJECTIVE - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::KILL - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LAYOUT - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIFETIME - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LIVE - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MAX - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MIN - 64))
      | (1ULL << (ClickHouseParser::MINUTE - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MONTH - 64))
      | (1ULL << (ClickHouseParser::MOVE - 64))
      | (1ULL << (ClickHouseParser::MUTATION - 64))
      | (1ULL << (ClickHouseParser::NO - 64))
      | (1ULL << (ClickHouseParser::NOT - 64))
      | (1ULL << (ClickHouseParser::NULLS - 64))
      | (1ULL << (ClickHouseParser::OFFSET - 64))
      | (1ULL << (ClickHouseParser::ON - 64))
      | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
      | (1ULL << (ClickHouseParser::OR - 64))
      | (1ULL << (ClickHouseParser::ORDER - 64))
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::POPULATE - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
      | (1ULL << (ClickHouseParser::RELOAD - 128))
      | (1ULL << (ClickHouseParser::REMOVE - 128))
      | (1ULL << (ClickHouseParser::RENAME - 128))
      | (1ULL << (ClickHouseParser::REPLACE - 128))
      | (1ULL << (ClickHouseParser::REPLICA - 128))
      | (1ULL << (ClickHouseParser::REPLICATED - 128))
      | (1ULL << (ClickHouseParser::RIGHT - 128))
      | (1ULL << (ClickHouseParser::ROLLUP - 128))
      | (1ULL << (ClickHouseParser::SAMPLE - 128))
      | (1ULL << (ClickHouseParser::SECOND - 128))
      | (1ULL << (ClickHouseParser::SELECT - 128))
      | (1ULL << (ClickHouseParser::SEMI - 128))
      | (1ULL << (ClickHouseParser::SENDS - 128))
      | (1ULL << (ClickHouseParser::SET - 128))
      | (1ULL << (ClickHouseParser::SETTINGS - 128))
      | (1ULL << (ClickHouseParser::SHOW - 128))
      | (1ULL << (ClickHouseParser::SOURCE - 128))
      | (1ULL << (ClickHouseParser::START - 128))
      | (1ULL << (ClickHouseParser::STOP - 128))
      | (1ULL << (ClickHouseParser::SUBSTRING - 128))
      | (1ULL << (ClickHouseParser::SYNC - 128))
      | (1ULL << (ClickHouseParser::SYNTAX - 128))
      | (1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::TEST - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMEOUT - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOP - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::UPDATE - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::UUID - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WATCH - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WHERE - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::YEAR - 128))
      | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
      | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
      | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
      setState(738);
      dictionaryArgExpr();
      setState(743);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(744);
    match(ClickHouseParser::RPAREN);
    setState(745);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LifetimeClauseContext ------------------------------------------------------------------

ClickHouseParser::LifetimeClauseContext::LifetimeClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::LIFETIME() {
  return getToken(ClickHouseParser::LIFETIME, 0);
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::LifetimeClauseContext::DECIMAL_LITERAL() {
  return getTokens(ClickHouseParser::DECIMAL_LITERAL);
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::DECIMAL_LITERAL(size_t i) {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, i);
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::MIN() {
  return getToken(ClickHouseParser::MIN, 0);
}

tree::TerminalNode* ClickHouseParser::LifetimeClauseContext::MAX() {
  return getToken(ClickHouseParser::MAX, 0);
}


size_t ClickHouseParser::LifetimeClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleLifetimeClause;
}

antlrcpp::Any ClickHouseParser::LifetimeClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLifetimeClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LifetimeClauseContext* ClickHouseParser::lifetimeClause() {
  LifetimeClauseContext *_localctx = _tracker.createInstance<LifetimeClauseContext>(_ctx, getState());
  enterRule(_localctx, 34, ClickHouseParser::RuleLifetimeClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(747);
    match(ClickHouseParser::LIFETIME);
    setState(748);
    match(ClickHouseParser::LPAREN);
    setState(758);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::DECIMAL_LITERAL: {
        setState(749);
        match(ClickHouseParser::DECIMAL_LITERAL);
        break;
      }

      case ClickHouseParser::MIN: {
        setState(750);
        match(ClickHouseParser::MIN);
        setState(751);
        match(ClickHouseParser::DECIMAL_LITERAL);
        setState(752);
        match(ClickHouseParser::MAX);
        setState(753);
        match(ClickHouseParser::DECIMAL_LITERAL);
        break;
      }

      case ClickHouseParser::MAX: {
        setState(754);
        match(ClickHouseParser::MAX);
        setState(755);
        match(ClickHouseParser::DECIMAL_LITERAL);
        setState(756);
        match(ClickHouseParser::MIN);
        setState(757);
        match(ClickHouseParser::DECIMAL_LITERAL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(760);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LayoutClauseContext ------------------------------------------------------------------

ClickHouseParser::LayoutClauseContext::LayoutClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::LayoutClauseContext::LAYOUT() {
  return getToken(ClickHouseParser::LAYOUT, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::LayoutClauseContext::LPAREN() {
  return getTokens(ClickHouseParser::LPAREN);
}

tree::TerminalNode* ClickHouseParser::LayoutClauseContext::LPAREN(size_t i) {
  return getToken(ClickHouseParser::LPAREN, i);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::LayoutClauseContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::LayoutClauseContext::RPAREN() {
  return getTokens(ClickHouseParser::RPAREN);
}

tree::TerminalNode* ClickHouseParser::LayoutClauseContext::RPAREN(size_t i) {
  return getToken(ClickHouseParser::RPAREN, i);
}

std::vector<ClickHouseParser::DictionaryArgExprContext *> ClickHouseParser::LayoutClauseContext::dictionaryArgExpr() {
  return getRuleContexts<ClickHouseParser::DictionaryArgExprContext>();
}

ClickHouseParser::DictionaryArgExprContext* ClickHouseParser::LayoutClauseContext::dictionaryArgExpr(size_t i) {
  return getRuleContext<ClickHouseParser::DictionaryArgExprContext>(i);
}


size_t ClickHouseParser::LayoutClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleLayoutClause;
}

antlrcpp::Any ClickHouseParser::LayoutClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLayoutClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LayoutClauseContext* ClickHouseParser::layoutClause() {
  LayoutClauseContext *_localctx = _tracker.createInstance<LayoutClauseContext>(_ctx, getState());
  enterRule(_localctx, 36, ClickHouseParser::RuleLayoutClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(762);
    match(ClickHouseParser::LAYOUT);
    setState(763);
    match(ClickHouseParser::LPAREN);
    setState(764);
    identifier();
    setState(765);
    match(ClickHouseParser::LPAREN);
    setState(769);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::AND)
      | (1ULL << ClickHouseParser::ANTI)
      | (1ULL << ClickHouseParser::ANY)
      | (1ULL << ClickHouseParser::ARRAY)
      | (1ULL << ClickHouseParser::AS)
      | (1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::ASOF)
      | (1ULL << ClickHouseParser::AST)
      | (1ULL << ClickHouseParser::ASYNC)
      | (1ULL << ClickHouseParser::ATTACH)
      | (1ULL << ClickHouseParser::BETWEEN)
      | (1ULL << ClickHouseParser::BOTH)
      | (1ULL << ClickHouseParser::BY)
      | (1ULL << ClickHouseParser::CASE)
      | (1ULL << ClickHouseParser::CAST)
      | (1ULL << ClickHouseParser::CHECK)
      | (1ULL << ClickHouseParser::CLEAR)
      | (1ULL << ClickHouseParser::CLUSTER)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::COLUMN)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::CONSTRAINT)
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::CUBE)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DATABASES)
      | (1ULL << ClickHouseParser::DATE)
      | (1ULL << ClickHouseParser::DAY)
      | (1ULL << ClickHouseParser::DEDUPLICATE)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::DELAY)
      | (1ULL << ClickHouseParser::DELETE)
      | (1ULL << ClickHouseParser::DESC)
      | (1ULL << ClickHouseParser::DESCENDING)
      | (1ULL << ClickHouseParser::DESCRIBE)
      | (1ULL << ClickHouseParser::DETACH)
      | (1ULL << ClickHouseParser::DICTIONARIES)
      | (1ULL << ClickHouseParser::DICTIONARY)
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EVENTS)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXPRESSION)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
      | (1ULL << (ClickHouseParser::FROM - 64))
      | (1ULL << (ClickHouseParser::FULL - 64))
      | (1ULL << (ClickHouseParser::FUNCTION - 64))
      | (1ULL << (ClickHouseParser::GLOBAL - 64))
      | (1ULL << (ClickHouseParser::GRANULARITY - 64))
      | (1ULL << (ClickHouseParser::GROUP - 64))
      | (1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
      | (1ULL << (ClickHouseParser::HOUR - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INJECTIVE - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::KILL - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LAYOUT - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIFETIME - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LIVE - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MAX - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MIN - 64))
      | (1ULL << (ClickHouseParser::MINUTE - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MONTH - 64))
      | (1ULL << (ClickHouseParser::MOVE - 64))
      | (1ULL << (ClickHouseParser::MUTATION - 64))
      | (1ULL << (ClickHouseParser::NO - 64))
      | (1ULL << (ClickHouseParser::NOT - 64))
      | (1ULL << (ClickHouseParser::NULLS - 64))
      | (1ULL << (ClickHouseParser::OFFSET - 64))
      | (1ULL << (ClickHouseParser::ON - 64))
      | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
      | (1ULL << (ClickHouseParser::OR - 64))
      | (1ULL << (ClickHouseParser::ORDER - 64))
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::POPULATE - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
      | (1ULL << (ClickHouseParser::RELOAD - 128))
      | (1ULL << (ClickHouseParser::REMOVE - 128))
      | (1ULL << (ClickHouseParser::RENAME - 128))
      | (1ULL << (ClickHouseParser::REPLACE - 128))
      | (1ULL << (ClickHouseParser::REPLICA - 128))
      | (1ULL << (ClickHouseParser::REPLICATED - 128))
      | (1ULL << (ClickHouseParser::RIGHT - 128))
      | (1ULL << (ClickHouseParser::ROLLUP - 128))
      | (1ULL << (ClickHouseParser::SAMPLE - 128))
      | (1ULL << (ClickHouseParser::SECOND - 128))
      | (1ULL << (ClickHouseParser::SELECT - 128))
      | (1ULL << (ClickHouseParser::SEMI - 128))
      | (1ULL << (ClickHouseParser::SENDS - 128))
      | (1ULL << (ClickHouseParser::SET - 128))
      | (1ULL << (ClickHouseParser::SETTINGS - 128))
      | (1ULL << (ClickHouseParser::SHOW - 128))
      | (1ULL << (ClickHouseParser::SOURCE - 128))
      | (1ULL << (ClickHouseParser::START - 128))
      | (1ULL << (ClickHouseParser::STOP - 128))
      | (1ULL << (ClickHouseParser::SUBSTRING - 128))
      | (1ULL << (ClickHouseParser::SYNC - 128))
      | (1ULL << (ClickHouseParser::SYNTAX - 128))
      | (1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::TEST - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMEOUT - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOP - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::UPDATE - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::UUID - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WATCH - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WHERE - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::YEAR - 128))
      | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
      | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
      | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
      setState(766);
      dictionaryArgExpr();
      setState(771);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(772);
    match(ClickHouseParser::RPAREN);
    setState(773);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RangeClauseContext ------------------------------------------------------------------

ClickHouseParser::RangeClauseContext::RangeClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::RangeClauseContext::RANGE() {
  return getToken(ClickHouseParser::RANGE, 0);
}

tree::TerminalNode* ClickHouseParser::RangeClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::RangeClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::RangeClauseContext::MIN() {
  return getToken(ClickHouseParser::MIN, 0);
}

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::RangeClauseContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::RangeClauseContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::RangeClauseContext::MAX() {
  return getToken(ClickHouseParser::MAX, 0);
}


size_t ClickHouseParser::RangeClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleRangeClause;
}

antlrcpp::Any ClickHouseParser::RangeClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitRangeClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::RangeClauseContext* ClickHouseParser::rangeClause() {
  RangeClauseContext *_localctx = _tracker.createInstance<RangeClauseContext>(_ctx, getState());
  enterRule(_localctx, 38, ClickHouseParser::RuleRangeClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(775);
    match(ClickHouseParser::RANGE);
    setState(776);
    match(ClickHouseParser::LPAREN);
    setState(787);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::MIN: {
        setState(777);
        match(ClickHouseParser::MIN);
        setState(778);
        identifier();
        setState(779);
        match(ClickHouseParser::MAX);
        setState(780);
        identifier();
        break;
      }

      case ClickHouseParser::MAX: {
        setState(782);
        match(ClickHouseParser::MAX);
        setState(783);
        identifier();
        setState(784);
        match(ClickHouseParser::MIN);
        setState(785);
        identifier();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(789);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DictionarySettingsClauseContext ------------------------------------------------------------------

ClickHouseParser::DictionarySettingsClauseContext::DictionarySettingsClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::DictionarySettingsClauseContext::SETTINGS() {
  return getToken(ClickHouseParser::SETTINGS, 0);
}

tree::TerminalNode* ClickHouseParser::DictionarySettingsClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SettingExprListContext* ClickHouseParser::DictionarySettingsClauseContext::settingExprList() {
  return getRuleContext<ClickHouseParser::SettingExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::DictionarySettingsClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}


size_t ClickHouseParser::DictionarySettingsClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDictionarySettingsClause;
}

antlrcpp::Any ClickHouseParser::DictionarySettingsClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDictionarySettingsClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DictionarySettingsClauseContext* ClickHouseParser::dictionarySettingsClause() {
  DictionarySettingsClauseContext *_localctx = _tracker.createInstance<DictionarySettingsClauseContext>(_ctx, getState());
  enterRule(_localctx, 40, ClickHouseParser::RuleDictionarySettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(791);
    match(ClickHouseParser::SETTINGS);
    setState(792);
    match(ClickHouseParser::LPAREN);
    setState(793);
    settingExprList();
    setState(794);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ClusterClauseContext ------------------------------------------------------------------

ClickHouseParser::ClusterClauseContext::ClusterClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ClusterClauseContext::ON() {
  return getToken(ClickHouseParser::ON, 0);
}

tree::TerminalNode* ClickHouseParser::ClusterClauseContext::CLUSTER() {
  return getToken(ClickHouseParser::CLUSTER, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ClusterClauseContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ClusterClauseContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}


size_t ClickHouseParser::ClusterClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleClusterClause;
}

antlrcpp::Any ClickHouseParser::ClusterClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitClusterClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::clusterClause() {
  ClusterClauseContext *_localctx = _tracker.createInstance<ClusterClauseContext>(_ctx, getState());
  enterRule(_localctx, 42, ClickHouseParser::RuleClusterClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(796);
    match(ClickHouseParser::ON);
    setState(797);
    match(ClickHouseParser::CLUSTER);
    setState(800);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::AST:
      case ClickHouseParser::ASYNC:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CLEAR:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::CODEC:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COLUMN:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CONSTRAINT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::CUBE:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DATABASES:
      case ClickHouseParser::DATE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELAY:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DICTIONARIES:
      case ClickHouseParser::DICTIONARY:
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EVENTS:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXPRESSION:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FREEZE:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HIERARCHICAL:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INJECTIVE:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::IS_OBJECT_ID:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::KILL:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LAYOUT:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIFETIME:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LIVE:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZE:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MAX:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MIN:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::MOVE:
      case ClickHouseParser::MUTATION:
      case ClickHouseParser::NO:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::POPULATE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RANGE:
      case ClickHouseParser::RELOAD:
      case ClickHouseParser::REMOVE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::REPLICATED:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::ROLLUP:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SENDS:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SOURCE:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::TEST:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMEOUT:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOP:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::UPDATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::UUID:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        setState(798);
        identifier();
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        setState(799);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UuidClauseContext ------------------------------------------------------------------

ClickHouseParser::UuidClauseContext::UuidClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::UuidClauseContext::UUID() {
  return getToken(ClickHouseParser::UUID, 0);
}

tree::TerminalNode* ClickHouseParser::UuidClauseContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}


size_t ClickHouseParser::UuidClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleUuidClause;
}

antlrcpp::Any ClickHouseParser::UuidClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitUuidClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::UuidClauseContext* ClickHouseParser::uuidClause() {
  UuidClauseContext *_localctx = _tracker.createInstance<UuidClauseContext>(_ctx, getState());
  enterRule(_localctx, 44, ClickHouseParser::RuleUuidClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(802);
    match(ClickHouseParser::UUID);
    setState(803);
    match(ClickHouseParser::STRING_LITERAL);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DestinationClauseContext ------------------------------------------------------------------

ClickHouseParser::DestinationClauseContext::DestinationClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::DestinationClauseContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::DestinationClauseContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}


size_t ClickHouseParser::DestinationClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDestinationClause;
}

antlrcpp::Any ClickHouseParser::DestinationClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDestinationClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DestinationClauseContext* ClickHouseParser::destinationClause() {
  DestinationClauseContext *_localctx = _tracker.createInstance<DestinationClauseContext>(_ctx, getState());
  enterRule(_localctx, 46, ClickHouseParser::RuleDestinationClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(805);
    match(ClickHouseParser::TO);
    setState(806);
    tableIdentifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubqueryClauseContext ------------------------------------------------------------------

ClickHouseParser::SubqueryClauseContext::SubqueryClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SubqueryClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::SubqueryClauseContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}


size_t ClickHouseParser::SubqueryClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSubqueryClause;
}

antlrcpp::Any ClickHouseParser::SubqueryClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSubqueryClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SubqueryClauseContext* ClickHouseParser::subqueryClause() {
  SubqueryClauseContext *_localctx = _tracker.createInstance<SubqueryClauseContext>(_ctx, getState());
  enterRule(_localctx, 48, ClickHouseParser::RuleSubqueryClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(808);
    match(ClickHouseParser::AS);
    setState(809);
    selectUnionStmt();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableSchemaClauseContext ------------------------------------------------------------------

ClickHouseParser::TableSchemaClauseContext::TableSchemaClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::TableSchemaClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableSchemaClause;
}

void ClickHouseParser::TableSchemaClauseContext::copyFrom(TableSchemaClauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SchemaAsTableClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaAsTableClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SchemaAsTableClauseContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SchemaAsTableClauseContext::SchemaAsTableClauseContext(TableSchemaClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::SchemaAsTableClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSchemaAsTableClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SchemaAsFunctionClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaAsFunctionClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::TableFunctionExprContext* ClickHouseParser::SchemaAsFunctionClauseContext::tableFunctionExpr() {
  return getRuleContext<ClickHouseParser::TableFunctionExprContext>(0);
}

ClickHouseParser::SchemaAsFunctionClauseContext::SchemaAsFunctionClauseContext(TableSchemaClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::SchemaAsFunctionClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSchemaAsFunctionClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SchemaDescriptionClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaDescriptionClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::TableElementExprContext *> ClickHouseParser::SchemaDescriptionClauseContext::tableElementExpr() {
  return getRuleContexts<ClickHouseParser::TableElementExprContext>();
}

ClickHouseParser::TableElementExprContext* ClickHouseParser::SchemaDescriptionClauseContext::tableElementExpr(size_t i) {
  return getRuleContext<ClickHouseParser::TableElementExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::SchemaDescriptionClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SchemaDescriptionClauseContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::SchemaDescriptionClauseContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::SchemaDescriptionClauseContext::SchemaDescriptionClauseContext(TableSchemaClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::SchemaDescriptionClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSchemaDescriptionClause(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::TableSchemaClauseContext* ClickHouseParser::tableSchemaClause() {
  TableSchemaClauseContext *_localctx = _tracker.createInstance<TableSchemaClauseContext>(_ctx, getState());
  enterRule(_localctx, 50, ClickHouseParser::RuleTableSchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(826);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 86, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<TableSchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaDescriptionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(811);
      match(ClickHouseParser::LPAREN);
      setState(812);
      tableElementExpr();
      setState(817);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(813);
        match(ClickHouseParser::COMMA);
        setState(814);
        tableElementExpr();
        setState(819);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(820);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<TableSchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsTableClauseContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(822);
      match(ClickHouseParser::AS);
      setState(823);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<TableSchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsFunctionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(824);
      match(ClickHouseParser::AS);
      setState(825);
      tableFunctionExpr();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- EngineClauseContext ------------------------------------------------------------------

ClickHouseParser::EngineClauseContext::EngineClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::EngineExprContext* ClickHouseParser::EngineClauseContext::engineExpr() {
  return getRuleContext<ClickHouseParser::EngineExprContext>(0);
}

std::vector<ClickHouseParser::OrderByClauseContext *> ClickHouseParser::EngineClauseContext::orderByClause() {
  return getRuleContexts<ClickHouseParser::OrderByClauseContext>();
}

ClickHouseParser::OrderByClauseContext* ClickHouseParser::EngineClauseContext::orderByClause(size_t i) {
  return getRuleContext<ClickHouseParser::OrderByClauseContext>(i);
}

std::vector<ClickHouseParser::PartitionByClauseContext *> ClickHouseParser::EngineClauseContext::partitionByClause() {
  return getRuleContexts<ClickHouseParser::PartitionByClauseContext>();
}

ClickHouseParser::PartitionByClauseContext* ClickHouseParser::EngineClauseContext::partitionByClause(size_t i) {
  return getRuleContext<ClickHouseParser::PartitionByClauseContext>(i);
}

std::vector<ClickHouseParser::PrimaryKeyClauseContext *> ClickHouseParser::EngineClauseContext::primaryKeyClause() {
  return getRuleContexts<ClickHouseParser::PrimaryKeyClauseContext>();
}

ClickHouseParser::PrimaryKeyClauseContext* ClickHouseParser::EngineClauseContext::primaryKeyClause(size_t i) {
  return getRuleContext<ClickHouseParser::PrimaryKeyClauseContext>(i);
}

std::vector<ClickHouseParser::SampleByClauseContext *> ClickHouseParser::EngineClauseContext::sampleByClause() {
  return getRuleContexts<ClickHouseParser::SampleByClauseContext>();
}

ClickHouseParser::SampleByClauseContext* ClickHouseParser::EngineClauseContext::sampleByClause(size_t i) {
  return getRuleContext<ClickHouseParser::SampleByClauseContext>(i);
}

std::vector<ClickHouseParser::TtlClauseContext *> ClickHouseParser::EngineClauseContext::ttlClause() {
  return getRuleContexts<ClickHouseParser::TtlClauseContext>();
}

ClickHouseParser::TtlClauseContext* ClickHouseParser::EngineClauseContext::ttlClause(size_t i) {
  return getRuleContext<ClickHouseParser::TtlClauseContext>(i);
}

std::vector<ClickHouseParser::SettingsClauseContext *> ClickHouseParser::EngineClauseContext::settingsClause() {
  return getRuleContexts<ClickHouseParser::SettingsClauseContext>();
}

ClickHouseParser::SettingsClauseContext* ClickHouseParser::EngineClauseContext::settingsClause(size_t i) {
  return getRuleContext<ClickHouseParser::SettingsClauseContext>(i);
}


size_t ClickHouseParser::EngineClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleEngineClause;
}

antlrcpp::Any ClickHouseParser::EngineClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitEngineClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::EngineClauseContext* ClickHouseParser::engineClause() {
  EngineClauseContext *_localctx = _tracker.createInstance<EngineClauseContext>(_ctx, getState());
  enterRule(_localctx, 52, ClickHouseParser::RuleEngineClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(828);
    engineExpr();
    setState(855);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(853);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 87, _ctx)) {
        case 1: {
          setState(829);

          if (!(!_localctx->clauses.count("orderByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"orderByClause\")");
          setState(830);
          orderByClause();
          _localctx->clauses.insert("orderByClause");
          break;
        }

        case 2: {
          setState(833);

          if (!(!_localctx->clauses.count("partitionByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"partitionByClause\")");
          setState(834);
          partitionByClause();
          _localctx->clauses.insert("partitionByClause");
          break;
        }

        case 3: {
          setState(837);

          if (!(!_localctx->clauses.count("primaryKeyClause"))) throw FailedPredicateException(this, "!$clauses.count(\"primaryKeyClause\")");
          setState(838);
          primaryKeyClause();
          _localctx->clauses.insert("primaryKeyClause");
          break;
        }

        case 4: {
          setState(841);

          if (!(!_localctx->clauses.count("sampleByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"sampleByClause\")");
          setState(842);
          sampleByClause();
          _localctx->clauses.insert("sampleByClause");
          break;
        }

        case 5: {
          setState(845);

          if (!(!_localctx->clauses.count("ttlClause"))) throw FailedPredicateException(this, "!$clauses.count(\"ttlClause\")");
          setState(846);
          ttlClause();
          _localctx->clauses.insert("ttlClause");
          break;
        }

        case 6: {
          setState(849);

          if (!(!_localctx->clauses.count("settingsClause"))) throw FailedPredicateException(this, "!$clauses.count(\"settingsClause\")");
          setState(850);
          settingsClause();
          _localctx->clauses.insert("settingsClause");
          break;
        }

        } 
      }
      setState(857);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PartitionByClauseContext ------------------------------------------------------------------

ClickHouseParser::PartitionByClauseContext::PartitionByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::PartitionByClauseContext::PARTITION() {
  return getToken(ClickHouseParser::PARTITION, 0);
}

tree::TerminalNode* ClickHouseParser::PartitionByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::PartitionByClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::PartitionByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RulePartitionByClause;
}

antlrcpp::Any ClickHouseParser::PartitionByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitPartitionByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::PartitionByClauseContext* ClickHouseParser::partitionByClause() {
  PartitionByClauseContext *_localctx = _tracker.createInstance<PartitionByClauseContext>(_ctx, getState());
  enterRule(_localctx, 54, ClickHouseParser::RulePartitionByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(858);
    match(ClickHouseParser::PARTITION);
    setState(859);
    match(ClickHouseParser::BY);
    setState(860);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryKeyClauseContext ------------------------------------------------------------------

ClickHouseParser::PrimaryKeyClauseContext::PrimaryKeyClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::PrimaryKeyClauseContext::PRIMARY() {
  return getToken(ClickHouseParser::PRIMARY, 0);
}

tree::TerminalNode* ClickHouseParser::PrimaryKeyClauseContext::KEY() {
  return getToken(ClickHouseParser::KEY, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::PrimaryKeyClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::PrimaryKeyClauseContext::getRuleIndex() const {
  return ClickHouseParser::RulePrimaryKeyClause;
}

antlrcpp::Any ClickHouseParser::PrimaryKeyClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitPrimaryKeyClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::PrimaryKeyClauseContext* ClickHouseParser::primaryKeyClause() {
  PrimaryKeyClauseContext *_localctx = _tracker.createInstance<PrimaryKeyClauseContext>(_ctx, getState());
  enterRule(_localctx, 56, ClickHouseParser::RulePrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(862);
    match(ClickHouseParser::PRIMARY);
    setState(863);
    match(ClickHouseParser::KEY);
    setState(864);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SampleByClauseContext ------------------------------------------------------------------

ClickHouseParser::SampleByClauseContext::SampleByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SampleByClauseContext::SAMPLE() {
  return getToken(ClickHouseParser::SAMPLE, 0);
}

tree::TerminalNode* ClickHouseParser::SampleByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::SampleByClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::SampleByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSampleByClause;
}

antlrcpp::Any ClickHouseParser::SampleByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSampleByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SampleByClauseContext* ClickHouseParser::sampleByClause() {
  SampleByClauseContext *_localctx = _tracker.createInstance<SampleByClauseContext>(_ctx, getState());
  enterRule(_localctx, 58, ClickHouseParser::RuleSampleByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(866);
    match(ClickHouseParser::SAMPLE);
    setState(867);
    match(ClickHouseParser::BY);
    setState(868);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TtlClauseContext ------------------------------------------------------------------

ClickHouseParser::TtlClauseContext::TtlClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::TtlClauseContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

std::vector<ClickHouseParser::TtlExprContext *> ClickHouseParser::TtlClauseContext::ttlExpr() {
  return getRuleContexts<ClickHouseParser::TtlExprContext>();
}

ClickHouseParser::TtlExprContext* ClickHouseParser::TtlClauseContext::ttlExpr(size_t i) {
  return getRuleContext<ClickHouseParser::TtlExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::TtlClauseContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::TtlClauseContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::TtlClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleTtlClause;
}

antlrcpp::Any ClickHouseParser::TtlClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTtlClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TtlClauseContext* ClickHouseParser::ttlClause() {
  TtlClauseContext *_localctx = _tracker.createInstance<TtlClauseContext>(_ctx, getState());
  enterRule(_localctx, 60, ClickHouseParser::RuleTtlClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(870);
    match(ClickHouseParser::TTL);
    setState(871);
    ttlExpr();
    setState(876);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 89, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(872);
        match(ClickHouseParser::COMMA);
        setState(873);
        ttlExpr(); 
      }
      setState(878);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 89, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- EngineExprContext ------------------------------------------------------------------

ClickHouseParser::EngineExprContext::EngineExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::EngineExprContext::ENGINE() {
  return getToken(ClickHouseParser::ENGINE, 0);
}

ClickHouseParser::IdentifierOrNullContext* ClickHouseParser::EngineExprContext::identifierOrNull() {
  return getRuleContext<ClickHouseParser::IdentifierOrNullContext>(0);
}

tree::TerminalNode* ClickHouseParser::EngineExprContext::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

tree::TerminalNode* ClickHouseParser::EngineExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::EngineExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::EngineExprContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::EngineExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleEngineExpr;
}

antlrcpp::Any ClickHouseParser::EngineExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitEngineExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::EngineExprContext* ClickHouseParser::engineExpr() {
  EngineExprContext *_localctx = _tracker.createInstance<EngineExprContext>(_ctx, getState());
  enterRule(_localctx, 62, ClickHouseParser::RuleEngineExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(879);
    match(ClickHouseParser::ENGINE);
    setState(881);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EQ_SINGLE) {
      setState(880);
      match(ClickHouseParser::EQ_SINGLE);
    }
    setState(883);
    identifierOrNull();
    setState(889);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 92, _ctx)) {
    case 1: {
      setState(884);
      match(ClickHouseParser::LPAREN);
      setState(886);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NAN_SQL - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULL_SQL - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
        | (1ULL << (ClickHouseParser::DOT - 197))
        | (1ULL << (ClickHouseParser::LBRACKET - 197))
        | (1ULL << (ClickHouseParser::LPAREN - 197))
        | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
        setState(885);
        columnExprList();
      }
      setState(888);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableElementExprContext ------------------------------------------------------------------

ClickHouseParser::TableElementExprContext::TableElementExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::TableElementExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableElementExpr;
}

void ClickHouseParser::TableElementExprContext::copyFrom(TableElementExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TableElementExprProjectionContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::TableElementExprProjectionContext::PROJECTION() {
  return getToken(ClickHouseParser::PROJECTION, 0);
}

ClickHouseParser::TableProjectionDfntContext* ClickHouseParser::TableElementExprProjectionContext::tableProjectionDfnt() {
  return getRuleContext<ClickHouseParser::TableProjectionDfntContext>(0);
}

ClickHouseParser::TableElementExprProjectionContext::TableElementExprProjectionContext(TableElementExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableElementExprProjectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprProjection(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableElementExprConstraintContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::TableElementExprConstraintContext::CONSTRAINT() {
  return getToken(ClickHouseParser::CONSTRAINT, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::TableElementExprConstraintContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprConstraintContext::CHECK() {
  return getToken(ClickHouseParser::CHECK, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableElementExprConstraintContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::TableElementExprConstraintContext::TableElementExprConstraintContext(TableElementExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableElementExprConstraintContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprConstraint(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableElementExprColumnContext ------------------------------------------------------------------

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::TableElementExprColumnContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

ClickHouseParser::TableElementExprColumnContext::TableElementExprColumnContext(TableElementExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableElementExprColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableElementExprIndexContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::TableElementExprIndexContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

ClickHouseParser::TableIndexDfntContext* ClickHouseParser::TableElementExprIndexContext::tableIndexDfnt() {
  return getRuleContext<ClickHouseParser::TableIndexDfntContext>(0);
}

ClickHouseParser::TableElementExprIndexContext::TableElementExprIndexContext(TableElementExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableElementExprIndexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprIndex(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::TableElementExprContext* ClickHouseParser::tableElementExpr() {
  TableElementExprContext *_localctx = _tracker.createInstance<TableElementExprContext>(_ctx, getState());
  enterRule(_localctx, 64, ClickHouseParser::RuleTableElementExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(901);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 93, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(891);
      tableColumnDfnt();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprConstraintContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(892);
      match(ClickHouseParser::CONSTRAINT);
      setState(893);
      identifier();
      setState(894);
      match(ClickHouseParser::CHECK);
      setState(895);
      columnExpr(0);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprIndexContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(897);
      match(ClickHouseParser::INDEX);
      setState(898);
      tableIndexDfnt();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprProjectionContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(899);
      match(ClickHouseParser::PROJECTION);
      setState(900);
      tableProjectionDfnt();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableColumnDfntContext ------------------------------------------------------------------

ClickHouseParser::TableColumnDfntContext::TableColumnDfntContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::TableColumnDfntContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::TableColumnDfntContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

ClickHouseParser::TableColumnPropertyExprContext* ClickHouseParser::TableColumnDfntContext::tableColumnPropertyExpr() {
  return getRuleContext<ClickHouseParser::TableColumnPropertyExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableColumnDfntContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnDfntContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

ClickHouseParser::CodecExprContext* ClickHouseParser::TableColumnDfntContext::codecExpr() {
  return getRuleContext<ClickHouseParser::CodecExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableColumnDfntContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableColumnDfntContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::TableColumnDfntContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableColumnDfnt;
}

antlrcpp::Any ClickHouseParser::TableColumnDfntContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableColumnDfnt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::tableColumnDfnt() {
  TableColumnDfntContext *_localctx = _tracker.createInstance<TableColumnDfntContext>(_ctx, getState());
  enterRule(_localctx, 66, ClickHouseParser::RuleTableColumnDfnt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(935);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 102, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(903);
      nestedIdentifier();
      setState(904);
      columnTypeExpr();
      setState(906);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ALIAS

      || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED) {
        setState(905);
        tableColumnPropertyExpr();
      }
      setState(910);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(908);
        match(ClickHouseParser::COMMENT);
        setState(909);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(913);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::CODEC) {
        setState(912);
        codecExpr();
      }
      setState(917);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(915);
        match(ClickHouseParser::TTL);
        setState(916);
        columnExpr(0);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(919);
      nestedIdentifier();
      setState(921);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 98, _ctx)) {
      case 1: {
        setState(920);
        columnTypeExpr();
        break;
      }

      }
      setState(923);
      tableColumnPropertyExpr();
      setState(926);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(924);
        match(ClickHouseParser::COMMENT);
        setState(925);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(929);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::CODEC) {
        setState(928);
        codecExpr();
      }
      setState(933);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(931);
        match(ClickHouseParser::TTL);
        setState(932);
        columnExpr(0);
      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableColumnPropertyExprContext ------------------------------------------------------------------

ClickHouseParser::TableColumnPropertyExprContext::TableColumnPropertyExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableColumnPropertyExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyExprContext::DEFAULT() {
  return getToken(ClickHouseParser::DEFAULT, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyExprContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnPropertyExprContext::ALIAS() {
  return getToken(ClickHouseParser::ALIAS, 0);
}


size_t ClickHouseParser::TableColumnPropertyExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableColumnPropertyExpr;
}

antlrcpp::Any ClickHouseParser::TableColumnPropertyExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableColumnPropertyExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableColumnPropertyExprContext* ClickHouseParser::tableColumnPropertyExpr() {
  TableColumnPropertyExprContext *_localctx = _tracker.createInstance<TableColumnPropertyExprContext>(_ctx, getState());
  enterRule(_localctx, 68, ClickHouseParser::RuleTableColumnPropertyExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(937);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::ALIAS

    || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(938);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableIndexDfntContext ------------------------------------------------------------------

ClickHouseParser::TableIndexDfntContext::TableIndexDfntContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::TableIndexDfntContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableIndexDfntContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableIndexDfntContext::TYPE() {
  return getToken(ClickHouseParser::TYPE, 0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::TableIndexDfntContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableIndexDfntContext::GRANULARITY() {
  return getToken(ClickHouseParser::GRANULARITY, 0);
}

tree::TerminalNode* ClickHouseParser::TableIndexDfntContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}


size_t ClickHouseParser::TableIndexDfntContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableIndexDfnt;
}

antlrcpp::Any ClickHouseParser::TableIndexDfntContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableIndexDfnt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableIndexDfntContext* ClickHouseParser::tableIndexDfnt() {
  TableIndexDfntContext *_localctx = _tracker.createInstance<TableIndexDfntContext>(_ctx, getState());
  enterRule(_localctx, 70, ClickHouseParser::RuleTableIndexDfnt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(940);
    nestedIdentifier();
    setState(941);
    columnExpr(0);
    setState(942);
    match(ClickHouseParser::TYPE);
    setState(943);
    columnTypeExpr();
    setState(944);
    match(ClickHouseParser::GRANULARITY);
    setState(945);
    match(ClickHouseParser::DECIMAL_LITERAL);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableProjectionDfntContext ------------------------------------------------------------------

ClickHouseParser::TableProjectionDfntContext::TableProjectionDfntContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::TableProjectionDfntContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::ProjectionSelectStmtContext* ClickHouseParser::TableProjectionDfntContext::projectionSelectStmt() {
  return getRuleContext<ClickHouseParser::ProjectionSelectStmtContext>(0);
}


size_t ClickHouseParser::TableProjectionDfntContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableProjectionDfnt;
}

antlrcpp::Any ClickHouseParser::TableProjectionDfntContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableProjectionDfnt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableProjectionDfntContext* ClickHouseParser::tableProjectionDfnt() {
  TableProjectionDfntContext *_localctx = _tracker.createInstance<TableProjectionDfntContext>(_ctx, getState());
  enterRule(_localctx, 72, ClickHouseParser::RuleTableProjectionDfnt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(947);
    nestedIdentifier();
    setState(948);
    projectionSelectStmt();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CodecExprContext ------------------------------------------------------------------

ClickHouseParser::CodecExprContext::CodecExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::CODEC() {
  return getToken(ClickHouseParser::CODEC, 0);
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::CodecArgExprContext *> ClickHouseParser::CodecExprContext::codecArgExpr() {
  return getRuleContexts<ClickHouseParser::CodecArgExprContext>();
}

ClickHouseParser::CodecArgExprContext* ClickHouseParser::CodecExprContext::codecArgExpr(size_t i) {
  return getRuleContext<ClickHouseParser::CodecArgExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::CodecExprContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::CodecExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleCodecExpr;
}

antlrcpp::Any ClickHouseParser::CodecExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCodecExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::CodecExprContext* ClickHouseParser::codecExpr() {
  CodecExprContext *_localctx = _tracker.createInstance<CodecExprContext>(_ctx, getState());
  enterRule(_localctx, 74, ClickHouseParser::RuleCodecExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(950);
    match(ClickHouseParser::CODEC);
    setState(951);
    match(ClickHouseParser::LPAREN);
    setState(952);
    codecArgExpr();
    setState(957);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(953);
      match(ClickHouseParser::COMMA);
      setState(954);
      codecArgExpr();
      setState(959);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(960);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CodecArgExprContext ------------------------------------------------------------------

ClickHouseParser::CodecArgExprContext::CodecArgExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::CodecArgExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::CodecArgExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::CodecArgExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::CodecArgExprContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::CodecArgExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleCodecArgExpr;
}

antlrcpp::Any ClickHouseParser::CodecArgExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCodecArgExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::CodecArgExprContext* ClickHouseParser::codecArgExpr() {
  CodecArgExprContext *_localctx = _tracker.createInstance<CodecArgExprContext>(_ctx, getState());
  enterRule(_localctx, 76, ClickHouseParser::RuleCodecArgExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(962);
    identifier();
    setState(968);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(963);
      match(ClickHouseParser::LPAREN);
      setState(965);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NAN_SQL - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULL_SQL - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
        | (1ULL << (ClickHouseParser::DOT - 197))
        | (1ULL << (ClickHouseParser::LBRACKET - 197))
        | (1ULL << (ClickHouseParser::LPAREN - 197))
        | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
        setState(964);
        columnExprList();
      }
      setState(967);
      match(ClickHouseParser::RPAREN);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TtlExprContext ------------------------------------------------------------------

ClickHouseParser::TtlExprContext::TtlExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TtlExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TtlExprContext::DELETE() {
  return getToken(ClickHouseParser::DELETE, 0);
}

tree::TerminalNode* ClickHouseParser::TtlExprContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

tree::TerminalNode* ClickHouseParser::TtlExprContext::DISK() {
  return getToken(ClickHouseParser::DISK, 0);
}

tree::TerminalNode* ClickHouseParser::TtlExprContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::TtlExprContext::VOLUME() {
  return getToken(ClickHouseParser::VOLUME, 0);
}


size_t ClickHouseParser::TtlExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTtlExpr;
}

antlrcpp::Any ClickHouseParser::TtlExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTtlExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TtlExprContext* ClickHouseParser::ttlExpr() {
  TtlExprContext *_localctx = _tracker.createInstance<TtlExprContext>(_ctx, getState());
  enterRule(_localctx, 78, ClickHouseParser::RuleTtlExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(970);
    columnExpr(0);
    setState(978);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 106, _ctx)) {
    case 1: {
      setState(971);
      match(ClickHouseParser::DELETE);
      break;
    }

    case 2: {
      setState(972);
      match(ClickHouseParser::TO);
      setState(973);
      match(ClickHouseParser::DISK);
      setState(974);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 3: {
      setState(975);
      match(ClickHouseParser::TO);
      setState(976);
      match(ClickHouseParser::VOLUME);
      setState(977);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DescribeStmtContext ------------------------------------------------------------------

ClickHouseParser::DescribeStmtContext::DescribeStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::TableExprContext* ClickHouseParser::DescribeStmtContext::tableExpr() {
  return getRuleContext<ClickHouseParser::TableExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::DESCRIBE() {
  return getToken(ClickHouseParser::DESCRIBE, 0);
}

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::DESC() {
  return getToken(ClickHouseParser::DESC, 0);
}

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}


size_t ClickHouseParser::DescribeStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleDescribeStmt;
}

antlrcpp::Any ClickHouseParser::DescribeStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDescribeStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DescribeStmtContext* ClickHouseParser::describeStmt() {
  DescribeStmtContext *_localctx = _tracker.createInstance<DescribeStmtContext>(_ctx, getState());
  enterRule(_localctx, 80, ClickHouseParser::RuleDescribeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(980);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DESC

    || _la == ClickHouseParser::DESCRIBE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(982);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 107, _ctx)) {
    case 1: {
      setState(981);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(984);
    tableExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DropStmtContext ------------------------------------------------------------------

ClickHouseParser::DropStmtContext::DropStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::DropStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleDropStmt;
}

void ClickHouseParser::DropStmtContext::copyFrom(DropStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DropDatabaseStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::DropDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::DETACH() {
  return getToken(ClickHouseParser::DETACH, 0);
}

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::DropDatabaseStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::DropDatabaseStmtContext::DropDatabaseStmtContext(DropStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DropDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDropDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropTableStmtContext ------------------------------------------------------------------

ClickHouseParser::TableIdentifierContext* ClickHouseParser::DropTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DETACH() {
  return getToken(ClickHouseParser::DETACH, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::DropTableStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::NO() {
  return getToken(ClickHouseParser::NO, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DELAY() {
  return getToken(ClickHouseParser::DELAY, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

ClickHouseParser::DropTableStmtContext::DropTableStmtContext(DropStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DropTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDropTableStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::DropStmtContext* ClickHouseParser::dropStmt() {
  DropStmtContext *_localctx = _tracker.createInstance<DropStmtContext>(_ctx, getState());
  enterRule(_localctx, 82, ClickHouseParser::RuleDropStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1017);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(986);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(987);
      match(ClickHouseParser::DATABASE);
      setState(990);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 108, _ctx)) {
      case 1: {
        setState(988);
        match(ClickHouseParser::IF);
        setState(989);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(992);
      databaseIdentifier();
      setState(994);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(993);
        clusterClause();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(996);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1003);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::DICTIONARY: {
          setState(997);
          match(ClickHouseParser::DICTIONARY);
          break;
        }

        case ClickHouseParser::TABLE:
        case ClickHouseParser::TEMPORARY: {
          setState(999);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::TEMPORARY) {
            setState(998);
            match(ClickHouseParser::TEMPORARY);
          }
          setState(1001);
          match(ClickHouseParser::TABLE);
          break;
        }

        case ClickHouseParser::VIEW: {
          setState(1002);
          match(ClickHouseParser::VIEW);
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(1007);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 112, _ctx)) {
      case 1: {
        setState(1005);
        match(ClickHouseParser::IF);
        setState(1006);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(1009);
      tableIdentifier();
      setState(1011);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ON) {
        setState(1010);
        clusterClause();
      }
      setState(1015);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NO) {
        setState(1013);
        match(ClickHouseParser::NO);
        setState(1014);
        match(ClickHouseParser::DELAY);
      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExistsStmtContext ------------------------------------------------------------------

ClickHouseParser::ExistsStmtContext::ExistsStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ExistsStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleExistsStmt;
}

void ClickHouseParser::ExistsStmtContext::copyFrom(ExistsStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ExistsTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ExistsTableStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ExistsTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ExistsTableStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

tree::TerminalNode* ClickHouseParser::ExistsTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

tree::TerminalNode* ClickHouseParser::ExistsTableStmtContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

tree::TerminalNode* ClickHouseParser::ExistsTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

ClickHouseParser::ExistsTableStmtContext::ExistsTableStmtContext(ExistsStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ExistsTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExistsTableStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExistsDatabaseStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ExistsDatabaseStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::ExistsDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::ExistsDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

ClickHouseParser::ExistsDatabaseStmtContext::ExistsDatabaseStmtContext(ExistsStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ExistsDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExistsDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ExistsStmtContext* ClickHouseParser::existsStmt() {
  ExistsStmtContext *_localctx = _tracker.createInstance<ExistsStmtContext>(_ctx, getState());
  enterRule(_localctx, 84, ClickHouseParser::RuleExistsStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1032);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 118, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ExistsStmtContext *>(_tracker.createInstance<ClickHouseParser::ExistsDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1019);
      match(ClickHouseParser::EXISTS);
      setState(1020);
      match(ClickHouseParser::DATABASE);
      setState(1021);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ExistsStmtContext *>(_tracker.createInstance<ClickHouseParser::ExistsTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1022);
      match(ClickHouseParser::EXISTS);
      setState(1029);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 117, _ctx)) {
      case 1: {
        setState(1023);
        match(ClickHouseParser::DICTIONARY);
        break;
      }

      case 2: {
        setState(1025);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::TEMPORARY) {
          setState(1024);
          match(ClickHouseParser::TEMPORARY);
        }
        setState(1027);
        match(ClickHouseParser::TABLE);
        break;
      }

      case 3: {
        setState(1028);
        match(ClickHouseParser::VIEW);
        break;
      }

      }
      setState(1031);
      tableIdentifier();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExplainStmtContext ------------------------------------------------------------------

ClickHouseParser::ExplainStmtContext::ExplainStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ExplainStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleExplainStmt;
}

void ClickHouseParser::ExplainStmtContext::copyFrom(ExplainStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ExplainSyntaxStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ExplainSyntaxStmtContext::EXPLAIN() {
  return getToken(ClickHouseParser::EXPLAIN, 0);
}

tree::TerminalNode* ClickHouseParser::ExplainSyntaxStmtContext::SYNTAX() {
  return getToken(ClickHouseParser::SYNTAX, 0);
}

ClickHouseParser::QueryContext* ClickHouseParser::ExplainSyntaxStmtContext::query() {
  return getRuleContext<ClickHouseParser::QueryContext>(0);
}

ClickHouseParser::ExplainSyntaxStmtContext::ExplainSyntaxStmtContext(ExplainStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ExplainSyntaxStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExplainSyntaxStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExplainASTStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ExplainASTStmtContext::EXPLAIN() {
  return getToken(ClickHouseParser::EXPLAIN, 0);
}

tree::TerminalNode* ClickHouseParser::ExplainASTStmtContext::AST() {
  return getToken(ClickHouseParser::AST, 0);
}

ClickHouseParser::QueryContext* ClickHouseParser::ExplainASTStmtContext::query() {
  return getRuleContext<ClickHouseParser::QueryContext>(0);
}

ClickHouseParser::ExplainASTStmtContext::ExplainASTStmtContext(ExplainStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ExplainASTStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExplainASTStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ExplainStmtContext* ClickHouseParser::explainStmt() {
  ExplainStmtContext *_localctx = _tracker.createInstance<ExplainStmtContext>(_ctx, getState());
  enterRule(_localctx, 86, ClickHouseParser::RuleExplainStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1040);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 119, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ExplainStmtContext *>(_tracker.createInstance<ClickHouseParser::ExplainASTStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1034);
      match(ClickHouseParser::EXPLAIN);
      setState(1035);
      match(ClickHouseParser::AST);
      setState(1036);
      query();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ExplainStmtContext *>(_tracker.createInstance<ClickHouseParser::ExplainSyntaxStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1037);
      match(ClickHouseParser::EXPLAIN);
      setState(1038);
      match(ClickHouseParser::SYNTAX);
      setState(1039);
      query();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InsertStmtContext ------------------------------------------------------------------

ClickHouseParser::InsertStmtContext::InsertStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::INSERT() {
  return getToken(ClickHouseParser::INSERT, 0);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::INTO() {
  return getToken(ClickHouseParser::INTO, 0);
}

ClickHouseParser::DataClauseContext* ClickHouseParser::InsertStmtContext::dataClause() {
  return getRuleContext<ClickHouseParser::DataClauseContext>(0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::InsertStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::FUNCTION() {
  return getToken(ClickHouseParser::FUNCTION, 0);
}

ClickHouseParser::TableFunctionExprContext* ClickHouseParser::InsertStmtContext::tableFunctionExpr() {
  return getRuleContext<ClickHouseParser::TableFunctionExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::ColumnsClauseContext* ClickHouseParser::InsertStmtContext::columnsClause() {
  return getRuleContext<ClickHouseParser::ColumnsClauseContext>(0);
}


size_t ClickHouseParser::InsertStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleInsertStmt;
}

antlrcpp::Any ClickHouseParser::InsertStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitInsertStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::InsertStmtContext* ClickHouseParser::insertStmt() {
  InsertStmtContext *_localctx = _tracker.createInstance<InsertStmtContext>(_ctx, getState());
  enterRule(_localctx, 88, ClickHouseParser::RuleInsertStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1042);
    match(ClickHouseParser::INSERT);
    setState(1043);
    match(ClickHouseParser::INTO);
    setState(1045);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 120, _ctx)) {
    case 1: {
      setState(1044);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(1050);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 121, _ctx)) {
    case 1: {
      setState(1047);
      tableIdentifier();
      break;
    }

    case 2: {
      setState(1048);
      match(ClickHouseParser::FUNCTION);
      setState(1049);
      tableFunctionExpr();
      break;
    }

    }
    setState(1053);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 122, _ctx)) {
    case 1: {
      setState(1052);
      columnsClause();
      break;
    }

    }
    setState(1055);
    dataClause();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnsClauseContext ------------------------------------------------------------------

ClickHouseParser::ColumnsClauseContext::ColumnsClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ColumnsClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::NestedIdentifierContext *> ClickHouseParser::ColumnsClauseContext::nestedIdentifier() {
  return getRuleContexts<ClickHouseParser::NestedIdentifierContext>();
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::ColumnsClauseContext::nestedIdentifier(size_t i) {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnsClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnsClauseContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnsClauseContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ColumnsClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnsClause;
}

antlrcpp::Any ClickHouseParser::ColumnsClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnsClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnsClauseContext* ClickHouseParser::columnsClause() {
  ColumnsClauseContext *_localctx = _tracker.createInstance<ColumnsClauseContext>(_ctx, getState());
  enterRule(_localctx, 90, ClickHouseParser::RuleColumnsClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1057);
    match(ClickHouseParser::LPAREN);
    setState(1058);
    nestedIdentifier();
    setState(1063);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1059);
      match(ClickHouseParser::COMMA);
      setState(1060);
      nestedIdentifier();
      setState(1065);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1066);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataClauseContext ------------------------------------------------------------------

ClickHouseParser::DataClauseContext::DataClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::DataClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleDataClause;
}

void ClickHouseParser::DataClauseContext::copyFrom(DataClauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DataClauseValuesContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::DataClauseValuesContext::VALUES() {
  return getToken(ClickHouseParser::VALUES, 0);
}

ClickHouseParser::DataClauseValuesContext::DataClauseValuesContext(DataClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataClauseValuesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataClauseValues(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DataClauseFormatContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::DataClauseFormatContext::FORMAT() {
  return getToken(ClickHouseParser::FORMAT, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::DataClauseFormatContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::DataClauseFormatContext::DataClauseFormatContext(DataClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataClauseFormatContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataClauseFormat(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DataClauseSelectContext ------------------------------------------------------------------

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::DataClauseSelectContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::DataClauseSelectContext::EOF() {
  return getToken(ClickHouseParser::EOF, 0);
}

tree::TerminalNode* ClickHouseParser::DataClauseSelectContext::SEMICOLON() {
  return getToken(ClickHouseParser::SEMICOLON, 0);
}

ClickHouseParser::DataClauseSelectContext::DataClauseSelectContext(DataClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataClauseSelectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataClauseSelect(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::DataClauseContext* ClickHouseParser::dataClause() {
  DataClauseContext *_localctx = _tracker.createInstance<DataClauseContext>(_ctx, getState());
  enterRule(_localctx, 92, ClickHouseParser::RuleDataClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1077);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FORMAT: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseFormatContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(1068);
        match(ClickHouseParser::FORMAT);
        setState(1069);
        identifier();
        break;
      }

      case ClickHouseParser::VALUES: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseValuesContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(1070);
        match(ClickHouseParser::VALUES);
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseSelectContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(1071);
        selectUnionStmt();
        setState(1073);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::SEMICOLON) {
          setState(1072);
          match(ClickHouseParser::SEMICOLON);
        }
        setState(1075);
        match(ClickHouseParser::EOF);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KillStmtContext ------------------------------------------------------------------

ClickHouseParser::KillStmtContext::KillStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::KillStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleKillStmt;
}

void ClickHouseParser::KillStmtContext::copyFrom(KillStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- KillMutationStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::KillMutationStmtContext::KILL() {
  return getToken(ClickHouseParser::KILL, 0);
}

tree::TerminalNode* ClickHouseParser::KillMutationStmtContext::MUTATION() {
  return getToken(ClickHouseParser::MUTATION, 0);
}

ClickHouseParser::WhereClauseContext* ClickHouseParser::KillMutationStmtContext::whereClause() {
  return getRuleContext<ClickHouseParser::WhereClauseContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::KillMutationStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::KillMutationStmtContext::SYNC() {
  return getToken(ClickHouseParser::SYNC, 0);
}

tree::TerminalNode* ClickHouseParser::KillMutationStmtContext::ASYNC() {
  return getToken(ClickHouseParser::ASYNC, 0);
}

tree::TerminalNode* ClickHouseParser::KillMutationStmtContext::TEST() {
  return getToken(ClickHouseParser::TEST, 0);
}

ClickHouseParser::KillMutationStmtContext::KillMutationStmtContext(KillStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::KillMutationStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitKillMutationStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::KillStmtContext* ClickHouseParser::killStmt() {
  KillStmtContext *_localctx = _tracker.createInstance<KillStmtContext>(_ctx, getState());
  enterRule(_localctx, 94, ClickHouseParser::RuleKillStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<KillStmtContext *>(_tracker.createInstance<ClickHouseParser::KillMutationStmtContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(1079);
    match(ClickHouseParser::KILL);
    setState(1080);
    match(ClickHouseParser::MUTATION);
    setState(1082);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(1081);
      clusterClause();
    }
    setState(1084);
    whereClause();
    setState(1086);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ASYNC || _la == ClickHouseParser::SYNC

    || _la == ClickHouseParser::TEST) {
      setState(1085);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ASYNC || _la == ClickHouseParser::SYNC

      || _la == ClickHouseParser::TEST)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OptimizeStmtContext ------------------------------------------------------------------

ClickHouseParser::OptimizeStmtContext::OptimizeStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::OptimizeStmtContext::OPTIMIZE() {
  return getToken(ClickHouseParser::OPTIMIZE, 0);
}

tree::TerminalNode* ClickHouseParser::OptimizeStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::OptimizeStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::OptimizeStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::OptimizeStmtContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::OptimizeStmtContext::FINAL() {
  return getToken(ClickHouseParser::FINAL, 0);
}

tree::TerminalNode* ClickHouseParser::OptimizeStmtContext::DEDUPLICATE() {
  return getToken(ClickHouseParser::DEDUPLICATE, 0);
}


size_t ClickHouseParser::OptimizeStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleOptimizeStmt;
}

antlrcpp::Any ClickHouseParser::OptimizeStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitOptimizeStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::OptimizeStmtContext* ClickHouseParser::optimizeStmt() {
  OptimizeStmtContext *_localctx = _tracker.createInstance<OptimizeStmtContext>(_ctx, getState());
  enterRule(_localctx, 96, ClickHouseParser::RuleOptimizeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1088);
    match(ClickHouseParser::OPTIMIZE);
    setState(1089);
    match(ClickHouseParser::TABLE);
    setState(1090);
    tableIdentifier();
    setState(1092);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(1091);
      clusterClause();
    }
    setState(1095);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(1094);
      partitionClause();
    }
    setState(1098);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(1097);
      match(ClickHouseParser::FINAL);
    }
    setState(1101);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DEDUPLICATE) {
      setState(1100);
      match(ClickHouseParser::DEDUPLICATE);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RenameStmtContext ------------------------------------------------------------------

ClickHouseParser::RenameStmtContext::RenameStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::RenameStmtContext::RENAME() {
  return getToken(ClickHouseParser::RENAME, 0);
}

tree::TerminalNode* ClickHouseParser::RenameStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

std::vector<ClickHouseParser::TableIdentifierContext *> ClickHouseParser::RenameStmtContext::tableIdentifier() {
  return getRuleContexts<ClickHouseParser::TableIdentifierContext>();
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::RenameStmtContext::tableIdentifier(size_t i) {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::RenameStmtContext::TO() {
  return getTokens(ClickHouseParser::TO);
}

tree::TerminalNode* ClickHouseParser::RenameStmtContext::TO(size_t i) {
  return getToken(ClickHouseParser::TO, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::RenameStmtContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::RenameStmtContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::RenameStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}


size_t ClickHouseParser::RenameStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleRenameStmt;
}

antlrcpp::Any ClickHouseParser::RenameStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitRenameStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::RenameStmtContext* ClickHouseParser::renameStmt() {
  RenameStmtContext *_localctx = _tracker.createInstance<RenameStmtContext>(_ctx, getState());
  enterRule(_localctx, 98, ClickHouseParser::RuleRenameStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1103);
    match(ClickHouseParser::RENAME);
    setState(1104);
    match(ClickHouseParser::TABLE);
    setState(1105);
    tableIdentifier();
    setState(1106);
    match(ClickHouseParser::TO);
    setState(1107);
    tableIdentifier();
    setState(1115);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1108);
      match(ClickHouseParser::COMMA);
      setState(1109);
      tableIdentifier();
      setState(1110);
      match(ClickHouseParser::TO);
      setState(1111);
      tableIdentifier();
      setState(1117);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1119);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(1118);
      clusterClause();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ProjectionSelectStmtContext ------------------------------------------------------------------

ClickHouseParser::ProjectionSelectStmtContext::ProjectionSelectStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ProjectionSelectStmtContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::ProjectionSelectStmtContext::SELECT() {
  return getToken(ClickHouseParser::SELECT, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ProjectionSelectStmtContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::ProjectionSelectStmtContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::WithClauseContext* ClickHouseParser::ProjectionSelectStmtContext::withClause() {
  return getRuleContext<ClickHouseParser::WithClauseContext>(0);
}

ClickHouseParser::GroupByClauseContext* ClickHouseParser::ProjectionSelectStmtContext::groupByClause() {
  return getRuleContext<ClickHouseParser::GroupByClauseContext>(0);
}

ClickHouseParser::ProjectionOrderByClauseContext* ClickHouseParser::ProjectionSelectStmtContext::projectionOrderByClause() {
  return getRuleContext<ClickHouseParser::ProjectionOrderByClauseContext>(0);
}


size_t ClickHouseParser::ProjectionSelectStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleProjectionSelectStmt;
}

antlrcpp::Any ClickHouseParser::ProjectionSelectStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitProjectionSelectStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ProjectionSelectStmtContext* ClickHouseParser::projectionSelectStmt() {
  ProjectionSelectStmtContext *_localctx = _tracker.createInstance<ProjectionSelectStmtContext>(_ctx, getState());
  enterRule(_localctx, 100, ClickHouseParser::RuleProjectionSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1121);
    match(ClickHouseParser::LPAREN);
    setState(1123);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(1122);
      withClause();
    }
    setState(1125);
    match(ClickHouseParser::SELECT);
    setState(1126);
    columnExprList();
    setState(1128);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(1127);
      groupByClause();
    }
    setState(1131);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(1130);
      projectionOrderByClause();
    }
    setState(1133);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectUnionStmtContext ------------------------------------------------------------------

ClickHouseParser::SelectUnionStmtContext::SelectUnionStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::SelectStmtWithParensContext *> ClickHouseParser::SelectUnionStmtContext::selectStmtWithParens() {
  return getRuleContexts<ClickHouseParser::SelectStmtWithParensContext>();
}

ClickHouseParser::SelectStmtWithParensContext* ClickHouseParser::SelectUnionStmtContext::selectStmtWithParens(size_t i) {
  return getRuleContext<ClickHouseParser::SelectStmtWithParensContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SelectUnionStmtContext::UNION() {
  return getTokens(ClickHouseParser::UNION);
}

tree::TerminalNode* ClickHouseParser::SelectUnionStmtContext::UNION(size_t i) {
  return getToken(ClickHouseParser::UNION, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SelectUnionStmtContext::ALL() {
  return getTokens(ClickHouseParser::ALL);
}

tree::TerminalNode* ClickHouseParser::SelectUnionStmtContext::ALL(size_t i) {
  return getToken(ClickHouseParser::ALL, i);
}


size_t ClickHouseParser::SelectUnionStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleSelectUnionStmt;
}

antlrcpp::Any ClickHouseParser::SelectUnionStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSelectUnionStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::selectUnionStmt() {
  SelectUnionStmtContext *_localctx = _tracker.createInstance<SelectUnionStmtContext>(_ctx, getState());
  enterRule(_localctx, 102, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1135);
    selectStmtWithParens();
    setState(1141);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(1136);
      match(ClickHouseParser::UNION);
      setState(1137);
      match(ClickHouseParser::ALL);
      setState(1138);
      selectStmtWithParens();
      setState(1143);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectStmtWithParensContext ------------------------------------------------------------------

ClickHouseParser::SelectStmtWithParensContext::SelectStmtWithParensContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::SelectStmtContext* ClickHouseParser::SelectStmtWithParensContext::selectStmt() {
  return getRuleContext<ClickHouseParser::SelectStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::SelectStmtWithParensContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::SelectStmtWithParensContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::SelectStmtWithParensContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}


size_t ClickHouseParser::SelectStmtWithParensContext::getRuleIndex() const {
  return ClickHouseParser::RuleSelectStmtWithParens;
}

antlrcpp::Any ClickHouseParser::SelectStmtWithParensContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSelectStmtWithParens(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SelectStmtWithParensContext* ClickHouseParser::selectStmtWithParens() {
  SelectStmtWithParensContext *_localctx = _tracker.createInstance<SelectStmtWithParensContext>(_ctx, getState());
  enterRule(_localctx, 104, ClickHouseParser::RuleSelectStmtWithParens);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1149);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 1);
        setState(1144);
        selectStmt();
        break;
      }

      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 2);
        setState(1145);
        match(ClickHouseParser::LPAREN);
        setState(1146);
        selectUnionStmt();
        setState(1147);
        match(ClickHouseParser::RPAREN);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectStmtContext ------------------------------------------------------------------

ClickHouseParser::SelectStmtContext::SelectStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::SELECT() {
  return getToken(ClickHouseParser::SELECT, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::SelectStmtContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

ClickHouseParser::WithClauseContext* ClickHouseParser::SelectStmtContext::withClause() {
  return getRuleContext<ClickHouseParser::WithClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::DISTINCT() {
  return getToken(ClickHouseParser::DISTINCT, 0);
}

ClickHouseParser::TopClauseContext* ClickHouseParser::SelectStmtContext::topClause() {
  return getRuleContext<ClickHouseParser::TopClauseContext>(0);
}

ClickHouseParser::FromClauseContext* ClickHouseParser::SelectStmtContext::fromClause() {
  return getRuleContext<ClickHouseParser::FromClauseContext>(0);
}

ClickHouseParser::ArrayJoinClauseContext* ClickHouseParser::SelectStmtContext::arrayJoinClause() {
  return getRuleContext<ClickHouseParser::ArrayJoinClauseContext>(0);
}

ClickHouseParser::PrewhereClauseContext* ClickHouseParser::SelectStmtContext::prewhereClause() {
  return getRuleContext<ClickHouseParser::PrewhereClauseContext>(0);
}

ClickHouseParser::WhereClauseContext* ClickHouseParser::SelectStmtContext::whereClause() {
  return getRuleContext<ClickHouseParser::WhereClauseContext>(0);
}

ClickHouseParser::GroupByClauseContext* ClickHouseParser::SelectStmtContext::groupByClause() {
  return getRuleContext<ClickHouseParser::GroupByClauseContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SelectStmtContext::WITH() {
  return getTokens(ClickHouseParser::WITH);
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::WITH(size_t i) {
  return getToken(ClickHouseParser::WITH, i);
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::TOTALS() {
  return getToken(ClickHouseParser::TOTALS, 0);
}

ClickHouseParser::HavingClauseContext* ClickHouseParser::SelectStmtContext::havingClause() {
  return getRuleContext<ClickHouseParser::HavingClauseContext>(0);
}

ClickHouseParser::OrderByClauseContext* ClickHouseParser::SelectStmtContext::orderByClause() {
  return getRuleContext<ClickHouseParser::OrderByClauseContext>(0);
}

ClickHouseParser::LimitByClauseContext* ClickHouseParser::SelectStmtContext::limitByClause() {
  return getRuleContext<ClickHouseParser::LimitByClauseContext>(0);
}

ClickHouseParser::LimitClauseContext* ClickHouseParser::SelectStmtContext::limitClause() {
  return getRuleContext<ClickHouseParser::LimitClauseContext>(0);
}

ClickHouseParser::SettingsClauseContext* ClickHouseParser::SelectStmtContext::settingsClause() {
  return getRuleContext<ClickHouseParser::SettingsClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::CUBE() {
  return getToken(ClickHouseParser::CUBE, 0);
}

tree::TerminalNode* ClickHouseParser::SelectStmtContext::ROLLUP() {
  return getToken(ClickHouseParser::ROLLUP, 0);
}


size_t ClickHouseParser::SelectStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleSelectStmt;
}

antlrcpp::Any ClickHouseParser::SelectStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSelectStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SelectStmtContext* ClickHouseParser::selectStmt() {
  SelectStmtContext *_localctx = _tracker.createInstance<SelectStmtContext>(_ctx, getState());
  enterRule(_localctx, 106, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1152);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(1151);
      withClause();
    }
    setState(1154);
    match(ClickHouseParser::SELECT);
    setState(1156);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 140, _ctx)) {
    case 1: {
      setState(1155);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(1159);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 141, _ctx)) {
    case 1: {
      setState(1158);
      topClause();
      break;
    }

    }
    setState(1161);
    columnExprList();
    setState(1163);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(1162);
      fromClause();
    }
    setState(1166);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY || _la == ClickHouseParser::INNER

    || _la == ClickHouseParser::LEFT) {
      setState(1165);
      arrayJoinClause();
    }
    setState(1169);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(1168);
      prewhereClause();
    }
    setState(1172);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(1171);
      whereClause();
    }
    setState(1175);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(1174);
      groupByClause();
    }
    setState(1179);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 147, _ctx)) {
    case 1: {
      setState(1177);
      match(ClickHouseParser::WITH);
      setState(1178);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::CUBE || _la == ClickHouseParser::ROLLUP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    }
    setState(1183);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(1181);
      match(ClickHouseParser::WITH);
      setState(1182);
      match(ClickHouseParser::TOTALS);
    }
    setState(1186);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(1185);
      havingClause();
    }
    setState(1189);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(1188);
      orderByClause();
    }
    setState(1192);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 151, _ctx)) {
    case 1: {
      setState(1191);
      limitByClause();
      break;
    }

    }
    setState(1195);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(1194);
      limitClause();
    }
    setState(1198);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(1197);
      settingsClause();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WithClauseContext ------------------------------------------------------------------

ClickHouseParser::WithClauseContext::WithClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::WithClauseContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::WithClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::WithClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleWithClause;
}

antlrcpp::Any ClickHouseParser::WithClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitWithClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::WithClauseContext* ClickHouseParser::withClause() {
  WithClauseContext *_localctx = _tracker.createInstance<WithClauseContext>(_ctx, getState());
  enterRule(_localctx, 108, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1200);
    match(ClickHouseParser::WITH);
    setState(1201);
    columnExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TopClauseContext ------------------------------------------------------------------

ClickHouseParser::TopClauseContext::TopClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::TopClauseContext::TOP() {
  return getToken(ClickHouseParser::TOP, 0);
}

tree::TerminalNode* ClickHouseParser::TopClauseContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::TopClauseContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}

tree::TerminalNode* ClickHouseParser::TopClauseContext::TIES() {
  return getToken(ClickHouseParser::TIES, 0);
}


size_t ClickHouseParser::TopClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleTopClause;
}

antlrcpp::Any ClickHouseParser::TopClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTopClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TopClauseContext* ClickHouseParser::topClause() {
  TopClauseContext *_localctx = _tracker.createInstance<TopClauseContext>(_ctx, getState());
  enterRule(_localctx, 110, ClickHouseParser::RuleTopClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1203);
    match(ClickHouseParser::TOP);
    setState(1204);
    match(ClickHouseParser::DECIMAL_LITERAL);
    setState(1207);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 154, _ctx)) {
    case 1: {
      setState(1205);
      match(ClickHouseParser::WITH);
      setState(1206);
      match(ClickHouseParser::TIES);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FromClauseContext ------------------------------------------------------------------

ClickHouseParser::FromClauseContext::FromClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::FromClauseContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::JoinExprContext* ClickHouseParser::FromClauseContext::joinExpr() {
  return getRuleContext<ClickHouseParser::JoinExprContext>(0);
}


size_t ClickHouseParser::FromClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleFromClause;
}

antlrcpp::Any ClickHouseParser::FromClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitFromClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::FromClauseContext* ClickHouseParser::fromClause() {
  FromClauseContext *_localctx = _tracker.createInstance<FromClauseContext>(_ctx, getState());
  enterRule(_localctx, 112, ClickHouseParser::RuleFromClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1209);
    match(ClickHouseParser::FROM);
    setState(1210);
    joinExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ArrayJoinClauseContext ------------------------------------------------------------------

ClickHouseParser::ArrayJoinClauseContext::ArrayJoinClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ArrayJoinClauseContext::ARRAY() {
  return getToken(ClickHouseParser::ARRAY, 0);
}

tree::TerminalNode* ClickHouseParser::ArrayJoinClauseContext::JOIN() {
  return getToken(ClickHouseParser::JOIN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ArrayJoinClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::ArrayJoinClauseContext::LEFT() {
  return getToken(ClickHouseParser::LEFT, 0);
}

tree::TerminalNode* ClickHouseParser::ArrayJoinClauseContext::INNER() {
  return getToken(ClickHouseParser::INNER, 0);
}


size_t ClickHouseParser::ArrayJoinClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleArrayJoinClause;
}

antlrcpp::Any ClickHouseParser::ArrayJoinClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitArrayJoinClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ArrayJoinClauseContext* ClickHouseParser::arrayJoinClause() {
  ArrayJoinClauseContext *_localctx = _tracker.createInstance<ArrayJoinClauseContext>(_ctx, getState());
  enterRule(_localctx, 114, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1213);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INNER

    || _la == ClickHouseParser::LEFT) {
      setState(1212);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::INNER

      || _la == ClickHouseParser::LEFT)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(1215);
    match(ClickHouseParser::ARRAY);
    setState(1216);
    match(ClickHouseParser::JOIN);
    setState(1217);
    columnExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrewhereClauseContext ------------------------------------------------------------------

ClickHouseParser::PrewhereClauseContext::PrewhereClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::PrewhereClauseContext::PREWHERE() {
  return getToken(ClickHouseParser::PREWHERE, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::PrewhereClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::PrewhereClauseContext::getRuleIndex() const {
  return ClickHouseParser::RulePrewhereClause;
}

antlrcpp::Any ClickHouseParser::PrewhereClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitPrewhereClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::PrewhereClauseContext* ClickHouseParser::prewhereClause() {
  PrewhereClauseContext *_localctx = _tracker.createInstance<PrewhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 116, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1219);
    match(ClickHouseParser::PREWHERE);
    setState(1220);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereClauseContext ------------------------------------------------------------------

ClickHouseParser::WhereClauseContext::WhereClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::WhereClauseContext::WHERE() {
  return getToken(ClickHouseParser::WHERE, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::WhereClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::WhereClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleWhereClause;
}

antlrcpp::Any ClickHouseParser::WhereClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitWhereClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::WhereClauseContext* ClickHouseParser::whereClause() {
  WhereClauseContext *_localctx = _tracker.createInstance<WhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 118, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1222);
    match(ClickHouseParser::WHERE);
    setState(1223);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupByClauseContext ------------------------------------------------------------------

ClickHouseParser::GroupByClauseContext::GroupByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::GROUP() {
  return getToken(ClickHouseParser::GROUP, 0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::GroupByClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::CUBE() {
  return getToken(ClickHouseParser::CUBE, 0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::ROLLUP() {
  return getToken(ClickHouseParser::ROLLUP, 0);
}


size_t ClickHouseParser::GroupByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleGroupByClause;
}

antlrcpp::Any ClickHouseParser::GroupByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitGroupByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::GroupByClauseContext* ClickHouseParser::groupByClause() {
  GroupByClauseContext *_localctx = _tracker.createInstance<GroupByClauseContext>(_ctx, getState());
  enterRule(_localctx, 120, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1225);
    match(ClickHouseParser::GROUP);
    setState(1226);
    match(ClickHouseParser::BY);
    setState(1233);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 156, _ctx)) {
    case 1: {
      setState(1227);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::CUBE || _la == ClickHouseParser::ROLLUP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1228);
      match(ClickHouseParser::LPAREN);
      setState(1229);
      columnExprList();
      setState(1230);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      setState(1232);
      columnExprList();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingClauseContext ------------------------------------------------------------------

ClickHouseParser::HavingClauseContext::HavingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::HavingClauseContext::HAVING() {
  return getToken(ClickHouseParser::HAVING, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::HavingClauseContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::HavingClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleHavingClause;
}

antlrcpp::Any ClickHouseParser::HavingClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitHavingClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::HavingClauseContext* ClickHouseParser::havingClause() {
  HavingClauseContext *_localctx = _tracker.createInstance<HavingClauseContext>(_ctx, getState());
  enterRule(_localctx, 122, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1235);
    match(ClickHouseParser::HAVING);
    setState(1236);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderByClauseContext ------------------------------------------------------------------

ClickHouseParser::OrderByClauseContext::OrderByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::OrderByClauseContext::ORDER() {
  return getToken(ClickHouseParser::ORDER, 0);
}

tree::TerminalNode* ClickHouseParser::OrderByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::OrderExprListContext* ClickHouseParser::OrderByClauseContext::orderExprList() {
  return getRuleContext<ClickHouseParser::OrderExprListContext>(0);
}


size_t ClickHouseParser::OrderByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleOrderByClause;
}

antlrcpp::Any ClickHouseParser::OrderByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitOrderByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::OrderByClauseContext* ClickHouseParser::orderByClause() {
  OrderByClauseContext *_localctx = _tracker.createInstance<OrderByClauseContext>(_ctx, getState());
  enterRule(_localctx, 124, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1238);
    match(ClickHouseParser::ORDER);
    setState(1239);
    match(ClickHouseParser::BY);
    setState(1240);
    orderExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ProjectionOrderByClauseContext ------------------------------------------------------------------

ClickHouseParser::ProjectionOrderByClauseContext::ProjectionOrderByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ProjectionOrderByClauseContext::ORDER() {
  return getToken(ClickHouseParser::ORDER, 0);
}

tree::TerminalNode* ClickHouseParser::ProjectionOrderByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ProjectionOrderByClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::ProjectionOrderByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleProjectionOrderByClause;
}

antlrcpp::Any ClickHouseParser::ProjectionOrderByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitProjectionOrderByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ProjectionOrderByClauseContext* ClickHouseParser::projectionOrderByClause() {
  ProjectionOrderByClauseContext *_localctx = _tracker.createInstance<ProjectionOrderByClauseContext>(_ctx, getState());
  enterRule(_localctx, 126, ClickHouseParser::RuleProjectionOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1242);
    match(ClickHouseParser::ORDER);
    setState(1243);
    match(ClickHouseParser::BY);
    setState(1244);
    columnExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitByClauseContext ------------------------------------------------------------------

ClickHouseParser::LimitByClauseContext::LimitByClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::LimitByClauseContext::LIMIT() {
  return getToken(ClickHouseParser::LIMIT, 0);
}

ClickHouseParser::LimitExprContext* ClickHouseParser::LimitByClauseContext::limitExpr() {
  return getRuleContext<ClickHouseParser::LimitExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::LimitByClauseContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::LimitByClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}


size_t ClickHouseParser::LimitByClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleLimitByClause;
}

antlrcpp::Any ClickHouseParser::LimitByClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLimitByClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LimitByClauseContext* ClickHouseParser::limitByClause() {
  LimitByClauseContext *_localctx = _tracker.createInstance<LimitByClauseContext>(_ctx, getState());
  enterRule(_localctx, 128, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1246);
    match(ClickHouseParser::LIMIT);
    setState(1247);
    limitExpr();
    setState(1248);
    match(ClickHouseParser::BY);
    setState(1249);
    columnExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitClauseContext ------------------------------------------------------------------

ClickHouseParser::LimitClauseContext::LimitClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::LimitClauseContext::LIMIT() {
  return getToken(ClickHouseParser::LIMIT, 0);
}

ClickHouseParser::LimitExprContext* ClickHouseParser::LimitClauseContext::limitExpr() {
  return getRuleContext<ClickHouseParser::LimitExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::LimitClauseContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}

tree::TerminalNode* ClickHouseParser::LimitClauseContext::TIES() {
  return getToken(ClickHouseParser::TIES, 0);
}


size_t ClickHouseParser::LimitClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleLimitClause;
}

antlrcpp::Any ClickHouseParser::LimitClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLimitClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LimitClauseContext* ClickHouseParser::limitClause() {
  LimitClauseContext *_localctx = _tracker.createInstance<LimitClauseContext>(_ctx, getState());
  enterRule(_localctx, 130, ClickHouseParser::RuleLimitClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1251);
    match(ClickHouseParser::LIMIT);
    setState(1252);
    limitExpr();
    setState(1255);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(1253);
      match(ClickHouseParser::WITH);
      setState(1254);
      match(ClickHouseParser::TIES);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SettingsClauseContext ------------------------------------------------------------------

ClickHouseParser::SettingsClauseContext::SettingsClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SettingsClauseContext::SETTINGS() {
  return getToken(ClickHouseParser::SETTINGS, 0);
}

ClickHouseParser::SettingExprListContext* ClickHouseParser::SettingsClauseContext::settingExprList() {
  return getRuleContext<ClickHouseParser::SettingExprListContext>(0);
}


size_t ClickHouseParser::SettingsClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSettingsClause;
}

antlrcpp::Any ClickHouseParser::SettingsClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSettingsClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SettingsClauseContext* ClickHouseParser::settingsClause() {
  SettingsClauseContext *_localctx = _tracker.createInstance<SettingsClauseContext>(_ctx, getState());
  enterRule(_localctx, 132, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1257);
    match(ClickHouseParser::SETTINGS);
    setState(1258);
    settingExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinExprContext ------------------------------------------------------------------

ClickHouseParser::JoinExprContext::JoinExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::JoinExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleJoinExpr;
}

void ClickHouseParser::JoinExprContext::copyFrom(JoinExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- JoinExprOpContext ------------------------------------------------------------------

std::vector<ClickHouseParser::JoinExprContext *> ClickHouseParser::JoinExprOpContext::joinExpr() {
  return getRuleContexts<ClickHouseParser::JoinExprContext>();
}

ClickHouseParser::JoinExprContext* ClickHouseParser::JoinExprOpContext::joinExpr(size_t i) {
  return getRuleContext<ClickHouseParser::JoinExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::JoinExprOpContext::JOIN() {
  return getToken(ClickHouseParser::JOIN, 0);
}

ClickHouseParser::JoinConstraintClauseContext* ClickHouseParser::JoinExprOpContext::joinConstraintClause() {
  return getRuleContext<ClickHouseParser::JoinConstraintClauseContext>(0);
}

ClickHouseParser::JoinOpContext* ClickHouseParser::JoinExprOpContext::joinOp() {
  return getRuleContext<ClickHouseParser::JoinOpContext>(0);
}

tree::TerminalNode* ClickHouseParser::JoinExprOpContext::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinExprOpContext::LOCAL() {
  return getToken(ClickHouseParser::LOCAL, 0);
}

ClickHouseParser::JoinExprOpContext::JoinExprOpContext(JoinExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinExprOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinExprOp(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinExprTableContext ------------------------------------------------------------------

ClickHouseParser::TableExprContext* ClickHouseParser::JoinExprTableContext::tableExpr() {
  return getRuleContext<ClickHouseParser::TableExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::JoinExprTableContext::FINAL() {
  return getToken(ClickHouseParser::FINAL, 0);
}

ClickHouseParser::SampleClauseContext* ClickHouseParser::JoinExprTableContext::sampleClause() {
  return getRuleContext<ClickHouseParser::SampleClauseContext>(0);
}

ClickHouseParser::JoinExprTableContext::JoinExprTableContext(JoinExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinExprTableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinExprTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinExprParensContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JoinExprParensContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::JoinExprContext* ClickHouseParser::JoinExprParensContext::joinExpr() {
  return getRuleContext<ClickHouseParser::JoinExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::JoinExprParensContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::JoinExprParensContext::JoinExprParensContext(JoinExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinExprParensContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinExprParens(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinExprCrossOpContext ------------------------------------------------------------------

std::vector<ClickHouseParser::JoinExprContext *> ClickHouseParser::JoinExprCrossOpContext::joinExpr() {
  return getRuleContexts<ClickHouseParser::JoinExprContext>();
}

ClickHouseParser::JoinExprContext* ClickHouseParser::JoinExprCrossOpContext::joinExpr(size_t i) {
  return getRuleContext<ClickHouseParser::JoinExprContext>(i);
}

ClickHouseParser::JoinOpCrossContext* ClickHouseParser::JoinExprCrossOpContext::joinOpCross() {
  return getRuleContext<ClickHouseParser::JoinOpCrossContext>(0);
}

ClickHouseParser::JoinExprCrossOpContext::JoinExprCrossOpContext(JoinExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinExprCrossOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinExprCrossOp(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::JoinExprContext* ClickHouseParser::joinExpr() {
   return joinExpr(0);
}

ClickHouseParser::JoinExprContext* ClickHouseParser::joinExpr(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  ClickHouseParser::JoinExprContext *_localctx = _tracker.createInstance<JoinExprContext>(_ctx, parentState);
  ClickHouseParser::JoinExprContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 134;
  enterRecursionRule(_localctx, 134, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1272);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 160, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1261);
      tableExpr(0);
      setState(1263);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 158, _ctx)) {
      case 1: {
        setState(1262);
        match(ClickHouseParser::FINAL);
        break;
      }

      }
      setState(1266);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 159, _ctx)) {
      case 1: {
        setState(1265);
        sampleClause();
        break;
      }

      }
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<JoinExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1268);
      match(ClickHouseParser::LPAREN);
      setState(1269);
      joinExpr(0);
      setState(1270);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1291);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 164, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1289);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 163, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(1274);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(1275);
          joinOpCross();
          setState(1276);
          joinExpr(4);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(1278);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(1280);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(1279);
            _la = _input->LA(1);
            if (!(_la == ClickHouseParser::GLOBAL

            || _la == ClickHouseParser::LOCAL)) {
            _errHandler->recoverInline(this);
            }
            else {
              _errHandler->reportMatch(this);
              consume();
            }
          }
          setState(1283);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (((((_la - 4) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 4)) & ((1ULL << (ClickHouseParser::ALL - 4))
            | (1ULL << (ClickHouseParser::ANTI - 4))
            | (1ULL << (ClickHouseParser::ANY - 4))
            | (1ULL << (ClickHouseParser::ASOF - 4))
            | (1ULL << (ClickHouseParser::FULL - 4)))) != 0) || ((((_la - 81) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 81)) & ((1ULL << (ClickHouseParser::INNER - 81))
            | (1ULL << (ClickHouseParser::LEFT - 81))
            | (1ULL << (ClickHouseParser::RIGHT - 81))
            | (1ULL << (ClickHouseParser::SEMI - 81)))) != 0)) {
            setState(1282);
            joinOp();
          }
          setState(1285);
          match(ClickHouseParser::JOIN);
          setState(1286);
          joinExpr(0);
          setState(1287);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(1293);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 164, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- JoinOpContext ------------------------------------------------------------------

ClickHouseParser::JoinOpContext::JoinOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::JoinOpContext::getRuleIndex() const {
  return ClickHouseParser::RuleJoinOp;
}

void ClickHouseParser::JoinOpContext::copyFrom(JoinOpContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- JoinOpFullContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JoinOpFullContext::FULL() {
  return getToken(ClickHouseParser::FULL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpFullContext::OUTER() {
  return getToken(ClickHouseParser::OUTER, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpFullContext::ALL() {
  return getToken(ClickHouseParser::ALL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpFullContext::ANY() {
  return getToken(ClickHouseParser::ANY, 0);
}

ClickHouseParser::JoinOpFullContext::JoinOpFullContext(JoinOpContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinOpFullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinOpFull(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinOpInnerContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JoinOpInnerContext::INNER() {
  return getToken(ClickHouseParser::INNER, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpInnerContext::ALL() {
  return getToken(ClickHouseParser::ALL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpInnerContext::ANY() {
  return getToken(ClickHouseParser::ANY, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpInnerContext::ASOF() {
  return getToken(ClickHouseParser::ASOF, 0);
}

ClickHouseParser::JoinOpInnerContext::JoinOpInnerContext(JoinOpContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinOpInnerContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinOpInner(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinOpLeftRightContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::LEFT() {
  return getToken(ClickHouseParser::LEFT, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::RIGHT() {
  return getToken(ClickHouseParser::RIGHT, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::OUTER() {
  return getToken(ClickHouseParser::OUTER, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::SEMI() {
  return getToken(ClickHouseParser::SEMI, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::ALL() {
  return getToken(ClickHouseParser::ALL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::ANTI() {
  return getToken(ClickHouseParser::ANTI, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::ANY() {
  return getToken(ClickHouseParser::ANY, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpLeftRightContext::ASOF() {
  return getToken(ClickHouseParser::ASOF, 0);
}

ClickHouseParser::JoinOpLeftRightContext::JoinOpLeftRightContext(JoinOpContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JoinOpLeftRightContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinOpLeftRight(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::JoinOpContext* ClickHouseParser::joinOp() {
  JoinOpContext *_localctx = _tracker.createInstance<JoinOpContext>(_ctx, getState());
  enterRule(_localctx, 136, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1337);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 178, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1303);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 167, _ctx)) {
      case 1: {
        setState(1295);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0)) {
          setState(1294);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(1297);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(1298);
        match(ClickHouseParser::INNER);
        setState(1300);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0)) {
          setState(1299);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        break;
      }

      case 3: {
        setState(1302);
        _la = _input->LA(1);
        if (!((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0))) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpLeftRightContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1319);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 172, _ctx)) {
      case 1: {
        setState(1306);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI) {
          setState(1305);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
            | (1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(1308);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(1310);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(1309);
          match(ClickHouseParser::OUTER);
        }
        break;
      }

      case 2: {
        setState(1312);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(1314);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(1313);
          match(ClickHouseParser::OUTER);
        }
        setState(1317);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI) {
          setState(1316);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
            | (1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        break;
      }

      }
      break;
    }

    case 3: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpFullContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1335);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 177, _ctx)) {
      case 1: {
        setState(1322);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ALL

        || _la == ClickHouseParser::ANY) {
          setState(1321);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ALL

          || _la == ClickHouseParser::ANY)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(1324);
        match(ClickHouseParser::FULL);
        setState(1326);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(1325);
          match(ClickHouseParser::OUTER);
        }
        break;
      }

      case 2: {
        setState(1328);
        match(ClickHouseParser::FULL);
        setState(1330);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(1329);
          match(ClickHouseParser::OUTER);
        }
        setState(1333);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ALL

        || _la == ClickHouseParser::ANY) {
          setState(1332);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ALL

          || _la == ClickHouseParser::ANY)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        break;
      }

      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinOpCrossContext ------------------------------------------------------------------

ClickHouseParser::JoinOpCrossContext::JoinOpCrossContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::JoinOpCrossContext::CROSS() {
  return getToken(ClickHouseParser::CROSS, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpCrossContext::JOIN() {
  return getToken(ClickHouseParser::JOIN, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpCrossContext::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpCrossContext::LOCAL() {
  return getToken(ClickHouseParser::LOCAL, 0);
}

tree::TerminalNode* ClickHouseParser::JoinOpCrossContext::COMMA() {
  return getToken(ClickHouseParser::COMMA, 0);
}


size_t ClickHouseParser::JoinOpCrossContext::getRuleIndex() const {
  return ClickHouseParser::RuleJoinOpCross;
}

antlrcpp::Any ClickHouseParser::JoinOpCrossContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinOpCross(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::JoinOpCrossContext* ClickHouseParser::joinOpCross() {
  JoinOpCrossContext *_localctx = _tracker.createInstance<JoinOpCrossContext>(_ctx, getState());
  enterRule(_localctx, 138, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1345);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(1340);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(1339);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(1342);
        match(ClickHouseParser::CROSS);
        setState(1343);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(1344);
        match(ClickHouseParser::COMMA);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinConstraintClauseContext ------------------------------------------------------------------

ClickHouseParser::JoinConstraintClauseContext::JoinConstraintClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::JoinConstraintClauseContext::ON() {
  return getToken(ClickHouseParser::ON, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::JoinConstraintClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::JoinConstraintClauseContext::USING() {
  return getToken(ClickHouseParser::USING, 0);
}

tree::TerminalNode* ClickHouseParser::JoinConstraintClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::JoinConstraintClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}


size_t ClickHouseParser::JoinConstraintClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleJoinConstraintClause;
}

antlrcpp::Any ClickHouseParser::JoinConstraintClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinConstraintClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::JoinConstraintClauseContext* ClickHouseParser::joinConstraintClause() {
  JoinConstraintClauseContext *_localctx = _tracker.createInstance<JoinConstraintClauseContext>(_ctx, getState());
  enterRule(_localctx, 140, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1356);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 181, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1347);
      match(ClickHouseParser::ON);
      setState(1348);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1349);
      match(ClickHouseParser::USING);
      setState(1350);
      match(ClickHouseParser::LPAREN);
      setState(1351);
      columnExprList();
      setState(1352);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1354);
      match(ClickHouseParser::USING);
      setState(1355);
      columnExprList();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SampleClauseContext ------------------------------------------------------------------

ClickHouseParser::SampleClauseContext::SampleClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SampleClauseContext::SAMPLE() {
  return getToken(ClickHouseParser::SAMPLE, 0);
}

std::vector<ClickHouseParser::RatioExprContext *> ClickHouseParser::SampleClauseContext::ratioExpr() {
  return getRuleContexts<ClickHouseParser::RatioExprContext>();
}

ClickHouseParser::RatioExprContext* ClickHouseParser::SampleClauseContext::ratioExpr(size_t i) {
  return getRuleContext<ClickHouseParser::RatioExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::SampleClauseContext::OFFSET() {
  return getToken(ClickHouseParser::OFFSET, 0);
}


size_t ClickHouseParser::SampleClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSampleClause;
}

antlrcpp::Any ClickHouseParser::SampleClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSampleClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SampleClauseContext* ClickHouseParser::sampleClause() {
  SampleClauseContext *_localctx = _tracker.createInstance<SampleClauseContext>(_ctx, getState());
  enterRule(_localctx, 142, ClickHouseParser::RuleSampleClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1358);
    match(ClickHouseParser::SAMPLE);
    setState(1359);
    ratioExpr();
    setState(1362);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 182, _ctx)) {
    case 1: {
      setState(1360);
      match(ClickHouseParser::OFFSET);
      setState(1361);
      ratioExpr();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitExprContext ------------------------------------------------------------------

ClickHouseParser::LimitExprContext::LimitExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::LimitExprContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::LimitExprContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::LimitExprContext::COMMA() {
  return getToken(ClickHouseParser::COMMA, 0);
}

tree::TerminalNode* ClickHouseParser::LimitExprContext::OFFSET() {
  return getToken(ClickHouseParser::OFFSET, 0);
}


size_t ClickHouseParser::LimitExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleLimitExpr;
}

antlrcpp::Any ClickHouseParser::LimitExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLimitExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LimitExprContext* ClickHouseParser::limitExpr() {
  LimitExprContext *_localctx = _tracker.createInstance<LimitExprContext>(_ctx, getState());
  enterRule(_localctx, 144, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1364);
    columnExpr(0);
    setState(1367);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET || _la == ClickHouseParser::COMMA) {
      setState(1365);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1366);
      columnExpr(0);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderExprListContext ------------------------------------------------------------------

ClickHouseParser::OrderExprListContext::OrderExprListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::OrderExprContext *> ClickHouseParser::OrderExprListContext::orderExpr() {
  return getRuleContexts<ClickHouseParser::OrderExprContext>();
}

ClickHouseParser::OrderExprContext* ClickHouseParser::OrderExprListContext::orderExpr(size_t i) {
  return getRuleContext<ClickHouseParser::OrderExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::OrderExprListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::OrderExprListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::OrderExprListContext::getRuleIndex() const {
  return ClickHouseParser::RuleOrderExprList;
}

antlrcpp::Any ClickHouseParser::OrderExprListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitOrderExprList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::OrderExprListContext* ClickHouseParser::orderExprList() {
  OrderExprListContext *_localctx = _tracker.createInstance<OrderExprListContext>(_ctx, getState());
  enterRule(_localctx, 146, ClickHouseParser::RuleOrderExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1369);
    orderExpr();
    setState(1374);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 184, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1370);
        match(ClickHouseParser::COMMA);
        setState(1371);
        orderExpr(); 
      }
      setState(1376);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 184, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderExprContext ------------------------------------------------------------------

ClickHouseParser::OrderExprContext::OrderExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::OrderExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::NULLS() {
  return getToken(ClickHouseParser::NULLS, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::COLLATE() {
  return getToken(ClickHouseParser::COLLATE, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::ASCENDING() {
  return getToken(ClickHouseParser::ASCENDING, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::DESCENDING() {
  return getToken(ClickHouseParser::DESCENDING, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::DESC() {
  return getToken(ClickHouseParser::DESC, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::FIRST() {
  return getToken(ClickHouseParser::FIRST, 0);
}

tree::TerminalNode* ClickHouseParser::OrderExprContext::LAST() {
  return getToken(ClickHouseParser::LAST, 0);
}


size_t ClickHouseParser::OrderExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleOrderExpr;
}

antlrcpp::Any ClickHouseParser::OrderExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitOrderExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::OrderExprContext* ClickHouseParser::orderExpr() {
  OrderExprContext *_localctx = _tracker.createInstance<OrderExprContext>(_ctx, getState());
  enterRule(_localctx, 148, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1377);
    columnExpr(0);
    setState(1379);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 185, _ctx)) {
    case 1: {
      setState(1378);
      _la = _input->LA(1);
      if (!((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING))) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    }
    setState(1383);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 186, _ctx)) {
    case 1: {
      setState(1381);
      match(ClickHouseParser::NULLS);
      setState(1382);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::FIRST

      || _la == ClickHouseParser::LAST)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    }
    setState(1387);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 187, _ctx)) {
    case 1: {
      setState(1385);
      match(ClickHouseParser::COLLATE);
      setState(1386);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RatioExprContext ------------------------------------------------------------------

ClickHouseParser::RatioExprContext::RatioExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::NumberLiteralContext *> ClickHouseParser::RatioExprContext::numberLiteral() {
  return getRuleContexts<ClickHouseParser::NumberLiteralContext>();
}

ClickHouseParser::NumberLiteralContext* ClickHouseParser::RatioExprContext::numberLiteral(size_t i) {
  return getRuleContext<ClickHouseParser::NumberLiteralContext>(i);
}

tree::TerminalNode* ClickHouseParser::RatioExprContext::SLASH() {
  return getToken(ClickHouseParser::SLASH, 0);
}


size_t ClickHouseParser::RatioExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleRatioExpr;
}

antlrcpp::Any ClickHouseParser::RatioExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitRatioExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::RatioExprContext* ClickHouseParser::ratioExpr() {
  RatioExprContext *_localctx = _tracker.createInstance<RatioExprContext>(_ctx, getState());
  enterRule(_localctx, 150, ClickHouseParser::RuleRatioExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1389);
    numberLiteral();
    setState(1392);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 188, _ctx)) {
    case 1: {
      setState(1390);
      match(ClickHouseParser::SLASH);
      setState(1391);
      numberLiteral();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SettingExprListContext ------------------------------------------------------------------

ClickHouseParser::SettingExprListContext::SettingExprListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::SettingExprContext *> ClickHouseParser::SettingExprListContext::settingExpr() {
  return getRuleContexts<ClickHouseParser::SettingExprContext>();
}

ClickHouseParser::SettingExprContext* ClickHouseParser::SettingExprListContext::settingExpr(size_t i) {
  return getRuleContext<ClickHouseParser::SettingExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::SettingExprListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::SettingExprListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::SettingExprListContext::getRuleIndex() const {
  return ClickHouseParser::RuleSettingExprList;
}

antlrcpp::Any ClickHouseParser::SettingExprListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSettingExprList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SettingExprListContext* ClickHouseParser::settingExprList() {
  SettingExprListContext *_localctx = _tracker.createInstance<SettingExprListContext>(_ctx, getState());
  enterRule(_localctx, 152, ClickHouseParser::RuleSettingExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1394);
    settingExpr();
    setState(1399);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 189, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1395);
        match(ClickHouseParser::COMMA);
        setState(1396);
        settingExpr(); 
      }
      setState(1401);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 189, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SettingExprContext ------------------------------------------------------------------

ClickHouseParser::SettingExprContext::SettingExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::SettingExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::SettingExprContext::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

ClickHouseParser::LiteralContext* ClickHouseParser::SettingExprContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}


size_t ClickHouseParser::SettingExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleSettingExpr;
}

antlrcpp::Any ClickHouseParser::SettingExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSettingExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SettingExprContext* ClickHouseParser::settingExpr() {
  SettingExprContext *_localctx = _tracker.createInstance<SettingExprContext>(_ctx, getState());
  enterRule(_localctx, 154, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1402);
    identifier();
    setState(1403);
    match(ClickHouseParser::EQ_SINGLE);
    setState(1404);
    literal();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SetStmtContext ------------------------------------------------------------------

ClickHouseParser::SetStmtContext::SetStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SetStmtContext::SET() {
  return getToken(ClickHouseParser::SET, 0);
}

ClickHouseParser::SettingExprListContext* ClickHouseParser::SetStmtContext::settingExprList() {
  return getRuleContext<ClickHouseParser::SettingExprListContext>(0);
}


size_t ClickHouseParser::SetStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleSetStmt;
}

antlrcpp::Any ClickHouseParser::SetStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSetStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SetStmtContext* ClickHouseParser::setStmt() {
  SetStmtContext *_localctx = _tracker.createInstance<SetStmtContext>(_ctx, getState());
  enterRule(_localctx, 156, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1406);
    match(ClickHouseParser::SET);
    setState(1407);
    settingExprList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ShowStmtContext ------------------------------------------------------------------

ClickHouseParser::ShowStmtContext::ShowStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ShowStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleShowStmt;
}

void ClickHouseParser::ShowStmtContext::copyFrom(ShowStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ShowCreateDatabaseStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowCreateDatabaseStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateDatabaseStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::ShowCreateDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

ClickHouseParser::ShowCreateDatabaseStmtContext::ShowCreateDatabaseStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowCreateDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowCreateDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowDatabasesStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowDatabasesStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowDatabasesStmtContext::DATABASES() {
  return getToken(ClickHouseParser::DATABASES, 0);
}

ClickHouseParser::ShowDatabasesStmtContext::ShowDatabasesStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowDatabasesStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowDatabasesStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ShowCreateTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::ShowCreateTableStmtContext::ShowCreateTableStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowCreateTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowCreateTableStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowTablesStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::TABLES() {
  return getToken(ClickHouseParser::TABLES, 0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::ShowTablesStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::LIKE() {
  return getToken(ClickHouseParser::LIKE, 0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

ClickHouseParser::WhereClauseContext* ClickHouseParser::ShowTablesStmtContext::whereClause() {
  return getRuleContext<ClickHouseParser::WhereClauseContext>(0);
}

ClickHouseParser::LimitClauseContext* ClickHouseParser::ShowTablesStmtContext::limitClause() {
  return getRuleContext<ClickHouseParser::LimitClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

tree::TerminalNode* ClickHouseParser::ShowTablesStmtContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::ShowTablesStmtContext::ShowTablesStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowTablesStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowTablesStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowDictionariesStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowDictionariesStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowDictionariesStmtContext::DICTIONARIES() {
  return getToken(ClickHouseParser::DICTIONARIES, 0);
}

tree::TerminalNode* ClickHouseParser::ShowDictionariesStmtContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::ShowDictionariesStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

ClickHouseParser::ShowDictionariesStmtContext::ShowDictionariesStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowDictionariesStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowDictionariesStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateDictionaryStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowCreateDictionaryStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateDictionaryStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateDictionaryStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ShowCreateDictionaryStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::ShowCreateDictionaryStmtContext::ShowCreateDictionaryStmtContext(ShowStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ShowCreateDictionaryStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitShowCreateDictionaryStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ShowStmtContext* ClickHouseParser::showStmt() {
  ShowStmtContext *_localctx = _tracker.createInstance<ShowStmtContext>(_ctx, getState());
  enterRule(_localctx, 158, ClickHouseParser::RuleShowStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1451);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 197, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1409);
      match(ClickHouseParser::SHOW);
      setState(1410);
      match(ClickHouseParser::CREATE);
      setState(1411);
      match(ClickHouseParser::DATABASE);
      setState(1412);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateDictionaryStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1413);
      match(ClickHouseParser::SHOW);
      setState(1414);
      match(ClickHouseParser::CREATE);
      setState(1415);
      match(ClickHouseParser::DICTIONARY);
      setState(1416);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1417);
      match(ClickHouseParser::SHOW);
      setState(1418);
      match(ClickHouseParser::CREATE);
      setState(1420);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 190, _ctx)) {
      case 1: {
        setState(1419);
        match(ClickHouseParser::TEMPORARY);
        break;
      }

      }
      setState(1423);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 191, _ctx)) {
      case 1: {
        setState(1422);
        match(ClickHouseParser::TABLE);
        break;
      }

      }
      setState(1425);
      tableIdentifier();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowDatabasesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(1426);
      match(ClickHouseParser::SHOW);
      setState(1427);
      match(ClickHouseParser::DATABASES);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowDictionariesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(1428);
      match(ClickHouseParser::SHOW);
      setState(1429);
      match(ClickHouseParser::DICTIONARIES);
      setState(1432);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM) {
        setState(1430);
        match(ClickHouseParser::FROM);
        setState(1431);
        databaseIdentifier();
      }
      break;
    }

    case 6: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowTablesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(1434);
      match(ClickHouseParser::SHOW);
      setState(1436);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(1435);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(1438);
      match(ClickHouseParser::TABLES);
      setState(1441);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM

      || _la == ClickHouseParser::IN) {
        setState(1439);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::FROM

        || _la == ClickHouseParser::IN)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(1440);
        databaseIdentifier();
      }
      setState(1446);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::LIKE: {
          setState(1443);
          match(ClickHouseParser::LIKE);
          setState(1444);
          match(ClickHouseParser::STRING_LITERAL);
          break;
        }

        case ClickHouseParser::WHERE: {
          setState(1445);
          whereClause();
          break;
        }

        case ClickHouseParser::EOF:
        case ClickHouseParser::FORMAT:
        case ClickHouseParser::INTO:
        case ClickHouseParser::LIMIT:
        case ClickHouseParser::SEMICOLON: {
          break;
        }

      default:
        break;
      }
      setState(1449);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::LIMIT) {
        setState(1448);
        limitClause();
      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SystemStmtContext ------------------------------------------------------------------

ClickHouseParser::SystemStmtContext::SystemStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::SYSTEM() {
  return getToken(ClickHouseParser::SYSTEM, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::FLUSH() {
  return getToken(ClickHouseParser::FLUSH, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::DISTRIBUTED() {
  return getToken(ClickHouseParser::DISTRIBUTED, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SystemStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::LOGS() {
  return getToken(ClickHouseParser::LOGS, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::RELOAD() {
  return getToken(ClickHouseParser::RELOAD, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::DICTIONARIES() {
  return getToken(ClickHouseParser::DICTIONARIES, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::START() {
  return getToken(ClickHouseParser::START, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::STOP() {
  return getToken(ClickHouseParser::STOP, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::SENDS() {
  return getToken(ClickHouseParser::SENDS, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::FETCHES() {
  return getToken(ClickHouseParser::FETCHES, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::MERGES() {
  return getToken(ClickHouseParser::MERGES, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::REPLICATED() {
  return getToken(ClickHouseParser::REPLICATED, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::SYNC() {
  return getToken(ClickHouseParser::SYNC, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::REPLICA() {
  return getToken(ClickHouseParser::REPLICA, 0);
}


size_t ClickHouseParser::SystemStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleSystemStmt;
}

antlrcpp::Any ClickHouseParser::SystemStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSystemStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::SystemStmtContext* ClickHouseParser::systemStmt() {
  SystemStmtContext *_localctx = _tracker.createInstance<SystemStmtContext>(_ctx, getState());
  enterRule(_localctx, 160, ClickHouseParser::RuleSystemStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1487);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 200, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1453);
      match(ClickHouseParser::SYSTEM);
      setState(1454);
      match(ClickHouseParser::FLUSH);
      setState(1455);
      match(ClickHouseParser::DISTRIBUTED);
      setState(1456);
      tableIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1457);
      match(ClickHouseParser::SYSTEM);
      setState(1458);
      match(ClickHouseParser::FLUSH);
      setState(1459);
      match(ClickHouseParser::LOGS);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1460);
      match(ClickHouseParser::SYSTEM);
      setState(1461);
      match(ClickHouseParser::RELOAD);
      setState(1462);
      match(ClickHouseParser::DICTIONARIES);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(1463);
      match(ClickHouseParser::SYSTEM);
      setState(1464);
      match(ClickHouseParser::RELOAD);
      setState(1465);
      match(ClickHouseParser::DICTIONARY);
      setState(1466);
      tableIdentifier();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(1467);
      match(ClickHouseParser::SYSTEM);
      setState(1468);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::START

      || _la == ClickHouseParser::STOP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1476);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::DISTRIBUTED: {
          setState(1469);
          match(ClickHouseParser::DISTRIBUTED);
          setState(1470);
          match(ClickHouseParser::SENDS);
          break;
        }

        case ClickHouseParser::FETCHES: {
          setState(1471);
          match(ClickHouseParser::FETCHES);
          break;
        }

        case ClickHouseParser::MERGES:
        case ClickHouseParser::TTL: {
          setState(1473);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::TTL) {
            setState(1472);
            match(ClickHouseParser::TTL);
          }
          setState(1475);
          match(ClickHouseParser::MERGES);
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(1478);
      tableIdentifier();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(1479);
      match(ClickHouseParser::SYSTEM);
      setState(1480);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::START

      || _la == ClickHouseParser::STOP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1481);
      match(ClickHouseParser::REPLICATED);
      setState(1482);
      match(ClickHouseParser::SENDS);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(1483);
      match(ClickHouseParser::SYSTEM);
      setState(1484);
      match(ClickHouseParser::SYNC);
      setState(1485);
      match(ClickHouseParser::REPLICA);
      setState(1486);
      tableIdentifier();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TruncateStmtContext ------------------------------------------------------------------

ClickHouseParser::TruncateStmtContext::TruncateStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::TRUNCATE() {
  return getToken(ClickHouseParser::TRUNCATE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::TruncateStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::ClusterClauseContext* ClickHouseParser::TruncateStmtContext::clusterClause() {
  return getRuleContext<ClickHouseParser::ClusterClauseContext>(0);
}


size_t ClickHouseParser::TruncateStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleTruncateStmt;
}

antlrcpp::Any ClickHouseParser::TruncateStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTruncateStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TruncateStmtContext* ClickHouseParser::truncateStmt() {
  TruncateStmtContext *_localctx = _tracker.createInstance<TruncateStmtContext>(_ctx, getState());
  enterRule(_localctx, 162, ClickHouseParser::RuleTruncateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1489);
    match(ClickHouseParser::TRUNCATE);
    setState(1491);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 201, _ctx)) {
    case 1: {
      setState(1490);
      match(ClickHouseParser::TEMPORARY);
      break;
    }

    }
    setState(1494);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 202, _ctx)) {
    case 1: {
      setState(1493);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(1498);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 203, _ctx)) {
    case 1: {
      setState(1496);
      match(ClickHouseParser::IF);
      setState(1497);
      match(ClickHouseParser::EXISTS);
      break;
    }

    }
    setState(1500);
    tableIdentifier();
    setState(1502);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(1501);
      clusterClause();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UseStmtContext ------------------------------------------------------------------

ClickHouseParser::UseStmtContext::UseStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::UseStmtContext::USE() {
  return getToken(ClickHouseParser::USE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::UseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}


size_t ClickHouseParser::UseStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleUseStmt;
}

antlrcpp::Any ClickHouseParser::UseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitUseStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::UseStmtContext* ClickHouseParser::useStmt() {
  UseStmtContext *_localctx = _tracker.createInstance<UseStmtContext>(_ctx, getState());
  enterRule(_localctx, 164, ClickHouseParser::RuleUseStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1504);
    match(ClickHouseParser::USE);
    setState(1505);
    databaseIdentifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WatchStmtContext ------------------------------------------------------------------

ClickHouseParser::WatchStmtContext::WatchStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::WatchStmtContext::WATCH() {
  return getToken(ClickHouseParser::WATCH, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::WatchStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::WatchStmtContext::EVENTS() {
  return getToken(ClickHouseParser::EVENTS, 0);
}

tree::TerminalNode* ClickHouseParser::WatchStmtContext::LIMIT() {
  return getToken(ClickHouseParser::LIMIT, 0);
}

tree::TerminalNode* ClickHouseParser::WatchStmtContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}


size_t ClickHouseParser::WatchStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleWatchStmt;
}

antlrcpp::Any ClickHouseParser::WatchStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitWatchStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::WatchStmtContext* ClickHouseParser::watchStmt() {
  WatchStmtContext *_localctx = _tracker.createInstance<WatchStmtContext>(_ctx, getState());
  enterRule(_localctx, 166, ClickHouseParser::RuleWatchStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1507);
    match(ClickHouseParser::WATCH);
    setState(1508);
    tableIdentifier();
    setState(1510);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EVENTS) {
      setState(1509);
      match(ClickHouseParser::EVENTS);
    }
    setState(1514);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(1512);
      match(ClickHouseParser::LIMIT);
      setState(1513);
      match(ClickHouseParser::DECIMAL_LITERAL);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnTypeExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnTypeExprContext::ColumnTypeExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ColumnTypeExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnTypeExpr;
}

void ClickHouseParser::ColumnTypeExprContext::copyFrom(ColumnTypeExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ColumnTypeExprNestedContext ------------------------------------------------------------------

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::ColumnTypeExprNestedContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnTypeExprNestedContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprNestedContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::ColumnTypeExprContext *> ClickHouseParser::ColumnTypeExprNestedContext::columnTypeExpr() {
  return getRuleContexts<ClickHouseParser::ColumnTypeExprContext>();
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::ColumnTypeExprNestedContext::columnTypeExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprNestedContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnTypeExprNestedContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprNestedContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::ColumnTypeExprNestedContext::ColumnTypeExprNestedContext(ColumnTypeExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnTypeExprNestedContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnTypeExprNested(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnTypeExprParamContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnTypeExprParamContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprParamContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprParamContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ColumnTypeExprParamContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

ClickHouseParser::ColumnTypeExprParamContext::ColumnTypeExprParamContext(ColumnTypeExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnTypeExprParamContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnTypeExprParam(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnTypeExprSimpleContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnTypeExprSimpleContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::ColumnTypeExprSimpleContext::ColumnTypeExprSimpleContext(ColumnTypeExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnTypeExprSimpleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnTypeExprSimple(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnTypeExprComplexContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnTypeExprComplexContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprComplexContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::ColumnTypeExprContext *> ClickHouseParser::ColumnTypeExprComplexContext::columnTypeExpr() {
  return getRuleContexts<ClickHouseParser::ColumnTypeExprContext>();
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::ColumnTypeExprComplexContext::columnTypeExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprComplexContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnTypeExprComplexContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprComplexContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::ColumnTypeExprComplexContext::ColumnTypeExprComplexContext(ColumnTypeExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnTypeExprComplexContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnTypeExprComplex(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnTypeExprEnumContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnTypeExprEnumContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprEnumContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::EnumValueContext *> ClickHouseParser::ColumnTypeExprEnumContext::enumValue() {
  return getRuleContexts<ClickHouseParser::EnumValueContext>();
}

ClickHouseParser::EnumValueContext* ClickHouseParser::ColumnTypeExprEnumContext::enumValue(size_t i) {
  return getRuleContext<ClickHouseParser::EnumValueContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprEnumContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnTypeExprEnumContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprEnumContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::ColumnTypeExprEnumContext::ColumnTypeExprEnumContext(ColumnTypeExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnTypeExprEnumContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnTypeExprEnum(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::columnTypeExpr() {
  ColumnTypeExprContext *_localctx = _tracker.createInstance<ColumnTypeExprContext>(_ctx, getState());
  enterRule(_localctx, 168, ClickHouseParser::RuleColumnTypeExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1563);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 211, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprSimpleContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1516);
      identifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprNestedContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1517);
      identifier();
      setState(1518);
      match(ClickHouseParser::LPAREN);
      setState(1519);
      identifier();
      setState(1520);
      columnTypeExpr();
      setState(1527);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(1521);
        match(ClickHouseParser::COMMA);
        setState(1522);
        identifier();
        setState(1523);
        columnTypeExpr();
        setState(1529);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1530);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprEnumContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1532);
      identifier();
      setState(1533);
      match(ClickHouseParser::LPAREN);
      setState(1534);
      enumValue();
      setState(1539);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(1535);
        match(ClickHouseParser::COMMA);
        setState(1536);
        enumValue();
        setState(1541);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1542);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprComplexContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(1544);
      identifier();
      setState(1545);
      match(ClickHouseParser::LPAREN);
      setState(1546);
      columnTypeExpr();
      setState(1551);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(1547);
        match(ClickHouseParser::COMMA);
        setState(1548);
        columnTypeExpr();
        setState(1553);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1554);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprParamContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(1556);
      identifier();
      setState(1557);
      match(ClickHouseParser::LPAREN);
      setState(1559);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NAN_SQL - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULL_SQL - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
        | (1ULL << (ClickHouseParser::DOT - 197))
        | (1ULL << (ClickHouseParser::LBRACKET - 197))
        | (1ULL << (ClickHouseParser::LPAREN - 197))
        | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
        setState(1558);
        columnExprList();
      }
      setState(1561);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnExprListContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprListContext::ColumnExprListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::ColumnsExprContext *> ClickHouseParser::ColumnExprListContext::columnsExpr() {
  return getRuleContexts<ClickHouseParser::ColumnsExprContext>();
}

ClickHouseParser::ColumnsExprContext* ClickHouseParser::ColumnExprListContext::columnsExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnsExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnExprListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnExprListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ColumnExprListContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnExprList;
}

antlrcpp::Any ClickHouseParser::ColumnExprListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::columnExprList() {
  ColumnExprListContext *_localctx = _tracker.createInstance<ColumnExprListContext>(_ctx, getState());
  enterRule(_localctx, 170, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1565);
    columnsExpr();
    setState(1570);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 212, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1566);
        match(ClickHouseParser::COMMA);
        setState(1567);
        columnsExpr(); 
      }
      setState(1572);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 212, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnsExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnsExprContext::ColumnsExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ColumnsExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnsExpr;
}

void ClickHouseParser::ColumnsExprContext::copyFrom(ColumnsExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ColumnsExprColumnContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnsExprColumnContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::ColumnsExprColumnContext::ColumnsExprColumnContext(ColumnsExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnsExprColumnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnsExprColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnsExprAsteriskContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnsExprAsteriskContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ColumnsExprAsteriskContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnsExprAsteriskContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}

ClickHouseParser::ColumnsExprAsteriskContext::ColumnsExprAsteriskContext(ColumnsExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnsExprAsteriskContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnsExprAsterisk(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnsExprSubqueryContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnsExprSubqueryContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::ColumnsExprSubqueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnsExprSubqueryContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnsExprSubqueryContext::ColumnsExprSubqueryContext(ColumnsExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnsExprSubqueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnsExprSubquery(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ColumnsExprContext* ClickHouseParser::columnsExpr() {
  ColumnsExprContext *_localctx = _tracker.createInstance<ColumnsExprContext>(_ctx, getState());
  enterRule(_localctx, 172, ClickHouseParser::RuleColumnsExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1584);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 214, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprAsteriskContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1576);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
        setState(1573);
        tableIdentifier();
        setState(1574);
        match(ClickHouseParser::DOT);
      }
      setState(1578);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprSubqueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1579);
      match(ClickHouseParser::LPAREN);
      setState(1580);
      selectUnionStmt();
      setState(1581);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1583);
      columnExpr(0);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext::ColumnExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ColumnExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnExpr;
}

void ClickHouseParser::ColumnExprContext::copyFrom(ColumnExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ColumnExprTernaryOpContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprTernaryOpContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprTernaryOpContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTernaryOpContext::QUERY() {
  return getToken(ClickHouseParser::QUERY, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTernaryOpContext::COLON() {
  return getToken(ClickHouseParser::COLON, 0);
}

ClickHouseParser::ColumnExprTernaryOpContext::ColumnExprTernaryOpContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprTernaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprTernaryOp(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprAliasContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprAliasContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::AliasContext* ClickHouseParser::ColumnExprAliasContext::alias() {
  return getRuleContext<ClickHouseParser::AliasContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprAliasContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnExprAliasContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::ColumnExprAliasContext::ColumnExprAliasContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprAliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprAlias(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprExtractContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprExtractContext::EXTRACT() {
  return getToken(ClickHouseParser::EXTRACT, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprExtractContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::IntervalContext* ClickHouseParser::ColumnExprExtractContext::interval() {
  return getRuleContext<ClickHouseParser::IntervalContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprExtractContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprExtractContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprExtractContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprExtractContext::ColumnExprExtractContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprExtractContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprExtract(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprNegateContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprNegateContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprNegateContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::ColumnExprNegateContext::ColumnExprNegateContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprNegateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprNegate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprSubqueryContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprSubqueryContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::ColumnExprSubqueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprSubqueryContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprSubqueryContext::ColumnExprSubqueryContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprSubqueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprSubquery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprLiteralContext ------------------------------------------------------------------

ClickHouseParser::LiteralContext* ClickHouseParser::ColumnExprLiteralContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}

ClickHouseParser::ColumnExprLiteralContext::ColumnExprLiteralContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprArrayContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprArrayContext::LBRACKET() {
  return getToken(ClickHouseParser::LBRACKET, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprArrayContext::RBRACKET() {
  return getToken(ClickHouseParser::RBRACKET, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ColumnExprArrayContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

ClickHouseParser::ColumnExprArrayContext::ColumnExprArrayContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprArrayContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprArray(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprSubstringContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprSubstringContext::SUBSTRING() {
  return getToken(ClickHouseParser::SUBSTRING, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprSubstringContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprSubstringContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprSubstringContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprSubstringContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprSubstringContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprSubstringContext::FOR() {
  return getToken(ClickHouseParser::FOR, 0);
}

ClickHouseParser::ColumnExprSubstringContext::ColumnExprSubstringContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprSubstringContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprSubstring(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprCastContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprCastContext::CAST() {
  return getToken(ClickHouseParser::CAST, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCastContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprCastContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCastContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::ColumnExprCastContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCastContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprCastContext::ColumnExprCastContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprCastContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprCast(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprOrContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprOrContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprOrContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprOrContext::OR() {
  return getToken(ClickHouseParser::OR, 0);
}

ClickHouseParser::ColumnExprOrContext::ColumnExprOrContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprOrContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprOr(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprPrecedence1Context ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprPrecedence1Context::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprPrecedence1Context::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence1Context::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence1Context::SLASH() {
  return getToken(ClickHouseParser::SLASH, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence1Context::PERCENT() {
  return getToken(ClickHouseParser::PERCENT, 0);
}

ClickHouseParser::ColumnExprPrecedence1Context::ColumnExprPrecedence1Context(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprPrecedence1Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprPrecedence1(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprPrecedence2Context ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprPrecedence2Context::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprPrecedence2Context::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence2Context::PLUS() {
  return getToken(ClickHouseParser::PLUS, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence2Context::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence2Context::CONCAT() {
  return getToken(ClickHouseParser::CONCAT, 0);
}

ClickHouseParser::ColumnExprPrecedence2Context::ColumnExprPrecedence2Context(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprPrecedence2Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprPrecedence2(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprPrecedence3Context ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprPrecedence3Context::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprPrecedence3Context::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::EQ_DOUBLE() {
  return getToken(ClickHouseParser::EQ_DOUBLE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::NOT_EQ() {
  return getToken(ClickHouseParser::NOT_EQ, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::LE() {
  return getToken(ClickHouseParser::LE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::GE() {
  return getToken(ClickHouseParser::GE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::LT() {
  return getToken(ClickHouseParser::LT, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::GT() {
  return getToken(ClickHouseParser::GT, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::LIKE() {
  return getToken(ClickHouseParser::LIKE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::ILIKE() {
  return getToken(ClickHouseParser::ILIKE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprPrecedence3Context::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::ColumnExprPrecedence3Context::ColumnExprPrecedence3Context(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprPrecedence3Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprPrecedence3(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprIntervalContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprIntervalContext::INTERVAL() {
  return getToken(ClickHouseParser::INTERVAL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprIntervalContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::IntervalContext* ClickHouseParser::ColumnExprIntervalContext::interval() {
  return getRuleContext<ClickHouseParser::IntervalContext>(0);
}

ClickHouseParser::ColumnExprIntervalContext::ColumnExprIntervalContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprIntervalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprInterval(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprIsNullContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprIsNullContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprIsNullContext::IS() {
  return getToken(ClickHouseParser::IS, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprIsNullContext::NULL_SQL() {
  return getToken(ClickHouseParser::NULL_SQL, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprIsNullContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::ColumnExprIsNullContext::ColumnExprIsNullContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprIsNullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprIsNull(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprTrimContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::TRIM() {
  return getToken(ClickHouseParser::TRIM, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprTrimContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::BOTH() {
  return getToken(ClickHouseParser::BOTH, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::LEADING() {
  return getToken(ClickHouseParser::LEADING, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTrimContext::TRAILING() {
  return getToken(ClickHouseParser::TRAILING, 0);
}

ClickHouseParser::ColumnExprTrimContext::ColumnExprTrimContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprTrimContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprTrim(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprTupleContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprTupleContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ColumnExprTupleContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTupleContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprTupleContext::ColumnExprTupleContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprTupleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprTuple(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprArrayAccessContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprArrayAccessContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprArrayAccessContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprArrayAccessContext::LBRACKET() {
  return getToken(ClickHouseParser::LBRACKET, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprArrayAccessContext::RBRACKET() {
  return getToken(ClickHouseParser::RBRACKET, 0);
}

ClickHouseParser::ColumnExprArrayAccessContext::ColumnExprArrayAccessContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprArrayAccessContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprArrayAccess(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprBetweenContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprBetweenContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprBetweenContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprBetweenContext::BETWEEN() {
  return getToken(ClickHouseParser::BETWEEN, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprBetweenContext::AND() {
  return getToken(ClickHouseParser::AND, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprBetweenContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::ColumnExprBetweenContext::ColumnExprBetweenContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprBetweenContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprBetween(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprParensContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprParensContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprParensContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprParensContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::ColumnExprParensContext::ColumnExprParensContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprParensContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprParens(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprTimestampContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprTimestampContext::TIMESTAMP() {
  return getToken(ClickHouseParser::TIMESTAMP, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTimestampContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

ClickHouseParser::ColumnExprTimestampContext::ColumnExprTimestampContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprTimestampContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprTimestamp(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprAndContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprAndContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprAndContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprAndContext::AND() {
  return getToken(ClickHouseParser::AND, 0);
}

ClickHouseParser::ColumnExprAndContext::ColumnExprAndContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprAndContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprAnd(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprTupleAccessContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprTupleAccessContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTupleAccessContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprTupleAccessContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}

ClickHouseParser::ColumnExprTupleAccessContext::ColumnExprTupleAccessContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprTupleAccessContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprTupleAccess(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprCaseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprCaseContext::CASE() {
  return getToken(ClickHouseParser::CASE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCaseContext::END() {
  return getToken(ClickHouseParser::END, 0);
}

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprCaseContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprCaseContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnExprCaseContext::WHEN() {
  return getTokens(ClickHouseParser::WHEN);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCaseContext::WHEN(size_t i) {
  return getToken(ClickHouseParser::WHEN, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnExprCaseContext::THEN() {
  return getTokens(ClickHouseParser::THEN);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCaseContext::THEN(size_t i) {
  return getToken(ClickHouseParser::THEN, i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprCaseContext::ELSE() {
  return getToken(ClickHouseParser::ELSE, 0);
}

ClickHouseParser::ColumnExprCaseContext::ColumnExprCaseContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprCaseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprCase(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprDateContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprDateContext::DATE() {
  return getToken(ClickHouseParser::DATE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprDateContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

ClickHouseParser::ColumnExprDateContext::ColumnExprDateContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprDateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprDate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprNotContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprNotContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprNotContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::ColumnExprNotContext::ColumnExprNotContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprNotContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprNot(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprIdentifierContext ------------------------------------------------------------------

ClickHouseParser::ColumnIdentifierContext* ClickHouseParser::ColumnExprIdentifierContext::columnIdentifier() {
  return getRuleContext<ClickHouseParser::ColumnIdentifierContext>(0);
}

ClickHouseParser::ColumnExprIdentifierContext::ColumnExprIdentifierContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprIdentifier(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprFunctionContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnExprFunctionContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnExprFunctionContext::LPAREN() {
  return getTokens(ClickHouseParser::LPAREN);
}

tree::TerminalNode* ClickHouseParser::ColumnExprFunctionContext::LPAREN(size_t i) {
  return getToken(ClickHouseParser::LPAREN, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnExprFunctionContext::RPAREN() {
  return getTokens(ClickHouseParser::RPAREN);
}

tree::TerminalNode* ClickHouseParser::ColumnExprFunctionContext::RPAREN(size_t i) {
  return getToken(ClickHouseParser::RPAREN, i);
}

tree::TerminalNode* ClickHouseParser::ColumnExprFunctionContext::DISTINCT() {
  return getToken(ClickHouseParser::DISTINCT, 0);
}

ClickHouseParser::ColumnArgListContext* ClickHouseParser::ColumnExprFunctionContext::columnArgList() {
  return getRuleContext<ClickHouseParser::ColumnArgListContext>(0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ColumnExprFunctionContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

ClickHouseParser::ColumnExprFunctionContext::ColumnExprFunctionContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprFunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnExprAsteriskContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprAsteriskContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ColumnExprAsteriskContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprAsteriskContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}

ClickHouseParser::ColumnExprAsteriskContext::ColumnExprAsteriskContext(ColumnExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::ColumnExprAsteriskContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprAsterisk(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::columnExpr() {
   return columnExpr(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::columnExpr(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  ClickHouseParser::ColumnExprContext *_localctx = _tracker.createInstance<ColumnExprContext>(_ctx, parentState);
  ClickHouseParser::ColumnExprContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 174;
  enterRecursionRule(_localctx, 174, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1693);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 225, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1587);
      match(ClickHouseParser::CASE);
      setState(1589);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 215, _ctx)) {
      case 1: {
        setState(1588);
        columnExpr(0);
        break;
      }

      }
      setState(1596); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(1591);
        match(ClickHouseParser::WHEN);
        setState(1592);
        columnExpr(0);
        setState(1593);
        match(ClickHouseParser::THEN);
        setState(1594);
        columnExpr(0);
        setState(1598); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(1602);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(1600);
        match(ClickHouseParser::ELSE);
        setState(1601);
        columnExpr(0);
      }
      setState(1604);
      match(ClickHouseParser::END);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprCastContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1606);
      match(ClickHouseParser::CAST);
      setState(1607);
      match(ClickHouseParser::LPAREN);
      setState(1608);
      columnExpr(0);
      setState(1609);
      match(ClickHouseParser::AS);
      setState(1610);
      columnTypeExpr();
      setState(1611);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprDateContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1613);
      match(ClickHouseParser::DATE);
      setState(1614);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1615);
      match(ClickHouseParser::EXTRACT);
      setState(1616);
      match(ClickHouseParser::LPAREN);
      setState(1617);
      interval();
      setState(1618);
      match(ClickHouseParser::FROM);
      setState(1619);
      columnExpr(0);
      setState(1620);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1622);
      match(ClickHouseParser::INTERVAL);
      setState(1623);
      columnExpr(0);
      setState(1624);
      interval();
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprSubstringContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1626);
      match(ClickHouseParser::SUBSTRING);
      setState(1627);
      match(ClickHouseParser::LPAREN);
      setState(1628);
      columnExpr(0);
      setState(1629);
      match(ClickHouseParser::FROM);
      setState(1630);
      columnExpr(0);
      setState(1633);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FOR) {
        setState(1631);
        match(ClickHouseParser::FOR);
        setState(1632);
        columnExpr(0);
      }
      setState(1635);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprTimestampContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1637);
      match(ClickHouseParser::TIMESTAMP);
      setState(1638);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1639);
      match(ClickHouseParser::TRIM);
      setState(1640);
      match(ClickHouseParser::LPAREN);
      setState(1641);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::BOTH || _la == ClickHouseParser::LEADING || _la == ClickHouseParser::TRAILING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1642);
      match(ClickHouseParser::STRING_LITERAL);
      setState(1643);
      match(ClickHouseParser::FROM);
      setState(1644);
      columnExpr(0);
      setState(1645);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1647);
      identifier();
      setState(1653);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 220, _ctx)) {
      case 1: {
        setState(1648);
        match(ClickHouseParser::LPAREN);
        setState(1650);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
          | (1ULL << ClickHouseParser::ALIAS)
          | (1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ALTER)
          | (1ULL << ClickHouseParser::AND)
          | (1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ARRAY)
          | (1ULL << ClickHouseParser::AS)
          | (1ULL << ClickHouseParser::ASCENDING)
          | (1ULL << ClickHouseParser::ASOF)
          | (1ULL << ClickHouseParser::AST)
          | (1ULL << ClickHouseParser::ASYNC)
          | (1ULL << ClickHouseParser::ATTACH)
          | (1ULL << ClickHouseParser::BETWEEN)
          | (1ULL << ClickHouseParser::BOTH)
          | (1ULL << ClickHouseParser::BY)
          | (1ULL << ClickHouseParser::CASE)
          | (1ULL << ClickHouseParser::CAST)
          | (1ULL << ClickHouseParser::CHECK)
          | (1ULL << ClickHouseParser::CLEAR)
          | (1ULL << ClickHouseParser::CLUSTER)
          | (1ULL << ClickHouseParser::CODEC)
          | (1ULL << ClickHouseParser::COLLATE)
          | (1ULL << ClickHouseParser::COLUMN)
          | (1ULL << ClickHouseParser::COMMENT)
          | (1ULL << ClickHouseParser::CONSTRAINT)
          | (1ULL << ClickHouseParser::CREATE)
          | (1ULL << ClickHouseParser::CROSS)
          | (1ULL << ClickHouseParser::CUBE)
          | (1ULL << ClickHouseParser::DATABASE)
          | (1ULL << ClickHouseParser::DATABASES)
          | (1ULL << ClickHouseParser::DATE)
          | (1ULL << ClickHouseParser::DAY)
          | (1ULL << ClickHouseParser::DEDUPLICATE)
          | (1ULL << ClickHouseParser::DEFAULT)
          | (1ULL << ClickHouseParser::DELAY)
          | (1ULL << ClickHouseParser::DELETE)
          | (1ULL << ClickHouseParser::DESC)
          | (1ULL << ClickHouseParser::DESCENDING)
          | (1ULL << ClickHouseParser::DESCRIBE)
          | (1ULL << ClickHouseParser::DETACH)
          | (1ULL << ClickHouseParser::DICTIONARIES)
          | (1ULL << ClickHouseParser::DICTIONARY)
          | (1ULL << ClickHouseParser::DISK)
          | (1ULL << ClickHouseParser::DISTINCT)
          | (1ULL << ClickHouseParser::DISTRIBUTED)
          | (1ULL << ClickHouseParser::DROP)
          | (1ULL << ClickHouseParser::ELSE)
          | (1ULL << ClickHouseParser::END)
          | (1ULL << ClickHouseParser::ENGINE)
          | (1ULL << ClickHouseParser::EVENTS)
          | (1ULL << ClickHouseParser::EXISTS)
          | (1ULL << ClickHouseParser::EXPLAIN)
          | (1ULL << ClickHouseParser::EXPRESSION)
          | (1ULL << ClickHouseParser::EXTRACT)
          | (1ULL << ClickHouseParser::FETCHES)
          | (1ULL << ClickHouseParser::FINAL)
          | (1ULL << ClickHouseParser::FIRST)
          | (1ULL << ClickHouseParser::FLUSH)
          | (1ULL << ClickHouseParser::FOR)
          | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
          | (1ULL << (ClickHouseParser::FROM - 64))
          | (1ULL << (ClickHouseParser::FULL - 64))
          | (1ULL << (ClickHouseParser::FUNCTION - 64))
          | (1ULL << (ClickHouseParser::GLOBAL - 64))
          | (1ULL << (ClickHouseParser::GRANULARITY - 64))
          | (1ULL << (ClickHouseParser::GROUP - 64))
          | (1ULL << (ClickHouseParser::HAVING - 64))
          | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
          | (1ULL << (ClickHouseParser::HOUR - 64))
          | (1ULL << (ClickHouseParser::ID - 64))
          | (1ULL << (ClickHouseParser::IF - 64))
          | (1ULL << (ClickHouseParser::ILIKE - 64))
          | (1ULL << (ClickHouseParser::IN - 64))
          | (1ULL << (ClickHouseParser::INDEX - 64))
          | (1ULL << (ClickHouseParser::INF - 64))
          | (1ULL << (ClickHouseParser::INJECTIVE - 64))
          | (1ULL << (ClickHouseParser::INNER - 64))
          | (1ULL << (ClickHouseParser::INSERT - 64))
          | (1ULL << (ClickHouseParser::INTERVAL - 64))
          | (1ULL << (ClickHouseParser::INTO - 64))
          | (1ULL << (ClickHouseParser::IS - 64))
          | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
          | (1ULL << (ClickHouseParser::JOIN - 64))
          | (1ULL << (ClickHouseParser::KEY - 64))
          | (1ULL << (ClickHouseParser::KILL - 64))
          | (1ULL << (ClickHouseParser::LAST - 64))
          | (1ULL << (ClickHouseParser::LAYOUT - 64))
          | (1ULL << (ClickHouseParser::LEADING - 64))
          | (1ULL << (ClickHouseParser::LEFT - 64))
          | (1ULL << (ClickHouseParser::LIFETIME - 64))
          | (1ULL << (ClickHouseParser::LIKE - 64))
          | (1ULL << (ClickHouseParser::LIMIT - 64))
          | (1ULL << (ClickHouseParser::LIVE - 64))
          | (1ULL << (ClickHouseParser::LOCAL - 64))
          | (1ULL << (ClickHouseParser::LOGS - 64))
          | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
          | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
          | (1ULL << (ClickHouseParser::MAX - 64))
          | (1ULL << (ClickHouseParser::MERGES - 64))
          | (1ULL << (ClickHouseParser::MIN - 64))
          | (1ULL << (ClickHouseParser::MINUTE - 64))
          | (1ULL << (ClickHouseParser::MODIFY - 64))
          | (1ULL << (ClickHouseParser::MONTH - 64))
          | (1ULL << (ClickHouseParser::MOVE - 64))
          | (1ULL << (ClickHouseParser::MUTATION - 64))
          | (1ULL << (ClickHouseParser::NAN_SQL - 64))
          | (1ULL << (ClickHouseParser::NO - 64))
          | (1ULL << (ClickHouseParser::NOT - 64))
          | (1ULL << (ClickHouseParser::NULL_SQL - 64))
          | (1ULL << (ClickHouseParser::NULLS - 64))
          | (1ULL << (ClickHouseParser::OFFSET - 64))
          | (1ULL << (ClickHouseParser::ON - 64))
          | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
          | (1ULL << (ClickHouseParser::OR - 64))
          | (1ULL << (ClickHouseParser::ORDER - 64))
          | (1ULL << (ClickHouseParser::OUTER - 64))
          | (1ULL << (ClickHouseParser::OUTFILE - 64))
          | (1ULL << (ClickHouseParser::PARTITION - 64))
          | (1ULL << (ClickHouseParser::POPULATE - 64))
          | (1ULL << (ClickHouseParser::PREWHERE - 64))
          | (1ULL << (ClickHouseParser::PRIMARY - 64))
          | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
          | (1ULL << (ClickHouseParser::RELOAD - 128))
          | (1ULL << (ClickHouseParser::REMOVE - 128))
          | (1ULL << (ClickHouseParser::RENAME - 128))
          | (1ULL << (ClickHouseParser::REPLACE - 128))
          | (1ULL << (ClickHouseParser::REPLICA - 128))
          | (1ULL << (ClickHouseParser::REPLICATED - 128))
          | (1ULL << (ClickHouseParser::RIGHT - 128))
          | (1ULL << (ClickHouseParser::ROLLUP - 128))
          | (1ULL << (ClickHouseParser::SAMPLE - 128))
          | (1ULL << (ClickHouseParser::SECOND - 128))
          | (1ULL << (ClickHouseParser::SELECT - 128))
          | (1ULL << (ClickHouseParser::SEMI - 128))
          | (1ULL << (ClickHouseParser::SENDS - 128))
          | (1ULL << (ClickHouseParser::SET - 128))
          | (1ULL << (ClickHouseParser::SETTINGS - 128))
          | (1ULL << (ClickHouseParser::SHOW - 128))
          | (1ULL << (ClickHouseParser::SOURCE - 128))
          | (1ULL << (ClickHouseParser::START - 128))
          | (1ULL << (ClickHouseParser::STOP - 128))
          | (1ULL << (ClickHouseParser::SUBSTRING - 128))
          | (1ULL << (ClickHouseParser::SYNC - 128))
          | (1ULL << (ClickHouseParser::SYNTAX - 128))
          | (1ULL << (ClickHouseParser::SYSTEM - 128))
          | (1ULL << (ClickHouseParser::TABLE - 128))
          | (1ULL << (ClickHouseParser::TABLES - 128))
          | (1ULL << (ClickHouseParser::TEMPORARY - 128))
          | (1ULL << (ClickHouseParser::TEST - 128))
          | (1ULL << (ClickHouseParser::THEN - 128))
          | (1ULL << (ClickHouseParser::TIES - 128))
          | (1ULL << (ClickHouseParser::TIMEOUT - 128))
          | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
          | (1ULL << (ClickHouseParser::TO - 128))
          | (1ULL << (ClickHouseParser::TOP - 128))
          | (1ULL << (ClickHouseParser::TOTALS - 128))
          | (1ULL << (ClickHouseParser::TRAILING - 128))
          | (1ULL << (ClickHouseParser::TRIM - 128))
          | (1ULL << (ClickHouseParser::TRUNCATE - 128))
          | (1ULL << (ClickHouseParser::TTL - 128))
          | (1ULL << (ClickHouseParser::TYPE - 128))
          | (1ULL << (ClickHouseParser::UNION - 128))
          | (1ULL << (ClickHouseParser::UPDATE - 128))
          | (1ULL << (ClickHouseParser::USE - 128))
          | (1ULL << (ClickHouseParser::USING - 128))
          | (1ULL << (ClickHouseParser::UUID - 128))
          | (1ULL << (ClickHouseParser::VALUES - 128))
          | (1ULL << (ClickHouseParser::VIEW - 128))
          | (1ULL << (ClickHouseParser::VOLUME - 128))
          | (1ULL << (ClickHouseParser::WATCH - 128))
          | (1ULL << (ClickHouseParser::WEEK - 128))
          | (1ULL << (ClickHouseParser::WHEN - 128))
          | (1ULL << (ClickHouseParser::WHERE - 128))
          | (1ULL << (ClickHouseParser::WITH - 128))
          | (1ULL << (ClickHouseParser::YEAR - 128))
          | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
          | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
          | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
          | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
          | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
          | (1ULL << (ClickHouseParser::DOT - 197))
          | (1ULL << (ClickHouseParser::LBRACKET - 197))
          | (1ULL << (ClickHouseParser::LPAREN - 197))
          | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
          setState(1649);
          columnExprList();
        }
        setState(1652);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(1655);
      match(ClickHouseParser::LPAREN);
      setState(1657);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 221, _ctx)) {
      case 1: {
        setState(1656);
        match(ClickHouseParser::DISTINCT);
        break;
      }

      }
      setState(1660);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NAN_SQL - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULL_SQL - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
        | (1ULL << (ClickHouseParser::DOT - 197))
        | (1ULL << (ClickHouseParser::LBRACKET - 197))
        | (1ULL << (ClickHouseParser::LPAREN - 197))
        | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
        setState(1659);
        columnArgList();
      }
      setState(1662);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1664);
      literal();
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprNegateContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1665);
      match(ClickHouseParser::DASH);
      setState(1666);
      columnExpr(17);
      break;
    }

    case 12: {
      _localctx = _tracker.createInstance<ColumnExprNotContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1667);
      match(ClickHouseParser::NOT);
      setState(1668);
      columnExpr(12);
      break;
    }

    case 13: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1672);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
        setState(1669);
        tableIdentifier();
        setState(1670);
        match(ClickHouseParser::DOT);
      }
      setState(1674);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 14: {
      _localctx = _tracker.createInstance<ColumnExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1675);
      match(ClickHouseParser::LPAREN);
      setState(1676);
      selectUnionStmt();
      setState(1677);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 15: {
      _localctx = _tracker.createInstance<ColumnExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1679);
      match(ClickHouseParser::LPAREN);
      setState(1680);
      columnExpr(0);
      setState(1681);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 16: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1683);
      match(ClickHouseParser::LPAREN);
      setState(1684);
      columnExprList();
      setState(1685);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 17: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1687);
      match(ClickHouseParser::LBRACKET);
      setState(1689);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::AST)
        | (1ULL << ClickHouseParser::ASYNC)
        | (1ULL << ClickHouseParser::ATTACH)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CHECK)
        | (1ULL << ClickHouseParser::CLEAR)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::CODEC)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COLUMN)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CONSTRAINT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::CUBE)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DATABASES)
        | (1ULL << ClickHouseParser::DATE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELAY)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESC)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DESCRIBE)
        | (1ULL << ClickHouseParser::DETACH)
        | (1ULL << ClickHouseParser::DICTIONARIES)
        | (1ULL << ClickHouseParser::DICTIONARY)
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EVENTS)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXPRESSION)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
        | (1ULL << (ClickHouseParser::FROM - 64))
        | (1ULL << (ClickHouseParser::FULL - 64))
        | (1ULL << (ClickHouseParser::FUNCTION - 64))
        | (1ULL << (ClickHouseParser::GLOBAL - 64))
        | (1ULL << (ClickHouseParser::GRANULARITY - 64))
        | (1ULL << (ClickHouseParser::GROUP - 64))
        | (1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INJECTIVE - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::KILL - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LAYOUT - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIFETIME - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LIVE - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MAX - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MIN - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
        | (1ULL << (ClickHouseParser::MOVE - 64))
        | (1ULL << (ClickHouseParser::MUTATION - 64))
        | (1ULL << (ClickHouseParser::NAN_SQL - 64))
        | (1ULL << (ClickHouseParser::NO - 64))
        | (1ULL << (ClickHouseParser::NOT - 64))
        | (1ULL << (ClickHouseParser::NULL_SQL - 64))
        | (1ULL << (ClickHouseParser::NULLS - 64))
        | (1ULL << (ClickHouseParser::OFFSET - 64))
        | (1ULL << (ClickHouseParser::ON - 64))
        | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
        | (1ULL << (ClickHouseParser::OR - 64))
        | (1ULL << (ClickHouseParser::ORDER - 64))
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::POPULATE - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
        | (1ULL << (ClickHouseParser::RELOAD - 128))
        | (1ULL << (ClickHouseParser::REMOVE - 128))
        | (1ULL << (ClickHouseParser::RENAME - 128))
        | (1ULL << (ClickHouseParser::REPLACE - 128))
        | (1ULL << (ClickHouseParser::REPLICA - 128))
        | (1ULL << (ClickHouseParser::REPLICATED - 128))
        | (1ULL << (ClickHouseParser::RIGHT - 128))
        | (1ULL << (ClickHouseParser::ROLLUP - 128))
        | (1ULL << (ClickHouseParser::SAMPLE - 128))
        | (1ULL << (ClickHouseParser::SECOND - 128))
        | (1ULL << (ClickHouseParser::SELECT - 128))
        | (1ULL << (ClickHouseParser::SEMI - 128))
        | (1ULL << (ClickHouseParser::SENDS - 128))
        | (1ULL << (ClickHouseParser::SET - 128))
        | (1ULL << (ClickHouseParser::SETTINGS - 128))
        | (1ULL << (ClickHouseParser::SHOW - 128))
        | (1ULL << (ClickHouseParser::SOURCE - 128))
        | (1ULL << (ClickHouseParser::START - 128))
        | (1ULL << (ClickHouseParser::STOP - 128))
        | (1ULL << (ClickHouseParser::SUBSTRING - 128))
        | (1ULL << (ClickHouseParser::SYNC - 128))
        | (1ULL << (ClickHouseParser::SYNTAX - 128))
        | (1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::TEST - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMEOUT - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOP - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::UPDATE - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::UUID - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WATCH - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
        | (1ULL << (ClickHouseParser::DOT - 197))
        | (1ULL << (ClickHouseParser::LBRACKET - 197))
        | (1ULL << (ClickHouseParser::LPAREN - 197))
        | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
        setState(1688);
        columnExprList();
      }
      setState(1691);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 18: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1692);
      columnIdentifier();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1766);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 234, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1764);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 233, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence1Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1695);

          if (!(precpred(_ctx, 16))) throw FailedPredicateException(this, "precpred(_ctx, 16)");
          setState(1696);
          _la = _input->LA(1);
          if (!(((((_la - 191) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 191)) & ((1ULL << (ClickHouseParser::ASTERISK - 191))
            | (1ULL << (ClickHouseParser::PERCENT - 191))
            | (1ULL << (ClickHouseParser::SLASH - 191)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(1697);
          columnExpr(17);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence2Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1698);

          if (!(precpred(_ctx, 15))) throw FailedPredicateException(this, "precpred(_ctx, 15)");
          setState(1699);
          _la = _input->LA(1);
          if (!(((((_la - 196) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 196)) & ((1ULL << (ClickHouseParser::CONCAT - 196))
            | (1ULL << (ClickHouseParser::DASH - 196))
            | (1ULL << (ClickHouseParser::PLUS - 196)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(1700);
          columnExpr(16);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence3Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1701);

          if (!(precpred(_ctx, 14))) throw FailedPredicateException(this, "precpred(_ctx, 14)");
          setState(1720);
          _errHandler->sync(this);
          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 229, _ctx)) {
          case 1: {
            setState(1702);
            match(ClickHouseParser::EQ_DOUBLE);
            break;
          }

          case 2: {
            setState(1703);
            match(ClickHouseParser::EQ_SINGLE);
            break;
          }

          case 3: {
            setState(1704);
            match(ClickHouseParser::NOT_EQ);
            break;
          }

          case 4: {
            setState(1705);
            match(ClickHouseParser::LE);
            break;
          }

          case 5: {
            setState(1706);
            match(ClickHouseParser::GE);
            break;
          }

          case 6: {
            setState(1707);
            match(ClickHouseParser::LT);
            break;
          }

          case 7: {
            setState(1708);
            match(ClickHouseParser::GT);
            break;
          }

          case 8: {
            setState(1710);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::GLOBAL) {
              setState(1709);
              match(ClickHouseParser::GLOBAL);
            }
            setState(1713);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::NOT) {
              setState(1712);
              match(ClickHouseParser::NOT);
            }
            setState(1715);
            match(ClickHouseParser::IN);
            break;
          }

          case 9: {
            setState(1717);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::NOT) {
              setState(1716);
              match(ClickHouseParser::NOT);
            }
            setState(1719);
            _la = _input->LA(1);
            if (!(_la == ClickHouseParser::ILIKE

            || _la == ClickHouseParser::LIKE)) {
            _errHandler->recoverInline(this);
            }
            else {
              _errHandler->reportMatch(this);
              consume();
            }
            break;
          }

          }
          setState(1722);
          columnExpr(15);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprAndContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1723);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(1724);
          match(ClickHouseParser::AND);
          setState(1725);
          columnExpr(12);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprOrContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1726);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(1727);
          match(ClickHouseParser::OR);
          setState(1728);
          columnExpr(11);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1729);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(1731);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1730);
            match(ClickHouseParser::NOT);
          }
          setState(1733);
          match(ClickHouseParser::BETWEEN);
          setState(1734);
          columnExpr(0);
          setState(1735);
          match(ClickHouseParser::AND);
          setState(1736);
          columnExpr(10);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1738);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(1739);
          match(ClickHouseParser::QUERY);
          setState(1740);
          columnExpr(0);
          setState(1741);
          match(ClickHouseParser::COLON);
          setState(1742);
          columnExpr(8);
          break;
        }

        case 8: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1744);

          if (!(precpred(_ctx, 19))) throw FailedPredicateException(this, "precpred(_ctx, 19)");
          setState(1745);
          match(ClickHouseParser::LBRACKET);
          setState(1746);
          columnExpr(0);
          setState(1747);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 9: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1749);

          if (!(precpred(_ctx, 18))) throw FailedPredicateException(this, "precpred(_ctx, 18)");
          setState(1750);
          match(ClickHouseParser::DOT);
          setState(1751);
          match(ClickHouseParser::DECIMAL_LITERAL);
          break;
        }

        case 10: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1752);

          if (!(precpred(_ctx, 13))) throw FailedPredicateException(this, "precpred(_ctx, 13)");
          setState(1753);
          match(ClickHouseParser::IS);
          setState(1755);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1754);
            match(ClickHouseParser::NOT);
          }
          setState(1757);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 11: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1758);

          if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
          setState(1762);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case ClickHouseParser::DATE:
            case ClickHouseParser::FIRST:
            case ClickHouseParser::ID:
            case ClickHouseParser::KEY:
            case ClickHouseParser::IDENTIFIER: {
              setState(1759);
              alias();
              break;
            }

            case ClickHouseParser::AS: {
              setState(1760);
              match(ClickHouseParser::AS);
              setState(1761);
              identifier();
              break;
            }

          default:
            throw NoViableAltException(this);
          }
          break;
        }

        } 
      }
      setState(1768);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 234, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- ColumnArgListContext ------------------------------------------------------------------

ClickHouseParser::ColumnArgListContext::ColumnArgListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::ColumnArgExprContext *> ClickHouseParser::ColumnArgListContext::columnArgExpr() {
  return getRuleContexts<ClickHouseParser::ColumnArgExprContext>();
}

ClickHouseParser::ColumnArgExprContext* ClickHouseParser::ColumnArgListContext::columnArgExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnArgExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnArgListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnArgListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ColumnArgListContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnArgList;
}

antlrcpp::Any ClickHouseParser::ColumnArgListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnArgList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnArgListContext* ClickHouseParser::columnArgList() {
  ColumnArgListContext *_localctx = _tracker.createInstance<ColumnArgListContext>(_ctx, getState());
  enterRule(_localctx, 176, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1769);
    columnArgExpr();
    setState(1774);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1770);
      match(ClickHouseParser::COMMA);
      setState(1771);
      columnArgExpr();
      setState(1776);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnArgExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnArgExprContext::ColumnArgExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::ColumnLambdaExprContext* ClickHouseParser::ColumnArgExprContext::columnLambdaExpr() {
  return getRuleContext<ClickHouseParser::ColumnLambdaExprContext>(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnArgExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}


size_t ClickHouseParser::ColumnArgExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnArgExpr;
}

antlrcpp::Any ClickHouseParser::ColumnArgExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnArgExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnArgExprContext* ClickHouseParser::columnArgExpr() {
  ColumnArgExprContext *_localctx = _tracker.createInstance<ColumnArgExprContext>(_ctx, getState());
  enterRule(_localctx, 178, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1779);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 236, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1777);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1778);
      columnExpr(0);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnLambdaExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnLambdaExprContext::ColumnLambdaExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ColumnLambdaExprContext::ARROW() {
  return getToken(ClickHouseParser::ARROW, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnLambdaExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnLambdaExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::ColumnLambdaExprContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnLambdaExprContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::ColumnLambdaExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnLambdaExprContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnLambdaExprContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ColumnLambdaExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnLambdaExpr;
}

antlrcpp::Any ClickHouseParser::ColumnLambdaExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnLambdaExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnLambdaExprContext* ClickHouseParser::columnLambdaExpr() {
  ColumnLambdaExprContext *_localctx = _tracker.createInstance<ColumnLambdaExprContext>(_ctx, getState());
  enterRule(_localctx, 180, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1800);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(1781);
        match(ClickHouseParser::LPAREN);
        setState(1782);
        identifier();
        setState(1787);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1783);
          match(ClickHouseParser::COMMA);
          setState(1784);
          identifier();
          setState(1789);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1790);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::AST:
      case ClickHouseParser::ASYNC:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CLEAR:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::CODEC:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COLUMN:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CONSTRAINT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::CUBE:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DATABASES:
      case ClickHouseParser::DATE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELAY:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DICTIONARIES:
      case ClickHouseParser::DICTIONARY:
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EVENTS:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXPRESSION:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FREEZE:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HIERARCHICAL:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INJECTIVE:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::IS_OBJECT_ID:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::KILL:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LAYOUT:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIFETIME:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LIVE:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZE:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MAX:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MIN:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::MOVE:
      case ClickHouseParser::MUTATION:
      case ClickHouseParser::NO:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::POPULATE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RANGE:
      case ClickHouseParser::RELOAD:
      case ClickHouseParser::REMOVE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::REPLICATED:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::ROLLUP:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SENDS:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SOURCE:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::TEST:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMEOUT:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOP:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::UPDATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::UUID:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        setState(1792);
        identifier();
        setState(1797);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1793);
          match(ClickHouseParser::COMMA);
          setState(1794);
          identifier();
          setState(1799);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(1802);
    match(ClickHouseParser::ARROW);
    setState(1803);
    columnExpr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnIdentifierContext ------------------------------------------------------------------

ClickHouseParser::ColumnIdentifierContext::ColumnIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::ColumnIdentifierContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ColumnIdentifierContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnIdentifierContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}


size_t ClickHouseParser::ColumnIdentifierContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnIdentifier;
}

antlrcpp::Any ClickHouseParser::ColumnIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnIdentifier(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnIdentifierContext* ClickHouseParser::columnIdentifier() {
  ColumnIdentifierContext *_localctx = _tracker.createInstance<ColumnIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 182, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1808);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 240, _ctx)) {
    case 1: {
      setState(1805);
      tableIdentifier();
      setState(1806);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1810);
    nestedIdentifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NestedIdentifierContext ------------------------------------------------------------------

ClickHouseParser::NestedIdentifierContext::NestedIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::NestedIdentifierContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::NestedIdentifierContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::NestedIdentifierContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}


size_t ClickHouseParser::NestedIdentifierContext::getRuleIndex() const {
  return ClickHouseParser::RuleNestedIdentifier;
}

antlrcpp::Any ClickHouseParser::NestedIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitNestedIdentifier(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::nestedIdentifier() {
  NestedIdentifierContext *_localctx = _tracker.createInstance<NestedIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 184, ClickHouseParser::RuleNestedIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1812);
    identifier();
    setState(1815);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 241, _ctx)) {
    case 1: {
      setState(1813);
      match(ClickHouseParser::DOT);
      setState(1814);
      identifier();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableExprContext ------------------------------------------------------------------

ClickHouseParser::TableExprContext::TableExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::TableExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableExpr;
}

void ClickHouseParser::TableExprContext::copyFrom(TableExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TableExprIdentifierContext ------------------------------------------------------------------

ClickHouseParser::TableIdentifierContext* ClickHouseParser::TableExprIdentifierContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::TableExprIdentifierContext::TableExprIdentifierContext(TableExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableExprIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableExprIdentifier(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableExprSubqueryContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::TableExprSubqueryContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::TableExprSubqueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableExprSubqueryContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::TableExprSubqueryContext::TableExprSubqueryContext(TableExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableExprSubqueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableExprSubquery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableExprAliasContext ------------------------------------------------------------------

ClickHouseParser::TableExprContext* ClickHouseParser::TableExprAliasContext::tableExpr() {
  return getRuleContext<ClickHouseParser::TableExprContext>(0);
}

ClickHouseParser::AliasContext* ClickHouseParser::TableExprAliasContext::alias() {
  return getRuleContext<ClickHouseParser::AliasContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableExprAliasContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::TableExprAliasContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::TableExprAliasContext::TableExprAliasContext(TableExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableExprAliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableExprAlias(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableExprFunctionContext ------------------------------------------------------------------

ClickHouseParser::TableFunctionExprContext* ClickHouseParser::TableExprFunctionContext::tableFunctionExpr() {
  return getRuleContext<ClickHouseParser::TableFunctionExprContext>(0);
}

ClickHouseParser::TableExprFunctionContext::TableExprFunctionContext(TableExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::TableExprFunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableExprFunction(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableExprContext* ClickHouseParser::tableExpr() {
   return tableExpr(0);
}

ClickHouseParser::TableExprContext* ClickHouseParser::tableExpr(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  ClickHouseParser::TableExprContext *_localctx = _tracker.createInstance<TableExprContext>(_ctx, parentState);
  ClickHouseParser::TableExprContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 186;
  enterRecursionRule(_localctx, 186, ClickHouseParser::RuleTableExpr, precedence);

    

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1824);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 242, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1818);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1819);
      tableFunctionExpr();
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1820);
      match(ClickHouseParser::LPAREN);
      setState(1821);
      selectUnionStmt();
      setState(1822);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1834);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 244, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(1826);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(1830);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case ClickHouseParser::DATE:
          case ClickHouseParser::FIRST:
          case ClickHouseParser::ID:
          case ClickHouseParser::KEY:
          case ClickHouseParser::IDENTIFIER: {
            setState(1827);
            alias();
            break;
          }

          case ClickHouseParser::AS: {
            setState(1828);
            match(ClickHouseParser::AS);
            setState(1829);
            identifier();
            break;
          }

        default:
          throw NoViableAltException(this);
        } 
      }
      setState(1836);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 244, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- TableFunctionExprContext ------------------------------------------------------------------

ClickHouseParser::TableFunctionExprContext::TableFunctionExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::TableFunctionExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableFunctionExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::TableFunctionExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::TableArgListContext* ClickHouseParser::TableFunctionExprContext::tableArgList() {
  return getRuleContext<ClickHouseParser::TableArgListContext>(0);
}


size_t ClickHouseParser::TableFunctionExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableFunctionExpr;
}

antlrcpp::Any ClickHouseParser::TableFunctionExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableFunctionExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableFunctionExprContext* ClickHouseParser::tableFunctionExpr() {
  TableFunctionExprContext *_localctx = _tracker.createInstance<TableFunctionExprContext>(_ctx, getState());
  enterRule(_localctx, 188, ClickHouseParser::RuleTableFunctionExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1837);
    identifier();
    setState(1838);
    match(ClickHouseParser::LPAREN);
    setState(1840);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::AND)
      | (1ULL << ClickHouseParser::ANTI)
      | (1ULL << ClickHouseParser::ANY)
      | (1ULL << ClickHouseParser::ARRAY)
      | (1ULL << ClickHouseParser::AS)
      | (1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::ASOF)
      | (1ULL << ClickHouseParser::AST)
      | (1ULL << ClickHouseParser::ASYNC)
      | (1ULL << ClickHouseParser::ATTACH)
      | (1ULL << ClickHouseParser::BETWEEN)
      | (1ULL << ClickHouseParser::BOTH)
      | (1ULL << ClickHouseParser::BY)
      | (1ULL << ClickHouseParser::CASE)
      | (1ULL << ClickHouseParser::CAST)
      | (1ULL << ClickHouseParser::CHECK)
      | (1ULL << ClickHouseParser::CLEAR)
      | (1ULL << ClickHouseParser::CLUSTER)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::COLUMN)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::CONSTRAINT)
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::CUBE)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DATABASES)
      | (1ULL << ClickHouseParser::DATE)
      | (1ULL << ClickHouseParser::DAY)
      | (1ULL << ClickHouseParser::DEDUPLICATE)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::DELAY)
      | (1ULL << ClickHouseParser::DELETE)
      | (1ULL << ClickHouseParser::DESC)
      | (1ULL << ClickHouseParser::DESCENDING)
      | (1ULL << ClickHouseParser::DESCRIBE)
      | (1ULL << ClickHouseParser::DETACH)
      | (1ULL << ClickHouseParser::DICTIONARIES)
      | (1ULL << ClickHouseParser::DICTIONARY)
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EVENTS)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXPRESSION)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
      | (1ULL << (ClickHouseParser::FROM - 64))
      | (1ULL << (ClickHouseParser::FULL - 64))
      | (1ULL << (ClickHouseParser::FUNCTION - 64))
      | (1ULL << (ClickHouseParser::GLOBAL - 64))
      | (1ULL << (ClickHouseParser::GRANULARITY - 64))
      | (1ULL << (ClickHouseParser::GROUP - 64))
      | (1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
      | (1ULL << (ClickHouseParser::HOUR - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INF - 64))
      | (1ULL << (ClickHouseParser::INJECTIVE - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::KILL - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LAYOUT - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIFETIME - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LIVE - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MAX - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MIN - 64))
      | (1ULL << (ClickHouseParser::MINUTE - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MONTH - 64))
      | (1ULL << (ClickHouseParser::MOVE - 64))
      | (1ULL << (ClickHouseParser::MUTATION - 64))
      | (1ULL << (ClickHouseParser::NAN_SQL - 64))
      | (1ULL << (ClickHouseParser::NO - 64))
      | (1ULL << (ClickHouseParser::NOT - 64))
      | (1ULL << (ClickHouseParser::NULL_SQL - 64))
      | (1ULL << (ClickHouseParser::NULLS - 64))
      | (1ULL << (ClickHouseParser::OFFSET - 64))
      | (1ULL << (ClickHouseParser::ON - 64))
      | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
      | (1ULL << (ClickHouseParser::OR - 64))
      | (1ULL << (ClickHouseParser::ORDER - 64))
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::POPULATE - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
      | (1ULL << (ClickHouseParser::RELOAD - 128))
      | (1ULL << (ClickHouseParser::REMOVE - 128))
      | (1ULL << (ClickHouseParser::RENAME - 128))
      | (1ULL << (ClickHouseParser::REPLACE - 128))
      | (1ULL << (ClickHouseParser::REPLICA - 128))
      | (1ULL << (ClickHouseParser::REPLICATED - 128))
      | (1ULL << (ClickHouseParser::RIGHT - 128))
      | (1ULL << (ClickHouseParser::ROLLUP - 128))
      | (1ULL << (ClickHouseParser::SAMPLE - 128))
      | (1ULL << (ClickHouseParser::SECOND - 128))
      | (1ULL << (ClickHouseParser::SELECT - 128))
      | (1ULL << (ClickHouseParser::SEMI - 128))
      | (1ULL << (ClickHouseParser::SENDS - 128))
      | (1ULL << (ClickHouseParser::SET - 128))
      | (1ULL << (ClickHouseParser::SETTINGS - 128))
      | (1ULL << (ClickHouseParser::SHOW - 128))
      | (1ULL << (ClickHouseParser::SOURCE - 128))
      | (1ULL << (ClickHouseParser::START - 128))
      | (1ULL << (ClickHouseParser::STOP - 128))
      | (1ULL << (ClickHouseParser::SUBSTRING - 128))
      | (1ULL << (ClickHouseParser::SYNC - 128))
      | (1ULL << (ClickHouseParser::SYNTAX - 128))
      | (1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::TEST - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMEOUT - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOP - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::UPDATE - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::UUID - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WATCH - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WHERE - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::YEAR - 128))
      | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
      | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
      | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
      | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
      | (1ULL << (ClickHouseParser::OCTAL_LITERAL - 128))
      | (1ULL << (ClickHouseParser::DECIMAL_LITERAL - 128))
      | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
      | (1ULL << (ClickHouseParser::STRING_LITERAL - 128)))) != 0) || ((((_la - 197) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 197)) & ((1ULL << (ClickHouseParser::DASH - 197))
      | (1ULL << (ClickHouseParser::DOT - 197))
      | (1ULL << (ClickHouseParser::PLUS - 197)))) != 0)) {
      setState(1839);
      tableArgList();
    }
    setState(1842);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableIdentifierContext ------------------------------------------------------------------

ClickHouseParser::TableIdentifierContext::TableIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::TableIdentifierContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::TableIdentifierContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableIdentifierContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}


size_t ClickHouseParser::TableIdentifierContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableIdentifier;
}

antlrcpp::Any ClickHouseParser::TableIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableIdentifier(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::tableIdentifier() {
  TableIdentifierContext *_localctx = _tracker.createInstance<TableIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 190, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1847);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 246, _ctx)) {
    case 1: {
      setState(1844);
      databaseIdentifier();
      setState(1845);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1849);
    identifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableArgListContext ------------------------------------------------------------------

ClickHouseParser::TableArgListContext::TableArgListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::TableArgExprContext *> ClickHouseParser::TableArgListContext::tableArgExpr() {
  return getRuleContexts<ClickHouseParser::TableArgExprContext>();
}

ClickHouseParser::TableArgExprContext* ClickHouseParser::TableArgListContext::tableArgExpr(size_t i) {
  return getRuleContext<ClickHouseParser::TableArgExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::TableArgListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::TableArgListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::TableArgListContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableArgList;
}

antlrcpp::Any ClickHouseParser::TableArgListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableArgList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableArgListContext* ClickHouseParser::tableArgList() {
  TableArgListContext *_localctx = _tracker.createInstance<TableArgListContext>(_ctx, getState());
  enterRule(_localctx, 192, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1851);
    tableArgExpr();
    setState(1856);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1852);
      match(ClickHouseParser::COMMA);
      setState(1853);
      tableArgExpr();
      setState(1858);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableArgExprContext ------------------------------------------------------------------

ClickHouseParser::TableArgExprContext::TableArgExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::TableArgExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::TableFunctionExprContext* ClickHouseParser::TableArgExprContext::tableFunctionExpr() {
  return getRuleContext<ClickHouseParser::TableFunctionExprContext>(0);
}

ClickHouseParser::LiteralContext* ClickHouseParser::TableArgExprContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}


size_t ClickHouseParser::TableArgExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleTableArgExpr;
}

antlrcpp::Any ClickHouseParser::TableArgExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableArgExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::TableArgExprContext* ClickHouseParser::tableArgExpr() {
  TableArgExprContext *_localctx = _tracker.createInstance<TableArgExprContext>(_ctx, getState());
  enterRule(_localctx, 194, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1862);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 248, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1859);
      identifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1860);
      tableFunctionExpr();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1861);
      literal();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DatabaseIdentifierContext ------------------------------------------------------------------

ClickHouseParser::DatabaseIdentifierContext::DatabaseIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::DatabaseIdentifierContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}


size_t ClickHouseParser::DatabaseIdentifierContext::getRuleIndex() const {
  return ClickHouseParser::RuleDatabaseIdentifier;
}

antlrcpp::Any ClickHouseParser::DatabaseIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDatabaseIdentifier(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::databaseIdentifier() {
  DatabaseIdentifierContext *_localctx = _tracker.createInstance<DatabaseIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 196, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1864);
    identifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FloatingLiteralContext ------------------------------------------------------------------

ClickHouseParser::FloatingLiteralContext::FloatingLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::FLOATING_LITERAL() {
  return getToken(ClickHouseParser::FLOATING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::FloatingLiteralContext::DECIMAL_LITERAL() {
  return getTokens(ClickHouseParser::DECIMAL_LITERAL);
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::DECIMAL_LITERAL(size_t i) {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, i);
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::OCTAL_LITERAL() {
  return getToken(ClickHouseParser::OCTAL_LITERAL, 0);
}


size_t ClickHouseParser::FloatingLiteralContext::getRuleIndex() const {
  return ClickHouseParser::RuleFloatingLiteral;
}

antlrcpp::Any ClickHouseParser::FloatingLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitFloatingLiteral(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::FloatingLiteralContext* ClickHouseParser::floatingLiteral() {
  FloatingLiteralContext *_localctx = _tracker.createInstance<FloatingLiteralContext>(_ctx, getState());
  enterRule(_localctx, 198, ClickHouseParser::RuleFloatingLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1874);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FLOATING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(1866);
        match(ClickHouseParser::FLOATING_LITERAL);
        break;
      }

      case ClickHouseParser::DOT: {
        enterOuterAlt(_localctx, 2);
        setState(1867);
        match(ClickHouseParser::DOT);
        setState(1868);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::OCTAL_LITERAL

        || _la == ClickHouseParser::DECIMAL_LITERAL)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case ClickHouseParser::DECIMAL_LITERAL: {
        enterOuterAlt(_localctx, 3);
        setState(1869);
        match(ClickHouseParser::DECIMAL_LITERAL);
        setState(1870);
        match(ClickHouseParser::DOT);
        setState(1872);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 249, _ctx)) {
        case 1: {
          setState(1871);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::OCTAL_LITERAL

          || _la == ClickHouseParser::DECIMAL_LITERAL)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          break;
        }

        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumberLiteralContext ------------------------------------------------------------------

ClickHouseParser::NumberLiteralContext::NumberLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::FloatingLiteralContext* ClickHouseParser::NumberLiteralContext::floatingLiteral() {
  return getRuleContext<ClickHouseParser::FloatingLiteralContext>(0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::OCTAL_LITERAL() {
  return getToken(ClickHouseParser::OCTAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::HEXADECIMAL_LITERAL() {
  return getToken(ClickHouseParser::HEXADECIMAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::INF() {
  return getToken(ClickHouseParser::INF, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::NAN_SQL() {
  return getToken(ClickHouseParser::NAN_SQL, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::PLUS() {
  return getToken(ClickHouseParser::PLUS, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}


size_t ClickHouseParser::NumberLiteralContext::getRuleIndex() const {
  return ClickHouseParser::RuleNumberLiteral;
}

antlrcpp::Any ClickHouseParser::NumberLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitNumberLiteral(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::NumberLiteralContext* ClickHouseParser::numberLiteral() {
  NumberLiteralContext *_localctx = _tracker.createInstance<NumberLiteralContext>(_ctx, getState());
  enterRule(_localctx, 200, ClickHouseParser::RuleNumberLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1877);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DASH

    || _la == ClickHouseParser::PLUS) {
      setState(1876);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DASH

      || _la == ClickHouseParser::PLUS)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(1885);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 252, _ctx)) {
    case 1: {
      setState(1879);
      floatingLiteral();
      break;
    }

    case 2: {
      setState(1880);
      match(ClickHouseParser::OCTAL_LITERAL);
      break;
    }

    case 3: {
      setState(1881);
      match(ClickHouseParser::DECIMAL_LITERAL);
      break;
    }

    case 4: {
      setState(1882);
      match(ClickHouseParser::HEXADECIMAL_LITERAL);
      break;
    }

    case 5: {
      setState(1883);
      match(ClickHouseParser::INF);
      break;
    }

    case 6: {
      setState(1884);
      match(ClickHouseParser::NAN_SQL);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LiteralContext ------------------------------------------------------------------

ClickHouseParser::LiteralContext::LiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NumberLiteralContext* ClickHouseParser::LiteralContext::numberLiteral() {
  return getRuleContext<ClickHouseParser::NumberLiteralContext>(0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::NULL_SQL() {
  return getToken(ClickHouseParser::NULL_SQL, 0);
}


size_t ClickHouseParser::LiteralContext::getRuleIndex() const {
  return ClickHouseParser::RuleLiteral;
}

antlrcpp::Any ClickHouseParser::LiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLiteral(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::LiteralContext* ClickHouseParser::literal() {
  LiteralContext *_localctx = _tracker.createInstance<LiteralContext>(_ctx, getState());
  enterRule(_localctx, 202, ClickHouseParser::RuleLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1890);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::OCTAL_LITERAL:
      case ClickHouseParser::DECIMAL_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::DOT:
      case ClickHouseParser::PLUS: {
        enterOuterAlt(_localctx, 1);
        setState(1887);
        numberLiteral();
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1888);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 3);
        setState(1889);
        match(ClickHouseParser::NULL_SQL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntervalContext ------------------------------------------------------------------

ClickHouseParser::IntervalContext::IntervalContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::IntervalContext::SECOND() {
  return getToken(ClickHouseParser::SECOND, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::MINUTE() {
  return getToken(ClickHouseParser::MINUTE, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::HOUR() {
  return getToken(ClickHouseParser::HOUR, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::DAY() {
  return getToken(ClickHouseParser::DAY, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::WEEK() {
  return getToken(ClickHouseParser::WEEK, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::MONTH() {
  return getToken(ClickHouseParser::MONTH, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::QUARTER() {
  return getToken(ClickHouseParser::QUARTER, 0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::YEAR() {
  return getToken(ClickHouseParser::YEAR, 0);
}


size_t ClickHouseParser::IntervalContext::getRuleIndex() const {
  return ClickHouseParser::RuleInterval;
}

antlrcpp::Any ClickHouseParser::IntervalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitInterval(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::IntervalContext* ClickHouseParser::interval() {
  IntervalContext *_localctx = _tracker.createInstance<IntervalContext>(_ctx, getState());
  enterRule(_localctx, 204, ClickHouseParser::RuleInterval);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1892);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DAY || ((((_la - 73) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 73)) & ((1ULL << (ClickHouseParser::HOUR - 73))
      | (1ULL << (ClickHouseParser::MINUTE - 73))
      | (1ULL << (ClickHouseParser::MONTH - 73))
      | (1ULL << (ClickHouseParser::QUARTER - 73)))) != 0) || ((((_la - 138) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 138)) & ((1ULL << (ClickHouseParser::SECOND - 138))
      | (1ULL << (ClickHouseParser::WEEK - 138))
      | (1ULL << (ClickHouseParser::YEAR - 138)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KeywordContext ------------------------------------------------------------------

ClickHouseParser::KeywordContext::KeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::KeywordContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ALIAS() {
  return getToken(ClickHouseParser::ALIAS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ALL() {
  return getToken(ClickHouseParser::ALL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ALTER() {
  return getToken(ClickHouseParser::ALTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::AND() {
  return getToken(ClickHouseParser::AND, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ANTI() {
  return getToken(ClickHouseParser::ANTI, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ANY() {
  return getToken(ClickHouseParser::ANY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ARRAY() {
  return getToken(ClickHouseParser::ARRAY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ASCENDING() {
  return getToken(ClickHouseParser::ASCENDING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ASOF() {
  return getToken(ClickHouseParser::ASOF, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::AST() {
  return getToken(ClickHouseParser::AST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ASYNC() {
  return getToken(ClickHouseParser::ASYNC, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ATTACH() {
  return getToken(ClickHouseParser::ATTACH, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::BETWEEN() {
  return getToken(ClickHouseParser::BETWEEN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::BOTH() {
  return getToken(ClickHouseParser::BOTH, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CASE() {
  return getToken(ClickHouseParser::CASE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CAST() {
  return getToken(ClickHouseParser::CAST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CHECK() {
  return getToken(ClickHouseParser::CHECK, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CLUSTER() {
  return getToken(ClickHouseParser::CLUSTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CODEC() {
  return getToken(ClickHouseParser::CODEC, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::COLLATE() {
  return getToken(ClickHouseParser::COLLATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CONSTRAINT() {
  return getToken(ClickHouseParser::CONSTRAINT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CROSS() {
  return getToken(ClickHouseParser::CROSS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CUBE() {
  return getToken(ClickHouseParser::CUBE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DATABASES() {
  return getToken(ClickHouseParser::DATABASES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DATE() {
  return getToken(ClickHouseParser::DATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DEDUPLICATE() {
  return getToken(ClickHouseParser::DEDUPLICATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DEFAULT() {
  return getToken(ClickHouseParser::DEFAULT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DELAY() {
  return getToken(ClickHouseParser::DELAY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DELETE() {
  return getToken(ClickHouseParser::DELETE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCRIBE() {
  return getToken(ClickHouseParser::DESCRIBE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESC() {
  return getToken(ClickHouseParser::DESC, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCENDING() {
  return getToken(ClickHouseParser::DESCENDING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DETACH() {
  return getToken(ClickHouseParser::DETACH, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DICTIONARIES() {
  return getToken(ClickHouseParser::DICTIONARIES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DICTIONARY() {
  return getToken(ClickHouseParser::DICTIONARY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DISK() {
  return getToken(ClickHouseParser::DISK, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DISTINCT() {
  return getToken(ClickHouseParser::DISTINCT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DISTRIBUTED() {
  return getToken(ClickHouseParser::DISTRIBUTED, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ELSE() {
  return getToken(ClickHouseParser::ELSE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::END() {
  return getToken(ClickHouseParser::END, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ENGINE() {
  return getToken(ClickHouseParser::ENGINE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EVENTS() {
  return getToken(ClickHouseParser::EVENTS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EXPLAIN() {
  return getToken(ClickHouseParser::EXPLAIN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EXPRESSION() {
  return getToken(ClickHouseParser::EXPRESSION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EXTRACT() {
  return getToken(ClickHouseParser::EXTRACT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FETCHES() {
  return getToken(ClickHouseParser::FETCHES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FINAL() {
  return getToken(ClickHouseParser::FINAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FIRST() {
  return getToken(ClickHouseParser::FIRST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FLUSH() {
  return getToken(ClickHouseParser::FLUSH, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FOR() {
  return getToken(ClickHouseParser::FOR, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FORMAT() {
  return getToken(ClickHouseParser::FORMAT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FREEZE() {
  return getToken(ClickHouseParser::FREEZE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FULL() {
  return getToken(ClickHouseParser::FULL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FUNCTION() {
  return getToken(ClickHouseParser::FUNCTION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::GRANULARITY() {
  return getToken(ClickHouseParser::GRANULARITY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::GROUP() {
  return getToken(ClickHouseParser::GROUP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::HAVING() {
  return getToken(ClickHouseParser::HAVING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::HIERARCHICAL() {
  return getToken(ClickHouseParser::HIERARCHICAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ID() {
  return getToken(ClickHouseParser::ID, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ILIKE() {
  return getToken(ClickHouseParser::ILIKE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INDEX() {
  return getToken(ClickHouseParser::INDEX, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INJECTIVE() {
  return getToken(ClickHouseParser::INJECTIVE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INNER() {
  return getToken(ClickHouseParser::INNER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INSERT() {
  return getToken(ClickHouseParser::INSERT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INTERVAL() {
  return getToken(ClickHouseParser::INTERVAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::INTO() {
  return getToken(ClickHouseParser::INTO, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::IS() {
  return getToken(ClickHouseParser::IS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::IS_OBJECT_ID() {
  return getToken(ClickHouseParser::IS_OBJECT_ID, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::JOIN() {
  return getToken(ClickHouseParser::JOIN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::JSON_FALSE() {
  return getToken(ClickHouseParser::JSON_FALSE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::JSON_TRUE() {
  return getToken(ClickHouseParser::JSON_TRUE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::KEY() {
  return getToken(ClickHouseParser::KEY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::KILL() {
  return getToken(ClickHouseParser::KILL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LAST() {
  return getToken(ClickHouseParser::LAST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LAYOUT() {
  return getToken(ClickHouseParser::LAYOUT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LEADING() {
  return getToken(ClickHouseParser::LEADING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LEFT() {
  return getToken(ClickHouseParser::LEFT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIFETIME() {
  return getToken(ClickHouseParser::LIFETIME, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIKE() {
  return getToken(ClickHouseParser::LIKE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIMIT() {
  return getToken(ClickHouseParser::LIMIT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIVE() {
  return getToken(ClickHouseParser::LIVE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LOCAL() {
  return getToken(ClickHouseParser::LOCAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LOGS() {
  return getToken(ClickHouseParser::LOGS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MATERIALIZE() {
  return getToken(ClickHouseParser::MATERIALIZE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MAX() {
  return getToken(ClickHouseParser::MAX, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MERGES() {
  return getToken(ClickHouseParser::MERGES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MIN() {
  return getToken(ClickHouseParser::MIN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MOVE() {
  return getToken(ClickHouseParser::MOVE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MUTATION() {
  return getToken(ClickHouseParser::MUTATION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::NO() {
  return getToken(ClickHouseParser::NO, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::NULLS() {
  return getToken(ClickHouseParser::NULLS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::OFFSET() {
  return getToken(ClickHouseParser::OFFSET, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ON() {
  return getToken(ClickHouseParser::ON, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::OPTIMIZE() {
  return getToken(ClickHouseParser::OPTIMIZE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::OR() {
  return getToken(ClickHouseParser::OR, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ORDER() {
  return getToken(ClickHouseParser::ORDER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::OUTER() {
  return getToken(ClickHouseParser::OUTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::OUTFILE() {
  return getToken(ClickHouseParser::OUTFILE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::PARTITION() {
  return getToken(ClickHouseParser::PARTITION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::POPULATE() {
  return getToken(ClickHouseParser::POPULATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::PREWHERE() {
  return getToken(ClickHouseParser::PREWHERE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::PRIMARY() {
  return getToken(ClickHouseParser::PRIMARY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RANGE() {
  return getToken(ClickHouseParser::RANGE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RELOAD() {
  return getToken(ClickHouseParser::RELOAD, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::REMOVE() {
  return getToken(ClickHouseParser::REMOVE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RENAME() {
  return getToken(ClickHouseParser::RENAME, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::REPLACE() {
  return getToken(ClickHouseParser::REPLACE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::REPLICA() {
  return getToken(ClickHouseParser::REPLICA, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::REPLICATED() {
  return getToken(ClickHouseParser::REPLICATED, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RIGHT() {
  return getToken(ClickHouseParser::RIGHT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::ROLLUP() {
  return getToken(ClickHouseParser::ROLLUP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SAMPLE() {
  return getToken(ClickHouseParser::SAMPLE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SELECT() {
  return getToken(ClickHouseParser::SELECT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SEMI() {
  return getToken(ClickHouseParser::SEMI, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SENDS() {
  return getToken(ClickHouseParser::SENDS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SET() {
  return getToken(ClickHouseParser::SET, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SETTINGS() {
  return getToken(ClickHouseParser::SETTINGS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SOURCE() {
  return getToken(ClickHouseParser::SOURCE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::START() {
  return getToken(ClickHouseParser::START, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::STOP() {
  return getToken(ClickHouseParser::STOP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SUBSTRING() {
  return getToken(ClickHouseParser::SUBSTRING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SYNC() {
  return getToken(ClickHouseParser::SYNC, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SYNTAX() {
  return getToken(ClickHouseParser::SYNTAX, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SYSTEM() {
  return getToken(ClickHouseParser::SYSTEM, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TABLES() {
  return getToken(ClickHouseParser::TABLES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TEST() {
  return getToken(ClickHouseParser::TEST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::THEN() {
  return getToken(ClickHouseParser::THEN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TIES() {
  return getToken(ClickHouseParser::TIES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TIMEOUT() {
  return getToken(ClickHouseParser::TIMEOUT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TIMESTAMP() {
  return getToken(ClickHouseParser::TIMESTAMP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TOTALS() {
  return getToken(ClickHouseParser::TOTALS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TRAILING() {
  return getToken(ClickHouseParser::TRAILING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TRIM() {
  return getToken(ClickHouseParser::TRIM, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TRUNCATE() {
  return getToken(ClickHouseParser::TRUNCATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TOP() {
  return getToken(ClickHouseParser::TOP, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TYPE() {
  return getToken(ClickHouseParser::TYPE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::UNION() {
  return getToken(ClickHouseParser::UNION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::UPDATE() {
  return getToken(ClickHouseParser::UPDATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USE() {
  return getToken(ClickHouseParser::USE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USING() {
  return getToken(ClickHouseParser::USING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::UUID() {
  return getToken(ClickHouseParser::UUID, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::VALUES() {
  return getToken(ClickHouseParser::VALUES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::VIEW() {
  return getToken(ClickHouseParser::VIEW, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::VOLUME() {
  return getToken(ClickHouseParser::VOLUME, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::WATCH() {
  return getToken(ClickHouseParser::WATCH, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::WHEN() {
  return getToken(ClickHouseParser::WHEN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::WHERE() {
  return getToken(ClickHouseParser::WHERE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}


size_t ClickHouseParser::KeywordContext::getRuleIndex() const {
  return ClickHouseParser::RuleKeyword;
}

antlrcpp::Any ClickHouseParser::KeywordContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitKeyword(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::KeywordContext* ClickHouseParser::keyword() {
  KeywordContext *_localctx = _tracker.createInstance<KeywordContext>(_ctx, getState());
  enterRule(_localctx, 206, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1894);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::AND)
      | (1ULL << ClickHouseParser::ANTI)
      | (1ULL << ClickHouseParser::ANY)
      | (1ULL << ClickHouseParser::ARRAY)
      | (1ULL << ClickHouseParser::AS)
      | (1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::ASOF)
      | (1ULL << ClickHouseParser::AST)
      | (1ULL << ClickHouseParser::ASYNC)
      | (1ULL << ClickHouseParser::ATTACH)
      | (1ULL << ClickHouseParser::BETWEEN)
      | (1ULL << ClickHouseParser::BOTH)
      | (1ULL << ClickHouseParser::BY)
      | (1ULL << ClickHouseParser::CASE)
      | (1ULL << ClickHouseParser::CAST)
      | (1ULL << ClickHouseParser::CHECK)
      | (1ULL << ClickHouseParser::CLEAR)
      | (1ULL << ClickHouseParser::CLUSTER)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::COLUMN)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::CONSTRAINT)
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::CUBE)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DATABASES)
      | (1ULL << ClickHouseParser::DATE)
      | (1ULL << ClickHouseParser::DEDUPLICATE)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::DELAY)
      | (1ULL << ClickHouseParser::DELETE)
      | (1ULL << ClickHouseParser::DESC)
      | (1ULL << ClickHouseParser::DESCENDING)
      | (1ULL << ClickHouseParser::DESCRIBE)
      | (1ULL << ClickHouseParser::DETACH)
      | (1ULL << ClickHouseParser::DICTIONARIES)
      | (1ULL << ClickHouseParser::DICTIONARY)
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EVENTS)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXPRESSION)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::FREEZE - 64))
      | (1ULL << (ClickHouseParser::FROM - 64))
      | (1ULL << (ClickHouseParser::FULL - 64))
      | (1ULL << (ClickHouseParser::FUNCTION - 64))
      | (1ULL << (ClickHouseParser::GLOBAL - 64))
      | (1ULL << (ClickHouseParser::GRANULARITY - 64))
      | (1ULL << (ClickHouseParser::GROUP - 64))
      | (1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HIERARCHICAL - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INJECTIVE - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::IS_OBJECT_ID - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::KILL - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LAYOUT - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIFETIME - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LIVE - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZE - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MAX - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MIN - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MOVE - 64))
      | (1ULL << (ClickHouseParser::MUTATION - 64))
      | (1ULL << (ClickHouseParser::NO - 64))
      | (1ULL << (ClickHouseParser::NOT - 64))
      | (1ULL << (ClickHouseParser::NULLS - 64))
      | (1ULL << (ClickHouseParser::OFFSET - 64))
      | (1ULL << (ClickHouseParser::ON - 64))
      | (1ULL << (ClickHouseParser::OPTIMIZE - 64))
      | (1ULL << (ClickHouseParser::OR - 64))
      | (1ULL << (ClickHouseParser::ORDER - 64))
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::POPULATE - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::RANGE - 128))
      | (1ULL << (ClickHouseParser::RELOAD - 128))
      | (1ULL << (ClickHouseParser::REMOVE - 128))
      | (1ULL << (ClickHouseParser::RENAME - 128))
      | (1ULL << (ClickHouseParser::REPLACE - 128))
      | (1ULL << (ClickHouseParser::REPLICA - 128))
      | (1ULL << (ClickHouseParser::REPLICATED - 128))
      | (1ULL << (ClickHouseParser::RIGHT - 128))
      | (1ULL << (ClickHouseParser::ROLLUP - 128))
      | (1ULL << (ClickHouseParser::SAMPLE - 128))
      | (1ULL << (ClickHouseParser::SELECT - 128))
      | (1ULL << (ClickHouseParser::SEMI - 128))
      | (1ULL << (ClickHouseParser::SENDS - 128))
      | (1ULL << (ClickHouseParser::SET - 128))
      | (1ULL << (ClickHouseParser::SETTINGS - 128))
      | (1ULL << (ClickHouseParser::SHOW - 128))
      | (1ULL << (ClickHouseParser::SOURCE - 128))
      | (1ULL << (ClickHouseParser::START - 128))
      | (1ULL << (ClickHouseParser::STOP - 128))
      | (1ULL << (ClickHouseParser::SUBSTRING - 128))
      | (1ULL << (ClickHouseParser::SYNC - 128))
      | (1ULL << (ClickHouseParser::SYNTAX - 128))
      | (1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::TEST - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMEOUT - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOP - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::UPDATE - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::UUID - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WATCH - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WHERE - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
      | (1ULL << (ClickHouseParser::JSON_TRUE - 128)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KeywordForAliasContext ------------------------------------------------------------------

ClickHouseParser::KeywordForAliasContext::KeywordForAliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::KeywordForAliasContext::DATE() {
  return getToken(ClickHouseParser::DATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordForAliasContext::FIRST() {
  return getToken(ClickHouseParser::FIRST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordForAliasContext::ID() {
  return getToken(ClickHouseParser::ID, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordForAliasContext::KEY() {
  return getToken(ClickHouseParser::KEY, 0);
}


size_t ClickHouseParser::KeywordForAliasContext::getRuleIndex() const {
  return ClickHouseParser::RuleKeywordForAlias;
}

antlrcpp::Any ClickHouseParser::KeywordForAliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitKeywordForAlias(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::KeywordForAliasContext* ClickHouseParser::keywordForAlias() {
  KeywordForAliasContext *_localctx = _tracker.createInstance<KeywordForAliasContext>(_ctx, getState());
  enterRule(_localctx, 208, ClickHouseParser::RuleKeywordForAlias);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1896);
    _la = _input->LA(1);
    if (!(((((_la - 34) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 34)) & ((1ULL << (ClickHouseParser::DATE - 34))
      | (1ULL << (ClickHouseParser::FIRST - 34))
      | (1ULL << (ClickHouseParser::ID - 34))
      | (1ULL << (ClickHouseParser::KEY - 34)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AliasContext ------------------------------------------------------------------

ClickHouseParser::AliasContext::AliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::AliasContext::IDENTIFIER() {
  return getToken(ClickHouseParser::IDENTIFIER, 0);
}

ClickHouseParser::KeywordForAliasContext* ClickHouseParser::AliasContext::keywordForAlias() {
  return getRuleContext<ClickHouseParser::KeywordForAliasContext>(0);
}


size_t ClickHouseParser::AliasContext::getRuleIndex() const {
  return ClickHouseParser::RuleAlias;
}

antlrcpp::Any ClickHouseParser::AliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlias(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::AliasContext* ClickHouseParser::alias() {
  AliasContext *_localctx = _tracker.createInstance<AliasContext>(_ctx, getState());
  enterRule(_localctx, 210, ClickHouseParser::RuleAlias);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1900);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1898);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::DATE:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::ID:
      case ClickHouseParser::KEY: {
        enterOuterAlt(_localctx, 2);
        setState(1899);
        keywordForAlias();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext::IdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::IdentifierContext::IDENTIFIER() {
  return getToken(ClickHouseParser::IDENTIFIER, 0);
}

ClickHouseParser::IntervalContext* ClickHouseParser::IdentifierContext::interval() {
  return getRuleContext<ClickHouseParser::IntervalContext>(0);
}

ClickHouseParser::KeywordContext* ClickHouseParser::IdentifierContext::keyword() {
  return getRuleContext<ClickHouseParser::KeywordContext>(0);
}


size_t ClickHouseParser::IdentifierContext::getRuleIndex() const {
  return ClickHouseParser::RuleIdentifier;
}

antlrcpp::Any ClickHouseParser::IdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitIdentifier(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::identifier() {
  IdentifierContext *_localctx = _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 212, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1905);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1902);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::DAY:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::YEAR: {
        enterOuterAlt(_localctx, 2);
        setState(1903);
        interval();
        break;
      }

      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::AST:
      case ClickHouseParser::ASYNC:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CLEAR:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::CODEC:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COLUMN:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CONSTRAINT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::CUBE:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DATABASES:
      case ClickHouseParser::DATE:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELAY:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DICTIONARIES:
      case ClickHouseParser::DICTIONARY:
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EVENTS:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXPRESSION:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FREEZE:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HIERARCHICAL:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INJECTIVE:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::IS_OBJECT_ID:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::KILL:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LAYOUT:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIFETIME:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LIVE:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZE:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MAX:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MIN:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MOVE:
      case ClickHouseParser::MUTATION:
      case ClickHouseParser::NO:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::POPULATE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::RANGE:
      case ClickHouseParser::RELOAD:
      case ClickHouseParser::REMOVE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::REPLICATED:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::ROLLUP:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SENDS:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SOURCE:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::TEST:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMEOUT:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOP:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::UPDATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::UUID:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE: {
        enterOuterAlt(_localctx, 3);
        setState(1904);
        keyword();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierOrNullContext ------------------------------------------------------------------

ClickHouseParser::IdentifierOrNullContext::IdentifierOrNullContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::IdentifierOrNullContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::IdentifierOrNullContext::NULL_SQL() {
  return getToken(ClickHouseParser::NULL_SQL, 0);
}


size_t ClickHouseParser::IdentifierOrNullContext::getRuleIndex() const {
  return ClickHouseParser::RuleIdentifierOrNull;
}

antlrcpp::Any ClickHouseParser::IdentifierOrNullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitIdentifierOrNull(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::IdentifierOrNullContext* ClickHouseParser::identifierOrNull() {
  IdentifierOrNullContext *_localctx = _tracker.createInstance<IdentifierOrNullContext>(_ctx, getState());
  enterRule(_localctx, 214, ClickHouseParser::RuleIdentifierOrNull);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1909);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::AST:
      case ClickHouseParser::ASYNC:
      case ClickHouseParser::ATTACH:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CHECK:
      case ClickHouseParser::CLEAR:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::CODEC:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COLUMN:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CONSTRAINT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::CUBE:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DATABASES:
      case ClickHouseParser::DATE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELAY:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DESCRIBE:
      case ClickHouseParser::DETACH:
      case ClickHouseParser::DICTIONARIES:
      case ClickHouseParser::DICTIONARY:
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EVENTS:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXPRESSION:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FREEZE:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HIERARCHICAL:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INJECTIVE:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::IS_OBJECT_ID:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::KILL:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LAYOUT:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIFETIME:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LIVE:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZE:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MAX:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MIN:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::MOVE:
      case ClickHouseParser::MUTATION:
      case ClickHouseParser::NO:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::POPULATE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RANGE:
      case ClickHouseParser::RELOAD:
      case ClickHouseParser::REMOVE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::REPLICATED:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::ROLLUP:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SENDS:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SOURCE:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::TEST:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMEOUT:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOP:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::UPDATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::UUID:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WATCH:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1907);
        identifier();
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 2);
        setState(1908);
        match(ClickHouseParser::NULL_SQL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- EnumValueContext ------------------------------------------------------------------

ClickHouseParser::EnumValueContext::EnumValueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::EnumValueContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::EnumValueContext::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

ClickHouseParser::NumberLiteralContext* ClickHouseParser::EnumValueContext::numberLiteral() {
  return getRuleContext<ClickHouseParser::NumberLiteralContext>(0);
}


size_t ClickHouseParser::EnumValueContext::getRuleIndex() const {
  return ClickHouseParser::RuleEnumValue;
}

antlrcpp::Any ClickHouseParser::EnumValueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitEnumValue(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::EnumValueContext* ClickHouseParser::enumValue() {
  EnumValueContext *_localctx = _tracker.createInstance<EnumValueContext>(_ctx, getState());
  enterRule(_localctx, 216, ClickHouseParser::RuleEnumValue);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1911);
    match(ClickHouseParser::STRING_LITERAL);
    setState(1912);
    match(ClickHouseParser::EQ_SINGLE);
    setState(1913);
    numberLiteral();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool ClickHouseParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 12: return dictionaryAttrDfntSempred(dynamic_cast<DictionaryAttrDfntContext *>(context), predicateIndex);
    case 13: return dictionaryEngineClauseSempred(dynamic_cast<DictionaryEngineClauseContext *>(context), predicateIndex);
    case 26: return engineClauseSempred(dynamic_cast<EngineClauseContext *>(context), predicateIndex);
    case 67: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 87: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 93: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::dictionaryAttrDfntSempred(DictionaryAttrDfntContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return !_localctx->attrs.count("default");
    case 1: return !_localctx->attrs.count("expression");
    case 2: return !_localctx->attrs.count("hierarchical");
    case 3: return !_localctx->attrs.count("injective");
    case 4: return !_localctx->attrs.count("is_object_id");

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::dictionaryEngineClauseSempred(DictionaryEngineClauseContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 5: return !_localctx->clauses.count("source");
    case 6: return !_localctx->clauses.count("lifetime");
    case 7: return !_localctx->clauses.count("layout");
    case 8: return !_localctx->clauses.count("range");
    case 9: return !_localctx->clauses.count("settings");

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::engineClauseSempred(EngineClauseContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 10: return !_localctx->clauses.count("orderByClause");
    case 11: return !_localctx->clauses.count("partitionByClause");
    case 12: return !_localctx->clauses.count("primaryKeyClause");
    case 13: return !_localctx->clauses.count("sampleByClause");
    case 14: return !_localctx->clauses.count("ttlClause");
    case 15: return !_localctx->clauses.count("settingsClause");

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::joinExprSempred(JoinExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 16: return precpred(_ctx, 3);
    case 17: return precpred(_ctx, 4);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 18: return precpred(_ctx, 16);
    case 19: return precpred(_ctx, 15);
    case 20: return precpred(_ctx, 14);
    case 21: return precpred(_ctx, 11);
    case 22: return precpred(_ctx, 10);
    case 23: return precpred(_ctx, 9);
    case 24: return precpred(_ctx, 8);
    case 25: return precpred(_ctx, 19);
    case 26: return precpred(_ctx, 18);
    case 27: return precpred(_ctx, 13);
    case 28: return precpred(_ctx, 7);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::tableExprSempred(TableExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 29: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> ClickHouseParser::_decisionToDFA;
atn::PredictionContextCache ClickHouseParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN ClickHouseParser::_atn;
std::vector<uint16_t> ClickHouseParser::_serializedATN;

std::vector<std::string> ClickHouseParser::_ruleNames = {
  "queryStmt", "query", "alterStmt", "alterTableClause", "assignmentExprList", 
  "assignmentExpr", "tableColumnPropertyType", "partitionClause", "attachStmt", 
  "checkStmt", "createStmt", "dictionarySchemaClause", "dictionaryAttrDfnt", 
  "dictionaryEngineClause", "dictionaryPrimaryKeyClause", "dictionaryArgExpr", 
  "sourceClause", "lifetimeClause", "layoutClause", "rangeClause", "dictionarySettingsClause", 
  "clusterClause", "uuidClause", "destinationClause", "subqueryClause", 
  "tableSchemaClause", "engineClause", "partitionByClause", "primaryKeyClause", 
  "sampleByClause", "ttlClause", "engineExpr", "tableElementExpr", "tableColumnDfnt", 
  "tableColumnPropertyExpr", "tableIndexDfnt", "tableProjectionDfnt", "codecExpr", 
  "codecArgExpr", "ttlExpr", "describeStmt", "dropStmt", "existsStmt", "explainStmt", 
  "insertStmt", "columnsClause", "dataClause", "killStmt", "optimizeStmt", 
  "renameStmt", "projectionSelectStmt", "selectUnionStmt", "selectStmtWithParens", 
  "selectStmt", "withClause", "topClause", "fromClause", "arrayJoinClause", 
  "prewhereClause", "whereClause", "groupByClause", "havingClause", "orderByClause", 
  "projectionOrderByClause", "limitByClause", "limitClause", "settingsClause", 
  "joinExpr", "joinOp", "joinOpCross", "joinConstraintClause", "sampleClause", 
  "limitExpr", "orderExprList", "orderExpr", "ratioExpr", "settingExprList", 
  "settingExpr", "setStmt", "showStmt", "systemStmt", "truncateStmt", "useStmt", 
  "watchStmt", "columnTypeExpr", "columnExprList", "columnsExpr", "columnExpr", 
  "columnArgList", "columnArgExpr", "columnLambdaExpr", "columnIdentifier", 
  "nestedIdentifier", "tableExpr", "tableFunctionExpr", "tableIdentifier", 
  "tableArgList", "tableArgExpr", "databaseIdentifier", "floatingLiteral", 
  "numberLiteral", "literal", "interval", "keyword", "keywordForAlias", 
  "alias", "identifier", "identifierOrNull", "enumValue"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "'false'", "'true'", "", "", "", "", "", "", "'->'", "'*'", "'`'", 
  "'\\'", "':'", "','", "'||'", "'-'", "'.'", "'=='", "'='", "'>='", "'>'", 
  "'{'", "'['", "'<='", "'('", "'<'", "", "'%'", "'+'", "'?'", "'\"'", "'''", 
  "'}'", "']'", "')'", "';'", "'/'", "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "ADD", "AFTER", "ALIAS", "ALL", "ALTER", "AND", "ANTI", "ANY", "ARRAY", 
  "AS", "ASCENDING", "ASOF", "AST", "ASYNC", "ATTACH", "BETWEEN", "BOTH", 
  "BY", "CASE", "CAST", "CHECK", "CLEAR", "CLUSTER", "CODEC", "COLLATE", 
  "COLUMN", "COMMENT", "CONSTRAINT", "CREATE", "CROSS", "CUBE", "DATABASE", 
  "DATABASES", "DATE", "DAY", "DEDUPLICATE", "DEFAULT", "DELAY", "DELETE", 
  "DESC", "DESCENDING", "DESCRIBE", "DETACH", "DICTIONARIES", "DICTIONARY", 
  "DISK", "DISTINCT", "DISTRIBUTED", "DROP", "ELSE", "END", "ENGINE", "EVENTS", 
  "EXISTS", "EXPLAIN", "EXPRESSION", "EXTRACT", "FETCHES", "FINAL", "FIRST", 
  "FLUSH", "FOR", "FORMAT", "FREEZE", "FROM", "FULL", "FUNCTION", "GLOBAL", 
  "GRANULARITY", "GROUP", "HAVING", "HIERARCHICAL", "HOUR", "ID", "IF", 
  "ILIKE", "IN", "INDEX", "INF", "INJECTIVE", "INNER", "INSERT", "INTERVAL", 
  "INTO", "IS", "IS_OBJECT_ID", "JOIN", "KEY", "KILL", "LAST", "LAYOUT", 
  "LEADING", "LEFT", "LIFETIME", "LIKE", "LIMIT", "LIVE", "LOCAL", "LOGS", 
  "MATERIALIZE", "MATERIALIZED", "MAX", "MERGES", "MIN", "MINUTE", "MODIFY", 
  "MONTH", "MOVE", "MUTATION", "NAN_SQL", "NO", "NOT", "NULL_SQL", "NULLS", 
  "OFFSET", "ON", "OPTIMIZE", "OR", "ORDER", "OUTER", "OUTFILE", "PARTITION", 
  "POPULATE", "PREWHERE", "PRIMARY", "PROJECTION", "QUARTER", "RANGE", "RELOAD", 
  "REMOVE", "RENAME", "REPLACE", "REPLICA", "REPLICATED", "RIGHT", "ROLLUP", 
  "SAMPLE", "SECOND", "SELECT", "SEMI", "SENDS", "SET", "SETTINGS", "SHOW", 
  "SOURCE", "START", "STOP", "SUBSTRING", "SYNC", "SYNTAX", "SYSTEM", "TABLE", 
  "TABLES", "TEMPORARY", "TEST", "THEN", "TIES", "TIMEOUT", "TIMESTAMP", 
  "TO", "TOP", "TOTALS", "TRAILING", "TRIM", "TRUNCATE", "TTL", "TYPE", 
  "UNION", "UPDATE", "USE", "USING", "UUID", "VALUES", "VIEW", "VOLUME", 
  "WATCH", "WEEK", "WHEN", "WHERE", "WITH", "YEAR", "JSON_FALSE", "JSON_TRUE", 
  "IDENTIFIER", "FLOATING_LITERAL", "OCTAL_LITERAL", "DECIMAL_LITERAL", 
  "HEXADECIMAL_LITERAL", "STRING_LITERAL", "ARROW", "ASTERISK", "BACKQUOTE", 
  "BACKSLASH", "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ_DOUBLE", "EQ_SINGLE", 
  "GE", "GT", "LBRACE", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", 
  "PLUS", "QUERY", "QUOTE_DOUBLE", "QUOTE_SINGLE", "RBRACE", "RBRACKET", 
  "RPAREN", "SEMICOLON", "SLASH", "UNDERSCORE", "MULTI_LINE_COMMENT", "SINGLE_LINE_COMMENT", 
  "WHITESPACE"
};

dfa::Vocabulary ClickHouseParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> ClickHouseParser::_tokenNames;

ClickHouseParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  _serializedATN = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
    0x3, 0xe0, 0x77e, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
    0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 0x7, 
    0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 0x4, 0xb, 
    0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 0xe, 0x9, 0xe, 
    0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 0x9, 0x11, 0x4, 
    0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 0x9, 0x14, 0x4, 0x15, 
    0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 0x9, 0x17, 0x4, 0x18, 0x9, 
    0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 
    0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 
    0x1f, 0x9, 0x1f, 0x4, 0x20, 0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 
    0x9, 0x22, 0x4, 0x23, 0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 
    0x25, 0x4, 0x26, 0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 
    0x4, 0x29, 0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 
    0x2c, 0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
    0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 0x9, 
    0x32, 0x4, 0x33, 0x9, 0x33, 0x4, 0x34, 0x9, 0x34, 0x4, 0x35, 0x9, 0x35, 
    0x4, 0x36, 0x9, 0x36, 0x4, 0x37, 0x9, 0x37, 0x4, 0x38, 0x9, 0x38, 0x4, 
    0x39, 0x9, 0x39, 0x4, 0x3a, 0x9, 0x3a, 0x4, 0x3b, 0x9, 0x3b, 0x4, 0x3c, 
    0x9, 0x3c, 0x4, 0x3d, 0x9, 0x3d, 0x4, 0x3e, 0x9, 0x3e, 0x4, 0x3f, 0x9, 
    0x3f, 0x4, 0x40, 0x9, 0x40, 0x4, 0x41, 0x9, 0x41, 0x4, 0x42, 0x9, 0x42, 
    0x4, 0x43, 0x9, 0x43, 0x4, 0x44, 0x9, 0x44, 0x4, 0x45, 0x9, 0x45, 0x4, 
    0x46, 0x9, 0x46, 0x4, 0x47, 0x9, 0x47, 0x4, 0x48, 0x9, 0x48, 0x4, 0x49, 
    0x9, 0x49, 0x4, 0x4a, 0x9, 0x4a, 0x4, 0x4b, 0x9, 0x4b, 0x4, 0x4c, 0x9, 
    0x4c, 0x4, 0x4d, 0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x4, 0x4f, 0x9, 0x4f, 
    0x4, 0x50, 0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x4, 0x52, 0x9, 0x52, 0x4, 
    0x53, 0x9, 0x53, 0x4, 0x54, 0x9, 0x54, 0x4, 0x55, 0x9, 0x55, 0x4, 0x56, 
    0x9, 0x56, 0x4, 0x57, 0x9, 0x57, 0x4, 0x58, 0x9, 0x58, 0x4, 0x59, 0x9, 
    0x59, 0x4, 0x5a, 0x9, 0x5a, 0x4, 0x5b, 0x9, 0x5b, 0x4, 0x5c, 0x9, 0x5c, 
    0x4, 0x5d, 0x9, 0x5d, 0x4, 0x5e, 0x9, 0x5e, 0x4, 0x5f, 0x9, 0x5f, 0x4, 
    0x60, 0x9, 0x60, 0x4, 0x61, 0x9, 0x61, 0x4, 0x62, 0x9, 0x62, 0x4, 0x63, 
    0x9, 0x63, 0x4, 0x64, 0x9, 0x64, 0x4, 0x65, 0x9, 0x65, 0x4, 0x66, 0x9, 
    0x66, 0x4, 0x67, 0x9, 0x67, 0x4, 0x68, 0x9, 0x68, 0x4, 0x69, 0x9, 0x69, 
    0x4, 0x6a, 0x9, 0x6a, 0x4, 0x6b, 0x9, 0x6b, 0x4, 0x6c, 0x9, 0x6c, 0x4, 
    0x6d, 0x9, 0x6d, 0x4, 0x6e, 0x9, 0x6e, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 
    0x3, 0x2, 0x5, 0x2, 0xe1, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x5, 0x2, 0xe5, 
    0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xe8, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xeb, 
    0xa, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0xff, 0xa, 
    0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0x105, 0xa, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x7, 0x4, 0x10a, 0xa, 0x4, 0xc, 0x4, 0xe, 
    0x4, 0x10d, 0xb, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x5, 0x5, 0x114, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x119, 
    0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0x120, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x125, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x12c, 
    0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x131, 0xa, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x137, 0xa, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x13d, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x5, 0x5, 0x142, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x5, 0x5, 0x148, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 
    0x5, 0x14d, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0x153, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x158, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x15e, 0xa, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x16c, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x173, 
    0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0x17a, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 
    0x5, 0x181, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0x187, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x18c, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x192, 0xa, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x197, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x19d, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x1a6, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x5, 0x5, 0x1b0, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x1ba, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x1ce, 0xa, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0x1d6, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x5, 0x5, 0x1e5, 0xa, 0x5, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 
    0x1ea, 0xa, 0x6, 0xc, 0x6, 0xe, 0x6, 0x1ed, 0xb, 0x6, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x1fa, 0xa, 0x9, 0x3, 0xa, 0x3, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x200, 0xa, 0xa, 0x3, 0xb, 0x3, 0xb, 0x3, 
    0xb, 0x3, 0xb, 0x5, 0xb, 0x206, 0xa, 0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x20d, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 
    0xc, 0x211, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x214, 0xa, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x21b, 0xa, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x5, 0xc, 0x21f, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x222, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x22d, 0xa, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x5, 0xc, 0x231, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x234, 0xa, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x239, 0xa, 0xc, 0x5, 0xc, 0x23b, 
    0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x23e, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x241, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x24b, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 
    0xc, 0x24f, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x252, 0xa, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x255, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x25a, 
    0xa, 0xc, 0x5, 0xc, 0x25c, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x5, 0xc, 0x262, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x268, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x26c, 0xa, 
    0xc, 0x3, 0xc, 0x5, 0xc, 0x26f, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x272, 
    0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x275, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x278, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x27d, 0xa, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x283, 0xa, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x5, 0xc, 0x287, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x28a, 0xa, 
    0xc, 0x3, 0xc, 0x5, 0xc, 0x28d, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 
    0x291, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x7, 0xd, 0x297, 
    0xa, 0xd, 0xc, 0xd, 0xe, 0xd, 0x29a, 0xb, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x7, 
    0xe, 0x2b3, 0xa, 0xe, 0xc, 0xe, 0xe, 0xe, 0x2b6, 0xb, 0xe, 0x3, 0xf, 
    0x5, 0xf, 0x2b9, 0xa, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x7, 0xf, 0x2cf, 0xa, 0xf, 0xc, 0xf, 0xe, 0xf, 0x2d2, 
    0xb, 0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 
    0x11, 0x3, 0x11, 0x3, 0x11, 0x5, 0x11, 0x2dc, 0xa, 0x11, 0x3, 0x11, 
    0x5, 0x11, 0x2df, 0xa, 0x11, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 
    0x3, 0x12, 0x7, 0x12, 0x2e6, 0xa, 0x12, 0xc, 0x12, 0xe, 0x12, 0x2e9, 
    0xb, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 
    0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x2f9, 0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x7, 0x14, 0x302, 
    0xa, 0x14, 0xc, 0x14, 0xe, 0x14, 0x305, 0xb, 0x14, 0x3, 0x14, 0x3, 0x14, 
    0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 
    0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 
    0x5, 0x15, 0x316, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 
    0x17, 0x5, 0x17, 0x323, 0xa, 0x17, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 
    0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 
    0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x7, 0x1b, 0x332, 0xa, 0x1b, 
    0xc, 0x1b, 0xe, 0x1b, 0x335, 0xb, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x33d, 0xa, 0x1b, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x7, 0x1c, 0x358, 
    0xa, 0x1c, 0xc, 0x1c, 0xe, 0x1c, 0x35b, 0xb, 0x1c, 0x3, 0x1d, 0x3, 0x1d, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 
    0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 
    0x3, 0x20, 0x7, 0x20, 0x36d, 0xa, 0x20, 0xc, 0x20, 0xe, 0x20, 0x370, 
    0xb, 0x20, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x374, 0xa, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x379, 0xa, 0x21, 0x3, 0x21, 0x5, 0x21, 
    0x37c, 0xa, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 
    0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x5, 0x22, 0x388, 
    0xa, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x38d, 0xa, 0x23, 
    0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x391, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 
    0x394, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x398, 0xa, 0x23, 
    0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x39c, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 
    0x3, 0x23, 0x5, 0x23, 0x3a1, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x3a4, 
    0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x3a8, 0xa, 0x23, 0x5, 0x23, 
    0x3aa, 0xa, 0x23, 0x3, 0x24, 0x3, 0x24, 0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 
    0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 0x3, 
    0x26, 0x3, 0x26, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 
    0x7, 0x27, 0x3be, 0xa, 0x27, 0xc, 0x27, 0xe, 0x27, 0x3c1, 0xb, 0x27, 
    0x3, 0x27, 0x3, 0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 0x3c8, 
    0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x3cb, 0xa, 0x28, 0x3, 0x29, 0x3, 0x29, 
    0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x5, 
    0x29, 0x3d5, 0xa, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x3d9, 0xa, 
    0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 
    0x5, 0x2b, 0x3e1, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x3e5, 
    0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x3ea, 0xa, 0x2b, 
    0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x3ee, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 
    0x5, 0x2b, 0x3f2, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x3f6, 
    0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x3fa, 0xa, 0x2b, 0x5, 0x2b, 
    0x3fc, 0xa, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 
    0x3, 0x2c, 0x5, 0x2c, 0x404, 0xa, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 
    0x408, 0xa, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 0x40b, 0xa, 0x2c, 0x3, 0x2d, 
    0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 0x413, 
    0xa, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x418, 0xa, 0x2e, 
    0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x41d, 0xa, 0x2e, 0x3, 0x2e, 
    0x5, 0x2e, 0x420, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 
    0x3, 0x2f, 0x3, 0x2f, 0x7, 0x2f, 0x428, 0xa, 0x2f, 0xc, 0x2f, 0xe, 0x2f, 
    0x42b, 0xb, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x434, 0xa, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x5, 0x30, 0x438, 0xa, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x5, 0x31, 
    0x43d, 0xa, 0x31, 0x3, 0x31, 0x3, 0x31, 0x5, 0x31, 0x441, 0xa, 0x31, 
    0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x5, 0x32, 0x447, 0xa, 0x32, 
    0x3, 0x32, 0x5, 0x32, 0x44a, 0xa, 0x32, 0x3, 0x32, 0x5, 0x32, 0x44d, 
    0xa, 0x32, 0x3, 0x32, 0x5, 0x32, 0x450, 0xa, 0x32, 0x3, 0x33, 0x3, 0x33, 
    0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 
    0x33, 0x3, 0x33, 0x7, 0x33, 0x45c, 0xa, 0x33, 0xc, 0x33, 0xe, 0x33, 
    0x45f, 0xb, 0x33, 0x3, 0x33, 0x5, 0x33, 0x462, 0xa, 0x33, 0x3, 0x34, 
    0x3, 0x34, 0x5, 0x34, 0x466, 0xa, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 
    0x5, 0x34, 0x46b, 0xa, 0x34, 0x3, 0x34, 0x5, 0x34, 0x46e, 0xa, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x7, 
    0x35, 0x476, 0xa, 0x35, 0xc, 0x35, 0xe, 0x35, 0x479, 0xb, 0x35, 0x3, 
    0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x480, 
    0xa, 0x36, 0x3, 0x37, 0x5, 0x37, 0x483, 0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x5, 0x37, 0x487, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x48a, 0xa, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x48e, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 
    0x491, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x494, 0xa, 0x37, 0x3, 0x37, 
    0x5, 0x37, 0x497, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x49a, 0xa, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x49e, 0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x5, 0x37, 0x4a2, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x4a5, 0xa, 0x37, 
    0x3, 0x37, 0x5, 0x37, 0x4a8, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x4ab, 
    0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x4ae, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 
    0x4b1, 0xa, 0x37, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x39, 0x3, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x4ba, 0xa, 0x39, 0x3, 0x3a, 0x3, 0x3a, 
    0x3, 0x3a, 0x3, 0x3b, 0x5, 0x3b, 0x4c0, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3d, 0x3, 
    0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x4d4, 0xa, 0x3e, 0x3, 0x3f, 
    0x3, 0x3f, 0x3, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 
    0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x5, 
    0x43, 0x4ea, 0xa, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x45, 
    0x3, 0x45, 0x3, 0x45, 0x5, 0x45, 0x4f2, 0xa, 0x45, 0x3, 0x45, 0x5, 0x45, 
    0x4f5, 0xa, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x5, 0x45, 
    0x4fb, 0xa, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 
    0x3, 0x45, 0x5, 0x45, 0x503, 0xa, 0x45, 0x3, 0x45, 0x5, 0x45, 0x506, 
    0xa, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x50c, 
    0xa, 0x45, 0xc, 0x45, 0xe, 0x45, 0x50f, 0xb, 0x45, 0x3, 0x46, 0x5, 0x46, 
    0x512, 0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x517, 
    0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 0x51a, 0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 
    0x51d, 0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x521, 0xa, 0x46, 
    0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x525, 0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 
    0x528, 0xa, 0x46, 0x5, 0x46, 0x52a, 0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 
    0x52d, 0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x531, 0xa, 0x46, 
    0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x535, 0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 
    0x538, 0xa, 0x46, 0x5, 0x46, 0x53a, 0xa, 0x46, 0x5, 0x46, 0x53c, 0xa, 
    0x46, 0x3, 0x47, 0x5, 0x47, 0x53f, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x5, 0x47, 0x544, 0xa, 0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 
    0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x5, 
    0x48, 0x54f, 0xa, 0x48, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 
    0x5, 0x49, 0x555, 0xa, 0x49, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 
    0x55a, 0xa, 0x4a, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x7, 0x4b, 0x55f, 
    0xa, 0x4b, 0xc, 0x4b, 0xe, 0x4b, 0x562, 0xb, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 
    0x5, 0x4c, 0x566, 0xa, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x56a, 
    0xa, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x56e, 0xa, 0x4c, 0x3, 0x4d, 
    0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x573, 0xa, 0x4d, 0x3, 0x4e, 0x3, 0x4e, 
    0x3, 0x4e, 0x7, 0x4e, 0x578, 0xa, 0x4e, 0xc, 0x4e, 0xe, 0x4e, 0x57b, 
    0xb, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x50, 0x3, 
    0x50, 0x3, 0x50, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 
    0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x5, 
    0x51, 0x58f, 0xa, 0x51, 0x3, 0x51, 0x5, 0x51, 0x592, 0xa, 0x51, 0x3, 
    0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 
    0x5, 0x51, 0x59b, 0xa, 0x51, 0x3, 0x51, 0x3, 0x51, 0x5, 0x51, 0x59f, 
    0xa, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x5, 0x51, 0x5a4, 0xa, 0x51, 
    0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x5, 0x51, 0x5a9, 0xa, 0x51, 0x3, 0x51, 
    0x5, 0x51, 0x5ac, 0xa, 0x51, 0x5, 0x51, 0x5ae, 0xa, 0x51, 0x3, 0x52, 
    0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 
    0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 
    0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x5, 
    0x52, 0x5c4, 0xa, 0x52, 0x3, 0x52, 0x5, 0x52, 0x5c7, 0xa, 0x52, 0x3, 
    0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 0x3, 0x52, 
    0x3, 0x52, 0x3, 0x52, 0x5, 0x52, 0x5d2, 0xa, 0x52, 0x3, 0x53, 0x3, 0x53, 
    0x5, 0x53, 0x5d6, 0xa, 0x53, 0x3, 0x53, 0x5, 0x53, 0x5d9, 0xa, 0x53, 
    0x3, 0x53, 0x3, 0x53, 0x5, 0x53, 0x5dd, 0xa, 0x53, 0x3, 0x53, 0x3, 0x53, 
    0x5, 0x53, 0x5e1, 0xa, 0x53, 0x3, 0x54, 0x3, 0x54, 0x3, 0x54, 0x3, 0x55, 
    0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 0x5e9, 0xa, 0x55, 0x3, 0x55, 0x3, 0x55, 
    0x5, 0x55, 0x5ed, 0xa, 0x55, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 
    0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x7, 0x56, 0x5f8, 
    0xa, 0x56, 0xc, 0x56, 0xe, 0x56, 0x5fb, 0xb, 0x56, 0x3, 0x56, 0x3, 0x56, 
    0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x7, 0x56, 0x604, 
    0xa, 0x56, 0xc, 0x56, 0xe, 0x56, 0x607, 0xb, 0x56, 0x3, 0x56, 0x3, 0x56, 
    0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x7, 0x56, 0x610, 
    0xa, 0x56, 0xc, 0x56, 0xe, 0x56, 0x613, 0xb, 0x56, 0x3, 0x56, 0x3, 0x56, 
    0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x5, 0x56, 0x61a, 0xa, 0x56, 0x3, 0x56, 
    0x3, 0x56, 0x5, 0x56, 0x61e, 0xa, 0x56, 0x3, 0x57, 0x3, 0x57, 0x3, 0x57, 
    0x7, 0x57, 0x623, 0xa, 0x57, 0xc, 0x57, 0xe, 0x57, 0x626, 0xb, 0x57, 
    0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x5, 0x58, 0x62b, 0xa, 0x58, 0x3, 0x58, 
    0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x5, 0x58, 0x633, 
    0xa, 0x58, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x638, 0xa, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x6, 0x59, 0x63f, 
    0xa, 0x59, 0xd, 0x59, 0xe, 0x59, 0x640, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 
    0x645, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x664, 
    0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x675, 0xa, 0x59, 0x3, 0x59, 
    0x5, 0x59, 0x678, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x67c, 
    0xa, 0x59, 0x3, 0x59, 0x5, 0x59, 0x67f, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x5, 0x59, 0x68b, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x5, 0x59, 0x69c, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x6a0, 
    0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x6b1, 0xa, 0x59, 0x3, 0x59, 
    0x5, 0x59, 0x6b4, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 0x6b8, 
    0xa, 0x59, 0x3, 0x59, 0x5, 0x59, 0x6bb, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x5, 0x59, 0x6c6, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 
    0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 
    0x59, 0x6de, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x5, 0x59, 0x6e5, 0xa, 0x59, 0x7, 0x59, 0x6e7, 0xa, 0x59, 
    0xc, 0x59, 0xe, 0x59, 0x6ea, 0xb, 0x59, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 
    0x7, 0x5a, 0x6ef, 0xa, 0x5a, 0xc, 0x5a, 0xe, 0x5a, 0x6f2, 0xb, 0x5a, 
    0x3, 0x5b, 0x3, 0x5b, 0x5, 0x5b, 0x6f6, 0xa, 0x5b, 0x3, 0x5c, 0x3, 0x5c, 
    0x3, 0x5c, 0x3, 0x5c, 0x7, 0x5c, 0x6fc, 0xa, 0x5c, 0xc, 0x5c, 0xe, 0x5c, 
    0x6ff, 0xb, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 
    0x7, 0x5c, 0x706, 0xa, 0x5c, 0xc, 0x5c, 0xe, 0x5c, 0x709, 0xb, 0x5c, 
    0x5, 0x5c, 0x70b, 0xa, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5d, 
    0x3, 0x5d, 0x3, 0x5d, 0x5, 0x5d, 0x713, 0xa, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 
    0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x5, 0x5e, 0x71a, 0xa, 0x5e, 0x3, 0x5f, 
    0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x5, 
    0x5f, 0x723, 0xa, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 
    0x5, 0x5f, 0x729, 0xa, 0x5f, 0x7, 0x5f, 0x72b, 0xa, 0x5f, 0xc, 0x5f, 
    0xe, 0x5f, 0x72e, 0xb, 0x5f, 0x3, 0x60, 0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 
    0x733, 0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x3, 0x61, 0x3, 0x61, 0x3, 0x61, 
    0x5, 0x61, 0x73a, 0xa, 0x61, 0x3, 0x61, 0x3, 0x61, 0x3, 0x62, 0x3, 0x62, 
    0x3, 0x62, 0x7, 0x62, 0x741, 0xa, 0x62, 0xc, 0x62, 0xe, 0x62, 0x744, 
    0xb, 0x62, 0x3, 0x63, 0x3, 0x63, 0x3, 0x63, 0x5, 0x63, 0x749, 0xa, 0x63, 
    0x3, 0x64, 0x3, 0x64, 0x3, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 
    0x65, 0x3, 0x65, 0x5, 0x65, 0x753, 0xa, 0x65, 0x5, 0x65, 0x755, 0xa, 
    0x65, 0x3, 0x66, 0x5, 0x66, 0x758, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 
    0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 0x760, 0xa, 0x66, 
    0x3, 0x67, 0x3, 0x67, 0x3, 0x67, 0x5, 0x67, 0x765, 0xa, 0x67, 0x3, 0x68, 
    0x3, 0x68, 0x3, 0x69, 0x3, 0x69, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6b, 0x3, 
    0x6b, 0x5, 0x6b, 0x76f, 0xa, 0x6b, 0x3, 0x6c, 0x3, 0x6c, 0x3, 0x6c, 
    0x5, 0x6c, 0x774, 0xa, 0x6c, 0x3, 0x6d, 0x3, 0x6d, 0x5, 0x6d, 0x778, 
    0xa, 0x6d, 0x3, 0x6e, 0x3, 0x6e, 0x3, 0x6e, 0x3, 0x6e, 0x3, 0x6e, 0x2, 
    0x5, 0x88, 0xb0, 0xbc, 0x6f, 0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 
    0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 
    0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 
    0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 
    0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 
    0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 
    0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 0xa0, 
    0xa2, 0xa4, 0xa6, 0xa8, 0xaa, 0xac, 0xae, 0xb0, 0xb2, 0xb4, 0xb6, 0xb8, 
    0xba, 0xbc, 0xbe, 0xc0, 0xc2, 0xc4, 0xc6, 0xc8, 0xca, 0xcc, 0xce, 0xd0, 
    0xd2, 0xd4, 0xd6, 0xd8, 0xda, 0x2, 0x1d, 0x8, 0x2, 0x5, 0x5, 0x1a, 0x1a, 
    0x1d, 0x1d, 0x27, 0x27, 0x67, 0x67, 0xa8, 0xa8, 0x4, 0x2, 0x11, 0x11, 
    0x1f, 0x1f, 0x5, 0x2, 0x5, 0x5, 0x27, 0x27, 0x67, 0x67, 0x4, 0x2, 0x2a, 
    0x2a, 0x2c, 0x2c, 0x4, 0x2, 0x2d, 0x2d, 0x33, 0x33, 0x5, 0x2, 0x10, 
    0x10, 0x97, 0x97, 0x9d, 0x9d, 0x4, 0x2, 0x21, 0x21, 0x8a, 0x8a, 0x4, 
    0x2, 0x53, 0x53, 0x5f, 0x5f, 0x4, 0x2, 0x46, 0x46, 0x64, 0x64, 0x5, 
    0x2, 0x6, 0x6, 0xa, 0xa, 0xe, 0xe, 0x6, 0x2, 0x6, 0x6, 0x9, 0xa, 0xe, 
    0xe, 0x8e, 0x8e, 0x4, 0x2, 0x5f, 0x5f, 0x89, 0x89, 0x4, 0x2, 0x6, 0x6, 
    0xa, 0xa, 0x4, 0x2, 0x75, 0x75, 0xc5, 0xc5, 0x4, 0x2, 0xd, 0xd, 0x2a, 
    0x2b, 0x4, 0x2, 0x3e, 0x3e, 0x5c, 0x5c, 0x4, 0x2, 0x43, 0x43, 0x4f, 
    0x4f, 0x3, 0x2, 0x94, 0x95, 0x5, 0x2, 0x13, 0x13, 0x5e, 0x5e, 0xa5, 
    0xa5, 0x5, 0x2, 0xc1, 0xc1, 0xd3, 0xd3, 0xdc, 0xdc, 0x4, 0x2, 0xc6, 
    0xc7, 0xd4, 0xd4, 0x4, 0x2, 0x4e, 0x4e, 0x61, 0x61, 0x3, 0x2, 0xbc, 
    0xbd, 0x4, 0x2, 0xc7, 0xc7, 0xd4, 0xd4, 0xa, 0x2, 0x25, 0x25, 0x4b, 
    0x4b, 0x6b, 0x6b, 0x6d, 0x6d, 0x81, 0x81, 0x8c, 0x8c, 0xb3, 0xb3, 0xb7, 
    0xb7, 0xe, 0x2, 0x4, 0x24, 0x26, 0x4a, 0x4c, 0x50, 0x52, 0x6a, 0x6c, 
    0x6c, 0x6e, 0x6f, 0x71, 0x72, 0x74, 0x7f, 0x82, 0x8b, 0x8d, 0xb2, 0xb4, 
    0xb6, 0xb8, 0xb9, 0x6, 0x2, 0x24, 0x24, 0x3e, 0x3e, 0x4c, 0x4c, 0x5a, 
    0x5a, 0x2, 0x88f, 0x2, 0xea, 0x3, 0x2, 0x2, 0x2, 0x4, 0xfe, 0x3, 0x2, 
    0x2, 0x2, 0x6, 0x100, 0x3, 0x2, 0x2, 0x2, 0x8, 0x1e4, 0x3, 0x2, 0x2, 
    0x2, 0xa, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0xc, 0x1ee, 0x3, 0x2, 0x2, 0x2, 
    0xe, 0x1f2, 0x3, 0x2, 0x2, 0x2, 0x10, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x12, 
    0x1fb, 0x3, 0x2, 0x2, 0x2, 0x14, 0x201, 0x3, 0x2, 0x2, 0x2, 0x16, 0x290, 
    0x3, 0x2, 0x2, 0x2, 0x18, 0x292, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x29d, 0x3, 
    0x2, 0x2, 0x2, 0x1c, 0x2b8, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x2d3, 0x3, 0x2, 
    0x2, 0x2, 0x20, 0x2d7, 0x3, 0x2, 0x2, 0x2, 0x22, 0x2e0, 0x3, 0x2, 0x2, 
    0x2, 0x24, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x26, 0x2fc, 0x3, 0x2, 0x2, 0x2, 
    0x28, 0x309, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x319, 0x3, 0x2, 0x2, 0x2, 0x2c, 
    0x31e, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x324, 0x3, 0x2, 0x2, 0x2, 0x30, 0x327, 
    0x3, 0x2, 0x2, 0x2, 0x32, 0x32a, 0x3, 0x2, 0x2, 0x2, 0x34, 0x33c, 0x3, 
    0x2, 0x2, 0x2, 0x36, 0x33e, 0x3, 0x2, 0x2, 0x2, 0x38, 0x35c, 0x3, 0x2, 
    0x2, 0x2, 0x3a, 0x360, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x364, 0x3, 0x2, 0x2, 
    0x2, 0x3e, 0x368, 0x3, 0x2, 0x2, 0x2, 0x40, 0x371, 0x3, 0x2, 0x2, 0x2, 
    0x42, 0x387, 0x3, 0x2, 0x2, 0x2, 0x44, 0x3a9, 0x3, 0x2, 0x2, 0x2, 0x46, 
    0x3ab, 0x3, 0x2, 0x2, 0x2, 0x48, 0x3ae, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x3b5, 
    0x3, 0x2, 0x2, 0x2, 0x4c, 0x3b8, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x3c4, 0x3, 
    0x2, 0x2, 0x2, 0x50, 0x3cc, 0x3, 0x2, 0x2, 0x2, 0x52, 0x3d6, 0x3, 0x2, 
    0x2, 0x2, 0x54, 0x3fb, 0x3, 0x2, 0x2, 0x2, 0x56, 0x40a, 0x3, 0x2, 0x2, 
    0x2, 0x58, 0x412, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x414, 0x3, 0x2, 0x2, 0x2, 
    0x5c, 0x423, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x437, 0x3, 0x2, 0x2, 0x2, 0x60, 
    0x439, 0x3, 0x2, 0x2, 0x2, 0x62, 0x442, 0x3, 0x2, 0x2, 0x2, 0x64, 0x451, 
    0x3, 0x2, 0x2, 0x2, 0x66, 0x463, 0x3, 0x2, 0x2, 0x2, 0x68, 0x471, 0x3, 
    0x2, 0x2, 0x2, 0x6a, 0x47f, 0x3, 0x2, 0x2, 0x2, 0x6c, 0x482, 0x3, 0x2, 
    0x2, 0x2, 0x6e, 0x4b2, 0x3, 0x2, 0x2, 0x2, 0x70, 0x4b5, 0x3, 0x2, 0x2, 
    0x2, 0x72, 0x4bb, 0x3, 0x2, 0x2, 0x2, 0x74, 0x4bf, 0x3, 0x2, 0x2, 0x2, 
    0x76, 0x4c5, 0x3, 0x2, 0x2, 0x2, 0x78, 0x4c8, 0x3, 0x2, 0x2, 0x2, 0x7a, 
    0x4cb, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x4d5, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x4d8, 
    0x3, 0x2, 0x2, 0x2, 0x80, 0x4dc, 0x3, 0x2, 0x2, 0x2, 0x82, 0x4e0, 0x3, 
    0x2, 0x2, 0x2, 0x84, 0x4e5, 0x3, 0x2, 0x2, 0x2, 0x86, 0x4eb, 0x3, 0x2, 
    0x2, 0x2, 0x88, 0x4fa, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x53b, 0x3, 0x2, 0x2, 
    0x2, 0x8c, 0x543, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x54e, 0x3, 0x2, 0x2, 0x2, 
    0x90, 0x550, 0x3, 0x2, 0x2, 0x2, 0x92, 0x556, 0x3, 0x2, 0x2, 0x2, 0x94, 
    0x55b, 0x3, 0x2, 0x2, 0x2, 0x96, 0x563, 0x3, 0x2, 0x2, 0x2, 0x98, 0x56f, 
    0x3, 0x2, 0x2, 0x2, 0x9a, 0x574, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x57c, 0x3, 
    0x2, 0x2, 0x2, 0x9e, 0x580, 0x3, 0x2, 0x2, 0x2, 0xa0, 0x5ad, 0x3, 0x2, 
    0x2, 0x2, 0xa2, 0x5d1, 0x3, 0x2, 0x2, 0x2, 0xa4, 0x5d3, 0x3, 0x2, 0x2, 
    0x2, 0xa6, 0x5e2, 0x3, 0x2, 0x2, 0x2, 0xa8, 0x5e5, 0x3, 0x2, 0x2, 0x2, 
    0xaa, 0x61d, 0x3, 0x2, 0x2, 0x2, 0xac, 0x61f, 0x3, 0x2, 0x2, 0x2, 0xae, 
    0x632, 0x3, 0x2, 0x2, 0x2, 0xb0, 0x69f, 0x3, 0x2, 0x2, 0x2, 0xb2, 0x6eb, 
    0x3, 0x2, 0x2, 0x2, 0xb4, 0x6f5, 0x3, 0x2, 0x2, 0x2, 0xb6, 0x70a, 0x3, 
    0x2, 0x2, 0x2, 0xb8, 0x712, 0x3, 0x2, 0x2, 0x2, 0xba, 0x716, 0x3, 0x2, 
    0x2, 0x2, 0xbc, 0x722, 0x3, 0x2, 0x2, 0x2, 0xbe, 0x72f, 0x3, 0x2, 0x2, 
    0x2, 0xc0, 0x739, 0x3, 0x2, 0x2, 0x2, 0xc2, 0x73d, 0x3, 0x2, 0x2, 0x2, 
    0xc4, 0x748, 0x3, 0x2, 0x2, 0x2, 0xc6, 0x74a, 0x3, 0x2, 0x2, 0x2, 0xc8, 
    0x754, 0x3, 0x2, 0x2, 0x2, 0xca, 0x757, 0x3, 0x2, 0x2, 0x2, 0xcc, 0x764, 
    0x3, 0x2, 0x2, 0x2, 0xce, 0x766, 0x3, 0x2, 0x2, 0x2, 0xd0, 0x768, 0x3, 
    0x2, 0x2, 0x2, 0xd2, 0x76a, 0x3, 0x2, 0x2, 0x2, 0xd4, 0x76e, 0x3, 0x2, 
    0x2, 0x2, 0xd6, 0x773, 0x3, 0x2, 0x2, 0x2, 0xd8, 0x777, 0x3, 0x2, 0x2, 
    0x2, 0xda, 0x779, 0x3, 0x2, 0x2, 0x2, 0xdc, 0xe0, 0x5, 0x4, 0x3, 0x2, 
    0xdd, 0xde, 0x7, 0x56, 0x2, 0x2, 0xde, 0xdf, 0x7, 0x7b, 0x2, 0x2, 0xdf, 
    0xe1, 0x7, 0xbf, 0x2, 0x2, 0xe0, 0xdd, 0x3, 0x2, 0x2, 0x2, 0xe0, 0xe1, 
    0x3, 0x2, 0x2, 0x2, 0xe1, 0xe4, 0x3, 0x2, 0x2, 0x2, 0xe2, 0xe3, 0x7, 
    0x41, 0x2, 0x2, 0xe3, 0xe5, 0x5, 0xd8, 0x6d, 0x2, 0xe4, 0xe2, 0x3, 0x2, 
    0x2, 0x2, 0xe4, 0xe5, 0x3, 0x2, 0x2, 0x2, 0xe5, 0xe7, 0x3, 0x2, 0x2, 
    0x2, 0xe6, 0xe8, 0x7, 0xdb, 0x2, 0x2, 0xe7, 0xe6, 0x3, 0x2, 0x2, 0x2, 
    0xe7, 0xe8, 0x3, 0x2, 0x2, 0x2, 0xe8, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xe9, 
    0xeb, 0x5, 0x5a, 0x2e, 0x2, 0xea, 0xdc, 0x3, 0x2, 0x2, 0x2, 0xea, 0xe9, 
    0x3, 0x2, 0x2, 0x2, 0xeb, 0x3, 0x3, 0x2, 0x2, 0x2, 0xec, 0xff, 0x5, 
    0x6, 0x4, 0x2, 0xed, 0xff, 0x5, 0x12, 0xa, 0x2, 0xee, 0xff, 0x5, 0x14, 
    0xb, 0x2, 0xef, 0xff, 0x5, 0x16, 0xc, 0x2, 0xf0, 0xff, 0x5, 0x52, 0x2a, 
    0x2, 0xf1, 0xff, 0x5, 0x54, 0x2b, 0x2, 0xf2, 0xff, 0x5, 0x56, 0x2c, 
    0x2, 0xf3, 0xff, 0x5, 0x58, 0x2d, 0x2, 0xf4, 0xff, 0x5, 0x60, 0x31, 
    0x2, 0xf5, 0xff, 0x5, 0x62, 0x32, 0x2, 0xf6, 0xff, 0x5, 0x64, 0x33, 
    0x2, 0xf7, 0xff, 0x5, 0x68, 0x35, 0x2, 0xf8, 0xff, 0x5, 0x9e, 0x50, 
    0x2, 0xf9, 0xff, 0x5, 0xa0, 0x51, 0x2, 0xfa, 0xff, 0x5, 0xa2, 0x52, 
    0x2, 0xfb, 0xff, 0x5, 0xa4, 0x53, 0x2, 0xfc, 0xff, 0x5, 0xa6, 0x54, 
    0x2, 0xfd, 0xff, 0x5, 0xa8, 0x55, 0x2, 0xfe, 0xec, 0x3, 0x2, 0x2, 0x2, 
    0xfe, 0xed, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xee, 0x3, 0x2, 0x2, 0x2, 0xfe, 
    0xef, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf0, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf1, 
    0x3, 0x2, 0x2, 0x2, 0xfe, 0xf2, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf3, 0x3, 
    0x2, 0x2, 0x2, 0xfe, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf5, 0x3, 0x2, 
    0x2, 0x2, 0xfe, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf7, 0x3, 0x2, 0x2, 
    0x2, 0xfe, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xf9, 0x3, 0x2, 0x2, 0x2, 
    0xfe, 0xfa, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xfb, 0x3, 0x2, 0x2, 0x2, 0xfe, 
    0xfc, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xfd, 0x3, 0x2, 0x2, 0x2, 0xff, 0x5, 
    0x3, 0x2, 0x2, 0x2, 0x100, 0x101, 0x7, 0x7, 0x2, 0x2, 0x101, 0x102, 
    0x7, 0x9a, 0x2, 0x2, 0x102, 0x104, 0x5, 0xc0, 0x61, 0x2, 0x103, 0x105, 
    0x5, 0x2c, 0x17, 0x2, 0x104, 0x103, 0x3, 0x2, 0x2, 0x2, 0x104, 0x105, 
    0x3, 0x2, 0x2, 0x2, 0x105, 0x106, 0x3, 0x2, 0x2, 0x2, 0x106, 0x10b, 
    0x5, 0x8, 0x5, 0x2, 0x107, 0x108, 0x7, 0xc5, 0x2, 0x2, 0x108, 0x10a, 
    0x5, 0x8, 0x5, 0x2, 0x109, 0x107, 0x3, 0x2, 0x2, 0x2, 0x10a, 0x10d, 
    0x3, 0x2, 0x2, 0x2, 0x10b, 0x109, 0x3, 0x2, 0x2, 0x2, 0x10b, 0x10c, 
    0x3, 0x2, 0x2, 0x2, 0x10c, 0x7, 0x3, 0x2, 0x2, 0x2, 0x10d, 0x10b, 0x3, 
    0x2, 0x2, 0x2, 0x10e, 0x10f, 0x7, 0x3, 0x2, 0x2, 0x10f, 0x113, 0x7, 
    0x1c, 0x2, 0x2, 0x110, 0x111, 0x7, 0x4d, 0x2, 0x2, 0x111, 0x112, 0x7, 
    0x72, 0x2, 0x2, 0x112, 0x114, 0x7, 0x38, 0x2, 0x2, 0x113, 0x110, 0x3, 
    0x2, 0x2, 0x2, 0x113, 0x114, 0x3, 0x2, 0x2, 0x2, 0x114, 0x115, 0x3, 
    0x2, 0x2, 0x2, 0x115, 0x118, 0x5, 0x44, 0x23, 0x2, 0x116, 0x117, 0x7, 
    0x4, 0x2, 0x2, 0x117, 0x119, 0x5, 0xba, 0x5e, 0x2, 0x118, 0x116, 0x3, 
    0x2, 0x2, 0x2, 0x118, 0x119, 0x3, 0x2, 0x2, 0x2, 0x119, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x11a, 0x11b, 0x7, 0x3, 0x2, 0x2, 0x11b, 0x11f, 0x7, 
    0x50, 0x2, 0x2, 0x11c, 0x11d, 0x7, 0x4d, 0x2, 0x2, 0x11d, 0x11e, 0x7, 
    0x72, 0x2, 0x2, 0x11e, 0x120, 0x7, 0x38, 0x2, 0x2, 0x11f, 0x11c, 0x3, 
    0x2, 0x2, 0x2, 0x11f, 0x120, 0x3, 0x2, 0x2, 0x2, 0x120, 0x121, 0x3, 
    0x2, 0x2, 0x2, 0x121, 0x124, 0x5, 0x48, 0x25, 0x2, 0x122, 0x123, 0x7, 
    0x4, 0x2, 0x2, 0x123, 0x125, 0x5, 0xba, 0x5e, 0x2, 0x124, 0x122, 0x3, 
    0x2, 0x2, 0x2, 0x124, 0x125, 0x3, 0x2, 0x2, 0x2, 0x125, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x126, 0x127, 0x7, 0x3, 0x2, 0x2, 0x127, 0x12b, 0x7, 
    0x80, 0x2, 0x2, 0x128, 0x129, 0x7, 0x4d, 0x2, 0x2, 0x129, 0x12a, 0x7, 
    0x72, 0x2, 0x2, 0x12a, 0x12c, 0x7, 0x38, 0x2, 0x2, 0x12b, 0x128, 0x3, 
    0x2, 0x2, 0x2, 0x12b, 0x12c, 0x3, 0x2, 0x2, 0x2, 0x12c, 0x12d, 0x3, 
    0x2, 0x2, 0x2, 0x12d, 0x130, 0x5, 0x4a, 0x26, 0x2, 0x12e, 0x12f, 0x7, 
    0x4, 0x2, 0x2, 0x12f, 0x131, 0x5, 0xba, 0x5e, 0x2, 0x130, 0x12e, 0x3, 
    0x2, 0x2, 0x2, 0x130, 0x131, 0x3, 0x2, 0x2, 0x2, 0x131, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x132, 0x133, 0x7, 0x11, 0x2, 0x2, 0x133, 0x136, 0x5, 
    0x10, 0x9, 0x2, 0x134, 0x135, 0x7, 0x43, 0x2, 0x2, 0x135, 0x137, 0x5, 
    0xc0, 0x61, 0x2, 0x136, 0x134, 0x3, 0x2, 0x2, 0x2, 0x136, 0x137, 0x3, 
    0x2, 0x2, 0x2, 0x137, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x138, 0x139, 0x7, 
    0x18, 0x2, 0x2, 0x139, 0x13c, 0x7, 0x1c, 0x2, 0x2, 0x13a, 0x13b, 0x7, 
    0x4d, 0x2, 0x2, 0x13b, 0x13d, 0x7, 0x38, 0x2, 0x2, 0x13c, 0x13a, 0x3, 
    0x2, 0x2, 0x2, 0x13c, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13d, 0x13e, 0x3, 
    0x2, 0x2, 0x2, 0x13e, 0x141, 0x5, 0xba, 0x5e, 0x2, 0x13f, 0x140, 0x7, 
    0x4f, 0x2, 0x2, 0x140, 0x142, 0x5, 0x10, 0x9, 0x2, 0x141, 0x13f, 0x3, 
    0x2, 0x2, 0x2, 0x141, 0x142, 0x3, 0x2, 0x2, 0x2, 0x142, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x143, 0x144, 0x7, 0x18, 0x2, 0x2, 0x144, 0x147, 0x7, 
    0x50, 0x2, 0x2, 0x145, 0x146, 0x7, 0x4d, 0x2, 0x2, 0x146, 0x148, 0x7, 
    0x38, 0x2, 0x2, 0x147, 0x145, 0x3, 0x2, 0x2, 0x2, 0x147, 0x148, 0x3, 
    0x2, 0x2, 0x2, 0x148, 0x149, 0x3, 0x2, 0x2, 0x2, 0x149, 0x14c, 0x5, 
    0xba, 0x5e, 0x2, 0x14a, 0x14b, 0x7, 0x4f, 0x2, 0x2, 0x14b, 0x14d, 0x5, 
    0x10, 0x9, 0x2, 0x14c, 0x14a, 0x3, 0x2, 0x2, 0x2, 0x14c, 0x14d, 0x3, 
    0x2, 0x2, 0x2, 0x14d, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x14e, 0x14f, 0x7, 
    0x18, 0x2, 0x2, 0x14f, 0x152, 0x7, 0x80, 0x2, 0x2, 0x150, 0x151, 0x7, 
    0x4d, 0x2, 0x2, 0x151, 0x153, 0x7, 0x38, 0x2, 0x2, 0x152, 0x150, 0x3, 
    0x2, 0x2, 0x2, 0x152, 0x153, 0x3, 0x2, 0x2, 0x2, 0x153, 0x154, 0x3, 
    0x2, 0x2, 0x2, 0x154, 0x157, 0x5, 0xba, 0x5e, 0x2, 0x155, 0x156, 0x7, 
    0x4f, 0x2, 0x2, 0x156, 0x158, 0x5, 0x10, 0x9, 0x2, 0x157, 0x155, 0x3, 
    0x2, 0x2, 0x2, 0x157, 0x158, 0x3, 0x2, 0x2, 0x2, 0x158, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x159, 0x15a, 0x7, 0x1d, 0x2, 0x2, 0x15a, 0x15d, 0x7, 
    0x1c, 0x2, 0x2, 0x15b, 0x15c, 0x7, 0x4d, 0x2, 0x2, 0x15c, 0x15e, 0x7, 
    0x38, 0x2, 0x2, 0x15d, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x15e, 0x3, 
    0x2, 0x2, 0x2, 0x15e, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15f, 0x160, 0x5, 
    0xba, 0x5e, 0x2, 0x160, 0x161, 0x7, 0xbf, 0x2, 0x2, 0x161, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x162, 0x163, 0x7, 0x29, 0x2, 0x2, 0x163, 0x164, 0x7, 
    0xb5, 0x2, 0x2, 0x164, 0x1e5, 0x5, 0xb0, 0x59, 0x2, 0x165, 0x166, 0x7, 
    0x2d, 0x2, 0x2, 0x166, 0x1e5, 0x5, 0x10, 0x9, 0x2, 0x167, 0x168, 0x7, 
    0x33, 0x2, 0x2, 0x168, 0x16b, 0x7, 0x1c, 0x2, 0x2, 0x169, 0x16a, 0x7, 
    0x4d, 0x2, 0x2, 0x16a, 0x16c, 0x7, 0x38, 0x2, 0x2, 0x16b, 0x169, 0x3, 
    0x2, 0x2, 0x2, 0x16b, 0x16c, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16d, 0x3, 
    0x2, 0x2, 0x2, 0x16d, 0x1e5, 0x5, 0xba, 0x5e, 0x2, 0x16e, 0x16f, 0x7, 
    0x33, 0x2, 0x2, 0x16f, 0x172, 0x7, 0x50, 0x2, 0x2, 0x170, 0x171, 0x7, 
    0x4d, 0x2, 0x2, 0x171, 0x173, 0x7, 0x38, 0x2, 0x2, 0x172, 0x170, 0x3, 
    0x2, 0x2, 0x2, 0x172, 0x173, 0x3, 0x2, 0x2, 0x2, 0x173, 0x174, 0x3, 
    0x2, 0x2, 0x2, 0x174, 0x1e5, 0x5, 0xba, 0x5e, 0x2, 0x175, 0x176, 0x7, 
    0x33, 0x2, 0x2, 0x176, 0x179, 0x7, 0x80, 0x2, 0x2, 0x177, 0x178, 0x7, 
    0x4d, 0x2, 0x2, 0x178, 0x17a, 0x7, 0x38, 0x2, 0x2, 0x179, 0x177, 0x3, 
    0x2, 0x2, 0x2, 0x179, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17a, 0x17b, 0x3, 
    0x2, 0x2, 0x2, 0x17b, 0x1e5, 0x5, 0xba, 0x5e, 0x2, 0x17c, 0x17d, 0x7, 
    0x33, 0x2, 0x2, 0x17d, 0x1e5, 0x5, 0x10, 0x9, 0x2, 0x17e, 0x180, 0x7, 
    0x42, 0x2, 0x2, 0x17f, 0x181, 0x5, 0x10, 0x9, 0x2, 0x180, 0x17f, 0x3, 
    0x2, 0x2, 0x2, 0x180, 0x181, 0x3, 0x2, 0x2, 0x2, 0x181, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x182, 0x183, 0x7, 0x66, 0x2, 0x2, 0x183, 0x186, 0x7, 
    0x50, 0x2, 0x2, 0x184, 0x185, 0x7, 0x4d, 0x2, 0x2, 0x185, 0x187, 0x7, 
    0x38, 0x2, 0x2, 0x186, 0x184, 0x3, 0x2, 0x2, 0x2, 0x186, 0x187, 0x3, 
    0x2, 0x2, 0x2, 0x187, 0x188, 0x3, 0x2, 0x2, 0x2, 0x188, 0x18b, 0x5, 
    0xba, 0x5e, 0x2, 0x189, 0x18a, 0x7, 0x4f, 0x2, 0x2, 0x18a, 0x18c, 0x5, 
    0x10, 0x9, 0x2, 0x18b, 0x189, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x18c, 0x3, 
    0x2, 0x2, 0x2, 0x18c, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x18d, 0x18e, 0x7, 
    0x66, 0x2, 0x2, 0x18e, 0x191, 0x7, 0x80, 0x2, 0x2, 0x18f, 0x190, 0x7, 
    0x4d, 0x2, 0x2, 0x190, 0x192, 0x7, 0x38, 0x2, 0x2, 0x191, 0x18f, 0x3, 
    0x2, 0x2, 0x2, 0x191, 0x192, 0x3, 0x2, 0x2, 0x2, 0x192, 0x193, 0x3, 
    0x2, 0x2, 0x2, 0x193, 0x196, 0x5, 0xba, 0x5e, 0x2, 0x194, 0x195, 0x7, 
    0x4f, 0x2, 0x2, 0x195, 0x197, 0x5, 0x10, 0x9, 0x2, 0x196, 0x194, 0x3, 
    0x2, 0x2, 0x2, 0x196, 0x197, 0x3, 0x2, 0x2, 0x2, 0x197, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x198, 0x199, 0x7, 0x6c, 0x2, 0x2, 0x199, 0x19c, 0x7, 
    0x1c, 0x2, 0x2, 0x19a, 0x19b, 0x7, 0x4d, 0x2, 0x2, 0x19b, 0x19d, 0x7, 
    0x38, 0x2, 0x2, 0x19c, 0x19a, 0x3, 0x2, 0x2, 0x2, 0x19c, 0x19d, 0x3, 
    0x2, 0x2, 0x2, 0x19d, 0x19e, 0x3, 0x2, 0x2, 0x2, 0x19e, 0x19f, 0x5, 
    0xba, 0x5e, 0x2, 0x19f, 0x1a0, 0x5, 0x4c, 0x27, 0x2, 0x1a0, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x1a1, 0x1a2, 0x7, 0x6c, 0x2, 0x2, 0x1a2, 0x1a5, 0x7, 
    0x1c, 0x2, 0x2, 0x1a3, 0x1a4, 0x7, 0x4d, 0x2, 0x2, 0x1a4, 0x1a6, 0x7, 
    0x38, 0x2, 0x2, 0x1a5, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x1a5, 0x1a6, 0x3, 
    0x2, 0x2, 0x2, 0x1a6, 0x1a7, 0x3, 0x2, 0x2, 0x2, 0x1a7, 0x1a8, 0x5, 
    0xba, 0x5e, 0x2, 0x1a8, 0x1a9, 0x7, 0x1d, 0x2, 0x2, 0x1a9, 0x1aa, 0x7, 
    0xbf, 0x2, 0x2, 0x1aa, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1ab, 0x1ac, 0x7, 
    0x6c, 0x2, 0x2, 0x1ac, 0x1af, 0x7, 0x1c, 0x2, 0x2, 0x1ad, 0x1ae, 0x7, 
    0x4d, 0x2, 0x2, 0x1ae, 0x1b0, 0x7, 0x38, 0x2, 0x2, 0x1af, 0x1ad, 0x3, 
    0x2, 0x2, 0x2, 0x1af, 0x1b0, 0x3, 0x2, 0x2, 0x2, 0x1b0, 0x1b1, 0x3, 
    0x2, 0x2, 0x2, 0x1b1, 0x1b2, 0x5, 0xba, 0x5e, 0x2, 0x1b2, 0x1b3, 0x7, 
    0x84, 0x2, 0x2, 0x1b3, 0x1b4, 0x5, 0xe, 0x8, 0x2, 0x1b4, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x1b5, 0x1b6, 0x7, 0x6c, 0x2, 0x2, 0x1b6, 0x1b9, 0x7, 
    0x1c, 0x2, 0x2, 0x1b7, 0x1b8, 0x7, 0x4d, 0x2, 0x2, 0x1b8, 0x1ba, 0x7, 
    0x38, 0x2, 0x2, 0x1b9, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1ba, 0x3, 
    0x2, 0x2, 0x2, 0x1ba, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1e5, 0x5, 
    0x44, 0x23, 0x2, 0x1bc, 0x1bd, 0x7, 0x6c, 0x2, 0x2, 0x1bd, 0x1be, 0x7, 
    0x79, 0x2, 0x2, 0x1be, 0x1bf, 0x7, 0x14, 0x2, 0x2, 0x1bf, 0x1e5, 0x5, 
    0xb0, 0x59, 0x2, 0x1c0, 0x1c1, 0x7, 0x6c, 0x2, 0x2, 0x1c1, 0x1e5, 0x5, 
    0x3e, 0x20, 0x2, 0x1c2, 0x1c3, 0x7, 0x6e, 0x2, 0x2, 0x1c3, 0x1cd, 0x5, 
    0x10, 0x9, 0x2, 0x1c4, 0x1c5, 0x7, 0xa2, 0x2, 0x2, 0x1c5, 0x1c6, 0x7, 
    0x30, 0x2, 0x2, 0x1c6, 0x1ce, 0x7, 0xbf, 0x2, 0x2, 0x1c7, 0x1c8, 0x7, 
    0xa2, 0x2, 0x2, 0x1c8, 0x1c9, 0x7, 0xb1, 0x2, 0x2, 0x1c9, 0x1ce, 0x7, 
    0xbf, 0x2, 0x2, 0x1ca, 0x1cb, 0x7, 0xa2, 0x2, 0x2, 0x1cb, 0x1cc, 0x7, 
    0x9a, 0x2, 0x2, 0x1cc, 0x1ce, 0x5, 0xc0, 0x61, 0x2, 0x1cd, 0x1c4, 0x3, 
    0x2, 0x2, 0x2, 0x1cd, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1cd, 0x1ca, 0x3, 
    0x2, 0x2, 0x2, 0x1ce, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d0, 0x7, 
    0x84, 0x2, 0x2, 0x1d0, 0x1e5, 0x7, 0xa8, 0x2, 0x2, 0x1d1, 0x1d2, 0x7, 
    0x85, 0x2, 0x2, 0x1d2, 0x1d5, 0x7, 0x1c, 0x2, 0x2, 0x1d3, 0x1d4, 0x7, 
    0x4d, 0x2, 0x2, 0x1d4, 0x1d6, 0x7, 0x38, 0x2, 0x2, 0x1d5, 0x1d3, 0x3, 
    0x2, 0x2, 0x2, 0x1d5, 0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d6, 0x1d7, 0x3, 
    0x2, 0x2, 0x2, 0x1d7, 0x1d8, 0x5, 0xba, 0x5e, 0x2, 0x1d8, 0x1d9, 0x7, 
    0xa2, 0x2, 0x2, 0x1d9, 0x1da, 0x5, 0xba, 0x5e, 0x2, 0x1da, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x1db, 0x1dc, 0x7, 0x86, 0x2, 0x2, 0x1dc, 0x1dd, 0x5, 
    0x10, 0x9, 0x2, 0x1dd, 0x1de, 0x7, 0x43, 0x2, 0x2, 0x1de, 0x1df, 0x5, 
    0xc0, 0x61, 0x2, 0x1df, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1e0, 0x1e1, 0x7, 
    0xab, 0x2, 0x2, 0x1e1, 0x1e2, 0x5, 0xa, 0x6, 0x2, 0x1e2, 0x1e3, 0x5, 
    0x78, 0x3d, 0x2, 0x1e3, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x10e, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x11a, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x126, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x132, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x138, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x143, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x14e, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x159, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x162, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x165, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x167, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x175, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x17c, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x17e, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x182, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x18d, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x198, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1a1, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1b5, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x1bc, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1c0, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1cf, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x1d1, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1db, 0x3, 
    0x2, 0x2, 0x2, 0x1e4, 0x1e0, 0x3, 0x2, 0x2, 0x2, 0x1e5, 0x9, 0x3, 0x2, 
    0x2, 0x2, 0x1e6, 0x1eb, 0x5, 0xc, 0x7, 0x2, 0x1e7, 0x1e8, 0x7, 0xc5, 
    0x2, 0x2, 0x1e8, 0x1ea, 0x5, 0xc, 0x7, 0x2, 0x1e9, 0x1e7, 0x3, 0x2, 
    0x2, 0x2, 0x1ea, 0x1ed, 0x3, 0x2, 0x2, 0x2, 0x1eb, 0x1e9, 0x3, 0x2, 
    0x2, 0x2, 0x1eb, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1ec, 0xb, 0x3, 0x2, 0x2, 
    0x2, 0x1ed, 0x1eb, 0x3, 0x2, 0x2, 0x2, 0x1ee, 0x1ef, 0x5, 0xba, 0x5e, 
    0x2, 0x1ef, 0x1f0, 0x7, 0xca, 0x2, 0x2, 0x1f0, 0x1f1, 0x5, 0xb0, 0x59, 
    0x2, 0x1f1, 0xd, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x9, 0x2, 0x2, 0x2, 
    0x1f3, 0xf, 0x3, 0x2, 0x2, 0x2, 0x1f4, 0x1f5, 0x7, 0x7c, 0x2, 0x2, 0x1f5, 
    0x1fa, 0x5, 0xb0, 0x59, 0x2, 0x1f6, 0x1f7, 0x7, 0x7c, 0x2, 0x2, 0x1f7, 
    0x1f8, 0x7, 0x4c, 0x2, 0x2, 0x1f8, 0x1fa, 0x7, 0xbf, 0x2, 0x2, 0x1f9, 
    0x1f4, 0x3, 0x2, 0x2, 0x2, 0x1f9, 0x1f6, 0x3, 0x2, 0x2, 0x2, 0x1fa, 
    0x11, 0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1fc, 0x7, 0x11, 0x2, 0x2, 0x1fc, 
    0x1fd, 0x7, 0x2f, 0x2, 0x2, 0x1fd, 0x1ff, 0x5, 0xc0, 0x61, 0x2, 0x1fe, 
    0x200, 0x5, 0x2c, 0x17, 0x2, 0x1ff, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1ff, 
    0x200, 0x3, 0x2, 0x2, 0x2, 0x200, 0x13, 0x3, 0x2, 0x2, 0x2, 0x201, 0x202, 
    0x7, 0x17, 0x2, 0x2, 0x202, 0x203, 0x7, 0x9a, 0x2, 0x2, 0x203, 0x205, 
    0x5, 0xc0, 0x61, 0x2, 0x204, 0x206, 0x5, 0x10, 0x9, 0x2, 0x205, 0x204, 
    0x3, 0x2, 0x2, 0x2, 0x205, 0x206, 0x3, 0x2, 0x2, 0x2, 0x206, 0x15, 0x3, 
    0x2, 0x2, 0x2, 0x207, 0x208, 0x9, 0x3, 0x2, 0x2, 0x208, 0x20c, 0x7, 
    0x22, 0x2, 0x2, 0x209, 0x20a, 0x7, 0x4d, 0x2, 0x2, 0x20a, 0x20b, 0x7, 
    0x72, 0x2, 0x2, 0x20b, 0x20d, 0x7, 0x38, 0x2, 0x2, 0x20c, 0x209, 0x3, 
    0x2, 0x2, 0x2, 0x20c, 0x20d, 0x3, 0x2, 0x2, 0x2, 0x20d, 0x20e, 0x3, 
    0x2, 0x2, 0x2, 0x20e, 0x210, 0x5, 0xc6, 0x64, 0x2, 0x20f, 0x211, 0x5, 
    0x2c, 0x17, 0x2, 0x210, 0x20f, 0x3, 0x2, 0x2, 0x2, 0x210, 0x211, 0x3, 
    0x2, 0x2, 0x2, 0x211, 0x213, 0x3, 0x2, 0x2, 0x2, 0x212, 0x214, 0x5, 
    0x40, 0x21, 0x2, 0x213, 0x212, 0x3, 0x2, 0x2, 0x2, 0x213, 0x214, 0x3, 
    0x2, 0x2, 0x2, 0x214, 0x291, 0x3, 0x2, 0x2, 0x2, 0x215, 0x216, 0x9, 
    0x3, 0x2, 0x2, 0x216, 0x21a, 0x7, 0x2f, 0x2, 0x2, 0x217, 0x218, 0x7, 
    0x4d, 0x2, 0x2, 0x218, 0x219, 0x7, 0x72, 0x2, 0x2, 0x219, 0x21b, 0x7, 
    0x38, 0x2, 0x2, 0x21a, 0x217, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x21b, 0x3, 
    0x2, 0x2, 0x2, 0x21b, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x21c, 0x21e, 0x5, 
    0xc0, 0x61, 0x2, 0x21d, 0x21f, 0x5, 0x2e, 0x18, 0x2, 0x21e, 0x21d, 0x3, 
    0x2, 0x2, 0x2, 0x21e, 0x21f, 0x3, 0x2, 0x2, 0x2, 0x21f, 0x221, 0x3, 
    0x2, 0x2, 0x2, 0x220, 0x222, 0x5, 0x2c, 0x17, 0x2, 0x221, 0x220, 0x3, 
    0x2, 0x2, 0x2, 0x221, 0x222, 0x3, 0x2, 0x2, 0x2, 0x222, 0x223, 0x3, 
    0x2, 0x2, 0x2, 0x223, 0x224, 0x5, 0x18, 0xd, 0x2, 0x224, 0x225, 0x5, 
    0x1c, 0xf, 0x2, 0x225, 0x291, 0x3, 0x2, 0x2, 0x2, 0x226, 0x227, 0x9, 
    0x3, 0x2, 0x2, 0x227, 0x228, 0x7, 0x63, 0x2, 0x2, 0x228, 0x22c, 0x7, 
    0xb0, 0x2, 0x2, 0x229, 0x22a, 0x7, 0x4d, 0x2, 0x2, 0x22a, 0x22b, 0x7, 
    0x72, 0x2, 0x2, 0x22b, 0x22d, 0x7, 0x38, 0x2, 0x2, 0x22c, 0x229, 0x3, 
    0x2, 0x2, 0x2, 0x22c, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22e, 0x3, 
    0x2, 0x2, 0x2, 0x22e, 0x230, 0x5, 0xc0, 0x61, 0x2, 0x22f, 0x231, 0x5, 
    0x2e, 0x18, 0x2, 0x230, 0x22f, 0x3, 0x2, 0x2, 0x2, 0x230, 0x231, 0x3, 
    0x2, 0x2, 0x2, 0x231, 0x233, 0x3, 0x2, 0x2, 0x2, 0x232, 0x234, 0x5, 
    0x2c, 0x17, 0x2, 0x233, 0x232, 0x3, 0x2, 0x2, 0x2, 0x233, 0x234, 0x3, 
    0x2, 0x2, 0x2, 0x234, 0x23a, 0x3, 0x2, 0x2, 0x2, 0x235, 0x236, 0x7, 
    0xb6, 0x2, 0x2, 0x236, 0x238, 0x7, 0xa0, 0x2, 0x2, 0x237, 0x239, 0x7, 
    0xbd, 0x2, 0x2, 0x238, 0x237, 0x3, 0x2, 0x2, 0x2, 0x238, 0x239, 0x3, 
    0x2, 0x2, 0x2, 0x239, 0x23b, 0x3, 0x2, 0x2, 0x2, 0x23a, 0x235, 0x3, 
    0x2, 0x2, 0x2, 0x23a, 0x23b, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x23d, 0x3, 
    0x2, 0x2, 0x2, 0x23c, 0x23e, 0x5, 0x30, 0x19, 0x2, 0x23d, 0x23c, 0x3, 
    0x2, 0x2, 0x2, 0x23d, 0x23e, 0x3, 0x2, 0x2, 0x2, 0x23e, 0x240, 0x3, 
    0x2, 0x2, 0x2, 0x23f, 0x241, 0x5, 0x34, 0x1b, 0x2, 0x240, 0x23f, 0x3, 
    0x2, 0x2, 0x2, 0x240, 0x241, 0x3, 0x2, 0x2, 0x2, 0x241, 0x242, 0x3, 
    0x2, 0x2, 0x2, 0x242, 0x243, 0x5, 0x32, 0x1a, 0x2, 0x243, 0x291, 0x3, 
    0x2, 0x2, 0x2, 0x244, 0x245, 0x9, 0x3, 0x2, 0x2, 0x245, 0x246, 0x7, 
    0x67, 0x2, 0x2, 0x246, 0x24a, 0x7, 0xb0, 0x2, 0x2, 0x247, 0x248, 0x7, 
    0x4d, 0x2, 0x2, 0x248, 0x249, 0x7, 0x72, 0x2, 0x2, 0x249, 0x24b, 0x7, 
    0x38, 0x2, 0x2, 0x24a, 0x247, 0x3, 0x2, 0x2, 0x2, 0x24a, 0x24b, 0x3, 
    0x2, 0x2, 0x2, 0x24b, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24c, 0x24e, 0x5, 
    0xc0, 0x61, 0x2, 0x24d, 0x24f, 0x5, 0x2e, 0x18, 0x2, 0x24e, 0x24d, 0x3, 
    0x2, 0x2, 0x2, 0x24e, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x24f, 0x251, 0x3, 
    0x2, 0x2, 0x2, 0x250, 0x252, 0x5, 0x2c, 0x17, 0x2, 0x251, 0x250, 0x3, 
    0x2, 0x2, 0x2, 0x251, 0x252, 0x3, 0x2, 0x2, 0x2, 0x252, 0x254, 0x3, 
    0x2, 0x2, 0x2, 0x253, 0x255, 0x5, 0x34, 0x1b, 0x2, 0x254, 0x253, 0x3, 
    0x2, 0x2, 0x2, 0x254, 0x255, 0x3, 0x2, 0x2, 0x2, 0x255, 0x25b, 0x3, 
    0x2, 0x2, 0x2, 0x256, 0x25c, 0x5, 0x30, 0x19, 0x2, 0x257, 0x259, 0x5, 
    0x36, 0x1c, 0x2, 0x258, 0x25a, 0x7, 0x7d, 0x2, 0x2, 0x259, 0x258, 0x3, 
    0x2, 0x2, 0x2, 0x259, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x25a, 0x25c, 0x3, 
    0x2, 0x2, 0x2, 0x25b, 0x256, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x257, 0x3, 
    0x2, 0x2, 0x2, 0x25c, 0x25d, 0x3, 0x2, 0x2, 0x2, 0x25d, 0x25e, 0x5, 
    0x32, 0x1a, 0x2, 0x25e, 0x291, 0x3, 0x2, 0x2, 0x2, 0x25f, 0x261, 0x9, 
    0x3, 0x2, 0x2, 0x260, 0x262, 0x7, 0x9c, 0x2, 0x2, 0x261, 0x260, 0x3, 
    0x2, 0x2, 0x2, 0x261, 0x262, 0x3, 0x2, 0x2, 0x2, 0x262, 0x263, 0x3, 
    0x2, 0x2, 0x2, 0x263, 0x267, 0x7, 0x9a, 0x2, 0x2, 0x264, 0x265, 0x7, 
    0x4d, 0x2, 0x2, 0x265, 0x266, 0x7, 0x72, 0x2, 0x2, 0x266, 0x268, 0x7, 
    0x38, 0x2, 0x2, 0x267, 0x264, 0x3, 0x2, 0x2, 0x2, 0x267, 0x268, 0x3, 
    0x2, 0x2, 0x2, 0x268, 0x269, 0x3, 0x2, 0x2, 0x2, 0x269, 0x26b, 0x5, 
    0xc0, 0x61, 0x2, 0x26a, 0x26c, 0x5, 0x2e, 0x18, 0x2, 0x26b, 0x26a, 0x3, 
    0x2, 0x2, 0x2, 0x26b, 0x26c, 0x3, 0x2, 0x2, 0x2, 0x26c, 0x26e, 0x3, 
    0x2, 0x2, 0x2, 0x26d, 0x26f, 0x5, 0x2c, 0x17, 0x2, 0x26e, 0x26d, 0x3, 
    0x2, 0x2, 0x2, 0x26e, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x271, 0x3, 
    0x2, 0x2, 0x2, 0x270, 0x272, 0x5, 0x34, 0x1b, 0x2, 0x271, 0x270, 0x3, 
    0x2, 0x2, 0x2, 0x271, 0x272, 0x3, 0x2, 0x2, 0x2, 0x272, 0x274, 0x3, 
    0x2, 0x2, 0x2, 0x273, 0x275, 0x5, 0x36, 0x1c, 0x2, 0x274, 0x273, 0x3, 
    0x2, 0x2, 0x2, 0x274, 0x275, 0x3, 0x2, 0x2, 0x2, 0x275, 0x277, 0x3, 
    0x2, 0x2, 0x2, 0x276, 0x278, 0x5, 0x32, 0x1a, 0x2, 0x277, 0x276, 0x3, 
    0x2, 0x2, 0x2, 0x277, 0x278, 0x3, 0x2, 0x2, 0x2, 0x278, 0x291, 0x3, 
    0x2, 0x2, 0x2, 0x279, 0x27c, 0x9, 0x3, 0x2, 0x2, 0x27a, 0x27b, 0x7, 
    0x78, 0x2, 0x2, 0x27b, 0x27d, 0x7, 0x86, 0x2, 0x2, 0x27c, 0x27a, 0x3, 
    0x2, 0x2, 0x2, 0x27c, 0x27d, 0x3, 0x2, 0x2, 0x2, 0x27d, 0x27e, 0x3, 
    0x2, 0x2, 0x2, 0x27e, 0x282, 0x7, 0xb0, 0x2, 0x2, 0x27f, 0x280, 0x7, 
    0x4d, 0x2, 0x2, 0x280, 0x281, 0x7, 0x72, 0x2, 0x2, 0x281, 0x283, 0x7, 
    0x38, 0x2, 0x2, 0x282, 0x27f, 0x3, 0x2, 0x2, 0x2, 0x282, 0x283, 0x3, 
    0x2, 0x2, 0x2, 0x283, 0x284, 0x3, 0x2, 0x2, 0x2, 0x284, 0x286, 0x5, 
    0xc0, 0x61, 0x2, 0x285, 0x287, 0x5, 0x2e, 0x18, 0x2, 0x286, 0x285, 0x3, 
    0x2, 0x2, 0x2, 0x286, 0x287, 0x3, 0x2, 0x2, 0x2, 0x287, 0x289, 0x3, 
    0x2, 0x2, 0x2, 0x288, 0x28a, 0x5, 0x2c, 0x17, 0x2, 0x289, 0x288, 0x3, 
    0x2, 0x2, 0x2, 0x289, 0x28a, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x28c, 0x3, 
    0x2, 0x2, 0x2, 0x28b, 0x28d, 0x5, 0x34, 0x1b, 0x2, 0x28c, 0x28b, 0x3, 
    0x2, 0x2, 0x2, 0x28c, 0x28d, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x28e, 0x3, 
    0x2, 0x2, 0x2, 0x28e, 0x28f, 0x5, 0x32, 0x1a, 0x2, 0x28f, 0x291, 0x3, 
    0x2, 0x2, 0x2, 0x290, 0x207, 0x3, 0x2, 0x2, 0x2, 0x290, 0x215, 0x3, 
    0x2, 0x2, 0x2, 0x290, 0x226, 0x3, 0x2, 0x2, 0x2, 0x290, 0x244, 0x3, 
    0x2, 0x2, 0x2, 0x290, 0x25f, 0x3, 0x2, 0x2, 0x2, 0x290, 0x279, 0x3, 
    0x2, 0x2, 0x2, 0x291, 0x17, 0x3, 0x2, 0x2, 0x2, 0x292, 0x293, 0x7, 0xd0, 
    0x2, 0x2, 0x293, 0x298, 0x5, 0x1a, 0xe, 0x2, 0x294, 0x295, 0x7, 0xc5, 
    0x2, 0x2, 0x295, 0x297, 0x5, 0x1a, 0xe, 0x2, 0x296, 0x294, 0x3, 0x2, 
    0x2, 0x2, 0x297, 0x29a, 0x3, 0x2, 0x2, 0x2, 0x298, 0x296, 0x3, 0x2, 
    0x2, 0x2, 0x298, 0x299, 0x3, 0x2, 0x2, 0x2, 0x299, 0x29b, 0x3, 0x2, 
    0x2, 0x2, 0x29a, 0x298, 0x3, 0x2, 0x2, 0x2, 0x29b, 0x29c, 0x7, 0xda, 
    0x2, 0x2, 0x29c, 0x19, 0x3, 0x2, 0x2, 0x2, 0x29d, 0x29e, 0x5, 0xd6, 
    0x6c, 0x2, 0x29e, 0x2b4, 0x5, 0xaa, 0x56, 0x2, 0x29f, 0x2a0, 0x6, 0xe, 
    0x2, 0x3, 0x2a0, 0x2a1, 0x7, 0x27, 0x2, 0x2, 0x2a1, 0x2a2, 0x5, 0xcc, 
    0x67, 0x2, 0x2a2, 0x2a3, 0x8, 0xe, 0x1, 0x2, 0x2a3, 0x2b3, 0x3, 0x2, 
    0x2, 0x2, 0x2a4, 0x2a5, 0x6, 0xe, 0x3, 0x3, 0x2a5, 0x2a6, 0x7, 0x3a, 
    0x2, 0x2, 0x2a6, 0x2a7, 0x5, 0xb0, 0x59, 0x2, 0x2a7, 0x2a8, 0x8, 0xe, 
    0x1, 0x2, 0x2a8, 0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2aa, 0x6, 0xe, 
    0x4, 0x3, 0x2aa, 0x2ab, 0x7, 0x4a, 0x2, 0x2, 0x2ab, 0x2b3, 0x8, 0xe, 
    0x1, 0x2, 0x2ac, 0x2ad, 0x6, 0xe, 0x5, 0x3, 0x2ad, 0x2ae, 0x7, 0x52, 
    0x2, 0x2, 0x2ae, 0x2b3, 0x8, 0xe, 0x1, 0x2, 0x2af, 0x2b0, 0x6, 0xe, 
    0x6, 0x3, 0x2b0, 0x2b1, 0x7, 0x58, 0x2, 0x2, 0x2b1, 0x2b3, 0x8, 0xe, 
    0x1, 0x2, 0x2b2, 0x29f, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x2a4, 0x3, 0x2, 
    0x2, 0x2, 0x2b2, 0x2a9, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x2ac, 0x3, 0x2, 
    0x2, 0x2, 0x2b2, 0x2af, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2b6, 0x3, 0x2, 
    0x2, 0x2, 0x2b4, 0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b4, 0x2b5, 0x3, 0x2, 
    0x2, 0x2, 0x2b5, 0x1b, 0x3, 0x2, 0x2, 0x2, 0x2b6, 0x2b4, 0x3, 0x2, 0x2, 
    0x2, 0x2b7, 0x2b9, 0x5, 0x1e, 0x10, 0x2, 0x2b8, 0x2b7, 0x3, 0x2, 0x2, 
    0x2, 0x2b8, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2b9, 0x2d0, 0x3, 0x2, 0x2, 
    0x2, 0x2ba, 0x2bb, 0x6, 0xf, 0x7, 0x3, 0x2bb, 0x2bc, 0x5, 0x22, 0x12, 
    0x2, 0x2bc, 0x2bd, 0x8, 0xf, 0x1, 0x2, 0x2bd, 0x2cf, 0x3, 0x2, 0x2, 
    0x2, 0x2be, 0x2bf, 0x6, 0xf, 0x8, 0x3, 0x2bf, 0x2c0, 0x5, 0x24, 0x13, 
    0x2, 0x2c0, 0x2c1, 0x8, 0xf, 0x1, 0x2, 0x2c1, 0x2cf, 0x3, 0x2, 0x2, 
    0x2, 0x2c2, 0x2c3, 0x6, 0xf, 0x9, 0x3, 0x2c3, 0x2c4, 0x5, 0x26, 0x14, 
    0x2, 0x2c4, 0x2c5, 0x8, 0xf, 0x1, 0x2, 0x2c5, 0x2cf, 0x3, 0x2, 0x2, 
    0x2, 0x2c6, 0x2c7, 0x6, 0xf, 0xa, 0x3, 0x2c7, 0x2c8, 0x5, 0x28, 0x15, 
    0x2, 0x2c8, 0x2c9, 0x8, 0xf, 0x1, 0x2, 0x2c9, 0x2cf, 0x3, 0x2, 0x2, 
    0x2, 0x2ca, 0x2cb, 0x6, 0xf, 0xb, 0x3, 0x2cb, 0x2cc, 0x5, 0x2a, 0x16, 
    0x2, 0x2cc, 0x2cd, 0x8, 0xf, 0x1, 0x2, 0x2cd, 0x2cf, 0x3, 0x2, 0x2, 
    0x2, 0x2ce, 0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2be, 0x3, 0x2, 0x2, 
    0x2, 0x2ce, 0x2c2, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2c6, 0x3, 0x2, 0x2, 
    0x2, 0x2ce, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2cf, 0x2d2, 0x3, 0x2, 0x2, 
    0x2, 0x2d0, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2d1, 0x3, 0x2, 0x2, 
    0x2, 0x2d1, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x2d2, 0x2d0, 0x3, 0x2, 0x2, 0x2, 
    0x2d3, 0x2d4, 0x7, 0x7f, 0x2, 0x2, 0x2d4, 0x2d5, 0x7, 0x5a, 0x2, 0x2, 
    0x2d5, 0x2d6, 0x5, 0xac, 0x57, 0x2, 0x2d6, 0x1f, 0x3, 0x2, 0x2, 0x2, 
    0x2d7, 0x2de, 0x5, 0xd6, 0x6c, 0x2, 0x2d8, 0x2db, 0x5, 0xd6, 0x6c, 0x2, 
    0x2d9, 0x2da, 0x7, 0xd0, 0x2, 0x2, 0x2da, 0x2dc, 0x7, 0xda, 0x2, 0x2, 
    0x2db, 0x2d9, 0x3, 0x2, 0x2, 0x2, 0x2db, 0x2dc, 0x3, 0x2, 0x2, 0x2, 
    0x2dc, 0x2df, 0x3, 0x2, 0x2, 0x2, 0x2dd, 0x2df, 0x5, 0xcc, 0x67, 0x2, 
    0x2de, 0x2d8, 0x3, 0x2, 0x2, 0x2, 0x2de, 0x2dd, 0x3, 0x2, 0x2, 0x2, 
    0x2df, 0x21, 0x3, 0x2, 0x2, 0x2, 0x2e0, 0x2e1, 0x7, 0x93, 0x2, 0x2, 
    0x2e1, 0x2e2, 0x7, 0xd0, 0x2, 0x2, 0x2e2, 0x2e3, 0x5, 0xd6, 0x6c, 0x2, 
    0x2e3, 0x2e7, 0x7, 0xd0, 0x2, 0x2, 0x2e4, 0x2e6, 0x5, 0x20, 0x11, 0x2, 
    0x2e5, 0x2e4, 0x3, 0x2, 0x2, 0x2, 0x2e6, 0x2e9, 0x3, 0x2, 0x2, 0x2, 
    0x2e7, 0x2e5, 0x3, 0x2, 0x2, 0x2, 0x2e7, 0x2e8, 0x3, 0x2, 0x2, 0x2, 
    0x2e8, 0x2ea, 0x3, 0x2, 0x2, 0x2, 0x2e9, 0x2e7, 0x3, 0x2, 0x2, 0x2, 
    0x2ea, 0x2eb, 0x7, 0xda, 0x2, 0x2, 0x2eb, 0x2ec, 0x7, 0xda, 0x2, 0x2, 
    0x2ec, 0x23, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2ee, 0x7, 0x60, 0x2, 0x2, 
    0x2ee, 0x2f8, 0x7, 0xd0, 0x2, 0x2, 0x2ef, 0x2f9, 0x7, 0xbd, 0x2, 0x2, 
    0x2f0, 0x2f1, 0x7, 0x6a, 0x2, 0x2, 0x2f1, 0x2f2, 0x7, 0xbd, 0x2, 0x2, 
    0x2f2, 0x2f3, 0x7, 0x68, 0x2, 0x2, 0x2f3, 0x2f9, 0x7, 0xbd, 0x2, 0x2, 
    0x2f4, 0x2f5, 0x7, 0x68, 0x2, 0x2, 0x2f5, 0x2f6, 0x7, 0xbd, 0x2, 0x2, 
    0x2f6, 0x2f7, 0x7, 0x6a, 0x2, 0x2, 0x2f7, 0x2f9, 0x7, 0xbd, 0x2, 0x2, 
    0x2f8, 0x2ef, 0x3, 0x2, 0x2, 0x2, 0x2f8, 0x2f0, 0x3, 0x2, 0x2, 0x2, 
    0x2f8, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x2f9, 0x2fa, 0x3, 0x2, 0x2, 0x2, 
    0x2fa, 0x2fb, 0x7, 0xda, 0x2, 0x2, 0x2fb, 0x25, 0x3, 0x2, 0x2, 0x2, 
    0x2fc, 0x2fd, 0x7, 0x5d, 0x2, 0x2, 0x2fd, 0x2fe, 0x7, 0xd0, 0x2, 0x2, 
    0x2fe, 0x2ff, 0x5, 0xd6, 0x6c, 0x2, 0x2ff, 0x303, 0x7, 0xd0, 0x2, 0x2, 
    0x300, 0x302, 0x5, 0x20, 0x11, 0x2, 0x301, 0x300, 0x3, 0x2, 0x2, 0x2, 
    0x302, 0x305, 0x3, 0x2, 0x2, 0x2, 0x303, 0x301, 0x3, 0x2, 0x2, 0x2, 
    0x303, 0x304, 0x3, 0x2, 0x2, 0x2, 0x304, 0x306, 0x3, 0x2, 0x2, 0x2, 
    0x305, 0x303, 0x3, 0x2, 0x2, 0x2, 0x306, 0x307, 0x7, 0xda, 0x2, 0x2, 
    0x307, 0x308, 0x7, 0xda, 0x2, 0x2, 0x308, 0x27, 0x3, 0x2, 0x2, 0x2, 
    0x309, 0x30a, 0x7, 0x82, 0x2, 0x2, 0x30a, 0x315, 0x7, 0xd0, 0x2, 0x2, 
    0x30b, 0x30c, 0x7, 0x6a, 0x2, 0x2, 0x30c, 0x30d, 0x5, 0xd6, 0x6c, 0x2, 
    0x30d, 0x30e, 0x7, 0x68, 0x2, 0x2, 0x30e, 0x30f, 0x5, 0xd6, 0x6c, 0x2, 
    0x30f, 0x316, 0x3, 0x2, 0x2, 0x2, 0x310, 0x311, 0x7, 0x68, 0x2, 0x2, 
    0x311, 0x312, 0x5, 0xd6, 0x6c, 0x2, 0x312, 0x313, 0x7, 0x6a, 0x2, 0x2, 
    0x313, 0x314, 0x5, 0xd6, 0x6c, 0x2, 0x314, 0x316, 0x3, 0x2, 0x2, 0x2, 
    0x315, 0x30b, 0x3, 0x2, 0x2, 0x2, 0x315, 0x310, 0x3, 0x2, 0x2, 0x2, 
    0x316, 0x317, 0x3, 0x2, 0x2, 0x2, 0x317, 0x318, 0x7, 0xda, 0x2, 0x2, 
    0x318, 0x29, 0x3, 0x2, 0x2, 0x2, 0x319, 0x31a, 0x7, 0x91, 0x2, 0x2, 
    0x31a, 0x31b, 0x7, 0xd0, 0x2, 0x2, 0x31b, 0x31c, 0x5, 0x9a, 0x4e, 0x2, 
    0x31c, 0x31d, 0x7, 0xda, 0x2, 0x2, 0x31d, 0x2b, 0x3, 0x2, 0x2, 0x2, 
    0x31e, 0x31f, 0x7, 0x76, 0x2, 0x2, 0x31f, 0x322, 0x7, 0x19, 0x2, 0x2, 
    0x320, 0x323, 0x5, 0xd6, 0x6c, 0x2, 0x321, 0x323, 0x7, 0xbf, 0x2, 0x2, 
    0x322, 0x320, 0x3, 0x2, 0x2, 0x2, 0x322, 0x321, 0x3, 0x2, 0x2, 0x2, 
    0x323, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x324, 0x325, 0x7, 0xae, 0x2, 0x2, 
    0x325, 0x326, 0x7, 0xbf, 0x2, 0x2, 0x326, 0x2f, 0x3, 0x2, 0x2, 0x2, 
    0x327, 0x328, 0x7, 0xa2, 0x2, 0x2, 0x328, 0x329, 0x5, 0xc0, 0x61, 0x2, 
    0x329, 0x31, 0x3, 0x2, 0x2, 0x2, 0x32a, 0x32b, 0x7, 0xc, 0x2, 0x2, 0x32b, 
    0x32c, 0x5, 0x68, 0x35, 0x2, 0x32c, 0x33, 0x3, 0x2, 0x2, 0x2, 0x32d, 
    0x32e, 0x7, 0xd0, 0x2, 0x2, 0x32e, 0x333, 0x5, 0x42, 0x22, 0x2, 0x32f, 
    0x330, 0x7, 0xc5, 0x2, 0x2, 0x330, 0x332, 0x5, 0x42, 0x22, 0x2, 0x331, 
    0x32f, 0x3, 0x2, 0x2, 0x2, 0x332, 0x335, 0x3, 0x2, 0x2, 0x2, 0x333, 
    0x331, 0x3, 0x2, 0x2, 0x2, 0x333, 0x334, 0x3, 0x2, 0x2, 0x2, 0x334, 
    0x336, 0x3, 0x2, 0x2, 0x2, 0x335, 0x333, 0x3, 0x2, 0x2, 0x2, 0x336, 
    0x337, 0x7, 0xda, 0x2, 0x2, 0x337, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x338, 
    0x339, 0x7, 0xc, 0x2, 0x2, 0x339, 0x33d, 0x5, 0xc0, 0x61, 0x2, 0x33a, 
    0x33b, 0x7, 0xc, 0x2, 0x2, 0x33b, 0x33d, 0x5, 0xbe, 0x60, 0x2, 0x33c, 
    0x32d, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x338, 0x3, 0x2, 0x2, 0x2, 0x33c, 
    0x33a, 0x3, 0x2, 0x2, 0x2, 0x33d, 0x35, 0x3, 0x2, 0x2, 0x2, 0x33e, 0x359, 
    0x5, 0x40, 0x21, 0x2, 0x33f, 0x340, 0x6, 0x1c, 0xc, 0x3, 0x340, 0x341, 
    0x5, 0x7e, 0x40, 0x2, 0x341, 0x342, 0x8, 0x1c, 0x1, 0x2, 0x342, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x343, 0x344, 0x6, 0x1c, 0xd, 0x3, 0x344, 0x345, 
    0x5, 0x38, 0x1d, 0x2, 0x345, 0x346, 0x8, 0x1c, 0x1, 0x2, 0x346, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x347, 0x348, 0x6, 0x1c, 0xe, 0x3, 0x348, 0x349, 
    0x5, 0x3a, 0x1e, 0x2, 0x349, 0x34a, 0x8, 0x1c, 0x1, 0x2, 0x34a, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x34b, 0x34c, 0x6, 0x1c, 0xf, 0x3, 0x34c, 0x34d, 
    0x5, 0x3c, 0x1f, 0x2, 0x34d, 0x34e, 0x8, 0x1c, 0x1, 0x2, 0x34e, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x34f, 0x350, 0x6, 0x1c, 0x10, 0x3, 0x350, 0x351, 
    0x5, 0x3e, 0x20, 0x2, 0x351, 0x352, 0x8, 0x1c, 0x1, 0x2, 0x352, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x353, 0x354, 0x6, 0x1c, 0x11, 0x3, 0x354, 0x355, 
    0x5, 0x86, 0x44, 0x2, 0x355, 0x356, 0x8, 0x1c, 0x1, 0x2, 0x356, 0x358, 
    0x3, 0x2, 0x2, 0x2, 0x357, 0x33f, 0x3, 0x2, 0x2, 0x2, 0x357, 0x343, 
    0x3, 0x2, 0x2, 0x2, 0x357, 0x347, 0x3, 0x2, 0x2, 0x2, 0x357, 0x34b, 
    0x3, 0x2, 0x2, 0x2, 0x357, 0x34f, 0x3, 0x2, 0x2, 0x2, 0x357, 0x353, 
    0x3, 0x2, 0x2, 0x2, 0x358, 0x35b, 0x3, 0x2, 0x2, 0x2, 0x359, 0x357, 
    0x3, 0x2, 0x2, 0x2, 0x359, 0x35a, 0x3, 0x2, 0x2, 0x2, 0x35a, 0x37, 0x3, 
    0x2, 0x2, 0x2, 0x35b, 0x359, 0x3, 0x2, 0x2, 0x2, 0x35c, 0x35d, 0x7, 
    0x7c, 0x2, 0x2, 0x35d, 0x35e, 0x7, 0x14, 0x2, 0x2, 0x35e, 0x35f, 0x5, 
    0xb0, 0x59, 0x2, 0x35f, 0x39, 0x3, 0x2, 0x2, 0x2, 0x360, 0x361, 0x7, 
    0x7f, 0x2, 0x2, 0x361, 0x362, 0x7, 0x5a, 0x2, 0x2, 0x362, 0x363, 0x5, 
    0xb0, 0x59, 0x2, 0x363, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x364, 0x365, 0x7, 
    0x8b, 0x2, 0x2, 0x365, 0x366, 0x7, 0x14, 0x2, 0x2, 0x366, 0x367, 0x5, 
    0xb0, 0x59, 0x2, 0x367, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x368, 0x369, 0x7, 
    0xa8, 0x2, 0x2, 0x369, 0x36e, 0x5, 0x50, 0x29, 0x2, 0x36a, 0x36b, 0x7, 
    0xc5, 0x2, 0x2, 0x36b, 0x36d, 0x5, 0x50, 0x29, 0x2, 0x36c, 0x36a, 0x3, 
    0x2, 0x2, 0x2, 0x36d, 0x370, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x36c, 0x3, 
    0x2, 0x2, 0x2, 0x36e, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x36f, 0x3f, 0x3, 0x2, 
    0x2, 0x2, 0x370, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x371, 0x373, 0x7, 0x36, 
    0x2, 0x2, 0x372, 0x374, 0x7, 0xca, 0x2, 0x2, 0x373, 0x372, 0x3, 0x2, 
    0x2, 0x2, 0x373, 0x374, 0x3, 0x2, 0x2, 0x2, 0x374, 0x375, 0x3, 0x2, 
    0x2, 0x2, 0x375, 0x37b, 0x5, 0xd8, 0x6d, 0x2, 0x376, 0x378, 0x7, 0xd0, 
    0x2, 0x2, 0x377, 0x379, 0x5, 0xac, 0x57, 0x2, 0x378, 0x377, 0x3, 0x2, 
    0x2, 0x2, 0x378, 0x379, 0x3, 0x2, 0x2, 0x2, 0x379, 0x37a, 0x3, 0x2, 
    0x2, 0x2, 0x37a, 0x37c, 0x7, 0xda, 0x2, 0x2, 0x37b, 0x376, 0x3, 0x2, 
    0x2, 0x2, 0x37b, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x37c, 0x41, 0x3, 0x2, 0x2, 
    0x2, 0x37d, 0x388, 0x5, 0x44, 0x23, 0x2, 0x37e, 0x37f, 0x7, 0x1e, 0x2, 
    0x2, 0x37f, 0x380, 0x5, 0xd6, 0x6c, 0x2, 0x380, 0x381, 0x7, 0x17, 0x2, 
    0x2, 0x381, 0x382, 0x5, 0xb0, 0x59, 0x2, 0x382, 0x388, 0x3, 0x2, 0x2, 
    0x2, 0x383, 0x384, 0x7, 0x50, 0x2, 0x2, 0x384, 0x388, 0x5, 0x48, 0x25, 
    0x2, 0x385, 0x386, 0x7, 0x80, 0x2, 0x2, 0x386, 0x388, 0x5, 0x4a, 0x26, 
    0x2, 0x387, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x387, 0x37e, 0x3, 0x2, 0x2, 
    0x2, 0x387, 0x383, 0x3, 0x2, 0x2, 0x2, 0x387, 0x385, 0x3, 0x2, 0x2, 
    0x2, 0x388, 0x43, 0x3, 0x2, 0x2, 0x2, 0x389, 0x38a, 0x5, 0xba, 0x5e, 
    0x2, 0x38a, 0x38c, 0x5, 0xaa, 0x56, 0x2, 0x38b, 0x38d, 0x5, 0x46, 0x24, 
    0x2, 0x38c, 0x38b, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38d, 0x3, 0x2, 0x2, 
    0x2, 0x38d, 0x390, 0x3, 0x2, 0x2, 0x2, 0x38e, 0x38f, 0x7, 0x1d, 0x2, 
    0x2, 0x38f, 0x391, 0x7, 0xbf, 0x2, 0x2, 0x390, 0x38e, 0x3, 0x2, 0x2, 
    0x2, 0x390, 0x391, 0x3, 0x2, 0x2, 0x2, 0x391, 0x393, 0x3, 0x2, 0x2, 
    0x2, 0x392, 0x394, 0x5, 0x4c, 0x27, 0x2, 0x393, 0x392, 0x3, 0x2, 0x2, 
    0x2, 0x393, 0x394, 0x3, 0x2, 0x2, 0x2, 0x394, 0x397, 0x3, 0x2, 0x2, 
    0x2, 0x395, 0x396, 0x7, 0xa8, 0x2, 0x2, 0x396, 0x398, 0x5, 0xb0, 0x59, 
    0x2, 0x397, 0x395, 0x3, 0x2, 0x2, 0x2, 0x397, 0x398, 0x3, 0x2, 0x2, 
    0x2, 0x398, 0x3aa, 0x3, 0x2, 0x2, 0x2, 0x399, 0x39b, 0x5, 0xba, 0x5e, 
    0x2, 0x39a, 0x39c, 0x5, 0xaa, 0x56, 0x2, 0x39b, 0x39a, 0x3, 0x2, 0x2, 
    0x2, 0x39b, 0x39c, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x39d, 0x3, 0x2, 0x2, 
    0x2, 0x39d, 0x3a0, 0x5, 0x46, 0x24, 0x2, 0x39e, 0x39f, 0x7, 0x1d, 0x2, 
    0x2, 0x39f, 0x3a1, 0x7, 0xbf, 0x2, 0x2, 0x3a0, 0x39e, 0x3, 0x2, 0x2, 
    0x2, 0x3a0, 0x3a1, 0x3, 0x2, 0x2, 0x2, 0x3a1, 0x3a3, 0x3, 0x2, 0x2, 
    0x2, 0x3a2, 0x3a4, 0x5, 0x4c, 0x27, 0x2, 0x3a3, 0x3a2, 0x3, 0x2, 0x2, 
    0x2, 0x3a3, 0x3a4, 0x3, 0x2, 0x2, 0x2, 0x3a4, 0x3a7, 0x3, 0x2, 0x2, 
    0x2, 0x3a5, 0x3a6, 0x7, 0xa8, 0x2, 0x2, 0x3a6, 0x3a8, 0x5, 0xb0, 0x59, 
    0x2, 0x3a7, 0x3a5, 0x3, 0x2, 0x2, 0x2, 0x3a7, 0x3a8, 0x3, 0x2, 0x2, 
    0x2, 0x3a8, 0x3aa, 0x3, 0x2, 0x2, 0x2, 0x3a9, 0x389, 0x3, 0x2, 0x2, 
    0x2, 0x3a9, 0x399, 0x3, 0x2, 0x2, 0x2, 0x3aa, 0x45, 0x3, 0x2, 0x2, 0x2, 
    0x3ab, 0x3ac, 0x9, 0x4, 0x2, 0x2, 0x3ac, 0x3ad, 0x5, 0xb0, 0x59, 0x2, 
    0x3ad, 0x47, 0x3, 0x2, 0x2, 0x2, 0x3ae, 0x3af, 0x5, 0xba, 0x5e, 0x2, 
    0x3af, 0x3b0, 0x5, 0xb0, 0x59, 0x2, 0x3b0, 0x3b1, 0x7, 0xa9, 0x2, 0x2, 
    0x3b1, 0x3b2, 0x5, 0xaa, 0x56, 0x2, 0x3b2, 0x3b3, 0x7, 0x47, 0x2, 0x2, 
    0x3b3, 0x3b4, 0x7, 0xbd, 0x2, 0x2, 0x3b4, 0x49, 0x3, 0x2, 0x2, 0x2, 
    0x3b5, 0x3b6, 0x5, 0xba, 0x5e, 0x2, 0x3b6, 0x3b7, 0x5, 0x66, 0x34, 0x2, 
    0x3b7, 0x4b, 0x3, 0x2, 0x2, 0x2, 0x3b8, 0x3b9, 0x7, 0x1a, 0x2, 0x2, 
    0x3b9, 0x3ba, 0x7, 0xd0, 0x2, 0x2, 0x3ba, 0x3bf, 0x5, 0x4e, 0x28, 0x2, 
    0x3bb, 0x3bc, 0x7, 0xc5, 0x2, 0x2, 0x3bc, 0x3be, 0x5, 0x4e, 0x28, 0x2, 
    0x3bd, 0x3bb, 0x3, 0x2, 0x2, 0x2, 0x3be, 0x3c1, 0x3, 0x2, 0x2, 0x2, 
    0x3bf, 0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3bf, 0x3c0, 0x3, 0x2, 0x2, 0x2, 
    0x3c0, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3bf, 0x3, 0x2, 0x2, 0x2, 
    0x3c2, 0x3c3, 0x7, 0xda, 0x2, 0x2, 0x3c3, 0x4d, 0x3, 0x2, 0x2, 0x2, 
    0x3c4, 0x3ca, 0x5, 0xd6, 0x6c, 0x2, 0x3c5, 0x3c7, 0x7, 0xd0, 0x2, 0x2, 
    0x3c6, 0x3c8, 0x5, 0xac, 0x57, 0x2, 0x3c7, 0x3c6, 0x3, 0x2, 0x2, 0x2, 
    0x3c7, 0x3c8, 0x3, 0x2, 0x2, 0x2, 0x3c8, 0x3c9, 0x3, 0x2, 0x2, 0x2, 
    0x3c9, 0x3cb, 0x7, 0xda, 0x2, 0x2, 0x3ca, 0x3c5, 0x3, 0x2, 0x2, 0x2, 
    0x3ca, 0x3cb, 0x3, 0x2, 0x2, 0x2, 0x3cb, 0x4f, 0x3, 0x2, 0x2, 0x2, 0x3cc, 
    0x3d4, 0x5, 0xb0, 0x59, 0x2, 0x3cd, 0x3d5, 0x7, 0x29, 0x2, 0x2, 0x3ce, 
    0x3cf, 0x7, 0xa2, 0x2, 0x2, 0x3cf, 0x3d0, 0x7, 0x30, 0x2, 0x2, 0x3d0, 
    0x3d5, 0x7, 0xbf, 0x2, 0x2, 0x3d1, 0x3d2, 0x7, 0xa2, 0x2, 0x2, 0x3d2, 
    0x3d3, 0x7, 0xb1, 0x2, 0x2, 0x3d3, 0x3d5, 0x7, 0xbf, 0x2, 0x2, 0x3d4, 
    0x3cd, 0x3, 0x2, 0x2, 0x2, 0x3d4, 0x3ce, 0x3, 0x2, 0x2, 0x2, 0x3d4, 
    0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3d4, 0x3d5, 0x3, 0x2, 0x2, 0x2, 0x3d5, 
    0x51, 0x3, 0x2, 0x2, 0x2, 0x3d6, 0x3d8, 0x9, 0x5, 0x2, 0x2, 0x3d7, 0x3d9, 
    0x7, 0x9a, 0x2, 0x2, 0x3d8, 0x3d7, 0x3, 0x2, 0x2, 0x2, 0x3d8, 0x3d9, 
    0x3, 0x2, 0x2, 0x2, 0x3d9, 0x3da, 0x3, 0x2, 0x2, 0x2, 0x3da, 0x3db, 
    0x5, 0xbc, 0x5f, 0x2, 0x3db, 0x53, 0x3, 0x2, 0x2, 0x2, 0x3dc, 0x3dd, 
    0x9, 0x6, 0x2, 0x2, 0x3dd, 0x3e0, 0x7, 0x22, 0x2, 0x2, 0x3de, 0x3df, 
    0x7, 0x4d, 0x2, 0x2, 0x3df, 0x3e1, 0x7, 0x38, 0x2, 0x2, 0x3e0, 0x3de, 
    0x3, 0x2, 0x2, 0x2, 0x3e0, 0x3e1, 0x3, 0x2, 0x2, 0x2, 0x3e1, 0x3e2, 
    0x3, 0x2, 0x2, 0x2, 0x3e2, 0x3e4, 0x5, 0xc6, 0x64, 0x2, 0x3e3, 0x3e5, 
    0x5, 0x2c, 0x17, 0x2, 0x3e4, 0x3e3, 0x3, 0x2, 0x2, 0x2, 0x3e4, 0x3e5, 
    0x3, 0x2, 0x2, 0x2, 0x3e5, 0x3fc, 0x3, 0x2, 0x2, 0x2, 0x3e6, 0x3ed, 
    0x9, 0x6, 0x2, 0x2, 0x3e7, 0x3ee, 0x7, 0x2f, 0x2, 0x2, 0x3e8, 0x3ea, 
    0x7, 0x9c, 0x2, 0x2, 0x3e9, 0x3e8, 0x3, 0x2, 0x2, 0x2, 0x3e9, 0x3ea, 
    0x3, 0x2, 0x2, 0x2, 0x3ea, 0x3eb, 0x3, 0x2, 0x2, 0x2, 0x3eb, 0x3ee, 
    0x7, 0x9a, 0x2, 0x2, 0x3ec, 0x3ee, 0x7, 0xb0, 0x2, 0x2, 0x3ed, 0x3e7, 
    0x3, 0x2, 0x2, 0x2, 0x3ed, 0x3e9, 0x3, 0x2, 0x2, 0x2, 0x3ed, 0x3ec, 
    0x3, 0x2, 0x2, 0x2, 0x3ee, 0x3f1, 0x3, 0x2, 0x2, 0x2, 0x3ef, 0x3f0, 
    0x7, 0x4d, 0x2, 0x2, 0x3f0, 0x3f2, 0x7, 0x38, 0x2, 0x2, 0x3f1, 0x3ef, 
    0x3, 0x2, 0x2, 0x2, 0x3f1, 0x3f2, 0x3, 0x2, 0x2, 0x2, 0x3f2, 0x3f3, 
    0x3, 0x2, 0x2, 0x2, 0x3f3, 0x3f5, 0x5, 0xc0, 0x61, 0x2, 0x3f4, 0x3f6, 
    0x5, 0x2c, 0x17, 0x2, 0x3f5, 0x3f4, 0x3, 0x2, 0x2, 0x2, 0x3f5, 0x3f6, 
    0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3f9, 0x3, 0x2, 0x2, 0x2, 0x3f7, 0x3f8, 
    0x7, 0x71, 0x2, 0x2, 0x3f8, 0x3fa, 0x7, 0x28, 0x2, 0x2, 0x3f9, 0x3f7, 
    0x3, 0x2, 0x2, 0x2, 0x3f9, 0x3fa, 0x3, 0x2, 0x2, 0x2, 0x3fa, 0x3fc, 
    0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3dc, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3e6, 
    0x3, 0x2, 0x2, 0x2, 0x3fc, 0x55, 0x3, 0x2, 0x2, 0x2, 0x3fd, 0x3fe, 0x7, 
    0x38, 0x2, 0x2, 0x3fe, 0x3ff, 0x7, 0x22, 0x2, 0x2, 0x3ff, 0x40b, 0x5, 
    0xc6, 0x64, 0x2, 0x400, 0x407, 0x7, 0x38, 0x2, 0x2, 0x401, 0x408, 0x7, 
    0x2f, 0x2, 0x2, 0x402, 0x404, 0x7, 0x9c, 0x2, 0x2, 0x403, 0x402, 0x3, 
    0x2, 0x2, 0x2, 0x403, 0x404, 0x3, 0x2, 0x2, 0x2, 0x404, 0x405, 0x3, 
    0x2, 0x2, 0x2, 0x405, 0x408, 0x7, 0x9a, 0x2, 0x2, 0x406, 0x408, 0x7, 
    0xb0, 0x2, 0x2, 0x407, 0x401, 0x3, 0x2, 0x2, 0x2, 0x407, 0x403, 0x3, 
    0x2, 0x2, 0x2, 0x407, 0x406, 0x3, 0x2, 0x2, 0x2, 0x407, 0x408, 0x3, 
    0x2, 0x2, 0x2, 0x408, 0x409, 0x3, 0x2, 0x2, 0x2, 0x409, 0x40b, 0x5, 
    0xc0, 0x61, 0x2, 0x40a, 0x3fd, 0x3, 0x2, 0x2, 0x2, 0x40a, 0x400, 0x3, 
    0x2, 0x2, 0x2, 0x40b, 0x57, 0x3, 0x2, 0x2, 0x2, 0x40c, 0x40d, 0x7, 0x39, 
    0x2, 0x2, 0x40d, 0x40e, 0x7, 0xf, 0x2, 0x2, 0x40e, 0x413, 0x5, 0x4, 
    0x3, 0x2, 0x40f, 0x410, 0x7, 0x39, 0x2, 0x2, 0x410, 0x411, 0x7, 0x98, 
    0x2, 0x2, 0x411, 0x413, 0x5, 0x4, 0x3, 0x2, 0x412, 0x40c, 0x3, 0x2, 
    0x2, 0x2, 0x412, 0x40f, 0x3, 0x2, 0x2, 0x2, 0x413, 0x59, 0x3, 0x2, 0x2, 
    0x2, 0x414, 0x415, 0x7, 0x54, 0x2, 0x2, 0x415, 0x417, 0x7, 0x56, 0x2, 
    0x2, 0x416, 0x418, 0x7, 0x9a, 0x2, 0x2, 0x417, 0x416, 0x3, 0x2, 0x2, 
    0x2, 0x417, 0x418, 0x3, 0x2, 0x2, 0x2, 0x418, 0x41c, 0x3, 0x2, 0x2, 
    0x2, 0x419, 0x41d, 0x5, 0xc0, 0x61, 0x2, 0x41a, 0x41b, 0x7, 0x45, 0x2, 
    0x2, 0x41b, 0x41d, 0x5, 0xbe, 0x60, 0x2, 0x41c, 0x419, 0x3, 0x2, 0x2, 
    0x2, 0x41c, 0x41a, 0x3, 0x2, 0x2, 0x2, 0x41d, 0x41f, 0x3, 0x2, 0x2, 
    0x2, 0x41e, 0x420, 0x5, 0x5c, 0x2f, 0x2, 0x41f, 0x41e, 0x3, 0x2, 0x2, 
    0x2, 0x41f, 0x420, 0x3, 0x2, 0x2, 0x2, 0x420, 0x421, 0x3, 0x2, 0x2, 
    0x2, 0x421, 0x422, 0x5, 0x5e, 0x30, 0x2, 0x422, 0x5b, 0x3, 0x2, 0x2, 
    0x2, 0x423, 0x424, 0x7, 0xd0, 0x2, 0x2, 0x424, 0x429, 0x5, 0xba, 0x5e, 
    0x2, 0x425, 0x426, 0x7, 0xc5, 0x2, 0x2, 0x426, 0x428, 0x5, 0xba, 0x5e, 
    0x2, 0x427, 0x425, 0x3, 0x2, 0x2, 0x2, 0x428, 0x42b, 0x3, 0x2, 0x2, 
    0x2, 0x429, 0x427, 0x3, 0x2, 0x2, 0x2, 0x429, 0x42a, 0x3, 0x2, 0x2, 
    0x2, 0x42a, 0x42c, 0x3, 0x2, 0x2, 0x2, 0x42b, 0x429, 0x3, 0x2, 0x2, 
    0x2, 0x42c, 0x42d, 0x7, 0xda, 0x2, 0x2, 0x42d, 0x5d, 0x3, 0x2, 0x2, 
    0x2, 0x42e, 0x42f, 0x7, 0x41, 0x2, 0x2, 0x42f, 0x438, 0x5, 0xd6, 0x6c, 
    0x2, 0x430, 0x438, 0x7, 0xaf, 0x2, 0x2, 0x431, 0x433, 0x5, 0x68, 0x35, 
    0x2, 0x432, 0x434, 0x7, 0xdb, 0x2, 0x2, 0x433, 0x432, 0x3, 0x2, 0x2, 
    0x2, 0x433, 0x434, 0x3, 0x2, 0x2, 0x2, 0x434, 0x435, 0x3, 0x2, 0x2, 
    0x2, 0x435, 0x436, 0x7, 0x2, 0x2, 0x3, 0x436, 0x438, 0x3, 0x2, 0x2, 
    0x2, 0x437, 0x42e, 0x3, 0x2, 0x2, 0x2, 0x437, 0x430, 0x3, 0x2, 0x2, 
    0x2, 0x437, 0x431, 0x3, 0x2, 0x2, 0x2, 0x438, 0x5f, 0x3, 0x2, 0x2, 0x2, 
    0x439, 0x43a, 0x7, 0x5b, 0x2, 0x2, 0x43a, 0x43c, 0x7, 0x6f, 0x2, 0x2, 
    0x43b, 0x43d, 0x5, 0x2c, 0x17, 0x2, 0x43c, 0x43b, 0x3, 0x2, 0x2, 0x2, 
    0x43c, 0x43d, 0x3, 0x2, 0x2, 0x2, 0x43d, 0x43e, 0x3, 0x2, 0x2, 0x2, 
    0x43e, 0x440, 0x5, 0x78, 0x3d, 0x2, 0x43f, 0x441, 0x9, 0x7, 0x2, 0x2, 
    0x440, 0x43f, 0x3, 0x2, 0x2, 0x2, 0x440, 0x441, 0x3, 0x2, 0x2, 0x2, 
    0x441, 0x61, 0x3, 0x2, 0x2, 0x2, 0x442, 0x443, 0x7, 0x77, 0x2, 0x2, 
    0x443, 0x444, 0x7, 0x9a, 0x2, 0x2, 0x444, 0x446, 0x5, 0xc0, 0x61, 0x2, 
    0x445, 0x447, 0x5, 0x2c, 0x17, 0x2, 0x446, 0x445, 0x3, 0x2, 0x2, 0x2, 
    0x446, 0x447, 0x3, 0x2, 0x2, 0x2, 0x447, 0x449, 0x3, 0x2, 0x2, 0x2, 
    0x448, 0x44a, 0x5, 0x10, 0x9, 0x2, 0x449, 0x448, 0x3, 0x2, 0x2, 0x2, 
    0x449, 0x44a, 0x3, 0x2, 0x2, 0x2, 0x44a, 0x44c, 0x3, 0x2, 0x2, 0x2, 
    0x44b, 0x44d, 0x7, 0x3d, 0x2, 0x2, 0x44c, 0x44b, 0x3, 0x2, 0x2, 0x2, 
    0x44c, 0x44d, 0x3, 0x2, 0x2, 0x2, 0x44d, 0x44f, 0x3, 0x2, 0x2, 0x2, 
    0x44e, 0x450, 0x7, 0x26, 0x2, 0x2, 0x44f, 0x44e, 0x3, 0x2, 0x2, 0x2, 
    0x44f, 0x450, 0x3, 0x2, 0x2, 0x2, 0x450, 0x63, 0x3, 0x2, 0x2, 0x2, 0x451, 
    0x452, 0x7, 0x85, 0x2, 0x2, 0x452, 0x453, 0x7, 0x9a, 0x2, 0x2, 0x453, 
    0x454, 0x5, 0xc0, 0x61, 0x2, 0x454, 0x455, 0x7, 0xa2, 0x2, 0x2, 0x455, 
    0x45d, 0x5, 0xc0, 0x61, 0x2, 0x456, 0x457, 0x7, 0xc5, 0x2, 0x2, 0x457, 
    0x458, 0x5, 0xc0, 0x61, 0x2, 0x458, 0x459, 0x7, 0xa2, 0x2, 0x2, 0x459, 
    0x45a, 0x5, 0xc0, 0x61, 0x2, 0x45a, 0x45c, 0x3, 0x2, 0x2, 0x2, 0x45b, 
    0x456, 0x3, 0x2, 0x2, 0x2, 0x45c, 0x45f, 0x3, 0x2, 0x2, 0x2, 0x45d, 
    0x45b, 0x3, 0x2, 0x2, 0x2, 0x45d, 0x45e, 0x3, 0x2, 0x2, 0x2, 0x45e, 
    0x461, 0x3, 0x2, 0x2, 0x2, 0x45f, 0x45d, 0x3, 0x2, 0x2, 0x2, 0x460, 
    0x462, 0x5, 0x2c, 0x17, 0x2, 0x461, 0x460, 0x3, 0x2, 0x2, 0x2, 0x461, 
    0x462, 0x3, 0x2, 0x2, 0x2, 0x462, 0x65, 0x3, 0x2, 0x2, 0x2, 0x463, 0x465, 
    0x7, 0xd0, 0x2, 0x2, 0x464, 0x466, 0x5, 0x6e, 0x38, 0x2, 0x465, 0x464, 
    0x3, 0x2, 0x2, 0x2, 0x465, 0x466, 0x3, 0x2, 0x2, 0x2, 0x466, 0x467, 
    0x3, 0x2, 0x2, 0x2, 0x467, 0x468, 0x7, 0x8d, 0x2, 0x2, 0x468, 0x46a, 
    0x5, 0xac, 0x57, 0x2, 0x469, 0x46b, 0x5, 0x7a, 0x3e, 0x2, 0x46a, 0x469, 
    0x3, 0x2, 0x2, 0x2, 0x46a, 0x46b, 0x3, 0x2, 0x2, 0x2, 0x46b, 0x46d, 
    0x3, 0x2, 0x2, 0x2, 0x46c, 0x46e, 0x5, 0x80, 0x41, 0x2, 0x46d, 0x46c, 
    0x3, 0x2, 0x2, 0x2, 0x46d, 0x46e, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x46f, 
    0x3, 0x2, 0x2, 0x2, 0x46f, 0x470, 0x7, 0xda, 0x2, 0x2, 0x470, 0x67, 
    0x3, 0x2, 0x2, 0x2, 0x471, 0x477, 0x5, 0x6a, 0x36, 0x2, 0x472, 0x473, 
    0x7, 0xaa, 0x2, 0x2, 0x473, 0x474, 0x7, 0x6, 0x2, 0x2, 0x474, 0x476, 
    0x5, 0x6a, 0x36, 0x2, 0x475, 0x472, 0x3, 0x2, 0x2, 0x2, 0x476, 0x479, 
    0x3, 0x2, 0x2, 0x2, 0x477, 0x475, 0x3, 0x2, 0x2, 0x2, 0x477, 0x478, 
    0x3, 0x2, 0x2, 0x2, 0x478, 0x69, 0x3, 0x2, 0x2, 0x2, 0x479, 0x477, 0x3, 
    0x2, 0x2, 0x2, 0x47a, 0x480, 0x5, 0x6c, 0x37, 0x2, 0x47b, 0x47c, 0x7, 
    0xd0, 0x2, 0x2, 0x47c, 0x47d, 0x5, 0x68, 0x35, 0x2, 0x47d, 0x47e, 0x7, 
    0xda, 0x2, 0x2, 0x47e, 0x480, 0x3, 0x2, 0x2, 0x2, 0x47f, 0x47a, 0x3, 
    0x2, 0x2, 0x2, 0x47f, 0x47b, 0x3, 0x2, 0x2, 0x2, 0x480, 0x6b, 0x3, 0x2, 
    0x2, 0x2, 0x481, 0x483, 0x5, 0x6e, 0x38, 0x2, 0x482, 0x481, 0x3, 0x2, 
    0x2, 0x2, 0x482, 0x483, 0x3, 0x2, 0x2, 0x2, 0x483, 0x484, 0x3, 0x2, 
    0x2, 0x2, 0x484, 0x486, 0x7, 0x8d, 0x2, 0x2, 0x485, 0x487, 0x7, 0x31, 
    0x2, 0x2, 0x486, 0x485, 0x3, 0x2, 0x2, 0x2, 0x486, 0x487, 0x3, 0x2, 
    0x2, 0x2, 0x487, 0x489, 0x3, 0x2, 0x2, 0x2, 0x488, 0x48a, 0x5, 0x70, 
    0x39, 0x2, 0x489, 0x488, 0x3, 0x2, 0x2, 0x2, 0x489, 0x48a, 0x3, 0x2, 
    0x2, 0x2, 0x48a, 0x48b, 0x3, 0x2, 0x2, 0x2, 0x48b, 0x48d, 0x5, 0xac, 
    0x57, 0x2, 0x48c, 0x48e, 0x5, 0x72, 0x3a, 0x2, 0x48d, 0x48c, 0x3, 0x2, 
    0x2, 0x2, 0x48d, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x48e, 0x490, 0x3, 0x2, 
    0x2, 0x2, 0x48f, 0x491, 0x5, 0x74, 0x3b, 0x2, 0x490, 0x48f, 0x3, 0x2, 
    0x2, 0x2, 0x490, 0x491, 0x3, 0x2, 0x2, 0x2, 0x491, 0x493, 0x3, 0x2, 
    0x2, 0x2, 0x492, 0x494, 0x5, 0x76, 0x3c, 0x2, 0x493, 0x492, 0x3, 0x2, 
    0x2, 0x2, 0x493, 0x494, 0x3, 0x2, 0x2, 0x2, 0x494, 0x496, 0x3, 0x2, 
    0x2, 0x2, 0x495, 0x497, 0x5, 0x78, 0x3d, 0x2, 0x496, 0x495, 0x3, 0x2, 
    0x2, 0x2, 0x496, 0x497, 0x3, 0x2, 0x2, 0x2, 0x497, 0x499, 0x3, 0x2, 
    0x2, 0x2, 0x498, 0x49a, 0x5, 0x7a, 0x3e, 0x2, 0x499, 0x498, 0x3, 0x2, 
    0x2, 0x2, 0x499, 0x49a, 0x3, 0x2, 0x2, 0x2, 0x49a, 0x49d, 0x3, 0x2, 
    0x2, 0x2, 0x49b, 0x49c, 0x7, 0xb6, 0x2, 0x2, 0x49c, 0x49e, 0x9, 0x8, 
    0x2, 0x2, 0x49d, 0x49b, 0x3, 0x2, 0x2, 0x2, 0x49d, 0x49e, 0x3, 0x2, 
    0x2, 0x2, 0x49e, 0x4a1, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x4a0, 0x7, 0xb6, 
    0x2, 0x2, 0x4a0, 0x4a2, 0x7, 0xa4, 0x2, 0x2, 0x4a1, 0x49f, 0x3, 0x2, 
    0x2, 0x2, 0x4a1, 0x4a2, 0x3, 0x2, 0x2, 0x2, 0x4a2, 0x4a4, 0x3, 0x2, 
    0x2, 0x2, 0x4a3, 0x4a5, 0x5, 0x7c, 0x3f, 0x2, 0x4a4, 0x4a3, 0x3, 0x2, 
    0x2, 0x2, 0x4a4, 0x4a5, 0x3, 0x2, 0x2, 0x2, 0x4a5, 0x4a7, 0x3, 0x2, 
    0x2, 0x2, 0x4a6, 0x4a8, 0x5, 0x7e, 0x40, 0x2, 0x4a7, 0x4a6, 0x3, 0x2, 
    0x2, 0x2, 0x4a7, 0x4a8, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x4aa, 0x3, 0x2, 
    0x2, 0x2, 0x4a9, 0x4ab, 0x5, 0x82, 0x42, 0x2, 0x4aa, 0x4a9, 0x3, 0x2, 
    0x2, 0x2, 0x4aa, 0x4ab, 0x3, 0x2, 0x2, 0x2, 0x4ab, 0x4ad, 0x3, 0x2, 
    0x2, 0x2, 0x4ac, 0x4ae, 0x5, 0x84, 0x43, 0x2, 0x4ad, 0x4ac, 0x3, 0x2, 
    0x2, 0x2, 0x4ad, 0x4ae, 0x3, 0x2, 0x2, 0x2, 0x4ae, 0x4b0, 0x3, 0x2, 
    0x2, 0x2, 0x4af, 0x4b1, 0x5, 0x86, 0x44, 0x2, 0x4b0, 0x4af, 0x3, 0x2, 
    0x2, 0x2, 0x4b0, 0x4b1, 0x3, 0x2, 0x2, 0x2, 0x4b1, 0x6d, 0x3, 0x2, 0x2, 
    0x2, 0x4b2, 0x4b3, 0x7, 0xb6, 0x2, 0x2, 0x4b3, 0x4b4, 0x5, 0xac, 0x57, 
    0x2, 0x4b4, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x4b5, 0x4b6, 0x7, 0xa3, 0x2, 
    0x2, 0x4b6, 0x4b9, 0x7, 0xbd, 0x2, 0x2, 0x4b7, 0x4b8, 0x7, 0xb6, 0x2, 
    0x2, 0x4b8, 0x4ba, 0x7, 0x9f, 0x2, 0x2, 0x4b9, 0x4b7, 0x3, 0x2, 0x2, 
    0x2, 0x4b9, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x4ba, 0x71, 0x3, 0x2, 0x2, 0x2, 
    0x4bb, 0x4bc, 0x7, 0x43, 0x2, 0x2, 0x4bc, 0x4bd, 0x5, 0x88, 0x45, 0x2, 
    0x4bd, 0x73, 0x3, 0x2, 0x2, 0x2, 0x4be, 0x4c0, 0x9, 0x9, 0x2, 0x2, 0x4bf, 
    0x4be, 0x3, 0x2, 0x2, 0x2, 0x4bf, 0x4c0, 0x3, 0x2, 0x2, 0x2, 0x4c0, 
    0x4c1, 0x3, 0x2, 0x2, 0x2, 0x4c1, 0x4c2, 0x7, 0xb, 0x2, 0x2, 0x4c2, 
    0x4c3, 0x7, 0x59, 0x2, 0x2, 0x4c3, 0x4c4, 0x5, 0xac, 0x57, 0x2, 0x4c4, 
    0x75, 0x3, 0x2, 0x2, 0x2, 0x4c5, 0x4c6, 0x7, 0x7e, 0x2, 0x2, 0x4c6, 
    0x4c7, 0x5, 0xb0, 0x59, 0x2, 0x4c7, 0x77, 0x3, 0x2, 0x2, 0x2, 0x4c8, 
    0x4c9, 0x7, 0xb5, 0x2, 0x2, 0x4c9, 0x4ca, 0x5, 0xb0, 0x59, 0x2, 0x4ca, 
    0x79, 0x3, 0x2, 0x2, 0x2, 0x4cb, 0x4cc, 0x7, 0x48, 0x2, 0x2, 0x4cc, 
    0x4d3, 0x7, 0x14, 0x2, 0x2, 0x4cd, 0x4ce, 0x9, 0x8, 0x2, 0x2, 0x4ce, 
    0x4cf, 0x7, 0xd0, 0x2, 0x2, 0x4cf, 0x4d0, 0x5, 0xac, 0x57, 0x2, 0x4d0, 
    0x4d1, 0x7, 0xda, 0x2, 0x2, 0x4d1, 0x4d4, 0x3, 0x2, 0x2, 0x2, 0x4d2, 
    0x4d4, 0x5, 0xac, 0x57, 0x2, 0x4d3, 0x4cd, 0x3, 0x2, 0x2, 0x2, 0x4d3, 
    0x4d2, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x4d5, 0x4d6, 
    0x7, 0x49, 0x2, 0x2, 0x4d6, 0x4d7, 0x5, 0xb0, 0x59, 0x2, 0x4d7, 0x7d, 
    0x3, 0x2, 0x2, 0x2, 0x4d8, 0x4d9, 0x7, 0x79, 0x2, 0x2, 0x4d9, 0x4da, 
    0x7, 0x14, 0x2, 0x2, 0x4da, 0x4db, 0x5, 0x94, 0x4b, 0x2, 0x4db, 0x7f, 
    0x3, 0x2, 0x2, 0x2, 0x4dc, 0x4dd, 0x7, 0x79, 0x2, 0x2, 0x4dd, 0x4de, 
    0x7, 0x14, 0x2, 0x2, 0x4de, 0x4df, 0x5, 0xac, 0x57, 0x2, 0x4df, 0x81, 
    0x3, 0x2, 0x2, 0x2, 0x4e0, 0x4e1, 0x7, 0x62, 0x2, 0x2, 0x4e1, 0x4e2, 
    0x5, 0x92, 0x4a, 0x2, 0x4e2, 0x4e3, 0x7, 0x14, 0x2, 0x2, 0x4e3, 0x4e4, 
    0x5, 0xac, 0x57, 0x2, 0x4e4, 0x83, 0x3, 0x2, 0x2, 0x2, 0x4e5, 0x4e6, 
    0x7, 0x62, 0x2, 0x2, 0x4e6, 0x4e9, 0x5, 0x92, 0x4a, 0x2, 0x4e7, 0x4e8, 
    0x7, 0xb6, 0x2, 0x2, 0x4e8, 0x4ea, 0x7, 0x9f, 0x2, 0x2, 0x4e9, 0x4e7, 
    0x3, 0x2, 0x2, 0x2, 0x4e9, 0x4ea, 0x3, 0x2, 0x2, 0x2, 0x4ea, 0x85, 0x3, 
    0x2, 0x2, 0x2, 0x4eb, 0x4ec, 0x7, 0x91, 0x2, 0x2, 0x4ec, 0x4ed, 0x5, 
    0x9a, 0x4e, 0x2, 0x4ed, 0x87, 0x3, 0x2, 0x2, 0x2, 0x4ee, 0x4ef, 0x8, 
    0x45, 0x1, 0x2, 0x4ef, 0x4f1, 0x5, 0xbc, 0x5f, 0x2, 0x4f0, 0x4f2, 0x7, 
    0x3d, 0x2, 0x2, 0x4f1, 0x4f0, 0x3, 0x2, 0x2, 0x2, 0x4f1, 0x4f2, 0x3, 
    0x2, 0x2, 0x2, 0x4f2, 0x4f4, 0x3, 0x2, 0x2, 0x2, 0x4f3, 0x4f5, 0x5, 
    0x90, 0x49, 0x2, 0x4f4, 0x4f3, 0x3, 0x2, 0x2, 0x2, 0x4f4, 0x4f5, 0x3, 
    0x2, 0x2, 0x2, 0x4f5, 0x4fb, 0x3, 0x2, 0x2, 0x2, 0x4f6, 0x4f7, 0x7, 
    0xd0, 0x2, 0x2, 0x4f7, 0x4f8, 0x5, 0x88, 0x45, 0x2, 0x4f8, 0x4f9, 0x7, 
    0xda, 0x2, 0x2, 0x4f9, 0x4fb, 0x3, 0x2, 0x2, 0x2, 0x4fa, 0x4ee, 0x3, 
    0x2, 0x2, 0x2, 0x4fa, 0x4f6, 0x3, 0x2, 0x2, 0x2, 0x4fb, 0x50d, 0x3, 
    0x2, 0x2, 0x2, 0x4fc, 0x4fd, 0xc, 0x5, 0x2, 0x2, 0x4fd, 0x4fe, 0x5, 
    0x8c, 0x47, 0x2, 0x4fe, 0x4ff, 0x5, 0x88, 0x45, 0x6, 0x4ff, 0x50c, 0x3, 
    0x2, 0x2, 0x2, 0x500, 0x502, 0xc, 0x6, 0x2, 0x2, 0x501, 0x503, 0x9, 
    0xa, 0x2, 0x2, 0x502, 0x501, 0x3, 0x2, 0x2, 0x2, 0x502, 0x503, 0x3, 
    0x2, 0x2, 0x2, 0x503, 0x505, 0x3, 0x2, 0x2, 0x2, 0x504, 0x506, 0x5, 
    0x8a, 0x46, 0x2, 0x505, 0x504, 0x3, 0x2, 0x2, 0x2, 0x505, 0x506, 0x3, 
    0x2, 0x2, 0x2, 0x506, 0x507, 0x3, 0x2, 0x2, 0x2, 0x507, 0x508, 0x7, 
    0x59, 0x2, 0x2, 0x508, 0x509, 0x5, 0x88, 0x45, 0x2, 0x509, 0x50a, 0x5, 
    0x8e, 0x48, 0x2, 0x50a, 0x50c, 0x3, 0x2, 0x2, 0x2, 0x50b, 0x4fc, 0x3, 
    0x2, 0x2, 0x2, 0x50b, 0x500, 0x3, 0x2, 0x2, 0x2, 0x50c, 0x50f, 0x3, 
    0x2, 0x2, 0x2, 0x50d, 0x50b, 0x3, 0x2, 0x2, 0x2, 0x50d, 0x50e, 0x3, 
    0x2, 0x2, 0x2, 0x50e, 0x89, 0x3, 0x2, 0x2, 0x2, 0x50f, 0x50d, 0x3, 0x2, 
    0x2, 0x2, 0x510, 0x512, 0x9, 0xb, 0x2, 0x2, 0x511, 0x510, 0x3, 0x2, 
    0x2, 0x2, 0x511, 0x512, 0x3, 0x2, 0x2, 0x2, 0x512, 0x513, 0x3, 0x2, 
    0x2, 0x2, 0x513, 0x51a, 0x7, 0x53, 0x2, 0x2, 0x514, 0x516, 0x7, 0x53, 
    0x2, 0x2, 0x515, 0x517, 0x9, 0xb, 0x2, 0x2, 0x516, 0x515, 0x3, 0x2, 
    0x2, 0x2, 0x516, 0x517, 0x3, 0x2, 0x2, 0x2, 0x517, 0x51a, 0x3, 0x2, 
    0x2, 0x2, 0x518, 0x51a, 0x9, 0xb, 0x2, 0x2, 0x519, 0x511, 0x3, 0x2, 
    0x2, 0x2, 0x519, 0x514, 0x3, 0x2, 0x2, 0x2, 0x519, 0x518, 0x3, 0x2, 
    0x2, 0x2, 0x51a, 0x53c, 0x3, 0x2, 0x2, 0x2, 0x51b, 0x51d, 0x9, 0xc, 
    0x2, 0x2, 0x51c, 0x51b, 0x3, 0x2, 0x2, 0x2, 0x51c, 0x51d, 0x3, 0x2, 
    0x2, 0x2, 0x51d, 0x51e, 0x3, 0x2, 0x2, 0x2, 0x51e, 0x520, 0x9, 0xd, 
    0x2, 0x2, 0x51f, 0x521, 0x7, 0x7a, 0x2, 0x2, 0x520, 0x51f, 0x3, 0x2, 
    0x2, 0x2, 0x520, 0x521, 0x3, 0x2, 0x2, 0x2, 0x521, 0x52a, 0x3, 0x2, 
    0x2, 0x2, 0x522, 0x524, 0x9, 0xd, 0x2, 0x2, 0x523, 0x525, 0x7, 0x7a, 
    0x2, 0x2, 0x524, 0x523, 0x3, 0x2, 0x2, 0x2, 0x524, 0x525, 0x3, 0x2, 
    0x2, 0x2, 0x525, 0x527, 0x3, 0x2, 0x2, 0x2, 0x526, 0x528, 0x9, 0xc, 
    0x2, 0x2, 0x527, 0x526, 0x3, 0x2, 0x2, 0x2, 0x527, 0x528, 0x3, 0x2, 
    0x2, 0x2, 0x528, 0x52a, 0x3, 0x2, 0x2, 0x2, 0x529, 0x51c, 0x3, 0x2, 
    0x2, 0x2, 0x529, 0x522, 0x3, 0x2, 0x2, 0x2, 0x52a, 0x53c, 0x3, 0x2, 
    0x2, 0x2, 0x52b, 0x52d, 0x9, 0xe, 0x2, 0x2, 0x52c, 0x52b, 0x3, 0x2, 
    0x2, 0x2, 0x52c, 0x52d, 0x3, 0x2, 0x2, 0x2, 0x52d, 0x52e, 0x3, 0x2, 
    0x2, 0x2, 0x52e, 0x530, 0x7, 0x44, 0x2, 0x2, 0x52f, 0x531, 0x7, 0x7a, 
    0x2, 0x2, 0x530, 0x52f, 0x3, 0x2, 0x2, 0x2, 0x530, 0x531, 0x3, 0x2, 
    0x2, 0x2, 0x531, 0x53a, 0x3, 0x2, 0x2, 0x2, 0x532, 0x534, 0x7, 0x44, 
    0x2, 0x2, 0x533, 0x535, 0x7, 0x7a, 0x2, 0x2, 0x534, 0x533, 0x3, 0x2, 
    0x2, 0x2, 0x534, 0x535, 0x3, 0x2, 0x2, 0x2, 0x535, 0x537, 0x3, 0x2, 
    0x2, 0x2, 0x536, 0x538, 0x9, 0xe, 0x2, 0x2, 0x537, 0x536, 0x3, 0x2, 
    0x2, 0x2, 0x537, 0x538, 0x3, 0x2, 0x2, 0x2, 0x538, 0x53a, 0x3, 0x2, 
    0x2, 0x2, 0x539, 0x52c, 0x3, 0x2, 0x2, 0x2, 0x539, 0x532, 0x3, 0x2, 
    0x2, 0x2, 0x53a, 0x53c, 0x3, 0x2, 0x2, 0x2, 0x53b, 0x519, 0x3, 0x2, 
    0x2, 0x2, 0x53b, 0x529, 0x3, 0x2, 0x2, 0x2, 0x53b, 0x539, 0x3, 0x2, 
    0x2, 0x2, 0x53c, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x53d, 0x53f, 0x9, 0xa, 0x2, 
    0x2, 0x53e, 0x53d, 0x3, 0x2, 0x2, 0x2, 0x53e, 0x53f, 0x3, 0x2, 0x2, 
    0x2, 0x53f, 0x540, 0x3, 0x2, 0x2, 0x2, 0x540, 0x541, 0x7, 0x20, 0x2, 
    0x2, 0x541, 0x544, 0x7, 0x59, 0x2, 0x2, 0x542, 0x544, 0x7, 0xc5, 0x2, 
    0x2, 0x543, 0x53e, 0x3, 0x2, 0x2, 0x2, 0x543, 0x542, 0x3, 0x2, 0x2, 
    0x2, 0x544, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x545, 0x546, 0x7, 0x76, 0x2, 
    0x2, 0x546, 0x54f, 0x5, 0xac, 0x57, 0x2, 0x547, 0x548, 0x7, 0xad, 0x2, 
    0x2, 0x548, 0x549, 0x7, 0xd0, 0x2, 0x2, 0x549, 0x54a, 0x5, 0xac, 0x57, 
    0x2, 0x54a, 0x54b, 0x7, 0xda, 0x2, 0x2, 0x54b, 0x54f, 0x3, 0x2, 0x2, 
    0x2, 0x54c, 0x54d, 0x7, 0xad, 0x2, 0x2, 0x54d, 0x54f, 0x5, 0xac, 0x57, 
    0x2, 0x54e, 0x545, 0x3, 0x2, 0x2, 0x2, 0x54e, 0x547, 0x3, 0x2, 0x2, 
    0x2, 0x54e, 0x54c, 0x3, 0x2, 0x2, 0x2, 0x54f, 0x8f, 0x3, 0x2, 0x2, 0x2, 
    0x550, 0x551, 0x7, 0x8b, 0x2, 0x2, 0x551, 0x554, 0x5, 0x98, 0x4d, 0x2, 
    0x552, 0x553, 0x7, 0x75, 0x2, 0x2, 0x553, 0x555, 0x5, 0x98, 0x4d, 0x2, 
    0x554, 0x552, 0x3, 0x2, 0x2, 0x2, 0x554, 0x555, 0x3, 0x2, 0x2, 0x2, 
    0x555, 0x91, 0x3, 0x2, 0x2, 0x2, 0x556, 0x559, 0x5, 0xb0, 0x59, 0x2, 
    0x557, 0x558, 0x9, 0xf, 0x2, 0x2, 0x558, 0x55a, 0x5, 0xb0, 0x59, 0x2, 
    0x559, 0x557, 0x3, 0x2, 0x2, 0x2, 0x559, 0x55a, 0x3, 0x2, 0x2, 0x2, 
    0x55a, 0x93, 0x3, 0x2, 0x2, 0x2, 0x55b, 0x560, 0x5, 0x96, 0x4c, 0x2, 
    0x55c, 0x55d, 0x7, 0xc5, 0x2, 0x2, 0x55d, 0x55f, 0x5, 0x96, 0x4c, 0x2, 
    0x55e, 0x55c, 0x3, 0x2, 0x2, 0x2, 0x55f, 0x562, 0x3, 0x2, 0x2, 0x2, 
    0x560, 0x55e, 0x3, 0x2, 0x2, 0x2, 0x560, 0x561, 0x3, 0x2, 0x2, 0x2, 
    0x561, 0x95, 0x3, 0x2, 0x2, 0x2, 0x562, 0x560, 0x3, 0x2, 0x2, 0x2, 0x563, 
    0x565, 0x5, 0xb0, 0x59, 0x2, 0x564, 0x566, 0x9, 0x10, 0x2, 0x2, 0x565, 
    0x564, 0x3, 0x2, 0x2, 0x2, 0x565, 0x566, 0x3, 0x2, 0x2, 0x2, 0x566, 
    0x569, 0x3, 0x2, 0x2, 0x2, 0x567, 0x568, 0x7, 0x74, 0x2, 0x2, 0x568, 
    0x56a, 0x9, 0x11, 0x2, 0x2, 0x569, 0x567, 0x3, 0x2, 0x2, 0x2, 0x569, 
    0x56a, 0x3, 0x2, 0x2, 0x2, 0x56a, 0x56d, 0x3, 0x2, 0x2, 0x2, 0x56b, 
    0x56c, 0x7, 0x1b, 0x2, 0x2, 0x56c, 0x56e, 0x7, 0xbf, 0x2, 0x2, 0x56d, 
    0x56b, 0x3, 0x2, 0x2, 0x2, 0x56d, 0x56e, 0x3, 0x2, 0x2, 0x2, 0x56e, 
    0x97, 0x3, 0x2, 0x2, 0x2, 0x56f, 0x572, 0x5, 0xca, 0x66, 0x2, 0x570, 
    0x571, 0x7, 0xdc, 0x2, 0x2, 0x571, 0x573, 0x5, 0xca, 0x66, 0x2, 0x572, 
    0x570, 0x3, 0x2, 0x2, 0x2, 0x572, 0x573, 0x3, 0x2, 0x2, 0x2, 0x573, 
    0x99, 0x3, 0x2, 0x2, 0x2, 0x574, 0x579, 0x5, 0x9c, 0x4f, 0x2, 0x575, 
    0x576, 0x7, 0xc5, 0x2, 0x2, 0x576, 0x578, 0x5, 0x9c, 0x4f, 0x2, 0x577, 
    0x575, 0x3, 0x2, 0x2, 0x2, 0x578, 0x57b, 0x3, 0x2, 0x2, 0x2, 0x579, 
    0x577, 0x3, 0x2, 0x2, 0x2, 0x579, 0x57a, 0x3, 0x2, 0x2, 0x2, 0x57a, 
    0x9b, 0x3, 0x2, 0x2, 0x2, 0x57b, 0x579, 0x3, 0x2, 0x2, 0x2, 0x57c, 0x57d, 
    0x5, 0xd6, 0x6c, 0x2, 0x57d, 0x57e, 0x7, 0xca, 0x2, 0x2, 0x57e, 0x57f, 
    0x5, 0xcc, 0x67, 0x2, 0x57f, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x580, 0x581, 
    0x7, 0x90, 0x2, 0x2, 0x581, 0x582, 0x5, 0x9a, 0x4e, 0x2, 0x582, 0x9f, 
    0x3, 0x2, 0x2, 0x2, 0x583, 0x584, 0x7, 0x92, 0x2, 0x2, 0x584, 0x585, 
    0x7, 0x1f, 0x2, 0x2, 0x585, 0x586, 0x7, 0x22, 0x2, 0x2, 0x586, 0x5ae, 
    0x5, 0xc6, 0x64, 0x2, 0x587, 0x588, 0x7, 0x92, 0x2, 0x2, 0x588, 0x589, 
    0x7, 0x1f, 0x2, 0x2, 0x589, 0x58a, 0x7, 0x2f, 0x2, 0x2, 0x58a, 0x5ae, 
    0x5, 0xc0, 0x61, 0x2, 0x58b, 0x58c, 0x7, 0x92, 0x2, 0x2, 0x58c, 0x58e, 
    0x7, 0x1f, 0x2, 0x2, 0x58d, 0x58f, 0x7, 0x9c, 0x2, 0x2, 0x58e, 0x58d, 
    0x3, 0x2, 0x2, 0x2, 0x58e, 0x58f, 0x3, 0x2, 0x2, 0x2, 0x58f, 0x591, 
    0x3, 0x2, 0x2, 0x2, 0x590, 0x592, 0x7, 0x9a, 0x2, 0x2, 0x591, 0x590, 
    0x3, 0x2, 0x2, 0x2, 0x591, 0x592, 0x3, 0x2, 0x2, 0x2, 0x592, 0x593, 
    0x3, 0x2, 0x2, 0x2, 0x593, 0x5ae, 0x5, 0xc0, 0x61, 0x2, 0x594, 0x595, 
    0x7, 0x92, 0x2, 0x2, 0x595, 0x5ae, 0x7, 0x23, 0x2, 0x2, 0x596, 0x597, 
    0x7, 0x92, 0x2, 0x2, 0x597, 0x59a, 0x7, 0x2e, 0x2, 0x2, 0x598, 0x599, 
    0x7, 0x43, 0x2, 0x2, 0x599, 0x59b, 0x5, 0xc6, 0x64, 0x2, 0x59a, 0x598, 
    0x3, 0x2, 0x2, 0x2, 0x59a, 0x59b, 0x3, 0x2, 0x2, 0x2, 0x59b, 0x5ae, 
    0x3, 0x2, 0x2, 0x2, 0x59c, 0x59e, 0x7, 0x92, 0x2, 0x2, 0x59d, 0x59f, 
    0x7, 0x9c, 0x2, 0x2, 0x59e, 0x59d, 0x3, 0x2, 0x2, 0x2, 0x59e, 0x59f, 
    0x3, 0x2, 0x2, 0x2, 0x59f, 0x5a0, 0x3, 0x2, 0x2, 0x2, 0x5a0, 0x5a3, 
    0x7, 0x9b, 0x2, 0x2, 0x5a1, 0x5a2, 0x9, 0x12, 0x2, 0x2, 0x5a2, 0x5a4, 
    0x5, 0xc6, 0x64, 0x2, 0x5a3, 0x5a1, 0x3, 0x2, 0x2, 0x2, 0x5a3, 0x5a4, 
    0x3, 0x2, 0x2, 0x2, 0x5a4, 0x5a8, 0x3, 0x2, 0x2, 0x2, 0x5a5, 0x5a6, 
    0x7, 0x61, 0x2, 0x2, 0x5a6, 0x5a9, 0x7, 0xbf, 0x2, 0x2, 0x5a7, 0x5a9, 
    0x5, 0x78, 0x3d, 0x2, 0x5a8, 0x5a5, 0x3, 0x2, 0x2, 0x2, 0x5a8, 0x5a7, 
    0x3, 0x2, 0x2, 0x2, 0x5a8, 0x5a9, 0x3, 0x2, 0x2, 0x2, 0x5a9, 0x5ab, 
    0x3, 0x2, 0x2, 0x2, 0x5aa, 0x5ac, 0x5, 0x84, 0x43, 0x2, 0x5ab, 0x5aa, 
    0x3, 0x2, 0x2, 0x2, 0x5ab, 0x5ac, 0x3, 0x2, 0x2, 0x2, 0x5ac, 0x5ae, 
    0x3, 0x2, 0x2, 0x2, 0x5ad, 0x583, 0x3, 0x2, 0x2, 0x2, 0x5ad, 0x587, 
    0x3, 0x2, 0x2, 0x2, 0x5ad, 0x58b, 0x3, 0x2, 0x2, 0x2, 0x5ad, 0x594, 
    0x3, 0x2, 0x2, 0x2, 0x5ad, 0x596, 0x3, 0x2, 0x2, 0x2, 0x5ad, 0x59c, 
    0x3, 0x2, 0x2, 0x2, 0x5ae, 0xa1, 0x3, 0x2, 0x2, 0x2, 0x5af, 0x5b0, 0x7, 
    0x99, 0x2, 0x2, 0x5b0, 0x5b1, 0x7, 0x3f, 0x2, 0x2, 0x5b1, 0x5b2, 0x7, 
    0x32, 0x2, 0x2, 0x5b2, 0x5d2, 0x5, 0xc0, 0x61, 0x2, 0x5b3, 0x5b4, 0x7, 
    0x99, 0x2, 0x2, 0x5b4, 0x5b5, 0x7, 0x3f, 0x2, 0x2, 0x5b5, 0x5d2, 0x7, 
    0x65, 0x2, 0x2, 0x5b6, 0x5b7, 0x7, 0x99, 0x2, 0x2, 0x5b7, 0x5b8, 0x7, 
    0x83, 0x2, 0x2, 0x5b8, 0x5d2, 0x7, 0x2e, 0x2, 0x2, 0x5b9, 0x5ba, 0x7, 
    0x99, 0x2, 0x2, 0x5ba, 0x5bb, 0x7, 0x83, 0x2, 0x2, 0x5bb, 0x5bc, 0x7, 
    0x2f, 0x2, 0x2, 0x5bc, 0x5d2, 0x5, 0xc0, 0x61, 0x2, 0x5bd, 0x5be, 0x7, 
    0x99, 0x2, 0x2, 0x5be, 0x5c6, 0x9, 0x13, 0x2, 0x2, 0x5bf, 0x5c0, 0x7, 
    0x32, 0x2, 0x2, 0x5c0, 0x5c7, 0x7, 0x8f, 0x2, 0x2, 0x5c1, 0x5c7, 0x7, 
    0x3c, 0x2, 0x2, 0x5c2, 0x5c4, 0x7, 0xa8, 0x2, 0x2, 0x5c3, 0x5c2, 0x3, 
    0x2, 0x2, 0x2, 0x5c3, 0x5c4, 0x3, 0x2, 0x2, 0x2, 0x5c4, 0x5c5, 0x3, 
    0x2, 0x2, 0x2, 0x5c5, 0x5c7, 0x7, 0x69, 0x2, 0x2, 0x5c6, 0x5bf, 0x3, 
    0x2, 0x2, 0x2, 0x5c6, 0x5c1, 0x3, 0x2, 0x2, 0x2, 0x5c6, 0x5c3, 0x3, 
    0x2, 0x2, 0x2, 0x5c7, 0x5c8, 0x3, 0x2, 0x2, 0x2, 0x5c8, 0x5d2, 0x5, 
    0xc0, 0x61, 0x2, 0x5c9, 0x5ca, 0x7, 0x99, 0x2, 0x2, 0x5ca, 0x5cb, 0x9, 
    0x13, 0x2, 0x2, 0x5cb, 0x5cc, 0x7, 0x88, 0x2, 0x2, 0x5cc, 0x5d2, 0x7, 
    0x8f, 0x2, 0x2, 0x5cd, 0x5ce, 0x7, 0x99, 0x2, 0x2, 0x5ce, 0x5cf, 0x7, 
    0x97, 0x2, 0x2, 0x5cf, 0x5d0, 0x7, 0x87, 0x2, 0x2, 0x5d0, 0x5d2, 0x5, 
    0xc0, 0x61, 0x2, 0x5d1, 0x5af, 0x3, 0x2, 0x2, 0x2, 0x5d1, 0x5b3, 0x3, 
    0x2, 0x2, 0x2, 0x5d1, 0x5b6, 0x3, 0x2, 0x2, 0x2, 0x5d1, 0x5b9, 0x3, 
    0x2, 0x2, 0x2, 0x5d1, 0x5bd, 0x3, 0x2, 0x2, 0x2, 0x5d1, 0x5c9, 0x3, 
    0x2, 0x2, 0x2, 0x5d1, 0x5cd, 0x3, 0x2, 0x2, 0x2, 0x5d2, 0xa3, 0x3, 0x2, 
    0x2, 0x2, 0x5d3, 0x5d5, 0x7, 0xa7, 0x2, 0x2, 0x5d4, 0x5d6, 0x7, 0x9c, 
    0x2, 0x2, 0x5d5, 0x5d4, 0x3, 0x2, 0x2, 0x2, 0x5d5, 0x5d6, 0x3, 0x2, 
    0x2, 0x2, 0x5d6, 0x5d8, 0x3, 0x2, 0x2, 0x2, 0x5d7, 0x5d9, 0x7, 0x9a, 
    0x2, 0x2, 0x5d8, 0x5d7, 0x3, 0x2, 0x2, 0x2, 0x5d8, 0x5d9, 0x3, 0x2, 
    0x2, 0x2, 0x5d9, 0x5dc, 0x3, 0x2, 0x2, 0x2, 0x5da, 0x5db, 0x7, 0x4d, 
    0x2, 0x2, 0x5db, 0x5dd, 0x7, 0x38, 0x2, 0x2, 0x5dc, 0x5da, 0x3, 0x2, 
    0x2, 0x2, 0x5dc, 0x5dd, 0x3, 0x2, 0x2, 0x2, 0x5dd, 0x5de, 0x3, 0x2, 
    0x2, 0x2, 0x5de, 0x5e0, 0x5, 0xc0, 0x61, 0x2, 0x5df, 0x5e1, 0x5, 0x2c, 
    0x17, 0x2, 0x5e0, 0x5df, 0x3, 0x2, 0x2, 0x2, 0x5e0, 0x5e1, 0x3, 0x2, 
    0x2, 0x2, 0x5e1, 0xa5, 0x3, 0x2, 0x2, 0x2, 0x5e2, 0x5e3, 0x7, 0xac, 
    0x2, 0x2, 0x5e3, 0x5e4, 0x5, 0xc6, 0x64, 0x2, 0x5e4, 0xa7, 0x3, 0x2, 
    0x2, 0x2, 0x5e5, 0x5e6, 0x7, 0xb2, 0x2, 0x2, 0x5e6, 0x5e8, 0x5, 0xc0, 
    0x61, 0x2, 0x5e7, 0x5e9, 0x7, 0x37, 0x2, 0x2, 0x5e8, 0x5e7, 0x3, 0x2, 
    0x2, 0x2, 0x5e8, 0x5e9, 0x3, 0x2, 0x2, 0x2, 0x5e9, 0x5ec, 0x3, 0x2, 
    0x2, 0x2, 0x5ea, 0x5eb, 0x7, 0x62, 0x2, 0x2, 0x5eb, 0x5ed, 0x7, 0xbd, 
    0x2, 0x2, 0x5ec, 0x5ea, 0x3, 0x2, 0x2, 0x2, 0x5ec, 0x5ed, 0x3, 0x2, 
    0x2, 0x2, 0x5ed, 0xa9, 0x3, 0x2, 0x2, 0x2, 0x5ee, 0x61e, 0x5, 0xd6, 
    0x6c, 0x2, 0x5ef, 0x5f0, 0x5, 0xd6, 0x6c, 0x2, 0x5f0, 0x5f1, 0x7, 0xd0, 
    0x2, 0x2, 0x5f1, 0x5f2, 0x5, 0xd6, 0x6c, 0x2, 0x5f2, 0x5f9, 0x5, 0xaa, 
    0x56, 0x2, 0x5f3, 0x5f4, 0x7, 0xc5, 0x2, 0x2, 0x5f4, 0x5f5, 0x5, 0xd6, 
    0x6c, 0x2, 0x5f5, 0x5f6, 0x5, 0xaa, 0x56, 0x2, 0x5f6, 0x5f8, 0x3, 0x2, 
    0x2, 0x2, 0x5f7, 0x5f3, 0x3, 0x2, 0x2, 0x2, 0x5f8, 0x5fb, 0x3, 0x2, 
    0x2, 0x2, 0x5f9, 0x5f7, 0x3, 0x2, 0x2, 0x2, 0x5f9, 0x5fa, 0x3, 0x2, 
    0x2, 0x2, 0x5fa, 0x5fc, 0x3, 0x2, 0x2, 0x2, 0x5fb, 0x5f9, 0x3, 0x2, 
    0x2, 0x2, 0x5fc, 0x5fd, 0x7, 0xda, 0x2, 0x2, 0x5fd, 0x61e, 0x3, 0x2, 
    0x2, 0x2, 0x5fe, 0x5ff, 0x5, 0xd6, 0x6c, 0x2, 0x5ff, 0x600, 0x7, 0xd0, 
    0x2, 0x2, 0x600, 0x605, 0x5, 0xda, 0x6e, 0x2, 0x601, 0x602, 0x7, 0xc5, 
    0x2, 0x2, 0x602, 0x604, 0x5, 0xda, 0x6e, 0x2, 0x603, 0x601, 0x3, 0x2, 
    0x2, 0x2, 0x604, 0x607, 0x3, 0x2, 0x2, 0x2, 0x605, 0x603, 0x3, 0x2, 
    0x2, 0x2, 0x605, 0x606, 0x3, 0x2, 0x2, 0x2, 0x606, 0x608, 0x3, 0x2, 
    0x2, 0x2, 0x607, 0x605, 0x3, 0x2, 0x2, 0x2, 0x608, 0x609, 0x7, 0xda, 
    0x2, 0x2, 0x609, 0x61e, 0x3, 0x2, 0x2, 0x2, 0x60a, 0x60b, 0x5, 0xd6, 
    0x6c, 0x2, 0x60b, 0x60c, 0x7, 0xd0, 0x2, 0x2, 0x60c, 0x611, 0x5, 0xaa, 
    0x56, 0x2, 0x60d, 0x60e, 0x7, 0xc5, 0x2, 0x2, 0x60e, 0x610, 0x5, 0xaa, 
    0x56, 0x2, 0x60f, 0x60d, 0x3, 0x2, 0x2, 0x2, 0x610, 0x613, 0x3, 0x2, 
    0x2, 0x2, 0x611, 0x60f, 0x3, 0x2, 0x2, 0x2, 0x611, 0x612, 0x3, 0x2, 
    0x2, 0x2, 0x612, 0x614, 0x3, 0x2, 0x2, 0x2, 0x613, 0x611, 0x3, 0x2, 
    0x2, 0x2, 0x614, 0x615, 0x7, 0xda, 0x2, 0x2, 0x615, 0x61e, 0x3, 0x2, 
    0x2, 0x2, 0x616, 0x617, 0x5, 0xd6, 0x6c, 0x2, 0x617, 0x619, 0x7, 0xd0, 
    0x2, 0x2, 0x618, 0x61a, 0x5, 0xac, 0x57, 0x2, 0x619, 0x618, 0x3, 0x2, 
    0x2, 0x2, 0x619, 0x61a, 0x3, 0x2, 0x2, 0x2, 0x61a, 0x61b, 0x3, 0x2, 
    0x2, 0x2, 0x61b, 0x61c, 0x7, 0xda, 0x2, 0x2, 0x61c, 0x61e, 0x3, 0x2, 
    0x2, 0x2, 0x61d, 0x5ee, 0x3, 0x2, 0x2, 0x2, 0x61d, 0x5ef, 0x3, 0x2, 
    0x2, 0x2, 0x61d, 0x5fe, 0x3, 0x2, 0x2, 0x2, 0x61d, 0x60a, 0x3, 0x2, 
    0x2, 0x2, 0x61d, 0x616, 0x3, 0x2, 0x2, 0x2, 0x61e, 0xab, 0x3, 0x2, 0x2, 
    0x2, 0x61f, 0x624, 0x5, 0xae, 0x58, 0x2, 0x620, 0x621, 0x7, 0xc5, 0x2, 
    0x2, 0x621, 0x623, 0x5, 0xae, 0x58, 0x2, 0x622, 0x620, 0x3, 0x2, 0x2, 
    0x2, 0x623, 0x626, 0x3, 0x2, 0x2, 0x2, 0x624, 0x622, 0x3, 0x2, 0x2, 
    0x2, 0x624, 0x625, 0x3, 0x2, 0x2, 0x2, 0x625, 0xad, 0x3, 0x2, 0x2, 0x2, 
    0x626, 0x624, 0x3, 0x2, 0x2, 0x2, 0x627, 0x628, 0x5, 0xc0, 0x61, 0x2, 
    0x628, 0x629, 0x7, 0xc8, 0x2, 0x2, 0x629, 0x62b, 0x3, 0x2, 0x2, 0x2, 
    0x62a, 0x627, 0x3, 0x2, 0x2, 0x2, 0x62a, 0x62b, 0x3, 0x2, 0x2, 0x2, 
    0x62b, 0x62c, 0x3, 0x2, 0x2, 0x2, 0x62c, 0x633, 0x7, 0xc1, 0x2, 0x2, 
    0x62d, 0x62e, 0x7, 0xd0, 0x2, 0x2, 0x62e, 0x62f, 0x5, 0x68, 0x35, 0x2, 
    0x62f, 0x630, 0x7, 0xda, 0x2, 0x2, 0x630, 0x633, 0x3, 0x2, 0x2, 0x2, 
    0x631, 0x633, 0x5, 0xb0, 0x59, 0x2, 0x632, 0x62a, 0x3, 0x2, 0x2, 0x2, 
    0x632, 0x62d, 0x3, 0x2, 0x2, 0x2, 0x632, 0x631, 0x3, 0x2, 0x2, 0x2, 
    0x633, 0xaf, 0x3, 0x2, 0x2, 0x2, 0x634, 0x635, 0x8, 0x59, 0x1, 0x2, 
    0x635, 0x637, 0x7, 0x15, 0x2, 0x2, 0x636, 0x638, 0x5, 0xb0, 0x59, 0x2, 
    0x637, 0x636, 0x3, 0x2, 0x2, 0x2, 0x637, 0x638, 0x3, 0x2, 0x2, 0x2, 
    0x638, 0x63e, 0x3, 0x2, 0x2, 0x2, 0x639, 0x63a, 0x7, 0xb4, 0x2, 0x2, 
    0x63a, 0x63b, 0x5, 0xb0, 0x59, 0x2, 0x63b, 0x63c, 0x7, 0x9e, 0x2, 0x2, 
    0x63c, 0x63d, 0x5, 0xb0, 0x59, 0x2, 0x63d, 0x63f, 0x3, 0x2, 0x2, 0x2, 
    0x63e, 0x639, 0x3, 0x2, 0x2, 0x2, 0x63f, 0x640, 0x3, 0x2, 0x2, 0x2, 
    0x640, 0x63e, 0x3, 0x2, 0x2, 0x2, 0x640, 0x641, 0x3, 0x2, 0x2, 0x2, 
    0x641, 0x644, 0x3, 0x2, 0x2, 0x2, 0x642, 0x643, 0x7, 0x34, 0x2, 0x2, 
    0x643, 0x645, 0x5, 0xb0, 0x59, 0x2, 0x644, 0x642, 0x3, 0x2, 0x2, 0x2, 
    0x644, 0x645, 0x3, 0x2, 0x2, 0x2, 0x645, 0x646, 0x3, 0x2, 0x2, 0x2, 
    0x646, 0x647, 0x7, 0x35, 0x2, 0x2, 0x647, 0x6a0, 0x3, 0x2, 0x2, 0x2, 
    0x648, 0x649, 0x7, 0x16, 0x2, 0x2, 0x649, 0x64a, 0x7, 0xd0, 0x2, 0x2, 
    0x64a, 0x64b, 0x5, 0xb0, 0x59, 0x2, 0x64b, 0x64c, 0x7, 0xc, 0x2, 0x2, 
    0x64c, 0x64d, 0x5, 0xaa, 0x56, 0x2, 0x64d, 0x64e, 0x7, 0xda, 0x2, 0x2, 
    0x64e, 0x6a0, 0x3, 0x2, 0x2, 0x2, 0x64f, 0x650, 0x7, 0x24, 0x2, 0x2, 
    0x650, 0x6a0, 0x7, 0xbf, 0x2, 0x2, 0x651, 0x652, 0x7, 0x3b, 0x2, 0x2, 
    0x652, 0x653, 0x7, 0xd0, 0x2, 0x2, 0x653, 0x654, 0x5, 0xce, 0x68, 0x2, 
    0x654, 0x655, 0x7, 0x43, 0x2, 0x2, 0x655, 0x656, 0x5, 0xb0, 0x59, 0x2, 
    0x656, 0x657, 0x7, 0xda, 0x2, 0x2, 0x657, 0x6a0, 0x3, 0x2, 0x2, 0x2, 
    0x658, 0x659, 0x7, 0x55, 0x2, 0x2, 0x659, 0x65a, 0x5, 0xb0, 0x59, 0x2, 
    0x65a, 0x65b, 0x5, 0xce, 0x68, 0x2, 0x65b, 0x6a0, 0x3, 0x2, 0x2, 0x2, 
    0x65c, 0x65d, 0x7, 0x96, 0x2, 0x2, 0x65d, 0x65e, 0x7, 0xd0, 0x2, 0x2, 
    0x65e, 0x65f, 0x5, 0xb0, 0x59, 0x2, 0x65f, 0x660, 0x7, 0x43, 0x2, 0x2, 
    0x660, 0x663, 0x5, 0xb0, 0x59, 0x2, 0x661, 0x662, 0x7, 0x40, 0x2, 0x2, 
    0x662, 0x664, 0x5, 0xb0, 0x59, 0x2, 0x663, 0x661, 0x3, 0x2, 0x2, 0x2, 
    0x663, 0x664, 0x3, 0x2, 0x2, 0x2, 0x664, 0x665, 0x3, 0x2, 0x2, 0x2, 
    0x665, 0x666, 0x7, 0xda, 0x2, 0x2, 0x666, 0x6a0, 0x3, 0x2, 0x2, 0x2, 
    0x667, 0x668, 0x7, 0xa1, 0x2, 0x2, 0x668, 0x6a0, 0x7, 0xbf, 0x2, 0x2, 
    0x669, 0x66a, 0x7, 0xa6, 0x2, 0x2, 0x66a, 0x66b, 0x7, 0xd0, 0x2, 0x2, 
    0x66b, 0x66c, 0x9, 0x14, 0x2, 0x2, 0x66c, 0x66d, 0x7, 0xbf, 0x2, 0x2, 
    0x66d, 0x66e, 0x7, 0x43, 0x2, 0x2, 0x66e, 0x66f, 0x5, 0xb0, 0x59, 0x2, 
    0x66f, 0x670, 0x7, 0xda, 0x2, 0x2, 0x670, 0x6a0, 0x3, 0x2, 0x2, 0x2, 
    0x671, 0x677, 0x5, 0xd6, 0x6c, 0x2, 0x672, 0x674, 0x7, 0xd0, 0x2, 0x2, 
    0x673, 0x675, 0x5, 0xac, 0x57, 0x2, 0x674, 0x673, 0x3, 0x2, 0x2, 0x2, 
    0x674, 0x675, 0x3, 0x2, 0x2, 0x2, 0x675, 0x676, 0x3, 0x2, 0x2, 0x2, 
    0x676, 0x678, 0x7, 0xda, 0x2, 0x2, 0x677, 0x672, 0x3, 0x2, 0x2, 0x2, 
    0x677, 0x678, 0x3, 0x2, 0x2, 0x2, 0x678, 0x679, 0x3, 0x2, 0x2, 0x2, 
    0x679, 0x67b, 0x7, 0xd0, 0x2, 0x2, 0x67a, 0x67c, 0x7, 0x31, 0x2, 0x2, 
    0x67b, 0x67a, 0x3, 0x2, 0x2, 0x2, 0x67b, 0x67c, 0x3, 0x2, 0x2, 0x2, 
    0x67c, 0x67e, 0x3, 0x2, 0x2, 0x2, 0x67d, 0x67f, 0x5, 0xb2, 0x5a, 0x2, 
    0x67e, 0x67d, 0x3, 0x2, 0x2, 0x2, 0x67e, 0x67f, 0x3, 0x2, 0x2, 0x2, 
    0x67f, 0x680, 0x3, 0x2, 0x2, 0x2, 0x680, 0x681, 0x7, 0xda, 0x2, 0x2, 
    0x681, 0x6a0, 0x3, 0x2, 0x2, 0x2, 0x682, 0x6a0, 0x5, 0xcc, 0x67, 0x2, 
    0x683, 0x684, 0x7, 0xc7, 0x2, 0x2, 0x684, 0x6a0, 0x5, 0xb0, 0x59, 0x13, 
    0x685, 0x686, 0x7, 0x72, 0x2, 0x2, 0x686, 0x6a0, 0x5, 0xb0, 0x59, 0xe, 
    0x687, 0x688, 0x5, 0xc0, 0x61, 0x2, 0x688, 0x689, 0x7, 0xc8, 0x2, 0x2, 
    0x689, 0x68b, 0x3, 0x2, 0x2, 0x2, 0x68a, 0x687, 0x3, 0x2, 0x2, 0x2, 
    0x68a, 0x68b, 0x3, 0x2, 0x2, 0x2, 0x68b, 0x68c, 0x3, 0x2, 0x2, 0x2, 
    0x68c, 0x6a0, 0x7, 0xc1, 0x2, 0x2, 0x68d, 0x68e, 0x7, 0xd0, 0x2, 0x2, 
    0x68e, 0x68f, 0x5, 0x68, 0x35, 0x2, 0x68f, 0x690, 0x7, 0xda, 0x2, 0x2, 
    0x690, 0x6a0, 0x3, 0x2, 0x2, 0x2, 0x691, 0x692, 0x7, 0xd0, 0x2, 0x2, 
    0x692, 0x693, 0x5, 0xb0, 0x59, 0x2, 0x693, 0x694, 0x7, 0xda, 0x2, 0x2, 
    0x694, 0x6a0, 0x3, 0x2, 0x2, 0x2, 0x695, 0x696, 0x7, 0xd0, 0x2, 0x2, 
    0x696, 0x697, 0x5, 0xac, 0x57, 0x2, 0x697, 0x698, 0x7, 0xda, 0x2, 0x2, 
    0x698, 0x6a0, 0x3, 0x2, 0x2, 0x2, 0x699, 0x69b, 0x7, 0xce, 0x2, 0x2, 
    0x69a, 0x69c, 0x5, 0xac, 0x57, 0x2, 0x69b, 0x69a, 0x3, 0x2, 0x2, 0x2, 
    0x69b, 0x69c, 0x3, 0x2, 0x2, 0x2, 0x69c, 0x69d, 0x3, 0x2, 0x2, 0x2, 
    0x69d, 0x6a0, 0x7, 0xd9, 0x2, 0x2, 0x69e, 0x6a0, 0x5, 0xb8, 0x5d, 0x2, 
    0x69f, 0x634, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x648, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x64f, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x651, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x658, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x65c, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x667, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x669, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x671, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x682, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x683, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x685, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x68a, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x68d, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x691, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x695, 0x3, 0x2, 0x2, 0x2, 
    0x69f, 0x699, 0x3, 0x2, 0x2, 0x2, 0x69f, 0x69e, 0x3, 0x2, 0x2, 0x2, 
    0x6a0, 0x6e8, 0x3, 0x2, 0x2, 0x2, 0x6a1, 0x6a2, 0xc, 0x12, 0x2, 0x2, 
    0x6a2, 0x6a3, 0x9, 0x15, 0x2, 0x2, 0x6a3, 0x6e7, 0x5, 0xb0, 0x59, 0x13, 
    0x6a4, 0x6a5, 0xc, 0x11, 0x2, 0x2, 0x6a5, 0x6a6, 0x9, 0x16, 0x2, 0x2, 
    0x6a6, 0x6e7, 0x5, 0xb0, 0x59, 0x12, 0x6a7, 0x6ba, 0xc, 0x10, 0x2, 0x2, 
    0x6a8, 0x6bb, 0x7, 0xc9, 0x2, 0x2, 0x6a9, 0x6bb, 0x7, 0xca, 0x2, 0x2, 
    0x6aa, 0x6bb, 0x7, 0xd2, 0x2, 0x2, 0x6ab, 0x6bb, 0x7, 0xcf, 0x2, 0x2, 
    0x6ac, 0x6bb, 0x7, 0xcb, 0x2, 0x2, 0x6ad, 0x6bb, 0x7, 0xd1, 0x2, 0x2, 
    0x6ae, 0x6bb, 0x7, 0xcc, 0x2, 0x2, 0x6af, 0x6b1, 0x7, 0x46, 0x2, 0x2, 
    0x6b0, 0x6af, 0x3, 0x2, 0x2, 0x2, 0x6b0, 0x6b1, 0x3, 0x2, 0x2, 0x2, 
    0x6b1, 0x6b3, 0x3, 0x2, 0x2, 0x2, 0x6b2, 0x6b4, 0x7, 0x72, 0x2, 0x2, 
    0x6b3, 0x6b2, 0x3, 0x2, 0x2, 0x2, 0x6b3, 0x6b4, 0x3, 0x2, 0x2, 0x2, 
    0x6b4, 0x6b5, 0x3, 0x2, 0x2, 0x2, 0x6b5, 0x6bb, 0x7, 0x4f, 0x2, 0x2, 
    0x6b6, 0x6b8, 0x7, 0x72, 0x2, 0x2, 0x6b7, 0x6b6, 0x3, 0x2, 0x2, 0x2, 
    0x6b7, 0x6b8, 0x3, 0x2, 0x2, 0x2, 0x6b8, 0x6b9, 0x3, 0x2, 0x2, 0x2, 
    0x6b9, 0x6bb, 0x9, 0x17, 0x2, 0x2, 0x6ba, 0x6a8, 0x3, 0x2, 0x2, 0x2, 
    0x6ba, 0x6a9, 0x3, 0x2, 0x2, 0x2, 0x6ba, 0x6aa, 0x3, 0x2, 0x2, 0x2, 
    0x6ba, 0x6ab, 0x3, 0x2, 0x2, 0x2, 0x6ba, 0x6ac, 0x3, 0x2, 0x2, 0x2, 
    0x6ba, 0x6ad, 0x3, 0x2, 0x2, 0x2, 0x6ba, 0x6ae, 0x3, 0x2, 0x2, 0x2, 
    0x6ba, 0x6b0, 0x3, 0x2, 0x2, 0x2, 0x6ba, 0x6b7, 0x3, 0x2, 0x2, 0x2, 
    0x6bb, 0x6bc, 0x3, 0x2, 0x2, 0x2, 0x6bc, 0x6e7, 0x5, 0xb0, 0x59, 0x11, 
    0x6bd, 0x6be, 0xc, 0xd, 0x2, 0x2, 0x6be, 0x6bf, 0x7, 0x8, 0x2, 0x2, 
    0x6bf, 0x6e7, 0x5, 0xb0, 0x59, 0xe, 0x6c0, 0x6c1, 0xc, 0xc, 0x2, 0x2, 
    0x6c1, 0x6c2, 0x7, 0x78, 0x2, 0x2, 0x6c2, 0x6e7, 0x5, 0xb0, 0x59, 0xd, 
    0x6c3, 0x6c5, 0xc, 0xb, 0x2, 0x2, 0x6c4, 0x6c6, 0x7, 0x72, 0x2, 0x2, 
    0x6c5, 0x6c4, 0x3, 0x2, 0x2, 0x2, 0x6c5, 0x6c6, 0x3, 0x2, 0x2, 0x2, 
    0x6c6, 0x6c7, 0x3, 0x2, 0x2, 0x2, 0x6c7, 0x6c8, 0x7, 0x12, 0x2, 0x2, 
    0x6c8, 0x6c9, 0x5, 0xb0, 0x59, 0x2, 0x6c9, 0x6ca, 0x7, 0x8, 0x2, 0x2, 
    0x6ca, 0x6cb, 0x5, 0xb0, 0x59, 0xc, 0x6cb, 0x6e7, 0x3, 0x2, 0x2, 0x2, 
    0x6cc, 0x6cd, 0xc, 0xa, 0x2, 0x2, 0x6cd, 0x6ce, 0x7, 0xd5, 0x2, 0x2, 
    0x6ce, 0x6cf, 0x5, 0xb0, 0x59, 0x2, 0x6cf, 0x6d0, 0x7, 0xc4, 0x2, 0x2, 
    0x6d0, 0x6d1, 0x5, 0xb0, 0x59, 0xa, 0x6d1, 0x6e7, 0x3, 0x2, 0x2, 0x2, 
    0x6d2, 0x6d3, 0xc, 0x15, 0x2, 0x2, 0x6d3, 0x6d4, 0x7, 0xce, 0x2, 0x2, 
    0x6d4, 0x6d5, 0x5, 0xb0, 0x59, 0x2, 0x6d5, 0x6d6, 0x7, 0xd9, 0x2, 0x2, 
    0x6d6, 0x6e7, 0x3, 0x2, 0x2, 0x2, 0x6d7, 0x6d8, 0xc, 0x14, 0x2, 0x2, 
    0x6d8, 0x6d9, 0x7, 0xc8, 0x2, 0x2, 0x6d9, 0x6e7, 0x7, 0xbd, 0x2, 0x2, 
    0x6da, 0x6db, 0xc, 0xf, 0x2, 0x2, 0x6db, 0x6dd, 0x7, 0x57, 0x2, 0x2, 
    0x6dc, 0x6de, 0x7, 0x72, 0x2, 0x2, 0x6dd, 0x6dc, 0x3, 0x2, 0x2, 0x2, 
    0x6dd, 0x6de, 0x3, 0x2, 0x2, 0x2, 0x6de, 0x6df, 0x3, 0x2, 0x2, 0x2, 
    0x6df, 0x6e7, 0x7, 0x73, 0x2, 0x2, 0x6e0, 0x6e4, 0xc, 0x9, 0x2, 0x2, 
    0x6e1, 0x6e5, 0x5, 0xd4, 0x6b, 0x2, 0x6e2, 0x6e3, 0x7, 0xc, 0x2, 0x2, 
    0x6e3, 0x6e5, 0x5, 0xd6, 0x6c, 0x2, 0x6e4, 0x6e1, 0x3, 0x2, 0x2, 0x2, 
    0x6e4, 0x6e2, 0x3, 0x2, 0x2, 0x2, 0x6e5, 0x6e7, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6a1, 0x3, 0x2, 0x2, 0x2, 0x6e6, 0x6a4, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6a7, 0x3, 0x2, 0x2, 0x2, 0x6e6, 0x6bd, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6c0, 0x3, 0x2, 0x2, 0x2, 0x6e6, 0x6c3, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6cc, 0x3, 0x2, 0x2, 0x2, 0x6e6, 0x6d2, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6d7, 0x3, 0x2, 0x2, 0x2, 0x6e6, 0x6da, 0x3, 0x2, 0x2, 0x2, 
    0x6e6, 0x6e0, 0x3, 0x2, 0x2, 0x2, 0x6e7, 0x6ea, 0x3, 0x2, 0x2, 0x2, 
    0x6e8, 0x6e6, 0x3, 0x2, 0x2, 0x2, 0x6e8, 0x6e9, 0x3, 0x2, 0x2, 0x2, 
    0x6e9, 0xb1, 0x3, 0x2, 0x2, 0x2, 0x6ea, 0x6e8, 0x3, 0x2, 0x2, 0x2, 0x6eb, 
    0x6f0, 0x5, 0xb4, 0x5b, 0x2, 0x6ec, 0x6ed, 0x7, 0xc5, 0x2, 0x2, 0x6ed, 
    0x6ef, 0x5, 0xb4, 0x5b, 0x2, 0x6ee, 0x6ec, 0x3, 0x2, 0x2, 0x2, 0x6ef, 
    0x6f2, 0x3, 0x2, 0x2, 0x2, 0x6f0, 0x6ee, 0x3, 0x2, 0x2, 0x2, 0x6f0, 
    0x6f1, 0x3, 0x2, 0x2, 0x2, 0x6f1, 0xb3, 0x3, 0x2, 0x2, 0x2, 0x6f2, 0x6f0, 
    0x3, 0x2, 0x2, 0x2, 0x6f3, 0x6f6, 0x5, 0xb6, 0x5c, 0x2, 0x6f4, 0x6f6, 
    0x5, 0xb0, 0x59, 0x2, 0x6f5, 0x6f3, 0x3, 0x2, 0x2, 0x2, 0x6f5, 0x6f4, 
    0x3, 0x2, 0x2, 0x2, 0x6f6, 0xb5, 0x3, 0x2, 0x2, 0x2, 0x6f7, 0x6f8, 0x7, 
    0xd0, 0x2, 0x2, 0x6f8, 0x6fd, 0x5, 0xd6, 0x6c, 0x2, 0x6f9, 0x6fa, 0x7, 
    0xc5, 0x2, 0x2, 0x6fa, 0x6fc, 0x5, 0xd6, 0x6c, 0x2, 0x6fb, 0x6f9, 0x3, 
    0x2, 0x2, 0x2, 0x6fc, 0x6ff, 0x3, 0x2, 0x2, 0x2, 0x6fd, 0x6fb, 0x3, 
    0x2, 0x2, 0x2, 0x6fd, 0x6fe, 0x3, 0x2, 0x2, 0x2, 0x6fe, 0x700, 0x3, 
    0x2, 0x2, 0x2, 0x6ff, 0x6fd, 0x3, 0x2, 0x2, 0x2, 0x700, 0x701, 0x7, 
    0xda, 0x2, 0x2, 0x701, 0x70b, 0x3, 0x2, 0x2, 0x2, 0x702, 0x707, 0x5, 
    0xd6, 0x6c, 0x2, 0x703, 0x704, 0x7, 0xc5, 0x2, 0x2, 0x704, 0x706, 0x5, 
    0xd6, 0x6c, 0x2, 0x705, 0x703, 0x3, 0x2, 0x2, 0x2, 0x706, 0x709, 0x3, 
    0x2, 0x2, 0x2, 0x707, 0x705, 0x3, 0x2, 0x2, 0x2, 0x707, 0x708, 0x3, 
    0x2, 0x2, 0x2, 0x708, 0x70b, 0x3, 0x2, 0x2, 0x2, 0x709, 0x707, 0x3, 
    0x2, 0x2, 0x2, 0x70a, 0x6f7, 0x3, 0x2, 0x2, 0x2, 0x70a, 0x702, 0x3, 
    0x2, 0x2, 0x2, 0x70b, 0x70c, 0x3, 0x2, 0x2, 0x2, 0x70c, 0x70d, 0x7, 
    0xc0, 0x2, 0x2, 0x70d, 0x70e, 0x5, 0xb0, 0x59, 0x2, 0x70e, 0xb7, 0x3, 
    0x2, 0x2, 0x2, 0x70f, 0x710, 0x5, 0xc0, 0x61, 0x2, 0x710, 0x711, 0x7, 
    0xc8, 0x2, 0x2, 0x711, 0x713, 0x3, 0x2, 0x2, 0x2, 0x712, 0x70f, 0x3, 
    0x2, 0x2, 0x2, 0x712, 0x713, 0x3, 0x2, 0x2, 0x2, 0x713, 0x714, 0x3, 
    0x2, 0x2, 0x2, 0x714, 0x715, 0x5, 0xba, 0x5e, 0x2, 0x715, 0xb9, 0x3, 
    0x2, 0x2, 0x2, 0x716, 0x719, 0x5, 0xd6, 0x6c, 0x2, 0x717, 0x718, 0x7, 
    0xc8, 0x2, 0x2, 0x718, 0x71a, 0x5, 0xd6, 0x6c, 0x2, 0x719, 0x717, 0x3, 
    0x2, 0x2, 0x2, 0x719, 0x71a, 0x3, 0x2, 0x2, 0x2, 0x71a, 0xbb, 0x3, 0x2, 
    0x2, 0x2, 0x71b, 0x71c, 0x8, 0x5f, 0x1, 0x2, 0x71c, 0x723, 0x5, 0xc0, 
    0x61, 0x2, 0x71d, 0x723, 0x5, 0xbe, 0x60, 0x2, 0x71e, 0x71f, 0x7, 0xd0, 
    0x2, 0x2, 0x71f, 0x720, 0x5, 0x68, 0x35, 0x2, 0x720, 0x721, 0x7, 0xda, 
    0x2, 0x2, 0x721, 0x723, 0x3, 0x2, 0x2, 0x2, 0x722, 0x71b, 0x3, 0x2, 
    0x2, 0x2, 0x722, 0x71d, 0x3, 0x2, 0x2, 0x2, 0x722, 0x71e, 0x3, 0x2, 
    0x2, 0x2, 0x723, 0x72c, 0x3, 0x2, 0x2, 0x2, 0x724, 0x728, 0xc, 0x3, 
    0x2, 0x2, 0x725, 0x729, 0x5, 0xd4, 0x6b, 0x2, 0x726, 0x727, 0x7, 0xc, 
    0x2, 0x2, 0x727, 0x729, 0x5, 0xd6, 0x6c, 0x2, 0x728, 0x725, 0x3, 0x2, 
    0x2, 0x2, 0x728, 0x726, 0x3, 0x2, 0x2, 0x2, 0x729, 0x72b, 0x3, 0x2, 
    0x2, 0x2, 0x72a, 0x724, 0x3, 0x2, 0x2, 0x2, 0x72b, 0x72e, 0x3, 0x2, 
    0x2, 0x2, 0x72c, 0x72a, 0x3, 0x2, 0x2, 0x2, 0x72c, 0x72d, 0x3, 0x2, 
    0x2, 0x2, 0x72d, 0xbd, 0x3, 0x2, 0x2, 0x2, 0x72e, 0x72c, 0x3, 0x2, 0x2, 
    0x2, 0x72f, 0x730, 0x5, 0xd6, 0x6c, 0x2, 0x730, 0x732, 0x7, 0xd0, 0x2, 
    0x2, 0x731, 0x733, 0x5, 0xc2, 0x62, 0x2, 0x732, 0x731, 0x3, 0x2, 0x2, 
    0x2, 0x732, 0x733, 0x3, 0x2, 0x2, 0x2, 0x733, 0x734, 0x3, 0x2, 0x2, 
    0x2, 0x734, 0x735, 0x7, 0xda, 0x2, 0x2, 0x735, 0xbf, 0x3, 0x2, 0x2, 
    0x2, 0x736, 0x737, 0x5, 0xc6, 0x64, 0x2, 0x737, 0x738, 0x7, 0xc8, 0x2, 
    0x2, 0x738, 0x73a, 0x3, 0x2, 0x2, 0x2, 0x739, 0x736, 0x3, 0x2, 0x2, 
    0x2, 0x739, 0x73a, 0x3, 0x2, 0x2, 0x2, 0x73a, 0x73b, 0x3, 0x2, 0x2, 
    0x2, 0x73b, 0x73c, 0x5, 0xd6, 0x6c, 0x2, 0x73c, 0xc1, 0x3, 0x2, 0x2, 
    0x2, 0x73d, 0x742, 0x5, 0xc4, 0x63, 0x2, 0x73e, 0x73f, 0x7, 0xc5, 0x2, 
    0x2, 0x73f, 0x741, 0x5, 0xc4, 0x63, 0x2, 0x740, 0x73e, 0x3, 0x2, 0x2, 
    0x2, 0x741, 0x744, 0x3, 0x2, 0x2, 0x2, 0x742, 0x740, 0x3, 0x2, 0x2, 
    0x2, 0x742, 0x743, 0x3, 0x2, 0x2, 0x2, 0x743, 0xc3, 0x3, 0x2, 0x2, 0x2, 
    0x744, 0x742, 0x3, 0x2, 0x2, 0x2, 0x745, 0x749, 0x5, 0xd6, 0x6c, 0x2, 
    0x746, 0x749, 0x5, 0xbe, 0x60, 0x2, 0x747, 0x749, 0x5, 0xcc, 0x67, 0x2, 
    0x748, 0x745, 0x3, 0x2, 0x2, 0x2, 0x748, 0x746, 0x3, 0x2, 0x2, 0x2, 
    0x748, 0x747, 0x3, 0x2, 0x2, 0x2, 0x749, 0xc5, 0x3, 0x2, 0x2, 0x2, 0x74a, 
    0x74b, 0x5, 0xd6, 0x6c, 0x2, 0x74b, 0xc7, 0x3, 0x2, 0x2, 0x2, 0x74c, 
    0x755, 0x7, 0xbb, 0x2, 0x2, 0x74d, 0x74e, 0x7, 0xc8, 0x2, 0x2, 0x74e, 
    0x755, 0x9, 0x18, 0x2, 0x2, 0x74f, 0x750, 0x7, 0xbd, 0x2, 0x2, 0x750, 
    0x752, 0x7, 0xc8, 0x2, 0x2, 0x751, 0x753, 0x9, 0x18, 0x2, 0x2, 0x752, 
    0x751, 0x3, 0x2, 0x2, 0x2, 0x752, 0x753, 0x3, 0x2, 0x2, 0x2, 0x753, 
    0x755, 0x3, 0x2, 0x2, 0x2, 0x754, 0x74c, 0x3, 0x2, 0x2, 0x2, 0x754, 
    0x74d, 0x3, 0x2, 0x2, 0x2, 0x754, 0x74f, 0x3, 0x2, 0x2, 0x2, 0x755, 
    0xc9, 0x3, 0x2, 0x2, 0x2, 0x756, 0x758, 0x9, 0x19, 0x2, 0x2, 0x757, 
    0x756, 0x3, 0x2, 0x2, 0x2, 0x757, 0x758, 0x3, 0x2, 0x2, 0x2, 0x758, 
    0x75f, 0x3, 0x2, 0x2, 0x2, 0x759, 0x760, 0x5, 0xc8, 0x65, 0x2, 0x75a, 
    0x760, 0x7, 0xbc, 0x2, 0x2, 0x75b, 0x760, 0x7, 0xbd, 0x2, 0x2, 0x75c, 
    0x760, 0x7, 0xbe, 0x2, 0x2, 0x75d, 0x760, 0x7, 0x51, 0x2, 0x2, 0x75e, 
    0x760, 0x7, 0x70, 0x2, 0x2, 0x75f, 0x759, 0x3, 0x2, 0x2, 0x2, 0x75f, 
    0x75a, 0x3, 0x2, 0x2, 0x2, 0x75f, 0x75b, 0x3, 0x2, 0x2, 0x2, 0x75f, 
    0x75c, 0x3, 0x2, 0x2, 0x2, 0x75f, 0x75d, 0x3, 0x2, 0x2, 0x2, 0x75f, 
    0x75e, 0x3, 0x2, 0x2, 0x2, 0x760, 0xcb, 0x3, 0x2, 0x2, 0x2, 0x761, 0x765, 
    0x5, 0xca, 0x66, 0x2, 0x762, 0x765, 0x7, 0xbf, 0x2, 0x2, 0x763, 0x765, 
    0x7, 0x73, 0x2, 0x2, 0x764, 0x761, 0x3, 0x2, 0x2, 0x2, 0x764, 0x762, 
    0x3, 0x2, 0x2, 0x2, 0x764, 0x763, 0x3, 0x2, 0x2, 0x2, 0x765, 0xcd, 0x3, 
    0x2, 0x2, 0x2, 0x766, 0x767, 0x9, 0x1a, 0x2, 0x2, 0x767, 0xcf, 0x3, 
    0x2, 0x2, 0x2, 0x768, 0x769, 0x9, 0x1b, 0x2, 0x2, 0x769, 0xd1, 0x3, 
    0x2, 0x2, 0x2, 0x76a, 0x76b, 0x9, 0x1c, 0x2, 0x2, 0x76b, 0xd3, 0x3, 
    0x2, 0x2, 0x2, 0x76c, 0x76f, 0x7, 0xba, 0x2, 0x2, 0x76d, 0x76f, 0x5, 
    0xd2, 0x6a, 0x2, 0x76e, 0x76c, 0x3, 0x2, 0x2, 0x2, 0x76e, 0x76d, 0x3, 
    0x2, 0x2, 0x2, 0x76f, 0xd5, 0x3, 0x2, 0x2, 0x2, 0x770, 0x774, 0x7, 0xba, 
    0x2, 0x2, 0x771, 0x774, 0x5, 0xce, 0x68, 0x2, 0x772, 0x774, 0x5, 0xd0, 
    0x69, 0x2, 0x773, 0x770, 0x3, 0x2, 0x2, 0x2, 0x773, 0x771, 0x3, 0x2, 
    0x2, 0x2, 0x773, 0x772, 0x3, 0x2, 0x2, 0x2, 0x774, 0xd7, 0x3, 0x2, 0x2, 
    0x2, 0x775, 0x778, 0x5, 0xd6, 0x6c, 0x2, 0x776, 0x778, 0x7, 0x73, 0x2, 
    0x2, 0x777, 0x775, 0x3, 0x2, 0x2, 0x2, 0x777, 0x776, 0x3, 0x2, 0x2, 
    0x2, 0x778, 0xd9, 0x3, 0x2, 0x2, 0x2, 0x779, 0x77a, 0x7, 0xbf, 0x2, 
    0x2, 0x77a, 0x77b, 0x7, 0xca, 0x2, 0x2, 0x77b, 0x77c, 0x5, 0xca, 0x66, 
    0x2, 0x77c, 0xdb, 0x3, 0x2, 0x2, 0x2, 0x103, 0xe0, 0xe4, 0xe7, 0xea, 
    0xfe, 0x104, 0x10b, 0x113, 0x118, 0x11f, 0x124, 0x12b, 0x130, 0x136, 
    0x13c, 0x141, 0x147, 0x14c, 0x152, 0x157, 0x15d, 0x16b, 0x172, 0x179, 
    0x180, 0x186, 0x18b, 0x191, 0x196, 0x19c, 0x1a5, 0x1af, 0x1b9, 0x1cd, 
    0x1d5, 0x1e4, 0x1eb, 0x1f9, 0x1ff, 0x205, 0x20c, 0x210, 0x213, 0x21a, 
    0x21e, 0x221, 0x22c, 0x230, 0x233, 0x238, 0x23a, 0x23d, 0x240, 0x24a, 
    0x24e, 0x251, 0x254, 0x259, 0x25b, 0x261, 0x267, 0x26b, 0x26e, 0x271, 
    0x274, 0x277, 0x27c, 0x282, 0x286, 0x289, 0x28c, 0x290, 0x298, 0x2b2, 
    0x2b4, 0x2b8, 0x2ce, 0x2d0, 0x2db, 0x2de, 0x2e7, 0x2f8, 0x303, 0x315, 
    0x322, 0x333, 0x33c, 0x357, 0x359, 0x36e, 0x373, 0x378, 0x37b, 0x387, 
    0x38c, 0x390, 0x393, 0x397, 0x39b, 0x3a0, 0x3a3, 0x3a7, 0x3a9, 0x3bf, 
    0x3c7, 0x3ca, 0x3d4, 0x3d8, 0x3e0, 0x3e4, 0x3e9, 0x3ed, 0x3f1, 0x3f5, 
    0x3f9, 0x3fb, 0x403, 0x407, 0x40a, 0x412, 0x417, 0x41c, 0x41f, 0x429, 
    0x433, 0x437, 0x43c, 0x440, 0x446, 0x449, 0x44c, 0x44f, 0x45d, 0x461, 
    0x465, 0x46a, 0x46d, 0x477, 0x47f, 0x482, 0x486, 0x489, 0x48d, 0x490, 
    0x493, 0x496, 0x499, 0x49d, 0x4a1, 0x4a4, 0x4a7, 0x4aa, 0x4ad, 0x4b0, 
    0x4b9, 0x4bf, 0x4d3, 0x4e9, 0x4f1, 0x4f4, 0x4fa, 0x502, 0x505, 0x50b, 
    0x50d, 0x511, 0x516, 0x519, 0x51c, 0x520, 0x524, 0x527, 0x529, 0x52c, 
    0x530, 0x534, 0x537, 0x539, 0x53b, 0x53e, 0x543, 0x54e, 0x554, 0x559, 
    0x560, 0x565, 0x569, 0x56d, 0x572, 0x579, 0x58e, 0x591, 0x59a, 0x59e, 
    0x5a3, 0x5a8, 0x5ab, 0x5ad, 0x5c3, 0x5c6, 0x5d1, 0x5d5, 0x5d8, 0x5dc, 
    0x5e0, 0x5e8, 0x5ec, 0x5f9, 0x605, 0x611, 0x619, 0x61d, 0x624, 0x62a, 
    0x632, 0x637, 0x640, 0x644, 0x663, 0x674, 0x677, 0x67b, 0x67e, 0x68a, 
    0x69b, 0x69f, 0x6b0, 0x6b3, 0x6b7, 0x6ba, 0x6c5, 0x6dd, 0x6e4, 0x6e6, 
    0x6e8, 0x6f0, 0x6f5, 0x6fd, 0x707, 0x70a, 0x712, 0x719, 0x722, 0x728, 
    0x72c, 0x732, 0x739, 0x742, 0x748, 0x752, 0x754, 0x757, 0x75f, 0x764, 
    0x76e, 0x773, 0x777, 
  };

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

ClickHouseParser::Initializer ClickHouseParser::_init;
