
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
    setState(186);
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
      case ClickHouseParser::OPTIMIZE:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SET:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::USE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 1);
        setState(172);
        query();
        setState(176);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::INTO) {
          setState(173);
          match(ClickHouseParser::INTO);
          setState(174);
          match(ClickHouseParser::OUTFILE);
          setState(175);
          match(ClickHouseParser::STRING_LITERAL);
        }
        setState(180);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::FORMAT) {
          setState(178);
          match(ClickHouseParser::FORMAT);
          setState(179);
          identifierOrNull();
        }
        setState(183);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::SEMICOLON) {
          setState(182);
          match(ClickHouseParser::SEMICOLON);
        }
        break;
      }

      case ClickHouseParser::INSERT: {
        enterOuterAlt(_localctx, 2);
        setState(185);
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
    setState(203);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::ALTER: {
        enterOuterAlt(_localctx, 1);
        setState(188);
        alterStmt();
        break;
      }

      case ClickHouseParser::CHECK: {
        enterOuterAlt(_localctx, 2);
        setState(189);
        checkStmt();
        break;
      }

      case ClickHouseParser::ATTACH:
      case ClickHouseParser::CREATE: {
        enterOuterAlt(_localctx, 3);
        setState(190);
        createStmt();
        break;
      }

      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCRIBE: {
        enterOuterAlt(_localctx, 4);
        setState(191);
        describeStmt();
        break;
      }

      case ClickHouseParser::DETACH:
      case ClickHouseParser::DROP: {
        enterOuterAlt(_localctx, 5);
        setState(192);
        dropStmt();
        break;
      }

      case ClickHouseParser::EXISTS: {
        enterOuterAlt(_localctx, 6);
        setState(193);
        existsStmt();
        break;
      }

      case ClickHouseParser::EXPLAIN: {
        enterOuterAlt(_localctx, 7);
        setState(194);
        explainStmt();
        break;
      }

      case ClickHouseParser::OPTIMIZE: {
        enterOuterAlt(_localctx, 8);
        setState(195);
        optimizeStmt();
        break;
      }

      case ClickHouseParser::RENAME: {
        enterOuterAlt(_localctx, 9);
        setState(196);
        renameStmt();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 10);
        setState(197);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::SET: {
        enterOuterAlt(_localctx, 11);
        setState(198);
        setStmt();
        break;
      }

      case ClickHouseParser::SHOW: {
        enterOuterAlt(_localctx, 12);
        setState(199);
        showStmt();
        break;
      }

      case ClickHouseParser::SYSTEM: {
        enterOuterAlt(_localctx, 13);
        setState(200);
        systemStmt();
        break;
      }

      case ClickHouseParser::TRUNCATE: {
        enterOuterAlt(_localctx, 14);
        setState(201);
        truncateStmt();
        break;
      }

      case ClickHouseParser::USE: {
        enterOuterAlt(_localctx, 15);
        setState(202);
        useStmt();
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
    setState(205);
    match(ClickHouseParser::ALTER);
    setState(206);
    match(ClickHouseParser::TABLE);
    setState(207);
    tableIdentifier();
    setState(208);
    alterTableClause();
    setState(213);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(209);
      match(ClickHouseParser::COMMA);
      setState(210);
      alterTableClause();
      setState(215);
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
//----------------- AlterTableClauseAddContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::ADD() {
  return getToken(ClickHouseParser::ADD, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::AlterTableClauseAddContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseAddContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseAddContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::AlterTableClauseAddContext::AlterTableClauseAddContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseAddContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseAdd(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClauseOrderByContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseOrderByContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseOrderByContext::ORDER() {
  return getToken(ClickHouseParser::ORDER, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseOrderByContext::BY() {
  return getToken(ClickHouseParser::BY, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::AlterTableClauseOrderByContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::AlterTableClauseOrderByContext::AlterTableClauseOrderByContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseOrderByContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseOrderBy(this);
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
//----------------- AlterTableClauseRemoveContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseRemoveContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveContext::REMOVE() {
  return getToken(ClickHouseParser::REMOVE, 0);
}

ClickHouseParser::TableColumnPropertyTypeContext* ClickHouseParser::AlterTableClauseRemoveContext::tableColumnPropertyType() {
  return getRuleContext<ClickHouseParser::TableColumnPropertyTypeContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseRemoveContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClauseRemoveContext::AlterTableClauseRemoveContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseRemoveContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseRemove(this);
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
//----------------- AlterTableClauseClearContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClauseClearContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseClearContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterTableClauseClearContext::AlterTableClauseClearContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseClearContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseClear(this);
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
//----------------- AlterTableClauseTTLContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClauseTTLContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseTTLContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::AlterTableClauseTTLContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::AlterTableClauseTTLContext::AlterTableClauseTTLContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::AlterTableClauseTTLContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClauseTTL(this);
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
    setState(309);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAddContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(216);
      match(ClickHouseParser::ADD);
      setState(217);
      match(ClickHouseParser::COLUMN);
      setState(221);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx)) {
      case 1: {
        setState(218);
        match(ClickHouseParser::IF);
        setState(219);
        match(ClickHouseParser::NOT);
        setState(220);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(223);
      tableColumnDfnt();
      setState(226);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AFTER) {
        setState(224);
        match(ClickHouseParser::AFTER);
        setState(225);
        nestedIdentifier();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAttachContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(228);
      match(ClickHouseParser::ATTACH);
      setState(229);
      partitionClause();
      setState(232);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM) {
        setState(230);
        match(ClickHouseParser::FROM);
        setState(231);
        tableIdentifier();
      }
      break;
    }

    case 3: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseClearContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(234);
      match(ClickHouseParser::CLEAR);
      setState(235);
      match(ClickHouseParser::COLUMN);
      setState(238);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
      case 1: {
        setState(236);
        match(ClickHouseParser::IF);
        setState(237);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(240);
      nestedIdentifier();
      setState(243);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::IN) {
        setState(241);
        match(ClickHouseParser::IN);
        setState(242);
        partitionClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseCommentContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(245);
      match(ClickHouseParser::COMMENT);
      setState(246);
      match(ClickHouseParser::COLUMN);
      setState(249);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
      case 1: {
        setState(247);
        match(ClickHouseParser::IF);
        setState(248);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(251);
      nestedIdentifier();
      setState(252);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDeleteContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(254);
      match(ClickHouseParser::DELETE);
      setState(255);
      match(ClickHouseParser::WHERE);
      setState(256);
      columnExpr(0);
      break;
    }

    case 6: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDetachContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(257);
      match(ClickHouseParser::DETACH);
      setState(258);
      partitionClause();
      break;
    }

    case 7: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropColumnContext>(_localctx));
      enterOuterAlt(_localctx, 7);
      setState(259);
      match(ClickHouseParser::DROP);
      setState(260);
      match(ClickHouseParser::COLUMN);
      setState(263);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
      case 1: {
        setState(261);
        match(ClickHouseParser::IF);
        setState(262);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(265);
      nestedIdentifier();
      break;
    }

    case 8: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropPartitionContext>(_localctx));
      enterOuterAlt(_localctx, 8);
      setState(266);
      match(ClickHouseParser::DROP);
      setState(267);
      partitionClause();
      break;
    }

    case 9: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyContext>(_localctx));
      enterOuterAlt(_localctx, 9);
      setState(268);
      match(ClickHouseParser::MODIFY);
      setState(269);
      match(ClickHouseParser::COLUMN);
      setState(272);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx)) {
      case 1: {
        setState(270);
        match(ClickHouseParser::IF);
        setState(271);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(274);
      tableColumnDfnt();
      break;
    }

    case 10: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseOrderByContext>(_localctx));
      enterOuterAlt(_localctx, 10);
      setState(275);
      match(ClickHouseParser::MODIFY);
      setState(276);
      match(ClickHouseParser::ORDER);
      setState(277);
      match(ClickHouseParser::BY);
      setState(278);
      columnExpr(0);
      break;
    }

    case 11: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseRemoveContext>(_localctx));
      enterOuterAlt(_localctx, 11);
      setState(279);
      match(ClickHouseParser::MODIFY);
      setState(280);
      match(ClickHouseParser::COLUMN);
      setState(283);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
      case 1: {
        setState(281);
        match(ClickHouseParser::IF);
        setState(282);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(285);
      nestedIdentifier();
      setState(286);
      match(ClickHouseParser::REMOVE);
      setState(287);
      tableColumnPropertyType();
      break;
    }

    case 12: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseTTLContext>(_localctx));
      enterOuterAlt(_localctx, 12);
      setState(289);
      match(ClickHouseParser::MODIFY);
      setState(290);
      match(ClickHouseParser::TTL);
      setState(291);
      columnExpr(0);
      break;
    }

    case 13: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseRemoveTTLContext>(_localctx));
      enterOuterAlt(_localctx, 13);
      setState(292);
      match(ClickHouseParser::REMOVE);
      setState(293);
      match(ClickHouseParser::TTL);
      break;
    }

    case 14: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseRenameContext>(_localctx));
      enterOuterAlt(_localctx, 14);
      setState(294);
      match(ClickHouseParser::RENAME);
      setState(295);
      match(ClickHouseParser::COLUMN);
      setState(298);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 15, _ctx)) {
      case 1: {
        setState(296);
        match(ClickHouseParser::IF);
        setState(297);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(300);
      nestedIdentifier();
      setState(301);
      match(ClickHouseParser::TO);
      setState(302);
      nestedIdentifier();
      break;
    }

    case 15: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseReplaceContext>(_localctx));
      enterOuterAlt(_localctx, 15);
      setState(304);
      match(ClickHouseParser::REPLACE);
      setState(305);
      partitionClause();
      setState(306);
      match(ClickHouseParser::FROM);
      setState(307);
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
  enterRule(_localctx, 8, ClickHouseParser::RuleTableColumnPropertyType);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(311);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::CODEC)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::DEFAULT))) != 0) || _la == ClickHouseParser::MATERIALIZED

    || _la == ClickHouseParser::TTL)) {
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
  enterRule(_localctx, 10, ClickHouseParser::RulePartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(318);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(313);
      match(ClickHouseParser::PARTITION);
      setState(314);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(315);
      match(ClickHouseParser::PARTITION);
      setState(316);
      match(ClickHouseParser::ID);
      setState(317);
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
  enterRule(_localctx, 12, ClickHouseParser::RuleCheckStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(320);
    match(ClickHouseParser::CHECK);
    setState(321);
    match(ClickHouseParser::TABLE);
    setState(322);
    tableIdentifier();
   
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

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::CreateViewStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::SchemaClauseContext* ClickHouseParser::CreateViewStmtContext::schemaClause() {
  return getRuleContext<ClickHouseParser::SchemaClauseContext>(0);
}

ClickHouseParser::CreateViewStmtContext::CreateViewStmtContext(CreateStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::CreateViewStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateViewStmt(this);
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

ClickHouseParser::SchemaClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::schemaClause() {
  return getRuleContext<ClickHouseParser::SchemaClauseContext>(0);
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

ClickHouseParser::SchemaClauseContext* ClickHouseParser::CreateTableStmtContext::schemaClause() {
  return getRuleContext<ClickHouseParser::SchemaClauseContext>(0);
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
  enterRule(_localctx, 14, ClickHouseParser::RuleCreateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(389);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(324);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(325);
      match(ClickHouseParser::DATABASE);
      setState(329);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
      case 1: {
        setState(326);
        match(ClickHouseParser::IF);
        setState(327);
        match(ClickHouseParser::NOT);
        setState(328);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(331);
      databaseIdentifier();
      setState(333);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(332);
        engineExpr();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateMaterializedViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(335);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(336);
      match(ClickHouseParser::MATERIALIZED);
      setState(337);
      match(ClickHouseParser::VIEW);
      setState(341);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx)) {
      case 1: {
        setState(338);
        match(ClickHouseParser::IF);
        setState(339);
        match(ClickHouseParser::NOT);
        setState(340);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(343);
      tableIdentifier();
      setState(345);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS || _la == ClickHouseParser::LPAREN) {
        setState(344);
        schemaClause();
      }
      setState(352);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::TO: {
          setState(347);
          destinationClause();
          break;
        }

        case ClickHouseParser::ENGINE: {
          setState(348);
          engineClause();
          setState(350);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::POPULATE) {
            setState(349);
            match(ClickHouseParser::POPULATE);
          }
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(354);
      subqueryClause();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(356);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(358);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(357);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(360);
      match(ClickHouseParser::TABLE);
      setState(364);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx)) {
      case 1: {
        setState(361);
        match(ClickHouseParser::IF);
        setState(362);
        match(ClickHouseParser::NOT);
        setState(363);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(366);
      tableIdentifier();
      setState(368);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
      case 1: {
        setState(367);
        schemaClause();
        break;
      }

      }
      setState(371);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(370);
        engineClause();
      }
      setState(374);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS) {
        setState(373);
        subqueryClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(376);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(377);
      match(ClickHouseParser::VIEW);
      setState(381);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx)) {
      case 1: {
        setState(378);
        match(ClickHouseParser::IF);
        setState(379);
        match(ClickHouseParser::NOT);
        setState(380);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(383);
      tableIdentifier();
      setState(385);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
      case 1: {
        setState(384);
        schemaClause();
        break;
      }

      }
      setState(387);
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
  enterRule(_localctx, 16, ClickHouseParser::RuleDestinationClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(391);
    match(ClickHouseParser::TO);
    setState(392);
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
  enterRule(_localctx, 18, ClickHouseParser::RuleSubqueryClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(394);
    match(ClickHouseParser::AS);
    setState(395);
    selectUnionStmt();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SchemaClauseContext ------------------------------------------------------------------

ClickHouseParser::SchemaClauseContext::SchemaClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::SchemaClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleSchemaClause;
}

void ClickHouseParser::SchemaClauseContext::copyFrom(SchemaClauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SchemaAsTableClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaAsTableClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SchemaAsTableClauseContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SchemaAsTableClauseContext::SchemaAsTableClauseContext(SchemaClauseContext *ctx) { copyFrom(ctx); }

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

ClickHouseParser::SchemaAsFunctionClauseContext::SchemaAsFunctionClauseContext(SchemaClauseContext *ctx) { copyFrom(ctx); }

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

ClickHouseParser::SchemaDescriptionClauseContext::SchemaDescriptionClauseContext(SchemaClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::SchemaDescriptionClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSchemaDescriptionClause(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::SchemaClauseContext* ClickHouseParser::schemaClause() {
  SchemaClauseContext *_localctx = _tracker.createInstance<SchemaClauseContext>(_ctx, getState());
  enterRule(_localctx, 20, ClickHouseParser::RuleSchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(412);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaDescriptionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(397);
      match(ClickHouseParser::LPAREN);
      setState(398);
      tableElementExpr();
      setState(403);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(399);
        match(ClickHouseParser::COMMA);
        setState(400);
        tableElementExpr();
        setState(405);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(406);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsTableClauseContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(408);
      match(ClickHouseParser::AS);
      setState(409);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsFunctionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(410);
      match(ClickHouseParser::AS);
      setState(411);
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
  enterRule(_localctx, 22, ClickHouseParser::RuleEngineClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(414);
    engineExpr();
    setState(441);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(439);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
        case 1: {
          setState(415);

          if (!(!_localctx->clauses.count("orderByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"orderByClause\")");
          setState(416);
          orderByClause();
          _localctx->clauses.insert("orderByClause");
          break;
        }

        case 2: {
          setState(419);

          if (!(!_localctx->clauses.count("partitionByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"partitionByClause\")");
          setState(420);
          partitionByClause();
          _localctx->clauses.insert("partitionByClause");
          break;
        }

        case 3: {
          setState(423);

          if (!(!_localctx->clauses.count("primaryKeyClause"))) throw FailedPredicateException(this, "!$clauses.count(\"primaryKeyClause\")");
          setState(424);
          primaryKeyClause();
          _localctx->clauses.insert("primaryKeyClause");
          break;
        }

        case 4: {
          setState(427);

          if (!(!_localctx->clauses.count("sampleByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"sampleByClause\")");
          setState(428);
          sampleByClause();
          _localctx->clauses.insert("sampleByClause");
          break;
        }

        case 5: {
          setState(431);

          if (!(!_localctx->clauses.count("ttlClause"))) throw FailedPredicateException(this, "!$clauses.count(\"ttlClause\")");
          setState(432);
          ttlClause();
          _localctx->clauses.insert("ttlClause");
          break;
        }

        case 6: {
          setState(435);

          if (!(!_localctx->clauses.count("settingsClause"))) throw FailedPredicateException(this, "!$clauses.count(\"settingsClause\")");
          setState(436);
          settingsClause();
          _localctx->clauses.insert("settingsClause");
          break;
        }

        } 
      }
      setState(443);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx);
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
  enterRule(_localctx, 24, ClickHouseParser::RulePartitionByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(444);
    match(ClickHouseParser::PARTITION);
    setState(445);
    match(ClickHouseParser::BY);
    setState(446);
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
  enterRule(_localctx, 26, ClickHouseParser::RulePrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(448);
    match(ClickHouseParser::PRIMARY);
    setState(449);
    match(ClickHouseParser::KEY);
    setState(450);
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
  enterRule(_localctx, 28, ClickHouseParser::RuleSampleByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(452);
    match(ClickHouseParser::SAMPLE);
    setState(453);
    match(ClickHouseParser::BY);
    setState(454);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleTtlClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(456);
    match(ClickHouseParser::TTL);
    setState(457);
    ttlExpr();
    setState(462);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(458);
        match(ClickHouseParser::COMMA);
        setState(459);
        ttlExpr(); 
      }
      setState(464);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx);
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
  enterRule(_localctx, 32, ClickHouseParser::RuleEngineExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(465);
    match(ClickHouseParser::ENGINE);
    setState(467);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EQ_SINGLE) {
      setState(466);
      match(ClickHouseParser::EQ_SINGLE);
    }
    setState(469);
    identifierOrNull();
    setState(475);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx)) {
    case 1: {
      setState(470);
      match(ClickHouseParser::LPAREN);
      setState(472);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
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
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(471);
        columnExprList();
      }
      setState(474);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::TableElementExprIndexContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableElementExprIndexContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprIndexContext::TYPE() {
  return getToken(ClickHouseParser::TYPE, 0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::TableElementExprIndexContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprIndexContext::GRANULARITY() {
  return getToken(ClickHouseParser::GRANULARITY, 0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprIndexContext::DECIMAL_LITERAL() {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, 0);
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
  enterRule(_localctx, 34, ClickHouseParser::RuleTableElementExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(491);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(477);
      tableColumnDfnt();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprConstraintContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(478);
      match(ClickHouseParser::CONSTRAINT);
      setState(479);
      identifier();
      setState(480);
      match(ClickHouseParser::CHECK);
      setState(481);
      columnExpr(0);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprIndexContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(483);
      match(ClickHouseParser::INDEX);
      setState(484);
      identifier();
      setState(485);
      columnExpr(0);
      setState(486);
      match(ClickHouseParser::TYPE);
      setState(487);
      columnTypeExpr();
      setState(488);
      match(ClickHouseParser::GRANULARITY);
      setState(489);
      match(ClickHouseParser::DECIMAL_LITERAL);
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
  enterRule(_localctx, 36, ClickHouseParser::RuleTableColumnDfnt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(525);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(493);
      nestedIdentifier();
      setState(494);
      columnTypeExpr();
      setState(496);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ALIAS

      || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED) {
        setState(495);
        tableColumnPropertyExpr();
      }
      setState(500);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(498);
        match(ClickHouseParser::COMMENT);
        setState(499);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(503);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::CODEC) {
        setState(502);
        codecExpr();
      }
      setState(507);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(505);
        match(ClickHouseParser::TTL);
        setState(506);
        columnExpr(0);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(509);
      nestedIdentifier();
      setState(511);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
      case 1: {
        setState(510);
        columnTypeExpr();
        break;
      }

      }
      setState(513);
      tableColumnPropertyExpr();
      setState(516);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(514);
        match(ClickHouseParser::COMMENT);
        setState(515);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(519);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::CODEC) {
        setState(518);
        codecExpr();
      }
      setState(523);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(521);
        match(ClickHouseParser::TTL);
        setState(522);
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
  enterRule(_localctx, 38, ClickHouseParser::RuleTableColumnPropertyExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(527);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::ALIAS

    || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(528);
    columnExpr(0);
   
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

std::vector<tree::TerminalNode *> ClickHouseParser::CodecExprContext::LPAREN() {
  return getTokens(ClickHouseParser::LPAREN);
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::LPAREN(size_t i) {
  return getToken(ClickHouseParser::LPAREN, i);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::CodecExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::CodecExprContext::RPAREN() {
  return getTokens(ClickHouseParser::RPAREN);
}

tree::TerminalNode* ClickHouseParser::CodecExprContext::RPAREN(size_t i) {
  return getToken(ClickHouseParser::RPAREN, i);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::CodecExprContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
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
  enterRule(_localctx, 40, ClickHouseParser::RuleCodecExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(530);
    match(ClickHouseParser::CODEC);
    setState(531);
    match(ClickHouseParser::LPAREN);
    setState(532);
    identifier();
    setState(538);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(533);
      match(ClickHouseParser::LPAREN);
      setState(535);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
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
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(534);
        columnExprList();
      }
      setState(537);
      match(ClickHouseParser::RPAREN);
    }
    setState(540);
    match(ClickHouseParser::RPAREN);
   
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
  enterRule(_localctx, 42, ClickHouseParser::RuleTtlExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(542);
    columnExpr(0);
    setState(550);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
    case 1: {
      setState(543);
      match(ClickHouseParser::DELETE);
      break;
    }

    case 2: {
      setState(544);
      match(ClickHouseParser::TO);
      setState(545);
      match(ClickHouseParser::DISK);
      setState(546);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 3: {
      setState(547);
      match(ClickHouseParser::TO);
      setState(548);
      match(ClickHouseParser::VOLUME);
      setState(549);
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
  enterRule(_localctx, 44, ClickHouseParser::RuleDescribeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(552);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DESC

    || _la == ClickHouseParser::DESCRIBE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(554);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
    case 1: {
      setState(553);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(556);
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

ClickHouseParser::DropDatabaseStmtContext::DropDatabaseStmtContext(DropStmtContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DropDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDropDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::DropTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DETACH() {
  return getToken(ClickHouseParser::DETACH, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::NO() {
  return getToken(ClickHouseParser::NO, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DELAY() {
  return getToken(ClickHouseParser::DELAY, 0);
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
  enterRule(_localctx, 46, ClickHouseParser::RuleDropStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(579);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 58, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(558);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(559);
      match(ClickHouseParser::DATABASE);
      setState(562);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
      case 1: {
        setState(560);
        match(ClickHouseParser::IF);
        setState(561);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(564);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(565);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(567);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(566);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(569);
      match(ClickHouseParser::TABLE);
      setState(572);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 56, _ctx)) {
      case 1: {
        setState(570);
        match(ClickHouseParser::IF);
        setState(571);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(574);
      tableIdentifier();
      setState(577);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NO) {
        setState(575);
        match(ClickHouseParser::NO);
        setState(576);
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

tree::TerminalNode* ClickHouseParser::ExistsStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::ExistsStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ExistsStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ExistsStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}


size_t ClickHouseParser::ExistsStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleExistsStmt;
}

antlrcpp::Any ClickHouseParser::ExistsStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExistsStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ExistsStmtContext* ClickHouseParser::existsStmt() {
  ExistsStmtContext *_localctx = _tracker.createInstance<ExistsStmtContext>(_ctx, getState());
  enterRule(_localctx, 48, ClickHouseParser::RuleExistsStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(581);
    match(ClickHouseParser::EXISTS);
    setState(583);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TEMPORARY) {
      setState(582);
      match(ClickHouseParser::TEMPORARY);
    }
    setState(585);
    match(ClickHouseParser::TABLE);
    setState(586);
    tableIdentifier();
   
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

tree::TerminalNode* ClickHouseParser::ExplainStmtContext::EXPLAIN() {
  return getToken(ClickHouseParser::EXPLAIN, 0);
}

tree::TerminalNode* ClickHouseParser::ExplainStmtContext::SYNTAX() {
  return getToken(ClickHouseParser::SYNTAX, 0);
}

ClickHouseParser::QueryContext* ClickHouseParser::ExplainStmtContext::query() {
  return getRuleContext<ClickHouseParser::QueryContext>(0);
}


size_t ClickHouseParser::ExplainStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleExplainStmt;
}

antlrcpp::Any ClickHouseParser::ExplainStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitExplainStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ExplainStmtContext* ClickHouseParser::explainStmt() {
  ExplainStmtContext *_localctx = _tracker.createInstance<ExplainStmtContext>(_ctx, getState());
  enterRule(_localctx, 50, ClickHouseParser::RuleExplainStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(588);
    match(ClickHouseParser::EXPLAIN);
    setState(589);
    match(ClickHouseParser::SYNTAX);
    setState(590);
    query();
   
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
  enterRule(_localctx, 52, ClickHouseParser::RuleInsertStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(592);
    match(ClickHouseParser::INSERT);
    setState(593);
    match(ClickHouseParser::INTO);
    setState(595);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 60, _ctx)) {
    case 1: {
      setState(594);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(600);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 61, _ctx)) {
    case 1: {
      setState(597);
      tableIdentifier();
      break;
    }

    case 2: {
      setState(598);
      match(ClickHouseParser::FUNCTION);
      setState(599);
      tableFunctionExpr();
      break;
    }

    }
    setState(603);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx)) {
    case 1: {
      setState(602);
      columnsClause();
      break;
    }

    }
    setState(605);
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
  enterRule(_localctx, 54, ClickHouseParser::RuleColumnsClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(607);
    match(ClickHouseParser::LPAREN);
    setState(608);
    nestedIdentifier();
    setState(613);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(609);
      match(ClickHouseParser::COMMA);
      setState(610);
      nestedIdentifier();
      setState(615);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(616);
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

ClickHouseParser::DataClauseSelectContext::DataClauseSelectContext(DataClauseContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataClauseSelectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataClauseSelect(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::DataClauseContext* ClickHouseParser::dataClause() {
  DataClauseContext *_localctx = _tracker.createInstance<DataClauseContext>(_ctx, getState());
  enterRule(_localctx, 56, ClickHouseParser::RuleDataClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(622);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FORMAT: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseFormatContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(618);
        match(ClickHouseParser::FORMAT);
        setState(619);
        identifier();
        break;
      }

      case ClickHouseParser::VALUES: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseValuesContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(620);
        match(ClickHouseParser::VALUES);
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseSelectContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(621);
        selectUnionStmt();
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
  enterRule(_localctx, 58, ClickHouseParser::RuleOptimizeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(624);
    match(ClickHouseParser::OPTIMIZE);
    setState(625);
    match(ClickHouseParser::TABLE);
    setState(626);
    tableIdentifier();
    setState(628);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(627);
      partitionClause();
    }
    setState(631);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(630);
      match(ClickHouseParser::FINAL);
    }
    setState(634);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DEDUPLICATE) {
      setState(633);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleRenameStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(636);
    match(ClickHouseParser::RENAME);
    setState(637);
    match(ClickHouseParser::TABLE);
    setState(638);
    tableIdentifier();
    setState(639);
    match(ClickHouseParser::TO);
    setState(640);
    tableIdentifier();
    setState(648);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(641);
      match(ClickHouseParser::COMMA);
      setState(642);
      tableIdentifier();
      setState(643);
      match(ClickHouseParser::TO);
      setState(644);
      tableIdentifier();
      setState(650);
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
  enterRule(_localctx, 62, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(651);
    selectStmtWithParens();
    setState(657);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(652);
      match(ClickHouseParser::UNION);
      setState(653);
      match(ClickHouseParser::ALL);
      setState(654);
      selectStmtWithParens();
      setState(659);
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
  enterRule(_localctx, 64, ClickHouseParser::RuleSelectStmtWithParens);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(665);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 1);
        setState(660);
        selectStmt();
        break;
      }

      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 2);
        setState(661);
        match(ClickHouseParser::LPAREN);
        setState(662);
        selectUnionStmt();
        setState(663);
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
  enterRule(_localctx, 66, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(668);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(667);
      withClause();
    }
    setState(670);
    match(ClickHouseParser::SELECT);
    setState(672);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx)) {
    case 1: {
      setState(671);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(674);
    columnExprList();
    setState(676);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(675);
      fromClause();
    }
    setState(679);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY || _la == ClickHouseParser::INNER

    || _la == ClickHouseParser::LEFT) {
      setState(678);
      arrayJoinClause();
    }
    setState(682);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(681);
      prewhereClause();
    }
    setState(685);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(684);
      whereClause();
    }
    setState(688);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(687);
      groupByClause();
    }
    setState(692);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx)) {
    case 1: {
      setState(690);
      match(ClickHouseParser::WITH);
      setState(691);
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
    setState(696);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(694);
      match(ClickHouseParser::WITH);
      setState(695);
      match(ClickHouseParser::TOTALS);
    }
    setState(699);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(698);
      havingClause();
    }
    setState(702);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(701);
      orderByClause();
    }
    setState(705);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 82, _ctx)) {
    case 1: {
      setState(704);
      limitByClause();
      break;
    }

    }
    setState(708);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(707);
      limitClause();
    }
    setState(711);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(710);
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
  enterRule(_localctx, 68, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(713);
    match(ClickHouseParser::WITH);
    setState(714);
    columnExprList();
   
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
  enterRule(_localctx, 70, ClickHouseParser::RuleFromClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(716);
    match(ClickHouseParser::FROM);
    setState(717);
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
  enterRule(_localctx, 72, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(720);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INNER

    || _la == ClickHouseParser::LEFT) {
      setState(719);
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
    setState(722);
    match(ClickHouseParser::ARRAY);
    setState(723);
    match(ClickHouseParser::JOIN);
    setState(724);
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
  enterRule(_localctx, 74, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(726);
    match(ClickHouseParser::PREWHERE);
    setState(727);
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
  enterRule(_localctx, 76, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(729);
    match(ClickHouseParser::WHERE);
    setState(730);
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

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::ROLLUP() {
  return getToken(ClickHouseParser::ROLLUP, 0);
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
  enterRule(_localctx, 78, ClickHouseParser::RuleGroupByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(732);
    match(ClickHouseParser::GROUP);
    setState(733);
    match(ClickHouseParser::BY);
    setState(740);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 86, _ctx)) {
    case 1: {
      setState(734);
      match(ClickHouseParser::ROLLUP);
      setState(735);
      match(ClickHouseParser::LPAREN);
      setState(736);
      columnExprList();
      setState(737);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      setState(739);
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
  enterRule(_localctx, 80, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(742);
    match(ClickHouseParser::HAVING);
    setState(743);
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
  enterRule(_localctx, 82, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(745);
    match(ClickHouseParser::ORDER);
    setState(746);
    match(ClickHouseParser::BY);
    setState(747);
    orderExprList();
   
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
  enterRule(_localctx, 84, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(749);
    match(ClickHouseParser::LIMIT);
    setState(750);
    limitExpr();
    setState(751);
    match(ClickHouseParser::BY);
    setState(752);
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
  enterRule(_localctx, 86, ClickHouseParser::RuleLimitClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(754);
    match(ClickHouseParser::LIMIT);
    setState(755);
    limitExpr();
    setState(758);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(756);
      match(ClickHouseParser::WITH);
      setState(757);
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
  enterRule(_localctx, 88, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(760);
    match(ClickHouseParser::SETTINGS);
    setState(761);
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
  size_t startState = 90;
  enterRecursionRule(_localctx, 90, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(775);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 90, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(764);
      tableExpr(0);
      setState(766);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx)) {
      case 1: {
        setState(765);
        match(ClickHouseParser::FINAL);
        break;
      }

      }
      setState(769);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 89, _ctx)) {
      case 1: {
        setState(768);
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
      setState(771);
      match(ClickHouseParser::LPAREN);
      setState(772);
      joinExpr(0);
      setState(773);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(794);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 94, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(792);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 93, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(777);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(778);
          joinOpCross();
          setState(779);
          joinExpr(4);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(781);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(783);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(782);
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
          setState(786);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if ((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
            | (1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF)
            | (1ULL << ClickHouseParser::FULL))) != 0) || ((((_la - 72) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 72)) & ((1ULL << (ClickHouseParser::INNER - 72))
            | (1ULL << (ClickHouseParser::LEFT - 72))
            | (1ULL << (ClickHouseParser::RIGHT - 72))
            | (1ULL << (ClickHouseParser::SEMI - 72)))) != 0)) {
            setState(785);
            joinOp();
          }
          setState(788);
          match(ClickHouseParser::JOIN);
          setState(789);
          joinExpr(0);
          setState(790);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(796);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 94, _ctx);
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
  enterRule(_localctx, 92, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(840);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 108, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(806);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 97, _ctx)) {
      case 1: {
        setState(798);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0)) {
          setState(797);
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
        setState(800);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(801);
        match(ClickHouseParser::INNER);
        setState(803);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0)) {
          setState(802);
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
        setState(805);
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
      setState(822);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 102, _ctx)) {
      case 1: {
        setState(809);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI) {
          setState(808);
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
        setState(811);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(813);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(812);
          match(ClickHouseParser::OUTER);
        }
        break;
      }

      case 2: {
        setState(815);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(817);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(816);
          match(ClickHouseParser::OUTER);
        }
        setState(820);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::SEMI) {
          setState(819);
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
      setState(838);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 107, _ctx)) {
      case 1: {
        setState(825);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ALL

        || _la == ClickHouseParser::ANY) {
          setState(824);
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
        setState(827);
        match(ClickHouseParser::FULL);
        setState(829);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(828);
          match(ClickHouseParser::OUTER);
        }
        break;
      }

      case 2: {
        setState(831);
        match(ClickHouseParser::FULL);
        setState(833);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::OUTER) {
          setState(832);
          match(ClickHouseParser::OUTER);
        }
        setState(836);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ALL

        || _la == ClickHouseParser::ANY) {
          setState(835);
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
  enterRule(_localctx, 94, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(848);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(843);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(842);
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
        setState(845);
        match(ClickHouseParser::CROSS);
        setState(846);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(847);
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
  enterRule(_localctx, 96, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(859);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 111, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(850);
      match(ClickHouseParser::ON);
      setState(851);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(852);
      match(ClickHouseParser::USING);
      setState(853);
      match(ClickHouseParser::LPAREN);
      setState(854);
      columnExprList();
      setState(855);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(857);
      match(ClickHouseParser::USING);
      setState(858);
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
  enterRule(_localctx, 98, ClickHouseParser::RuleSampleClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(861);
    match(ClickHouseParser::SAMPLE);
    setState(862);
    ratioExpr();
    setState(865);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 112, _ctx)) {
    case 1: {
      setState(863);
      match(ClickHouseParser::OFFSET);
      setState(864);
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

std::vector<tree::TerminalNode *> ClickHouseParser::LimitExprContext::DECIMAL_LITERAL() {
  return getTokens(ClickHouseParser::DECIMAL_LITERAL);
}

tree::TerminalNode* ClickHouseParser::LimitExprContext::DECIMAL_LITERAL(size_t i) {
  return getToken(ClickHouseParser::DECIMAL_LITERAL, i);
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
  enterRule(_localctx, 100, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(867);
    match(ClickHouseParser::DECIMAL_LITERAL);
    setState(870);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET || _la == ClickHouseParser::COMMA) {
      setState(868);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(869);
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
  enterRule(_localctx, 102, ClickHouseParser::RuleOrderExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(872);
    orderExpr();
    setState(877);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 114, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(873);
        match(ClickHouseParser::COMMA);
        setState(874);
        orderExpr(); 
      }
      setState(879);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 114, _ctx);
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
  enterRule(_localctx, 104, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(880);
    columnExpr(0);
    setState(882);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx)) {
    case 1: {
      setState(881);
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
    setState(886);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx)) {
    case 1: {
      setState(884);
      match(ClickHouseParser::NULLS);
      setState(885);
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
    setState(890);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 117, _ctx)) {
    case 1: {
      setState(888);
      match(ClickHouseParser::COLLATE);
      setState(889);
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
  enterRule(_localctx, 106, ClickHouseParser::RuleRatioExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(892);
    numberLiteral();
    setState(895);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 118, _ctx)) {
    case 1: {
      setState(893);
      match(ClickHouseParser::SLASH);
      setState(894);
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
  enterRule(_localctx, 108, ClickHouseParser::RuleSettingExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(897);
    settingExpr();
    setState(902);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 119, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(898);
        match(ClickHouseParser::COMMA);
        setState(899);
        settingExpr(); 
      }
      setState(904);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 119, _ctx);
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
  enterRule(_localctx, 110, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(905);
    identifier();
    setState(906);
    match(ClickHouseParser::EQ_SINGLE);
    setState(907);
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
  enterRule(_localctx, 112, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(909);
    match(ClickHouseParser::SET);
    setState(910);
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
ClickHouseParser::ShowStmtContext* ClickHouseParser::showStmt() {
  ShowStmtContext *_localctx = _tracker.createInstance<ShowStmtContext>(_ctx, getState());
  enterRule(_localctx, 114, ClickHouseParser::RuleShowStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(944);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 126, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(912);
      match(ClickHouseParser::SHOW);
      setState(913);
      match(ClickHouseParser::CREATE);
      setState(914);
      match(ClickHouseParser::DATABASE);
      setState(915);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(916);
      match(ClickHouseParser::SHOW);
      setState(917);
      match(ClickHouseParser::CREATE);
      setState(919);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 120, _ctx)) {
      case 1: {
        setState(918);
        match(ClickHouseParser::TEMPORARY);
        break;
      }

      }
      setState(922);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 121, _ctx)) {
      case 1: {
        setState(921);
        match(ClickHouseParser::TABLE);
        break;
      }

      }
      setState(924);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowTablesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(925);
      match(ClickHouseParser::SHOW);
      setState(927);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(926);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(929);
      match(ClickHouseParser::TABLES);
      setState(932);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM

      || _la == ClickHouseParser::IN) {
        setState(930);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::FROM

        || _la == ClickHouseParser::IN)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(931);
        databaseIdentifier();
      }
      setState(937);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::LIKE: {
          setState(934);
          match(ClickHouseParser::LIKE);
          setState(935);
          match(ClickHouseParser::STRING_LITERAL);
          break;
        }

        case ClickHouseParser::WHERE: {
          setState(936);
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
      setState(940);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::LIMIT) {
        setState(939);
        limitClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowDatabasesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(942);
      match(ClickHouseParser::SHOW);
      setState(943);
      match(ClickHouseParser::DATABASES);
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
  enterRule(_localctx, 116, ClickHouseParser::RuleSystemStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(970);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 128, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(946);
      match(ClickHouseParser::SYSTEM);
      setState(947);
      match(ClickHouseParser::FLUSH);
      setState(948);
      match(ClickHouseParser::DISTRIBUTED);
      setState(949);
      tableIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(950);
      match(ClickHouseParser::SYSTEM);
      setState(951);
      match(ClickHouseParser::FLUSH);
      setState(952);
      match(ClickHouseParser::LOGS);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(953);
      match(ClickHouseParser::SYSTEM);
      setState(954);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::START

      || _la == ClickHouseParser::STOP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(959);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::DISTRIBUTED: {
          setState(955);
          match(ClickHouseParser::DISTRIBUTED);
          setState(956);
          match(ClickHouseParser::SENDS);
          break;
        }

        case ClickHouseParser::FETCHES: {
          setState(957);
          match(ClickHouseParser::FETCHES);
          break;
        }

        case ClickHouseParser::MERGES: {
          setState(958);
          match(ClickHouseParser::MERGES);
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(961);
      tableIdentifier();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(962);
      match(ClickHouseParser::SYSTEM);
      setState(963);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::START

      || _la == ClickHouseParser::STOP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(964);
      match(ClickHouseParser::REPLICATED);
      setState(965);
      match(ClickHouseParser::SENDS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(966);
      match(ClickHouseParser::SYSTEM);
      setState(967);
      match(ClickHouseParser::SYNC);
      setState(968);
      match(ClickHouseParser::REPLICA);
      setState(969);
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

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::TruncateStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::TruncateStmtContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
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
  enterRule(_localctx, 118, ClickHouseParser::RuleTruncateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(972);
    match(ClickHouseParser::TRUNCATE);
    setState(974);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TEMPORARY) {
      setState(973);
      match(ClickHouseParser::TEMPORARY);
    }
    setState(976);
    match(ClickHouseParser::TABLE);
    setState(979);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 130, _ctx)) {
    case 1: {
      setState(977);
      match(ClickHouseParser::IF);
      setState(978);
      match(ClickHouseParser::EXISTS);
      break;
    }

    }
    setState(981);
    tableIdentifier();
   
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
  enterRule(_localctx, 120, ClickHouseParser::RuleUseStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(983);
    match(ClickHouseParser::USE);
    setState(984);
    databaseIdentifier();
   
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
  enterRule(_localctx, 122, ClickHouseParser::RuleColumnTypeExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1033);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 135, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprSimpleContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(986);
      identifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprNestedContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(987);
      identifier();
      setState(988);
      match(ClickHouseParser::LPAREN);
      setState(989);
      identifier();
      setState(990);
      columnTypeExpr();
      setState(997);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(991);
        match(ClickHouseParser::COMMA);
        setState(992);
        identifier();
        setState(993);
        columnTypeExpr();
        setState(999);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1000);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprEnumContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1002);
      identifier();
      setState(1003);
      match(ClickHouseParser::LPAREN);
      setState(1004);
      enumValue();
      setState(1009);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(1005);
        match(ClickHouseParser::COMMA);
        setState(1006);
        enumValue();
        setState(1011);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1012);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprComplexContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(1014);
      identifier();
      setState(1015);
      match(ClickHouseParser::LPAREN);
      setState(1016);
      columnTypeExpr();
      setState(1021);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(1017);
        match(ClickHouseParser::COMMA);
        setState(1018);
        columnTypeExpr();
        setState(1023);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1024);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprParamContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(1026);
      identifier();
      setState(1027);
      match(ClickHouseParser::LPAREN);
      setState(1029);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
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
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1028);
        columnExprList();
      }
      setState(1031);
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
  enterRule(_localctx, 124, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1035);
    columnsExpr();
    setState(1040);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 136, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1036);
        match(ClickHouseParser::COMMA);
        setState(1037);
        columnsExpr(); 
      }
      setState(1042);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 136, _ctx);
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
  enterRule(_localctx, 126, ClickHouseParser::RuleColumnsExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1054);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 138, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprAsteriskContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1046);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
        setState(1043);
        tableIdentifier();
        setState(1044);
        match(ClickHouseParser::DOT);
      }
      setState(1048);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprSubqueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1049);
      match(ClickHouseParser::LPAREN);
      setState(1050);
      selectUnionStmt();
      setState(1051);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1053);
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
  size_t startState = 128;
  enterRecursionRule(_localctx, 128, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1163);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 149, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1057);
      match(ClickHouseParser::CASE);
      setState(1059);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 139, _ctx)) {
      case 1: {
        setState(1058);
        columnExpr(0);
        break;
      }

      }
      setState(1066); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(1061);
        match(ClickHouseParser::WHEN);
        setState(1062);
        columnExpr(0);
        setState(1063);
        match(ClickHouseParser::THEN);
        setState(1064);
        columnExpr(0);
        setState(1068); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(1072);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(1070);
        match(ClickHouseParser::ELSE);
        setState(1071);
        columnExpr(0);
      }
      setState(1074);
      match(ClickHouseParser::END);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprCastContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1076);
      match(ClickHouseParser::CAST);
      setState(1077);
      match(ClickHouseParser::LPAREN);
      setState(1078);
      columnExpr(0);
      setState(1079);
      match(ClickHouseParser::AS);
      setState(1080);
      columnTypeExpr();
      setState(1081);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprDateContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1083);
      match(ClickHouseParser::DATE);
      setState(1084);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1085);
      match(ClickHouseParser::EXTRACT);
      setState(1086);
      match(ClickHouseParser::LPAREN);
      setState(1087);
      interval();
      setState(1088);
      match(ClickHouseParser::FROM);
      setState(1089);
      columnExpr(0);
      setState(1090);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1092);
      match(ClickHouseParser::INTERVAL);
      setState(1093);
      columnExpr(0);
      setState(1094);
      interval();
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprSubstringContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1096);
      match(ClickHouseParser::SUBSTRING);
      setState(1097);
      match(ClickHouseParser::LPAREN);
      setState(1098);
      columnExpr(0);
      setState(1099);
      match(ClickHouseParser::FROM);
      setState(1100);
      columnExpr(0);
      setState(1103);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FOR) {
        setState(1101);
        match(ClickHouseParser::FOR);
        setState(1102);
        columnExpr(0);
      }
      setState(1105);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprTimestampContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1107);
      match(ClickHouseParser::TIMESTAMP);
      setState(1108);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1109);
      match(ClickHouseParser::TRIM);
      setState(1110);
      match(ClickHouseParser::LPAREN);
      setState(1111);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::BOTH || _la == ClickHouseParser::LEADING

      || _la == ClickHouseParser::TRAILING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1112);
      match(ClickHouseParser::STRING_LITERAL);
      setState(1113);
      match(ClickHouseParser::FROM);
      setState(1114);
      columnExpr(0);
      setState(1115);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1117);
      identifier();
      setState(1123);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 144, _ctx)) {
      case 1: {
        setState(1118);
        match(ClickHouseParser::LPAREN);
        setState(1120);
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
          | (1ULL << ClickHouseParser::DISK)
          | (1ULL << ClickHouseParser::DISTINCT)
          | (1ULL << ClickHouseParser::DISTRIBUTED)
          | (1ULL << ClickHouseParser::DROP)
          | (1ULL << ClickHouseParser::ELSE)
          | (1ULL << ClickHouseParser::END)
          | (1ULL << ClickHouseParser::ENGINE)
          | (1ULL << ClickHouseParser::EXISTS)
          | (1ULL << ClickHouseParser::EXPLAIN)
          | (1ULL << ClickHouseParser::EXTRACT)
          | (1ULL << ClickHouseParser::FETCHES)
          | (1ULL << ClickHouseParser::FINAL)
          | (1ULL << ClickHouseParser::FIRST)
          | (1ULL << ClickHouseParser::FLUSH)
          | (1ULL << ClickHouseParser::FOR)
          | (1ULL << ClickHouseParser::FORMAT)
          | (1ULL << ClickHouseParser::FROM)
          | (1ULL << ClickHouseParser::FULL)
          | (1ULL << ClickHouseParser::FUNCTION)
          | (1ULL << ClickHouseParser::GLOBAL)
          | (1ULL << ClickHouseParser::GRANULARITY)
          | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
          | (1ULL << (ClickHouseParser::HOUR - 64))
          | (1ULL << (ClickHouseParser::ID - 64))
          | (1ULL << (ClickHouseParser::IF - 64))
          | (1ULL << (ClickHouseParser::ILIKE - 64))
          | (1ULL << (ClickHouseParser::IN - 64))
          | (1ULL << (ClickHouseParser::INDEX - 64))
          | (1ULL << (ClickHouseParser::INF - 64))
          | (1ULL << (ClickHouseParser::INNER - 64))
          | (1ULL << (ClickHouseParser::INSERT - 64))
          | (1ULL << (ClickHouseParser::INTERVAL - 64))
          | (1ULL << (ClickHouseParser::INTO - 64))
          | (1ULL << (ClickHouseParser::IS - 64))
          | (1ULL << (ClickHouseParser::JOIN - 64))
          | (1ULL << (ClickHouseParser::KEY - 64))
          | (1ULL << (ClickHouseParser::LAST - 64))
          | (1ULL << (ClickHouseParser::LEADING - 64))
          | (1ULL << (ClickHouseParser::LEFT - 64))
          | (1ULL << (ClickHouseParser::LIKE - 64))
          | (1ULL << (ClickHouseParser::LIMIT - 64))
          | (1ULL << (ClickHouseParser::LOCAL - 64))
          | (1ULL << (ClickHouseParser::LOGS - 64))
          | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
          | (1ULL << (ClickHouseParser::MERGES - 64))
          | (1ULL << (ClickHouseParser::MINUTE - 64))
          | (1ULL << (ClickHouseParser::MODIFY - 64))
          | (1ULL << (ClickHouseParser::MONTH - 64))
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
          | (1ULL << (ClickHouseParser::QUARTER - 64))
          | (1ULL << (ClickHouseParser::REMOVE - 64))
          | (1ULL << (ClickHouseParser::RENAME - 64))
          | (1ULL << (ClickHouseParser::REPLACE - 64))
          | (1ULL << (ClickHouseParser::REPLICA - 64))
          | (1ULL << (ClickHouseParser::REPLICATED - 64))
          | (1ULL << (ClickHouseParser::RIGHT - 64))
          | (1ULL << (ClickHouseParser::ROLLUP - 64))
          | (1ULL << (ClickHouseParser::SAMPLE - 64))
          | (1ULL << (ClickHouseParser::SECOND - 64))
          | (1ULL << (ClickHouseParser::SELECT - 64))
          | (1ULL << (ClickHouseParser::SEMI - 64))
          | (1ULL << (ClickHouseParser::SENDS - 64))
          | (1ULL << (ClickHouseParser::SET - 64))
          | (1ULL << (ClickHouseParser::SETTINGS - 64))
          | (1ULL << (ClickHouseParser::SHOW - 64))
          | (1ULL << (ClickHouseParser::START - 64))
          | (1ULL << (ClickHouseParser::STOP - 64))
          | (1ULL << (ClickHouseParser::SUBSTRING - 64))
          | (1ULL << (ClickHouseParser::SYNC - 64))
          | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
          | (1ULL << (ClickHouseParser::TABLE - 128))
          | (1ULL << (ClickHouseParser::TABLES - 128))
          | (1ULL << (ClickHouseParser::TEMPORARY - 128))
          | (1ULL << (ClickHouseParser::THEN - 128))
          | (1ULL << (ClickHouseParser::TIES - 128))
          | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
          | (1ULL << (ClickHouseParser::TO - 128))
          | (1ULL << (ClickHouseParser::TOTALS - 128))
          | (1ULL << (ClickHouseParser::TRAILING - 128))
          | (1ULL << (ClickHouseParser::TRIM - 128))
          | (1ULL << (ClickHouseParser::TRUNCATE - 128))
          | (1ULL << (ClickHouseParser::TTL - 128))
          | (1ULL << (ClickHouseParser::TYPE - 128))
          | (1ULL << (ClickHouseParser::UNION - 128))
          | (1ULL << (ClickHouseParser::USE - 128))
          | (1ULL << (ClickHouseParser::USING - 128))
          | (1ULL << (ClickHouseParser::VALUES - 128))
          | (1ULL << (ClickHouseParser::VIEW - 128))
          | (1ULL << (ClickHouseParser::VOLUME - 128))
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
          | (1ULL << (ClickHouseParser::ASTERISK - 128))
          | (1ULL << (ClickHouseParser::DASH - 128))
          | (1ULL << (ClickHouseParser::DOT - 128))
          | (1ULL << (ClickHouseParser::LBRACKET - 128))
          | (1ULL << (ClickHouseParser::LPAREN - 128))
          | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
          setState(1119);
          columnExprList();
        }
        setState(1122);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(1125);
      match(ClickHouseParser::LPAREN);
      setState(1127);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 145, _ctx)) {
      case 1: {
        setState(1126);
        match(ClickHouseParser::DISTINCT);
        break;
      }

      }
      setState(1130);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
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
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1129);
        columnArgList();
      }
      setState(1132);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1134);
      literal();
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprNegateContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1135);
      match(ClickHouseParser::DASH);
      setState(1136);
      columnExpr(17);
      break;
    }

    case 12: {
      _localctx = _tracker.createInstance<ColumnExprNotContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1137);
      match(ClickHouseParser::NOT);
      setState(1138);
      columnExpr(12);
      break;
    }

    case 13: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1142);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WHERE - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128)))) != 0)) {
        setState(1139);
        tableIdentifier();
        setState(1140);
        match(ClickHouseParser::DOT);
      }
      setState(1144);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 14: {
      _localctx = _tracker.createInstance<ColumnExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1145);
      match(ClickHouseParser::LPAREN);
      setState(1146);
      selectUnionStmt();
      setState(1147);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 15: {
      _localctx = _tracker.createInstance<ColumnExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1149);
      match(ClickHouseParser::LPAREN);
      setState(1150);
      columnExpr(0);
      setState(1151);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 16: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1153);
      match(ClickHouseParser::LPAREN);
      setState(1154);
      columnExprList();
      setState(1155);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 17: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1157);
      match(ClickHouseParser::LBRACKET);
      setState(1159);
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
        | (1ULL << ClickHouseParser::DISK)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DISTRIBUTED)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXPLAIN)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FLUSH)
        | (1ULL << ClickHouseParser::FOR)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GRANULARITY)
        | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
        | (1ULL << (ClickHouseParser::HOUR - 64))
        | (1ULL << (ClickHouseParser::ID - 64))
        | (1ULL << (ClickHouseParser::IF - 64))
        | (1ULL << (ClickHouseParser::ILIKE - 64))
        | (1ULL << (ClickHouseParser::IN - 64))
        | (1ULL << (ClickHouseParser::INDEX - 64))
        | (1ULL << (ClickHouseParser::INF - 64))
        | (1ULL << (ClickHouseParser::INNER - 64))
        | (1ULL << (ClickHouseParser::INSERT - 64))
        | (1ULL << (ClickHouseParser::INTERVAL - 64))
        | (1ULL << (ClickHouseParser::INTO - 64))
        | (1ULL << (ClickHouseParser::IS - 64))
        | (1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::LOGS - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
        | (1ULL << (ClickHouseParser::MERGES - 64))
        | (1ULL << (ClickHouseParser::MINUTE - 64))
        | (1ULL << (ClickHouseParser::MODIFY - 64))
        | (1ULL << (ClickHouseParser::MONTH - 64))
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
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::REMOVE - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::REPLICATED - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::ROLLUP - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SENDS - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
        | (1ULL << (ClickHouseParser::TABLE - 128))
        | (1ULL << (ClickHouseParser::TABLES - 128))
        | (1ULL << (ClickHouseParser::TEMPORARY - 128))
        | (1ULL << (ClickHouseParser::THEN - 128))
        | (1ULL << (ClickHouseParser::TIES - 128))
        | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
        | (1ULL << (ClickHouseParser::TO - 128))
        | (1ULL << (ClickHouseParser::TOTALS - 128))
        | (1ULL << (ClickHouseParser::TRAILING - 128))
        | (1ULL << (ClickHouseParser::TRIM - 128))
        | (1ULL << (ClickHouseParser::TRUNCATE - 128))
        | (1ULL << (ClickHouseParser::TTL - 128))
        | (1ULL << (ClickHouseParser::TYPE - 128))
        | (1ULL << (ClickHouseParser::UNION - 128))
        | (1ULL << (ClickHouseParser::USE - 128))
        | (1ULL << (ClickHouseParser::USING - 128))
        | (1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
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
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1158);
        columnExprList();
      }
      setState(1161);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 18: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1162);
      columnIdentifier();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1236);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 158, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1234);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 157, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence1Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1165);

          if (!(precpred(_ctx, 16))) throw FailedPredicateException(this, "precpred(_ctx, 16)");
          setState(1166);
          _la = _input->LA(1);
          if (!(((((_la - 162) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 162)) & ((1ULL << (ClickHouseParser::ASTERISK - 162))
            | (1ULL << (ClickHouseParser::PERCENT - 162))
            | (1ULL << (ClickHouseParser::SLASH - 162)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(1167);
          columnExpr(17);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence2Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1168);

          if (!(precpred(_ctx, 15))) throw FailedPredicateException(this, "precpred(_ctx, 15)");
          setState(1169);
          _la = _input->LA(1);
          if (!(((((_la - 167) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 167)) & ((1ULL << (ClickHouseParser::CONCAT - 167))
            | (1ULL << (ClickHouseParser::DASH - 167))
            | (1ULL << (ClickHouseParser::PLUS - 167)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(1170);
          columnExpr(16);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprPrecedence3Context>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1171);

          if (!(precpred(_ctx, 14))) throw FailedPredicateException(this, "precpred(_ctx, 14)");
          setState(1190);
          _errHandler->sync(this);
          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 153, _ctx)) {
          case 1: {
            setState(1172);
            match(ClickHouseParser::EQ_DOUBLE);
            break;
          }

          case 2: {
            setState(1173);
            match(ClickHouseParser::EQ_SINGLE);
            break;
          }

          case 3: {
            setState(1174);
            match(ClickHouseParser::NOT_EQ);
            break;
          }

          case 4: {
            setState(1175);
            match(ClickHouseParser::LE);
            break;
          }

          case 5: {
            setState(1176);
            match(ClickHouseParser::GE);
            break;
          }

          case 6: {
            setState(1177);
            match(ClickHouseParser::LT);
            break;
          }

          case 7: {
            setState(1178);
            match(ClickHouseParser::GT);
            break;
          }

          case 8: {
            setState(1180);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::GLOBAL) {
              setState(1179);
              match(ClickHouseParser::GLOBAL);
            }
            setState(1183);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::NOT) {
              setState(1182);
              match(ClickHouseParser::NOT);
            }
            setState(1185);
            match(ClickHouseParser::IN);
            break;
          }

          case 9: {
            setState(1187);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == ClickHouseParser::NOT) {
              setState(1186);
              match(ClickHouseParser::NOT);
            }
            setState(1189);
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
          setState(1192);
          columnExpr(15);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprAndContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1193);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(1194);
          match(ClickHouseParser::AND);
          setState(1195);
          columnExpr(12);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprOrContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1196);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(1197);
          match(ClickHouseParser::OR);
          setState(1198);
          columnExpr(11);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1199);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(1201);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1200);
            match(ClickHouseParser::NOT);
          }
          setState(1203);
          match(ClickHouseParser::BETWEEN);
          setState(1204);
          columnExpr(0);
          setState(1205);
          match(ClickHouseParser::AND);
          setState(1206);
          columnExpr(10);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1208);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(1209);
          match(ClickHouseParser::QUERY);
          setState(1210);
          columnExpr(0);
          setState(1211);
          match(ClickHouseParser::COLON);
          setState(1212);
          columnExpr(8);
          break;
        }

        case 8: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1214);

          if (!(precpred(_ctx, 19))) throw FailedPredicateException(this, "precpred(_ctx, 19)");
          setState(1215);
          match(ClickHouseParser::LBRACKET);
          setState(1216);
          columnExpr(0);
          setState(1217);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 9: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1219);

          if (!(precpred(_ctx, 18))) throw FailedPredicateException(this, "precpred(_ctx, 18)");
          setState(1220);
          match(ClickHouseParser::DOT);
          setState(1221);
          match(ClickHouseParser::DECIMAL_LITERAL);
          break;
        }

        case 10: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1222);

          if (!(precpred(_ctx, 13))) throw FailedPredicateException(this, "precpred(_ctx, 13)");
          setState(1223);
          match(ClickHouseParser::IS);
          setState(1225);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1224);
            match(ClickHouseParser::NOT);
          }
          setState(1227);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 11: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1228);

          if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
          setState(1232);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case ClickHouseParser::ID:
            case ClickHouseParser::KEY:
            case ClickHouseParser::IDENTIFIER: {
              setState(1229);
              alias();
              break;
            }

            case ClickHouseParser::AS: {
              setState(1230);
              match(ClickHouseParser::AS);
              setState(1231);
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
      setState(1238);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 158, _ctx);
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
  enterRule(_localctx, 130, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1239);
    columnArgExpr();
    setState(1244);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1240);
      match(ClickHouseParser::COMMA);
      setState(1241);
      columnArgExpr();
      setState(1246);
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
  enterRule(_localctx, 132, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1249);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 160, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1247);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1248);
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
  enterRule(_localctx, 134, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1270);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(1251);
        match(ClickHouseParser::LPAREN);
        setState(1252);
        identifier();
        setState(1257);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1253);
          match(ClickHouseParser::COMMA);
          setState(1254);
          identifier();
          setState(1259);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1260);
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
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
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
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        setState(1262);
        identifier();
        setState(1267);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1263);
          match(ClickHouseParser::COMMA);
          setState(1264);
          identifier();
          setState(1269);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(1272);
    match(ClickHouseParser::ARROW);
    setState(1273);
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
  enterRule(_localctx, 136, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1278);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 164, _ctx)) {
    case 1: {
      setState(1275);
      tableIdentifier();
      setState(1276);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1280);
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
  enterRule(_localctx, 138, ClickHouseParser::RuleNestedIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1282);
    identifier();
    setState(1285);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 165, _ctx)) {
    case 1: {
      setState(1283);
      match(ClickHouseParser::DOT);
      setState(1284);
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
  size_t startState = 140;
  enterRecursionRule(_localctx, 140, ClickHouseParser::RuleTableExpr, precedence);

    

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1294);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 166, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1288);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1289);
      tableFunctionExpr();
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1290);
      match(ClickHouseParser::LPAREN);
      setState(1291);
      selectUnionStmt();
      setState(1292);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1304);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 168, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(1296);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(1300);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case ClickHouseParser::ID:
          case ClickHouseParser::KEY:
          case ClickHouseParser::IDENTIFIER: {
            setState(1297);
            alias();
            break;
          }

          case ClickHouseParser::AS: {
            setState(1298);
            match(ClickHouseParser::AS);
            setState(1299);
            identifier();
            break;
          }

        default:
          throw NoViableAltException(this);
        } 
      }
      setState(1306);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 168, _ctx);
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
  enterRule(_localctx, 142, ClickHouseParser::RuleTableFunctionExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1307);
    identifier();
    setState(1308);
    match(ClickHouseParser::LPAREN);
    setState(1310);
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
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FROM)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::FUNCTION)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GRANULARITY)
      | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HOUR - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INF - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MINUTE - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MONTH - 64))
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
      | (1ULL << (ClickHouseParser::QUARTER - 64))
      | (1ULL << (ClickHouseParser::REMOVE - 64))
      | (1ULL << (ClickHouseParser::RENAME - 64))
      | (1ULL << (ClickHouseParser::REPLACE - 64))
      | (1ULL << (ClickHouseParser::REPLICA - 64))
      | (1ULL << (ClickHouseParser::REPLICATED - 64))
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::ROLLUP - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SELECT - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SENDS - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::SHOW - 64))
      | (1ULL << (ClickHouseParser::START - 64))
      | (1ULL << (ClickHouseParser::STOP - 64))
      | (1ULL << (ClickHouseParser::SUBSTRING - 64))
      | (1ULL << (ClickHouseParser::SYNC - 64))
      | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
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
      | (1ULL << (ClickHouseParser::DASH - 128))
      | (1ULL << (ClickHouseParser::DOT - 128))
      | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
      setState(1309);
      tableArgList();
    }
    setState(1312);
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
  enterRule(_localctx, 144, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1317);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 170, _ctx)) {
    case 1: {
      setState(1314);
      databaseIdentifier();
      setState(1315);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1319);
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
  enterRule(_localctx, 146, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1321);
    tableArgExpr();
    setState(1326);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1322);
      match(ClickHouseParser::COMMA);
      setState(1323);
      tableArgExpr();
      setState(1328);
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

ClickHouseParser::TableIdentifierContext* ClickHouseParser::TableArgExprContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
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
  enterRule(_localctx, 148, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1332);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 172, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1329);
      tableIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1330);
      tableFunctionExpr();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1331);
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
  enterRule(_localctx, 150, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1334);
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
  enterRule(_localctx, 152, ClickHouseParser::RuleFloatingLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1344);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FLOATING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(1336);
        match(ClickHouseParser::FLOATING_LITERAL);
        break;
      }

      case ClickHouseParser::DOT: {
        enterOuterAlt(_localctx, 2);
        setState(1337);
        match(ClickHouseParser::DOT);
        setState(1338);
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
        setState(1339);
        match(ClickHouseParser::DECIMAL_LITERAL);
        setState(1340);
        match(ClickHouseParser::DOT);
        setState(1342);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 173, _ctx)) {
        case 1: {
          setState(1341);
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
  enterRule(_localctx, 154, ClickHouseParser::RuleNumberLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1347);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DASH

    || _la == ClickHouseParser::PLUS) {
      setState(1346);
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
    setState(1355);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 176, _ctx)) {
    case 1: {
      setState(1349);
      floatingLiteral();
      break;
    }

    case 2: {
      setState(1350);
      match(ClickHouseParser::OCTAL_LITERAL);
      break;
    }

    case 3: {
      setState(1351);
      match(ClickHouseParser::DECIMAL_LITERAL);
      break;
    }

    case 4: {
      setState(1352);
      match(ClickHouseParser::HEXADECIMAL_LITERAL);
      break;
    }

    case 5: {
      setState(1353);
      match(ClickHouseParser::INF);
      break;
    }

    case 6: {
      setState(1354);
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
  enterRule(_localctx, 156, ClickHouseParser::RuleLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1360);
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
        setState(1357);
        numberLiteral();
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1358);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 3);
        setState(1359);
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
  enterRule(_localctx, 158, ClickHouseParser::RuleInterval);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1362);
    _la = _input->LA(1);
    if (!(((((_la - 33) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 33)) & ((1ULL << (ClickHouseParser::DAY - 33))
      | (1ULL << (ClickHouseParser::HOUR - 33))
      | (1ULL << (ClickHouseParser::MINUTE - 33))
      | (1ULL << (ClickHouseParser::MONTH - 33)))) != 0) || ((((_la - 107) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 107)) & ((1ULL << (ClickHouseParser::QUARTER - 107))
      | (1ULL << (ClickHouseParser::SECOND - 107))
      | (1ULL << (ClickHouseParser::WEEK - 107))
      | (1ULL << (ClickHouseParser::YEAR - 107)))) != 0))) {
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

tree::TerminalNode* ClickHouseParser::KeywordContext::DAY() {
  return getToken(ClickHouseParser::DAY, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::EXPLAIN() {
  return getToken(ClickHouseParser::EXPLAIN, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::HOUR() {
  return getToken(ClickHouseParser::HOUR, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::LAST() {
  return getToken(ClickHouseParser::LAST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LEADING() {
  return getToken(ClickHouseParser::LEADING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LEFT() {
  return getToken(ClickHouseParser::LEFT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIKE() {
  return getToken(ClickHouseParser::LIKE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LIMIT() {
  return getToken(ClickHouseParser::LIMIT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LOCAL() {
  return getToken(ClickHouseParser::LOCAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::LOGS() {
  return getToken(ClickHouseParser::LOGS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MERGES() {
  return getToken(ClickHouseParser::MERGES, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MINUTE() {
  return getToken(ClickHouseParser::MINUTE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MONTH() {
  return getToken(ClickHouseParser::MONTH, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::QUARTER() {
  return getToken(ClickHouseParser::QUARTER, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::SECOND() {
  return getToken(ClickHouseParser::SECOND, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::THEN() {
  return getToken(ClickHouseParser::THEN, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TIES() {
  return getToken(ClickHouseParser::TIES, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TYPE() {
  return getToken(ClickHouseParser::TYPE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::UNION() {
  return getToken(ClickHouseParser::UNION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USE() {
  return getToken(ClickHouseParser::USE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USING() {
  return getToken(ClickHouseParser::USING, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::WEEK() {
  return getToken(ClickHouseParser::WEEK, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::YEAR() {
  return getToken(ClickHouseParser::YEAR, 0);
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
  enterRule(_localctx, 160, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1364);
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
      | (1ULL << ClickHouseParser::DISK)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DISTRIBUTED)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXPLAIN)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FLUSH)
      | (1ULL << ClickHouseParser::FOR)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FROM)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::FUNCTION)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GRANULARITY)
      | (1ULL << ClickHouseParser::GROUP))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::HAVING - 64))
      | (1ULL << (ClickHouseParser::HOUR - 64))
      | (1ULL << (ClickHouseParser::ID - 64))
      | (1ULL << (ClickHouseParser::IF - 64))
      | (1ULL << (ClickHouseParser::ILIKE - 64))
      | (1ULL << (ClickHouseParser::IN - 64))
      | (1ULL << (ClickHouseParser::INDEX - 64))
      | (1ULL << (ClickHouseParser::INNER - 64))
      | (1ULL << (ClickHouseParser::INSERT - 64))
      | (1ULL << (ClickHouseParser::INTERVAL - 64))
      | (1ULL << (ClickHouseParser::INTO - 64))
      | (1ULL << (ClickHouseParser::IS - 64))
      | (1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::LOGS - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
      | (1ULL << (ClickHouseParser::MERGES - 64))
      | (1ULL << (ClickHouseParser::MINUTE - 64))
      | (1ULL << (ClickHouseParser::MODIFY - 64))
      | (1ULL << (ClickHouseParser::MONTH - 64))
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
      | (1ULL << (ClickHouseParser::QUARTER - 64))
      | (1ULL << (ClickHouseParser::REMOVE - 64))
      | (1ULL << (ClickHouseParser::RENAME - 64))
      | (1ULL << (ClickHouseParser::REPLACE - 64))
      | (1ULL << (ClickHouseParser::REPLICA - 64))
      | (1ULL << (ClickHouseParser::REPLICATED - 64))
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::ROLLUP - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SELECT - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SENDS - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::SHOW - 64))
      | (1ULL << (ClickHouseParser::START - 64))
      | (1ULL << (ClickHouseParser::STOP - 64))
      | (1ULL << (ClickHouseParser::SUBSTRING - 64))
      | (1ULL << (ClickHouseParser::SYNC - 64))
      | (1ULL << (ClickHouseParser::SYNTAX - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::SYSTEM - 128))
      | (1ULL << (ClickHouseParser::TABLE - 128))
      | (1ULL << (ClickHouseParser::TABLES - 128))
      | (1ULL << (ClickHouseParser::TEMPORARY - 128))
      | (1ULL << (ClickHouseParser::THEN - 128))
      | (1ULL << (ClickHouseParser::TIES - 128))
      | (1ULL << (ClickHouseParser::TIMESTAMP - 128))
      | (1ULL << (ClickHouseParser::TO - 128))
      | (1ULL << (ClickHouseParser::TOTALS - 128))
      | (1ULL << (ClickHouseParser::TRAILING - 128))
      | (1ULL << (ClickHouseParser::TRIM - 128))
      | (1ULL << (ClickHouseParser::TRUNCATE - 128))
      | (1ULL << (ClickHouseParser::TTL - 128))
      | (1ULL << (ClickHouseParser::TYPE - 128))
      | (1ULL << (ClickHouseParser::UNION - 128))
      | (1ULL << (ClickHouseParser::USE - 128))
      | (1ULL << (ClickHouseParser::USING - 128))
      | (1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WHERE - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::YEAR - 128))
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
  enterRule(_localctx, 162, ClickHouseParser::RuleKeywordForAlias);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1366);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::ID

    || _la == ClickHouseParser::KEY)) {
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
  enterRule(_localctx, 164, ClickHouseParser::RuleAlias);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1370);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1368);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::ID:
      case ClickHouseParser::KEY: {
        enterOuterAlt(_localctx, 2);
        setState(1369);
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
  enterRule(_localctx, 166, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1375);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 179, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1372);
      match(ClickHouseParser::IDENTIFIER);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1373);
      interval();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1374);
      keyword();
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
  enterRule(_localctx, 168, ClickHouseParser::RuleIdentifierOrNull);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1379);
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
      case ClickHouseParser::DISK:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DISTRIBUTED:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXPLAIN:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FLUSH:
      case ClickHouseParser::FOR:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GRANULARITY:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::ILIKE:
      case ClickHouseParser::IN:
      case ClickHouseParser::INDEX:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::KEY:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::LOGS:
      case ClickHouseParser::MATERIALIZED:
      case ClickHouseParser::MERGES:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MODIFY:
      case ClickHouseParser::MONTH:
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
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYNTAX:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TIMESTAMP:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TRUNCATE:
      case ClickHouseParser::TTL:
      case ClickHouseParser::TYPE:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::USING:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1377);
        identifier();
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 2);
        setState(1378);
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
  enterRule(_localctx, 170, ClickHouseParser::RuleEnumValue);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1381);
    match(ClickHouseParser::STRING_LITERAL);
    setState(1382);
    match(ClickHouseParser::EQ_SINGLE);
    setState(1383);
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
    case 11: return engineClauseSempred(dynamic_cast<EngineClauseContext *>(context), predicateIndex);
    case 45: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 64: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 70: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::engineClauseSempred(EngineClauseContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return !_localctx->clauses.count("orderByClause");
    case 1: return !_localctx->clauses.count("partitionByClause");
    case 2: return !_localctx->clauses.count("primaryKeyClause");
    case 3: return !_localctx->clauses.count("sampleByClause");
    case 4: return !_localctx->clauses.count("ttlClause");
    case 5: return !_localctx->clauses.count("settingsClause");

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::joinExprSempred(JoinExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 6: return precpred(_ctx, 3);
    case 7: return precpred(_ctx, 4);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 8: return precpred(_ctx, 16);
    case 9: return precpred(_ctx, 15);
    case 10: return precpred(_ctx, 14);
    case 11: return precpred(_ctx, 11);
    case 12: return precpred(_ctx, 10);
    case 13: return precpred(_ctx, 9);
    case 14: return precpred(_ctx, 8);
    case 15: return precpred(_ctx, 19);
    case 16: return precpred(_ctx, 18);
    case 17: return precpred(_ctx, 13);
    case 18: return precpred(_ctx, 7);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::tableExprSempred(TableExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 19: return precpred(_ctx, 1);

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
  "queryStmt", "query", "alterStmt", "alterTableClause", "tableColumnPropertyType", 
  "partitionClause", "checkStmt", "createStmt", "destinationClause", "subqueryClause", 
  "schemaClause", "engineClause", "partitionByClause", "primaryKeyClause", 
  "sampleByClause", "ttlClause", "engineExpr", "tableElementExpr", "tableColumnDfnt", 
  "tableColumnPropertyExpr", "codecExpr", "ttlExpr", "describeStmt", "dropStmt", 
  "existsStmt", "explainStmt", "insertStmt", "columnsClause", "dataClause", 
  "optimizeStmt", "renameStmt", "selectUnionStmt", "selectStmtWithParens", 
  "selectStmt", "withClause", "fromClause", "arrayJoinClause", "prewhereClause", 
  "whereClause", "groupByClause", "havingClause", "orderByClause", "limitByClause", 
  "limitClause", "settingsClause", "joinExpr", "joinOp", "joinOpCross", 
  "joinConstraintClause", "sampleClause", "limitExpr", "orderExprList", 
  "orderExpr", "ratioExpr", "settingExprList", "settingExpr", "setStmt", 
  "showStmt", "systemStmt", "truncateStmt", "useStmt", "columnTypeExpr", 
  "columnExprList", "columnsExpr", "columnExpr", "columnArgList", "columnArgExpr", 
  "columnLambdaExpr", "columnIdentifier", "nestedIdentifier", "tableExpr", 
  "tableFunctionExpr", "tableIdentifier", "tableArgList", "tableArgExpr", 
  "databaseIdentifier", "floatingLiteral", "numberLiteral", "literal", "interval", 
  "keyword", "keywordForAlias", "alias", "identifier", "identifierOrNull", 
  "enumValue"
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
  "", "", "", "", "", "", "", "", "", "'false'", "'true'", "", "", "", "", 
  "", "", "'->'", "'*'", "'`'", "'\\'", "':'", "','", "'||'", "'-'", "'.'", 
  "'=='", "'='", "'>='", "'>'", "'{'", "'['", "'<='", "'('", "'<'", "", 
  "'%'", "'+'", "'?'", "'\"'", "'''", "'}'", "']'", "')'", "';'", "'/'", 
  "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "ADD", "AFTER", "ALIAS", "ALL", "ALTER", "AND", "ANTI", "ANY", "ARRAY", 
  "AS", "ASCENDING", "ASOF", "ATTACH", "BETWEEN", "BOTH", "BY", "CASE", 
  "CAST", "CHECK", "CLEAR", "CLUSTER", "CODEC", "COLLATE", "COLUMN", "COMMENT", 
  "CONSTRAINT", "CREATE", "CROSS", "CUBE", "DATABASE", "DATABASES", "DATE", 
  "DAY", "DEDUPLICATE", "DEFAULT", "DELAY", "DELETE", "DESC", "DESCENDING", 
  "DESCRIBE", "DETACH", "DISK", "DISTINCT", "DISTRIBUTED", "DROP", "ELSE", 
  "END", "ENGINE", "EXISTS", "EXPLAIN", "EXTRACT", "FETCHES", "FINAL", "FIRST", 
  "FLUSH", "FOR", "FORMAT", "FROM", "FULL", "FUNCTION", "GLOBAL", "GRANULARITY", 
  "GROUP", "HAVING", "HOUR", "ID", "IF", "ILIKE", "IN", "INDEX", "INF", 
  "INNER", "INSERT", "INTERVAL", "INTO", "IS", "JOIN", "KEY", "LAST", "LEADING", 
  "LEFT", "LIKE", "LIMIT", "LOCAL", "LOGS", "MATERIALIZED", "MERGES", "MINUTE", 
  "MODIFY", "MONTH", "NAN_SQL", "NO", "NOT", "NULL_SQL", "NULLS", "OFFSET", 
  "ON", "OPTIMIZE", "OR", "ORDER", "OUTER", "OUTFILE", "PARTITION", "POPULATE", 
  "PREWHERE", "PRIMARY", "QUARTER", "REMOVE", "RENAME", "REPLACE", "REPLICA", 
  "REPLICATED", "RIGHT", "ROLLUP", "SAMPLE", "SECOND", "SELECT", "SEMI", 
  "SENDS", "SET", "SETTINGS", "SHOW", "START", "STOP", "SUBSTRING", "SYNC", 
  "SYNTAX", "SYSTEM", "TABLE", "TABLES", "TEMPORARY", "THEN", "TIES", "TIMESTAMP", 
  "TO", "TOTALS", "TRAILING", "TRIM", "TRUNCATE", "TTL", "TYPE", "UNION", 
  "USE", "USING", "VALUES", "VIEW", "VOLUME", "WEEK", "WHEN", "WHERE", "WITH", 
  "YEAR", "JSON_FALSE", "JSON_TRUE", "IDENTIFIER", "FLOATING_LITERAL", "OCTAL_LITERAL", 
  "DECIMAL_LITERAL", "HEXADECIMAL_LITERAL", "STRING_LITERAL", "ARROW", "ASTERISK", 
  "BACKQUOTE", "BACKSLASH", "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ_DOUBLE", 
  "EQ_SINGLE", "GE", "GT", "LBRACE", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", 
  "PERCENT", "PLUS", "QUERY", "QUOTE_DOUBLE", "QUOTE_SINGLE", "RBRACE", 
  "RBRACKET", "RPAREN", "SEMICOLON", "SLASH", "UNDERSCORE", "MULTI_LINE_COMMENT", 
  "SINGLE_LINE_COMMENT", "WHITESPACE"
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
    0x3, 0xc3, 0x56c, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x9, 0x56, 0x4, 0x57, 0x9, 0x57, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 
    0x2, 0x5, 0x2, 0xb3, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x5, 0x2, 0xb7, 0xa, 
    0x2, 0x3, 0x2, 0x5, 0x2, 0xba, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xbd, 0xa, 
    0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x5, 0x3, 0xce, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x7, 0x4, 0xd6, 0xa, 0x4, 0xc, 0x4, 0xe, 
    0x4, 0xd9, 0xb, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x5, 0x5, 0xe0, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xe5, 
    0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xeb, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xf1, 0xa, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xf6, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xfc, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x10a, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x113, 
    0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x11e, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x12d, 0xa, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x138, 0xa, 0x5, 0x3, 0x6, 0x3, 0x6, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x141, 0xa, 0x7, 
    0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 
    0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x14c, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 
    0x9, 0x150, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 
    0x3, 0x9, 0x5, 0x9, 0x158, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x15c, 
    0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x161, 0xa, 0x9, 0x5, 
    0x9, 0x163, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 
    0x169, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x16f, 
    0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x173, 0xa, 0x9, 0x3, 0x9, 0x5, 
    0x9, 0x176, 0xa, 0x9, 0x3, 0x9, 0x5, 0x9, 0x179, 0xa, 0x9, 0x3, 0x9, 
    0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x180, 0xa, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x5, 0x9, 0x184, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 
    0x188, 0xa, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xb, 0x3, 0xb, 0x3, 
    0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x7, 0xc, 0x194, 0xa, 0xc, 
    0xc, 0xc, 0xe, 0xc, 0x197, 0xb, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x19f, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x7, 0xd, 0x1ba, 0xa, 0xd, 0xc, 0xd, 0xe, 0xd, 0x1bd, 
    0xb, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 
    0x3, 0xf, 0x3, 0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 
    0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x7, 0x11, 0x1cf, 0xa, 0x11, 
    0xc, 0x11, 0xe, 0x11, 0x1d2, 0xb, 0x11, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 
    0x1d6, 0xa, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x1db, 
    0xa, 0x12, 0x3, 0x12, 0x5, 0x12, 0x1de, 0xa, 0x12, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 
    0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 
    0x1ee, 0xa, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x1f3, 
    0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x1f7, 0xa, 0x14, 0x3, 0x14, 
    0x5, 0x14, 0x1fa, 0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x1fe, 
    0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x202, 0xa, 0x14, 0x3, 0x14, 
    0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x207, 0xa, 0x14, 0x3, 0x14, 0x5, 0x14, 
    0x20a, 0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x20e, 0xa, 0x14, 
    0x5, 0x14, 0x210, 0xa, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x5, 0x16, 0x21a, 0xa, 0x16, 
    0x3, 0x16, 0x5, 0x16, 0x21d, 0xa, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x17, 
    0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 
    0x17, 0x5, 0x17, 0x229, 0xa, 0x17, 0x3, 0x18, 0x3, 0x18, 0x5, 0x18, 
    0x22d, 0xa, 0x18, 0x3, 0x18, 0x3, 0x18, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x3, 0x19, 0x5, 0x19, 0x235, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x5, 0x19, 0x23a, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 
    0x23f, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x244, 
    0xa, 0x19, 0x5, 0x19, 0x246, 0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x5, 0x1a, 
    0x24a, 0xa, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x256, 
    0xa, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x25b, 0xa, 0x1c, 
    0x3, 0x1c, 0x5, 0x1c, 0x25e, 0xa, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1d, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x7, 0x1d, 0x266, 0xa, 0x1d, 0xc, 0x1d, 
    0xe, 0x1d, 0x269, 0xb, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 
    0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x271, 0xa, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x3, 0x1f, 0x5, 0x1f, 0x277, 0xa, 0x1f, 0x3, 0x1f, 0x5, 0x1f, 
    0x27a, 0xa, 0x1f, 0x3, 0x1f, 0x5, 0x1f, 0x27d, 0xa, 0x1f, 0x3, 0x20, 
    0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 
    0x20, 0x3, 0x20, 0x3, 0x20, 0x7, 0x20, 0x289, 0xa, 0x20, 0xc, 0x20, 
    0xe, 0x20, 0x28c, 0xb, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x7, 0x21, 0x292, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x295, 0xb, 0x21, 
    0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x5, 0x22, 0x29c, 
    0xa, 0x22, 0x3, 0x23, 0x5, 0x23, 0x29f, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 
    0x5, 0x23, 0x2a3, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2a7, 
    0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2aa, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 
    0x2ad, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2b0, 0xa, 0x23, 0x3, 0x23, 
    0x5, 0x23, 0x2b3, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2b7, 
    0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2bb, 0xa, 0x23, 0x3, 0x23, 
    0x5, 0x23, 0x2be, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2c1, 0xa, 0x23, 
    0x3, 0x23, 0x5, 0x23, 0x2c4, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2c7, 
    0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x2ca, 0xa, 0x23, 0x3, 0x24, 0x3, 0x24, 
    0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 0x5, 0x26, 0x2d3, 
    0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x27, 0x3, 
    0x27, 0x3, 0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 
    0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x5, 
    0x29, 0x2e7, 0xa, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2b, 
    0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 
    0x2c, 0x3, 0x2c, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 
    0x2f9, 0xa, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 
    0x3, 0x2f, 0x5, 0x2f, 0x301, 0xa, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x304, 
    0xa, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x30a, 
    0xa, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 
    0x2f, 0x5, 0x2f, 0x312, 0xa, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x315, 0xa, 
    0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x7, 0x2f, 0x31b, 
    0xa, 0x2f, 0xc, 0x2f, 0xe, 0x2f, 0x31e, 0xb, 0x2f, 0x3, 0x30, 0x5, 0x30, 
    0x321, 0xa, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x326, 
    0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 0x329, 0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 
    0x32c, 0xa, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x330, 0xa, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x334, 0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 
    0x337, 0xa, 0x30, 0x5, 0x30, 0x339, 0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 
    0x33c, 0xa, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x340, 0xa, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x344, 0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 
    0x347, 0xa, 0x30, 0x5, 0x30, 0x349, 0xa, 0x30, 0x5, 0x30, 0x34b, 0xa, 
    0x30, 0x3, 0x31, 0x5, 0x31, 0x34e, 0xa, 0x31, 0x3, 0x31, 0x3, 0x31, 
    0x3, 0x31, 0x5, 0x31, 0x353, 0xa, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 
    0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x5, 
    0x32, 0x35e, 0xa, 0x32, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 
    0x5, 0x33, 0x364, 0xa, 0x33, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x5, 0x34, 
    0x369, 0xa, 0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x7, 0x35, 0x36e, 
    0xa, 0x35, 0xc, 0x35, 0xe, 0x35, 0x371, 0xb, 0x35, 0x3, 0x36, 0x3, 0x36, 
    0x5, 0x36, 0x375, 0xa, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x379, 
    0xa, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x37d, 0xa, 0x36, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x382, 0xa, 0x37, 0x3, 0x38, 0x3, 0x38, 
    0x3, 0x38, 0x7, 0x38, 0x387, 0xa, 0x38, 0xc, 0x38, 0xe, 0x38, 0x38a, 
    0xb, 0x38, 0x3, 0x39, 0x3, 0x39, 0x3, 0x39, 0x3, 0x39, 0x3, 0x3a, 0x3, 
    0x3a, 0x3, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x39a, 0xa, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 
    0x39d, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3a2, 
    0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3a7, 0xa, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3ac, 0xa, 0x3b, 0x3, 0x3b, 
    0x5, 0x3b, 0x3af, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x3b3, 
    0xa, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 
    0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 
    0x3, 0x3c, 0x5, 0x3c, 0x3c2, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 
    0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 
    0x3c, 0x3cd, 0xa, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x3d1, 0xa, 
    0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x3d6, 0xa, 0x3d, 
    0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3f, 0x3, 
    0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 
    0x3, 0x3f, 0x7, 0x3f, 0x3e6, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 0x3e9, 
    0xb, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 
    0x3f, 0x3, 0x3f, 0x7, 0x3f, 0x3f2, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 
    0x3f5, 0xb, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 
    0x3, 0x3f, 0x3, 0x3f, 0x7, 0x3f, 0x3fe, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 
    0x401, 0xb, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 
    0x5, 0x3f, 0x408, 0xa, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x5, 0x3f, 0x40c, 
    0xa, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x7, 0x40, 0x411, 0xa, 0x40, 
    0xc, 0x40, 0xe, 0x40, 0x414, 0xb, 0x40, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 
    0x5, 0x41, 0x419, 0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 
    0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x421, 0xa, 0x41, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x5, 0x42, 0x426, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x6, 0x42, 0x42d, 0xa, 0x42, 0xd, 0x42, 0xe, 0x42, 
    0x42e, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x433, 0xa, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x452, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x5, 0x42, 0x463, 0xa, 0x42, 0x3, 0x42, 0x5, 0x42, 0x466, 0xa, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x46a, 0xa, 0x42, 0x3, 0x42, 0x5, 0x42, 
    0x46d, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x479, 
    0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x48a, 0xa, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x5, 0x42, 0x48e, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 
    0x49f, 0xa, 0x42, 0x3, 0x42, 0x5, 0x42, 0x4a2, 0xa, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x5, 0x42, 0x4a6, 0xa, 0x42, 0x3, 0x42, 0x5, 0x42, 0x4a9, 
    0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x4b4, 0xa, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 
    0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x4cc, 0xa, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x4d3, 0xa, 0x42, 
    0x7, 0x42, 0x4d5, 0xa, 0x42, 0xc, 0x42, 0xe, 0x42, 0x4d8, 0xb, 0x42, 
    0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x7, 0x43, 0x4dd, 0xa, 0x43, 0xc, 0x43, 
    0xe, 0x43, 0x4e0, 0xb, 0x43, 0x3, 0x44, 0x3, 0x44, 0x5, 0x44, 0x4e4, 
    0xa, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x4ea, 
    0xa, 0x45, 0xc, 0x45, 0xe, 0x45, 0x4ed, 0xb, 0x45, 0x3, 0x45, 0x3, 0x45, 
    0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x4f4, 0xa, 0x45, 0xc, 0x45, 
    0xe, 0x45, 0x4f7, 0xb, 0x45, 0x5, 0x45, 0x4f9, 0xa, 0x45, 0x3, 0x45, 
    0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x501, 
    0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 
    0x47, 0x508, 0xa, 0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 
    0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x5, 0x48, 0x511, 0xa, 0x48, 0x3, 0x48, 
    0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x5, 0x48, 0x517, 0xa, 0x48, 0x7, 0x48, 
    0x519, 0xa, 0x48, 0xc, 0x48, 0xe, 0x48, 0x51c, 0xb, 0x48, 0x3, 0x49, 
    0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x521, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 
    0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x528, 0xa, 0x4a, 0x3, 0x4a, 
    0x3, 0x4a, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x7, 0x4b, 0x52f, 0xa, 0x4b, 
    0xc, 0x4b, 0xe, 0x4b, 0x532, 0xb, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 
    0x5, 0x4c, 0x537, 0xa, 0x4c, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4e, 0x3, 0x4e, 
    0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x5, 0x4e, 0x541, 0xa, 0x4e, 
    0x5, 0x4e, 0x543, 0xa, 0x4e, 0x3, 0x4f, 0x5, 0x4f, 0x546, 0xa, 0x4f, 
    0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x5, 
    0x4f, 0x54e, 0xa, 0x4f, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 
    0x553, 0xa, 0x50, 0x3, 0x51, 0x3, 0x51, 0x3, 0x52, 0x3, 0x52, 0x3, 0x53, 
    0x3, 0x53, 0x3, 0x54, 0x3, 0x54, 0x5, 0x54, 0x55d, 0xa, 0x54, 0x3, 0x55, 
    0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 0x562, 0xa, 0x55, 0x3, 0x56, 0x3, 0x56, 
    0x5, 0x56, 0x566, 0xa, 0x56, 0x3, 0x57, 0x3, 0x57, 0x3, 0x57, 0x3, 0x57, 
    0x3, 0x57, 0x2, 0x5, 0x5c, 0x82, 0x8e, 0x58, 0x2, 0x4, 0x6, 0x8, 0xa, 
    0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 
    0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 
    0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 
    0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 
    0x6c, 0x6e, 0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 
    0x84, 0x86, 0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 0x96, 0x98, 0x9a, 
    0x9c, 0x9e, 0xa0, 0xa2, 0xa4, 0xa6, 0xa8, 0xaa, 0xac, 0x2, 0x1c, 0x8, 
    0x2, 0x5, 0x5, 0x18, 0x18, 0x1b, 0x1b, 0x25, 0x25, 0x58, 0x58, 0x8e, 
    0x8e, 0x4, 0x2, 0xf, 0xf, 0x1d, 0x1d, 0x5, 0x2, 0x5, 0x5, 0x25, 0x25, 
    0x58, 0x58, 0x4, 0x2, 0x28, 0x28, 0x2a, 0x2a, 0x4, 0x2, 0x2b, 0x2b, 
    0x2f, 0x2f, 0x4, 0x2, 0x1f, 0x1f, 0x74, 0x74, 0x4, 0x2, 0x4a, 0x4a, 
    0x53, 0x53, 0x4, 0x2, 0x3f, 0x3f, 0x56, 0x56, 0x5, 0x2, 0x6, 0x6, 0xa, 
    0xa, 0xe, 0xe, 0x6, 0x2, 0x6, 0x6, 0x9, 0xa, 0xe, 0xe, 0x78, 0x78, 0x4, 
    0x2, 0x53, 0x53, 0x73, 0x73, 0x4, 0x2, 0x6, 0x6, 0xa, 0xa, 0x4, 0x2, 
    0x62, 0x62, 0xa8, 0xa8, 0x4, 0x2, 0xd, 0xd, 0x28, 0x29, 0x4, 0x2, 0x38, 
    0x38, 0x51, 0x51, 0x4, 0x2, 0x3c, 0x3c, 0x47, 0x47, 0x3, 0x2, 0x7d, 
    0x7e, 0x5, 0x2, 0x11, 0x11, 0x52, 0x52, 0x8b, 0x8b, 0x5, 0x2, 0xa4, 
    0xa4, 0xb6, 0xb6, 0xbf, 0xbf, 0x4, 0x2, 0xa9, 0xaa, 0xb7, 0xb7, 0x4, 
    0x2, 0x46, 0x46, 0x54, 0x54, 0x3, 0x2, 0x9f, 0xa0, 0x4, 0x2, 0xaa, 0xaa, 
    0xb7, 0xb7, 0xa, 0x2, 0x23, 0x23, 0x43, 0x43, 0x5a, 0x5a, 0x5c, 0x5c, 
    0x6d, 0x6d, 0x76, 0x76, 0x96, 0x96, 0x9a, 0x9a, 0x6, 0x2, 0x4, 0x48, 
    0x4a, 0x5c, 0x5e, 0x5f, 0x61, 0x9c, 0x4, 0x2, 0x44, 0x44, 0x50, 0x50, 
    0x2, 0x626, 0x2, 0xbc, 0x3, 0x2, 0x2, 0x2, 0x4, 0xcd, 0x3, 0x2, 0x2, 
    0x2, 0x6, 0xcf, 0x3, 0x2, 0x2, 0x2, 0x8, 0x137, 0x3, 0x2, 0x2, 0x2, 
    0xa, 0x139, 0x3, 0x2, 0x2, 0x2, 0xc, 0x140, 0x3, 0x2, 0x2, 0x2, 0xe, 
    0x142, 0x3, 0x2, 0x2, 0x2, 0x10, 0x187, 0x3, 0x2, 0x2, 0x2, 0x12, 0x189, 
    0x3, 0x2, 0x2, 0x2, 0x14, 0x18c, 0x3, 0x2, 0x2, 0x2, 0x16, 0x19e, 0x3, 
    0x2, 0x2, 0x2, 0x18, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x1be, 0x3, 0x2, 
    0x2, 0x2, 0x1c, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x1c6, 0x3, 0x2, 0x2, 
    0x2, 0x20, 0x1ca, 0x3, 0x2, 0x2, 0x2, 0x22, 0x1d3, 0x3, 0x2, 0x2, 0x2, 
    0x24, 0x1ed, 0x3, 0x2, 0x2, 0x2, 0x26, 0x20f, 0x3, 0x2, 0x2, 0x2, 0x28, 
    0x211, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x214, 0x3, 0x2, 0x2, 0x2, 0x2c, 0x220, 
    0x3, 0x2, 0x2, 0x2, 0x2e, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x30, 0x245, 0x3, 
    0x2, 0x2, 0x2, 0x32, 0x247, 0x3, 0x2, 0x2, 0x2, 0x34, 0x24e, 0x3, 0x2, 
    0x2, 0x2, 0x36, 0x252, 0x3, 0x2, 0x2, 0x2, 0x38, 0x261, 0x3, 0x2, 0x2, 
    0x2, 0x3a, 0x270, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x272, 0x3, 0x2, 0x2, 0x2, 
    0x3e, 0x27e, 0x3, 0x2, 0x2, 0x2, 0x40, 0x28d, 0x3, 0x2, 0x2, 0x2, 0x42, 
    0x29b, 0x3, 0x2, 0x2, 0x2, 0x44, 0x29e, 0x3, 0x2, 0x2, 0x2, 0x46, 0x2cb, 
    0x3, 0x2, 0x2, 0x2, 0x48, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x2d2, 0x3, 
    0x2, 0x2, 0x2, 0x4c, 0x2d8, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x2db, 0x3, 0x2, 
    0x2, 0x2, 0x50, 0x2de, 0x3, 0x2, 0x2, 0x2, 0x52, 0x2e8, 0x3, 0x2, 0x2, 
    0x2, 0x54, 0x2eb, 0x3, 0x2, 0x2, 0x2, 0x56, 0x2ef, 0x3, 0x2, 0x2, 0x2, 
    0x58, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x2fa, 0x3, 0x2, 0x2, 0x2, 0x5c, 
    0x309, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x34a, 0x3, 0x2, 0x2, 0x2, 0x60, 0x352, 
    0x3, 0x2, 0x2, 0x2, 0x62, 0x35d, 0x3, 0x2, 0x2, 0x2, 0x64, 0x35f, 0x3, 
    0x2, 0x2, 0x2, 0x66, 0x365, 0x3, 0x2, 0x2, 0x2, 0x68, 0x36a, 0x3, 0x2, 
    0x2, 0x2, 0x6a, 0x372, 0x3, 0x2, 0x2, 0x2, 0x6c, 0x37e, 0x3, 0x2, 0x2, 
    0x2, 0x6e, 0x383, 0x3, 0x2, 0x2, 0x2, 0x70, 0x38b, 0x3, 0x2, 0x2, 0x2, 
    0x72, 0x38f, 0x3, 0x2, 0x2, 0x2, 0x74, 0x3b2, 0x3, 0x2, 0x2, 0x2, 0x76, 
    0x3cc, 0x3, 0x2, 0x2, 0x2, 0x78, 0x3ce, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x3d9, 
    0x3, 0x2, 0x2, 0x2, 0x7c, 0x40b, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x40d, 0x3, 
    0x2, 0x2, 0x2, 0x80, 0x420, 0x3, 0x2, 0x2, 0x2, 0x82, 0x48d, 0x3, 0x2, 
    0x2, 0x2, 0x84, 0x4d9, 0x3, 0x2, 0x2, 0x2, 0x86, 0x4e3, 0x3, 0x2, 0x2, 
    0x2, 0x88, 0x4f8, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x500, 0x3, 0x2, 0x2, 0x2, 
    0x8c, 0x504, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x510, 0x3, 0x2, 0x2, 0x2, 0x90, 
    0x51d, 0x3, 0x2, 0x2, 0x2, 0x92, 0x527, 0x3, 0x2, 0x2, 0x2, 0x94, 0x52b, 
    0x3, 0x2, 0x2, 0x2, 0x96, 0x536, 0x3, 0x2, 0x2, 0x2, 0x98, 0x538, 0x3, 
    0x2, 0x2, 0x2, 0x9a, 0x542, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x545, 0x3, 0x2, 
    0x2, 0x2, 0x9e, 0x552, 0x3, 0x2, 0x2, 0x2, 0xa0, 0x554, 0x3, 0x2, 0x2, 
    0x2, 0xa2, 0x556, 0x3, 0x2, 0x2, 0x2, 0xa4, 0x558, 0x3, 0x2, 0x2, 0x2, 
    0xa6, 0x55c, 0x3, 0x2, 0x2, 0x2, 0xa8, 0x561, 0x3, 0x2, 0x2, 0x2, 0xaa, 
    0x565, 0x3, 0x2, 0x2, 0x2, 0xac, 0x567, 0x3, 0x2, 0x2, 0x2, 0xae, 0xb2, 
    0x5, 0x4, 0x3, 0x2, 0xaf, 0xb0, 0x7, 0x4d, 0x2, 0x2, 0xb0, 0xb1, 0x7, 
    0x68, 0x2, 0x2, 0xb1, 0xb3, 0x7, 0xa2, 0x2, 0x2, 0xb2, 0xaf, 0x3, 0x2, 
    0x2, 0x2, 0xb2, 0xb3, 0x3, 0x2, 0x2, 0x2, 0xb3, 0xb6, 0x3, 0x2, 0x2, 
    0x2, 0xb4, 0xb5, 0x7, 0x3b, 0x2, 0x2, 0xb5, 0xb7, 0x5, 0xaa, 0x56, 0x2, 
    0xb6, 0xb4, 0x3, 0x2, 0x2, 0x2, 0xb6, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xb7, 
    0xb9, 0x3, 0x2, 0x2, 0x2, 0xb8, 0xba, 0x7, 0xbe, 0x2, 0x2, 0xb9, 0xb8, 
    0x3, 0x2, 0x2, 0x2, 0xb9, 0xba, 0x3, 0x2, 0x2, 0x2, 0xba, 0xbd, 0x3, 
    0x2, 0x2, 0x2, 0xbb, 0xbd, 0x5, 0x36, 0x1c, 0x2, 0xbc, 0xae, 0x3, 0x2, 
    0x2, 0x2, 0xbc, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xbd, 0x3, 0x3, 0x2, 0x2, 
    0x2, 0xbe, 0xce, 0x5, 0x6, 0x4, 0x2, 0xbf, 0xce, 0x5, 0xe, 0x8, 0x2, 
    0xc0, 0xce, 0x5, 0x10, 0x9, 0x2, 0xc1, 0xce, 0x5, 0x2e, 0x18, 0x2, 0xc2, 
    0xce, 0x5, 0x30, 0x19, 0x2, 0xc3, 0xce, 0x5, 0x32, 0x1a, 0x2, 0xc4, 
    0xce, 0x5, 0x34, 0x1b, 0x2, 0xc5, 0xce, 0x5, 0x3c, 0x1f, 0x2, 0xc6, 
    0xce, 0x5, 0x3e, 0x20, 0x2, 0xc7, 0xce, 0x5, 0x40, 0x21, 0x2, 0xc8, 
    0xce, 0x5, 0x72, 0x3a, 0x2, 0xc9, 0xce, 0x5, 0x74, 0x3b, 0x2, 0xca, 
    0xce, 0x5, 0x76, 0x3c, 0x2, 0xcb, 0xce, 0x5, 0x78, 0x3d, 0x2, 0xcc, 
    0xce, 0x5, 0x7a, 0x3e, 0x2, 0xcd, 0xbe, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xbf, 
    0x3, 0x2, 0x2, 0x2, 0xcd, 0xc0, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xc1, 0x3, 
    0x2, 0x2, 0x2, 0xcd, 0xc2, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xc3, 0x3, 0x2, 
    0x2, 0x2, 0xcd, 0xc4, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xc5, 0x3, 0x2, 0x2, 
    0x2, 0xcd, 0xc6, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xc7, 0x3, 0x2, 0x2, 0x2, 
    0xcd, 0xc8, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xc9, 0x3, 0x2, 0x2, 0x2, 0xcd, 
    0xca, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xcb, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xcc, 
    0x3, 0x2, 0x2, 0x2, 0xce, 0x5, 0x3, 0x2, 0x2, 0x2, 0xcf, 0xd0, 0x7, 
    0x7, 0x2, 0x2, 0xd0, 0xd1, 0x7, 0x83, 0x2, 0x2, 0xd1, 0xd2, 0x5, 0x92, 
    0x4a, 0x2, 0xd2, 0xd7, 0x5, 0x8, 0x5, 0x2, 0xd3, 0xd4, 0x7, 0xa8, 0x2, 
    0x2, 0xd4, 0xd6, 0x5, 0x8, 0x5, 0x2, 0xd5, 0xd3, 0x3, 0x2, 0x2, 0x2, 
    0xd6, 0xd9, 0x3, 0x2, 0x2, 0x2, 0xd7, 0xd5, 0x3, 0x2, 0x2, 0x2, 0xd7, 
    0xd8, 0x3, 0x2, 0x2, 0x2, 0xd8, 0x7, 0x3, 0x2, 0x2, 0x2, 0xd9, 0xd7, 
    0x3, 0x2, 0x2, 0x2, 0xda, 0xdb, 0x7, 0x3, 0x2, 0x2, 0xdb, 0xdf, 0x7, 
    0x1a, 0x2, 0x2, 0xdc, 0xdd, 0x7, 0x45, 0x2, 0x2, 0xdd, 0xde, 0x7, 0x5f, 
    0x2, 0x2, 0xde, 0xe0, 0x7, 0x33, 0x2, 0x2, 0xdf, 0xdc, 0x3, 0x2, 0x2, 
    0x2, 0xdf, 0xe0, 0x3, 0x2, 0x2, 0x2, 0xe0, 0xe1, 0x3, 0x2, 0x2, 0x2, 
    0xe1, 0xe4, 0x5, 0x26, 0x14, 0x2, 0xe2, 0xe3, 0x7, 0x4, 0x2, 0x2, 0xe3, 
    0xe5, 0x5, 0x8c, 0x47, 0x2, 0xe4, 0xe2, 0x3, 0x2, 0x2, 0x2, 0xe4, 0xe5, 
    0x3, 0x2, 0x2, 0x2, 0xe5, 0x138, 0x3, 0x2, 0x2, 0x2, 0xe6, 0xe7, 0x7, 
    0xf, 0x2, 0x2, 0xe7, 0xea, 0x5, 0xc, 0x7, 0x2, 0xe8, 0xe9, 0x7, 0x3c, 
    0x2, 0x2, 0xe9, 0xeb, 0x5, 0x92, 0x4a, 0x2, 0xea, 0xe8, 0x3, 0x2, 0x2, 
    0x2, 0xea, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xeb, 0x138, 0x3, 0x2, 0x2, 0x2, 
    0xec, 0xed, 0x7, 0x16, 0x2, 0x2, 0xed, 0xf0, 0x7, 0x1a, 0x2, 0x2, 0xee, 
    0xef, 0x7, 0x45, 0x2, 0x2, 0xef, 0xf1, 0x7, 0x33, 0x2, 0x2, 0xf0, 0xee, 
    0x3, 0x2, 0x2, 0x2, 0xf0, 0xf1, 0x3, 0x2, 0x2, 0x2, 0xf1, 0xf2, 0x3, 
    0x2, 0x2, 0x2, 0xf2, 0xf5, 0x5, 0x8c, 0x47, 0x2, 0xf3, 0xf4, 0x7, 0x47, 
    0x2, 0x2, 0xf4, 0xf6, 0x5, 0xc, 0x7, 0x2, 0xf5, 0xf3, 0x3, 0x2, 0x2, 
    0x2, 0xf5, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xf6, 0x138, 0x3, 0x2, 0x2, 0x2, 
    0xf7, 0xf8, 0x7, 0x1b, 0x2, 0x2, 0xf8, 0xfb, 0x7, 0x1a, 0x2, 0x2, 0xf9, 
    0xfa, 0x7, 0x45, 0x2, 0x2, 0xfa, 0xfc, 0x7, 0x33, 0x2, 0x2, 0xfb, 0xf9, 
    0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfc, 0xfd, 0x3, 
    0x2, 0x2, 0x2, 0xfd, 0xfe, 0x5, 0x8c, 0x47, 0x2, 0xfe, 0xff, 0x7, 0xa2, 
    0x2, 0x2, 0xff, 0x138, 0x3, 0x2, 0x2, 0x2, 0x100, 0x101, 0x7, 0x27, 
    0x2, 0x2, 0x101, 0x102, 0x7, 0x98, 0x2, 0x2, 0x102, 0x138, 0x5, 0x82, 
    0x42, 0x2, 0x103, 0x104, 0x7, 0x2b, 0x2, 0x2, 0x104, 0x138, 0x5, 0xc, 
    0x7, 0x2, 0x105, 0x106, 0x7, 0x2f, 0x2, 0x2, 0x106, 0x109, 0x7, 0x1a, 
    0x2, 0x2, 0x107, 0x108, 0x7, 0x45, 0x2, 0x2, 0x108, 0x10a, 0x7, 0x33, 
    0x2, 0x2, 0x109, 0x107, 0x3, 0x2, 0x2, 0x2, 0x109, 0x10a, 0x3, 0x2, 
    0x2, 0x2, 0x10a, 0x10b, 0x3, 0x2, 0x2, 0x2, 0x10b, 0x138, 0x5, 0x8c, 
    0x47, 0x2, 0x10c, 0x10d, 0x7, 0x2f, 0x2, 0x2, 0x10d, 0x138, 0x5, 0xc, 
    0x7, 0x2, 0x10e, 0x10f, 0x7, 0x5b, 0x2, 0x2, 0x10f, 0x112, 0x7, 0x1a, 
    0x2, 0x2, 0x110, 0x111, 0x7, 0x45, 0x2, 0x2, 0x111, 0x113, 0x7, 0x33, 
    0x2, 0x2, 0x112, 0x110, 0x3, 0x2, 0x2, 0x2, 0x112, 0x113, 0x3, 0x2, 
    0x2, 0x2, 0x113, 0x114, 0x3, 0x2, 0x2, 0x2, 0x114, 0x138, 0x5, 0x26, 
    0x14, 0x2, 0x115, 0x116, 0x7, 0x5b, 0x2, 0x2, 0x116, 0x117, 0x7, 0x66, 
    0x2, 0x2, 0x117, 0x118, 0x7, 0x12, 0x2, 0x2, 0x118, 0x138, 0x5, 0x82, 
    0x42, 0x2, 0x119, 0x11a, 0x7, 0x5b, 0x2, 0x2, 0x11a, 0x11d, 0x7, 0x1a, 
    0x2, 0x2, 0x11b, 0x11c, 0x7, 0x45, 0x2, 0x2, 0x11c, 0x11e, 0x7, 0x33, 
    0x2, 0x2, 0x11d, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x11d, 0x11e, 0x3, 0x2, 
    0x2, 0x2, 0x11e, 0x11f, 0x3, 0x2, 0x2, 0x2, 0x11f, 0x120, 0x5, 0x8c, 
    0x47, 0x2, 0x120, 0x121, 0x7, 0x6e, 0x2, 0x2, 0x121, 0x122, 0x5, 0xa, 
    0x6, 0x2, 0x122, 0x138, 0x3, 0x2, 0x2, 0x2, 0x123, 0x124, 0x7, 0x5b, 
    0x2, 0x2, 0x124, 0x125, 0x7, 0x8e, 0x2, 0x2, 0x125, 0x138, 0x5, 0x82, 
    0x42, 0x2, 0x126, 0x127, 0x7, 0x6e, 0x2, 0x2, 0x127, 0x138, 0x7, 0x8e, 
    0x2, 0x2, 0x128, 0x129, 0x7, 0x6f, 0x2, 0x2, 0x129, 0x12c, 0x7, 0x1a, 
    0x2, 0x2, 0x12a, 0x12b, 0x7, 0x45, 0x2, 0x2, 0x12b, 0x12d, 0x7, 0x33, 
    0x2, 0x2, 0x12c, 0x12a, 0x3, 0x2, 0x2, 0x2, 0x12c, 0x12d, 0x3, 0x2, 
    0x2, 0x2, 0x12d, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x12e, 0x12f, 0x5, 0x8c, 
    0x47, 0x2, 0x12f, 0x130, 0x7, 0x89, 0x2, 0x2, 0x130, 0x131, 0x5, 0x8c, 
    0x47, 0x2, 0x131, 0x138, 0x3, 0x2, 0x2, 0x2, 0x132, 0x133, 0x7, 0x70, 
    0x2, 0x2, 0x133, 0x134, 0x5, 0xc, 0x7, 0x2, 0x134, 0x135, 0x7, 0x3c, 
    0x2, 0x2, 0x135, 0x136, 0x5, 0x92, 0x4a, 0x2, 0x136, 0x138, 0x3, 0x2, 
    0x2, 0x2, 0x137, 0xda, 0x3, 0x2, 0x2, 0x2, 0x137, 0xe6, 0x3, 0x2, 0x2, 
    0x2, 0x137, 0xec, 0x3, 0x2, 0x2, 0x2, 0x137, 0xf7, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x100, 0x3, 0x2, 0x2, 0x2, 0x137, 0x103, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x105, 0x3, 0x2, 0x2, 0x2, 0x137, 0x10c, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x10e, 0x3, 0x2, 0x2, 0x2, 0x137, 0x115, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x119, 0x3, 0x2, 0x2, 0x2, 0x137, 0x123, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x126, 0x3, 0x2, 0x2, 0x2, 0x137, 0x128, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x132, 0x3, 0x2, 0x2, 0x2, 0x138, 0x9, 0x3, 0x2, 0x2, 0x2, 0x139, 
    0x13a, 0x9, 0x2, 0x2, 0x2, 0x13a, 0xb, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x13c, 
    0x7, 0x69, 0x2, 0x2, 0x13c, 0x141, 0x5, 0x82, 0x42, 0x2, 0x13d, 0x13e, 
    0x7, 0x69, 0x2, 0x2, 0x13e, 0x13f, 0x7, 0x44, 0x2, 0x2, 0x13f, 0x141, 
    0x7, 0xa2, 0x2, 0x2, 0x140, 0x13b, 0x3, 0x2, 0x2, 0x2, 0x140, 0x13d, 
    0x3, 0x2, 0x2, 0x2, 0x141, 0xd, 0x3, 0x2, 0x2, 0x2, 0x142, 0x143, 0x7, 
    0x15, 0x2, 0x2, 0x143, 0x144, 0x7, 0x83, 0x2, 0x2, 0x144, 0x145, 0x5, 
    0x92, 0x4a, 0x2, 0x145, 0xf, 0x3, 0x2, 0x2, 0x2, 0x146, 0x147, 0x9, 
    0x3, 0x2, 0x2, 0x147, 0x14b, 0x7, 0x20, 0x2, 0x2, 0x148, 0x149, 0x7, 
    0x45, 0x2, 0x2, 0x149, 0x14a, 0x7, 0x5f, 0x2, 0x2, 0x14a, 0x14c, 0x7, 
    0x33, 0x2, 0x2, 0x14b, 0x148, 0x3, 0x2, 0x2, 0x2, 0x14b, 0x14c, 0x3, 
    0x2, 0x2, 0x2, 0x14c, 0x14d, 0x3, 0x2, 0x2, 0x2, 0x14d, 0x14f, 0x5, 
    0x98, 0x4d, 0x2, 0x14e, 0x150, 0x5, 0x22, 0x12, 0x2, 0x14f, 0x14e, 0x3, 
    0x2, 0x2, 0x2, 0x14f, 0x150, 0x3, 0x2, 0x2, 0x2, 0x150, 0x188, 0x3, 
    0x2, 0x2, 0x2, 0x151, 0x152, 0x9, 0x3, 0x2, 0x2, 0x152, 0x153, 0x7, 
    0x58, 0x2, 0x2, 0x153, 0x157, 0x7, 0x94, 0x2, 0x2, 0x154, 0x155, 0x7, 
    0x45, 0x2, 0x2, 0x155, 0x156, 0x7, 0x5f, 0x2, 0x2, 0x156, 0x158, 0x7, 
    0x33, 0x2, 0x2, 0x157, 0x154, 0x3, 0x2, 0x2, 0x2, 0x157, 0x158, 0x3, 
    0x2, 0x2, 0x2, 0x158, 0x159, 0x3, 0x2, 0x2, 0x2, 0x159, 0x15b, 0x5, 
    0x92, 0x4a, 0x2, 0x15a, 0x15c, 0x5, 0x16, 0xc, 0x2, 0x15b, 0x15a, 0x3, 
    0x2, 0x2, 0x2, 0x15b, 0x15c, 0x3, 0x2, 0x2, 0x2, 0x15c, 0x162, 0x3, 
    0x2, 0x2, 0x2, 0x15d, 0x163, 0x5, 0x12, 0xa, 0x2, 0x15e, 0x160, 0x5, 
    0x18, 0xd, 0x2, 0x15f, 0x161, 0x7, 0x6a, 0x2, 0x2, 0x160, 0x15f, 0x3, 
    0x2, 0x2, 0x2, 0x160, 0x161, 0x3, 0x2, 0x2, 0x2, 0x161, 0x163, 0x3, 
    0x2, 0x2, 0x2, 0x162, 0x15d, 0x3, 0x2, 0x2, 0x2, 0x162, 0x15e, 0x3, 
    0x2, 0x2, 0x2, 0x163, 0x164, 0x3, 0x2, 0x2, 0x2, 0x164, 0x165, 0x5, 
    0x14, 0xb, 0x2, 0x165, 0x188, 0x3, 0x2, 0x2, 0x2, 0x166, 0x168, 0x9, 
    0x3, 0x2, 0x2, 0x167, 0x169, 0x7, 0x85, 0x2, 0x2, 0x168, 0x167, 0x3, 
    0x2, 0x2, 0x2, 0x168, 0x169, 0x3, 0x2, 0x2, 0x2, 0x169, 0x16a, 0x3, 
    0x2, 0x2, 0x2, 0x16a, 0x16e, 0x7, 0x83, 0x2, 0x2, 0x16b, 0x16c, 0x7, 
    0x45, 0x2, 0x2, 0x16c, 0x16d, 0x7, 0x5f, 0x2, 0x2, 0x16d, 0x16f, 0x7, 
    0x33, 0x2, 0x2, 0x16e, 0x16b, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x16f, 0x3, 
    0x2, 0x2, 0x2, 0x16f, 0x170, 0x3, 0x2, 0x2, 0x2, 0x170, 0x172, 0x5, 
    0x92, 0x4a, 0x2, 0x171, 0x173, 0x5, 0x16, 0xc, 0x2, 0x172, 0x171, 0x3, 
    0x2, 0x2, 0x2, 0x172, 0x173, 0x3, 0x2, 0x2, 0x2, 0x173, 0x175, 0x3, 
    0x2, 0x2, 0x2, 0x174, 0x176, 0x5, 0x18, 0xd, 0x2, 0x175, 0x174, 0x3, 
    0x2, 0x2, 0x2, 0x175, 0x176, 0x3, 0x2, 0x2, 0x2, 0x176, 0x178, 0x3, 
    0x2, 0x2, 0x2, 0x177, 0x179, 0x5, 0x14, 0xb, 0x2, 0x178, 0x177, 0x3, 
    0x2, 0x2, 0x2, 0x178, 0x179, 0x3, 0x2, 0x2, 0x2, 0x179, 0x188, 0x3, 
    0x2, 0x2, 0x2, 0x17a, 0x17b, 0x9, 0x3, 0x2, 0x2, 0x17b, 0x17f, 0x7, 
    0x94, 0x2, 0x2, 0x17c, 0x17d, 0x7, 0x45, 0x2, 0x2, 0x17d, 0x17e, 0x7, 
    0x5f, 0x2, 0x2, 0x17e, 0x180, 0x7, 0x33, 0x2, 0x2, 0x17f, 0x17c, 0x3, 
    0x2, 0x2, 0x2, 0x17f, 0x180, 0x3, 0x2, 0x2, 0x2, 0x180, 0x181, 0x3, 
    0x2, 0x2, 0x2, 0x181, 0x183, 0x5, 0x92, 0x4a, 0x2, 0x182, 0x184, 0x5, 
    0x16, 0xc, 0x2, 0x183, 0x182, 0x3, 0x2, 0x2, 0x2, 0x183, 0x184, 0x3, 
    0x2, 0x2, 0x2, 0x184, 0x185, 0x3, 0x2, 0x2, 0x2, 0x185, 0x186, 0x5, 
    0x14, 0xb, 0x2, 0x186, 0x188, 0x3, 0x2, 0x2, 0x2, 0x187, 0x146, 0x3, 
    0x2, 0x2, 0x2, 0x187, 0x151, 0x3, 0x2, 0x2, 0x2, 0x187, 0x166, 0x3, 
    0x2, 0x2, 0x2, 0x187, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x188, 0x11, 0x3, 0x2, 
    0x2, 0x2, 0x189, 0x18a, 0x7, 0x89, 0x2, 0x2, 0x18a, 0x18b, 0x5, 0x92, 
    0x4a, 0x2, 0x18b, 0x13, 0x3, 0x2, 0x2, 0x2, 0x18c, 0x18d, 0x7, 0xc, 
    0x2, 0x2, 0x18d, 0x18e, 0x5, 0x40, 0x21, 0x2, 0x18e, 0x15, 0x3, 0x2, 
    0x2, 0x2, 0x18f, 0x190, 0x7, 0xb3, 0x2, 0x2, 0x190, 0x195, 0x5, 0x24, 
    0x13, 0x2, 0x191, 0x192, 0x7, 0xa8, 0x2, 0x2, 0x192, 0x194, 0x5, 0x24, 
    0x13, 0x2, 0x193, 0x191, 0x3, 0x2, 0x2, 0x2, 0x194, 0x197, 0x3, 0x2, 
    0x2, 0x2, 0x195, 0x193, 0x3, 0x2, 0x2, 0x2, 0x195, 0x196, 0x3, 0x2, 
    0x2, 0x2, 0x196, 0x198, 0x3, 0x2, 0x2, 0x2, 0x197, 0x195, 0x3, 0x2, 
    0x2, 0x2, 0x198, 0x199, 0x7, 0xbd, 0x2, 0x2, 0x199, 0x19f, 0x3, 0x2, 
    0x2, 0x2, 0x19a, 0x19b, 0x7, 0xc, 0x2, 0x2, 0x19b, 0x19f, 0x5, 0x92, 
    0x4a, 0x2, 0x19c, 0x19d, 0x7, 0xc, 0x2, 0x2, 0x19d, 0x19f, 0x5, 0x90, 
    0x49, 0x2, 0x19e, 0x18f, 0x3, 0x2, 0x2, 0x2, 0x19e, 0x19a, 0x3, 0x2, 
    0x2, 0x2, 0x19e, 0x19c, 0x3, 0x2, 0x2, 0x2, 0x19f, 0x17, 0x3, 0x2, 0x2, 
    0x2, 0x1a0, 0x1bb, 0x5, 0x22, 0x12, 0x2, 0x1a1, 0x1a2, 0x6, 0xd, 0x2, 
    0x3, 0x1a2, 0x1a3, 0x5, 0x54, 0x2b, 0x2, 0x1a3, 0x1a4, 0x8, 0xd, 0x1, 
    0x2, 0x1a4, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1a5, 0x1a6, 0x6, 0xd, 0x3, 
    0x3, 0x1a6, 0x1a7, 0x5, 0x1a, 0xe, 0x2, 0x1a7, 0x1a8, 0x8, 0xd, 0x1, 
    0x2, 0x1a8, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1a9, 0x1aa, 0x6, 0xd, 0x4, 
    0x3, 0x1aa, 0x1ab, 0x5, 0x1c, 0xf, 0x2, 0x1ab, 0x1ac, 0x8, 0xd, 0x1, 
    0x2, 0x1ac, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1ad, 0x1ae, 0x6, 0xd, 0x5, 
    0x3, 0x1ae, 0x1af, 0x5, 0x1e, 0x10, 0x2, 0x1af, 0x1b0, 0x8, 0xd, 0x1, 
    0x2, 0x1b0, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b2, 0x6, 0xd, 0x6, 
    0x3, 0x1b2, 0x1b3, 0x5, 0x20, 0x11, 0x2, 0x1b3, 0x1b4, 0x8, 0xd, 0x1, 
    0x2, 0x1b4, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1b5, 0x1b6, 0x6, 0xd, 0x7, 
    0x3, 0x1b6, 0x1b7, 0x5, 0x5a, 0x2e, 0x2, 0x1b7, 0x1b8, 0x8, 0xd, 0x1, 
    0x2, 0x1b8, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1a1, 0x3, 0x2, 0x2, 
    0x2, 0x1b9, 0x1a5, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1a9, 0x3, 0x2, 0x2, 
    0x2, 0x1b9, 0x1ad, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1b1, 0x3, 0x2, 0x2, 
    0x2, 0x1b9, 0x1b5, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1bd, 0x3, 0x2, 0x2, 
    0x2, 0x1bb, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x3, 0x2, 0x2, 
    0x2, 0x1bc, 0x19, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1bb, 0x3, 0x2, 0x2, 0x2, 
    0x1be, 0x1bf, 0x7, 0x69, 0x2, 0x2, 0x1bf, 0x1c0, 0x7, 0x12, 0x2, 0x2, 
    0x1c0, 0x1c1, 0x5, 0x82, 0x42, 0x2, 0x1c1, 0x1b, 0x3, 0x2, 0x2, 0x2, 
    0x1c2, 0x1c3, 0x7, 0x6c, 0x2, 0x2, 0x1c3, 0x1c4, 0x7, 0x50, 0x2, 0x2, 
    0x1c4, 0x1c5, 0x5, 0x82, 0x42, 0x2, 0x1c5, 0x1d, 0x3, 0x2, 0x2, 0x2, 
    0x1c6, 0x1c7, 0x7, 0x75, 0x2, 0x2, 0x1c7, 0x1c8, 0x7, 0x12, 0x2, 0x2, 
    0x1c8, 0x1c9, 0x5, 0x82, 0x42, 0x2, 0x1c9, 0x1f, 0x3, 0x2, 0x2, 0x2, 
    0x1ca, 0x1cb, 0x7, 0x8e, 0x2, 0x2, 0x1cb, 0x1d0, 0x5, 0x2c, 0x17, 0x2, 
    0x1cc, 0x1cd, 0x7, 0xa8, 0x2, 0x2, 0x1cd, 0x1cf, 0x5, 0x2c, 0x17, 0x2, 
    0x1ce, 0x1cc, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d2, 0x3, 0x2, 0x2, 0x2, 
    0x1d0, 0x1ce, 0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x3, 0x2, 0x2, 0x2, 
    0x1d1, 0x21, 0x3, 0x2, 0x2, 0x2, 0x1d2, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1d3, 
    0x1d5, 0x7, 0x32, 0x2, 0x2, 0x1d4, 0x1d6, 0x7, 0xad, 0x2, 0x2, 0x1d5, 
    0x1d4, 0x3, 0x2, 0x2, 0x2, 0x1d5, 0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d6, 
    0x1d7, 0x3, 0x2, 0x2, 0x2, 0x1d7, 0x1dd, 0x5, 0xaa, 0x56, 0x2, 0x1d8, 
    0x1da, 0x7, 0xb3, 0x2, 0x2, 0x1d9, 0x1db, 0x5, 0x7e, 0x40, 0x2, 0x1da, 
    0x1d9, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1db, 0x3, 0x2, 0x2, 0x2, 0x1db, 
    0x1dc, 0x3, 0x2, 0x2, 0x2, 0x1dc, 0x1de, 0x7, 0xbd, 0x2, 0x2, 0x1dd, 
    0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1de, 0x3, 0x2, 0x2, 0x2, 0x1de, 
    0x23, 0x3, 0x2, 0x2, 0x2, 0x1df, 0x1ee, 0x5, 0x26, 0x14, 0x2, 0x1e0, 
    0x1e1, 0x7, 0x1c, 0x2, 0x2, 0x1e1, 0x1e2, 0x5, 0xa8, 0x55, 0x2, 0x1e2, 
    0x1e3, 0x7, 0x15, 0x2, 0x2, 0x1e3, 0x1e4, 0x5, 0x82, 0x42, 0x2, 0x1e4, 
    0x1ee, 0x3, 0x2, 0x2, 0x2, 0x1e5, 0x1e6, 0x7, 0x48, 0x2, 0x2, 0x1e6, 
    0x1e7, 0x5, 0xa8, 0x55, 0x2, 0x1e7, 0x1e8, 0x5, 0x82, 0x42, 0x2, 0x1e8, 
    0x1e9, 0x7, 0x8f, 0x2, 0x2, 0x1e9, 0x1ea, 0x5, 0x7c, 0x3f, 0x2, 0x1ea, 
    0x1eb, 0x7, 0x40, 0x2, 0x2, 0x1eb, 0x1ec, 0x7, 0xa0, 0x2, 0x2, 0x1ec, 
    0x1ee, 0x3, 0x2, 0x2, 0x2, 0x1ed, 0x1df, 0x3, 0x2, 0x2, 0x2, 0x1ed, 
    0x1e0, 0x3, 0x2, 0x2, 0x2, 0x1ed, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1ee, 
    0x25, 0x3, 0x2, 0x2, 0x2, 0x1ef, 0x1f0, 0x5, 0x8c, 0x47, 0x2, 0x1f0, 
    0x1f2, 0x5, 0x7c, 0x3f, 0x2, 0x1f1, 0x1f3, 0x5, 0x28, 0x15, 0x2, 0x1f2, 
    0x1f1, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f3, 
    0x1f6, 0x3, 0x2, 0x2, 0x2, 0x1f4, 0x1f5, 0x7, 0x1b, 0x2, 0x2, 0x1f5, 
    0x1f7, 0x7, 0xa2, 0x2, 0x2, 0x1f6, 0x1f4, 0x3, 0x2, 0x2, 0x2, 0x1f6, 
    0x1f7, 0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x1f8, 
    0x1fa, 0x5, 0x2a, 0x16, 0x2, 0x1f9, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1f9, 
    0x1fa, 0x3, 0x2, 0x2, 0x2, 0x1fa, 0x1fd, 0x3, 0x2, 0x2, 0x2, 0x1fb, 
    0x1fc, 0x7, 0x8e, 0x2, 0x2, 0x1fc, 0x1fe, 0x5, 0x82, 0x42, 0x2, 0x1fd, 
    0x1fb, 0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1fe, 
    0x210, 0x3, 0x2, 0x2, 0x2, 0x1ff, 0x201, 0x5, 0x8c, 0x47, 0x2, 0x200, 
    0x202, 0x5, 0x7c, 0x3f, 0x2, 0x201, 0x200, 0x3, 0x2, 0x2, 0x2, 0x201, 
    0x202, 0x3, 0x2, 0x2, 0x2, 0x202, 0x203, 0x3, 0x2, 0x2, 0x2, 0x203, 
    0x206, 0x5, 0x28, 0x15, 0x2, 0x204, 0x205, 0x7, 0x1b, 0x2, 0x2, 0x205, 
    0x207, 0x7, 0xa2, 0x2, 0x2, 0x206, 0x204, 0x3, 0x2, 0x2, 0x2, 0x206, 
    0x207, 0x3, 0x2, 0x2, 0x2, 0x207, 0x209, 0x3, 0x2, 0x2, 0x2, 0x208, 
    0x20a, 0x5, 0x2a, 0x16, 0x2, 0x209, 0x208, 0x3, 0x2, 0x2, 0x2, 0x209, 
    0x20a, 0x3, 0x2, 0x2, 0x2, 0x20a, 0x20d, 0x3, 0x2, 0x2, 0x2, 0x20b, 
    0x20c, 0x7, 0x8e, 0x2, 0x2, 0x20c, 0x20e, 0x5, 0x82, 0x42, 0x2, 0x20d, 
    0x20b, 0x3, 0x2, 0x2, 0x2, 0x20d, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x20e, 
    0x210, 0x3, 0x2, 0x2, 0x2, 0x20f, 0x1ef, 0x3, 0x2, 0x2, 0x2, 0x20f, 
    0x1ff, 0x3, 0x2, 0x2, 0x2, 0x210, 0x27, 0x3, 0x2, 0x2, 0x2, 0x211, 0x212, 
    0x9, 0x4, 0x2, 0x2, 0x212, 0x213, 0x5, 0x82, 0x42, 0x2, 0x213, 0x29, 
    0x3, 0x2, 0x2, 0x2, 0x214, 0x215, 0x7, 0x18, 0x2, 0x2, 0x215, 0x216, 
    0x7, 0xb3, 0x2, 0x2, 0x216, 0x21c, 0x5, 0xa8, 0x55, 0x2, 0x217, 0x219, 
    0x7, 0xb3, 0x2, 0x2, 0x218, 0x21a, 0x5, 0x7e, 0x40, 0x2, 0x219, 0x218, 
    0x3, 0x2, 0x2, 0x2, 0x219, 0x21a, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x21b, 
    0x3, 0x2, 0x2, 0x2, 0x21b, 0x21d, 0x7, 0xbd, 0x2, 0x2, 0x21c, 0x217, 
    0x3, 0x2, 0x2, 0x2, 0x21c, 0x21d, 0x3, 0x2, 0x2, 0x2, 0x21d, 0x21e, 
    0x3, 0x2, 0x2, 0x2, 0x21e, 0x21f, 0x7, 0xbd, 0x2, 0x2, 0x21f, 0x2b, 
    0x3, 0x2, 0x2, 0x2, 0x220, 0x228, 0x5, 0x82, 0x42, 0x2, 0x221, 0x229, 
    0x7, 0x27, 0x2, 0x2, 0x222, 0x223, 0x7, 0x89, 0x2, 0x2, 0x223, 0x224, 
    0x7, 0x2c, 0x2, 0x2, 0x224, 0x229, 0x7, 0xa2, 0x2, 0x2, 0x225, 0x226, 
    0x7, 0x89, 0x2, 0x2, 0x226, 0x227, 0x7, 0x95, 0x2, 0x2, 0x227, 0x229, 
    0x7, 0xa2, 0x2, 0x2, 0x228, 0x221, 0x3, 0x2, 0x2, 0x2, 0x228, 0x222, 
    0x3, 0x2, 0x2, 0x2, 0x228, 0x225, 0x3, 0x2, 0x2, 0x2, 0x228, 0x229, 
    0x3, 0x2, 0x2, 0x2, 0x229, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x22a, 0x22c, 0x9, 
    0x5, 0x2, 0x2, 0x22b, 0x22d, 0x7, 0x83, 0x2, 0x2, 0x22c, 0x22b, 0x3, 
    0x2, 0x2, 0x2, 0x22c, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22e, 0x3, 
    0x2, 0x2, 0x2, 0x22e, 0x22f, 0x5, 0x8e, 0x48, 0x2, 0x22f, 0x2f, 0x3, 
    0x2, 0x2, 0x2, 0x230, 0x231, 0x9, 0x6, 0x2, 0x2, 0x231, 0x234, 0x7, 
    0x20, 0x2, 0x2, 0x232, 0x233, 0x7, 0x45, 0x2, 0x2, 0x233, 0x235, 0x7, 
    0x33, 0x2, 0x2, 0x234, 0x232, 0x3, 0x2, 0x2, 0x2, 0x234, 0x235, 0x3, 
    0x2, 0x2, 0x2, 0x235, 0x236, 0x3, 0x2, 0x2, 0x2, 0x236, 0x246, 0x5, 
    0x98, 0x4d, 0x2, 0x237, 0x239, 0x9, 0x6, 0x2, 0x2, 0x238, 0x23a, 0x7, 
    0x85, 0x2, 0x2, 0x239, 0x238, 0x3, 0x2, 0x2, 0x2, 0x239, 0x23a, 0x3, 
    0x2, 0x2, 0x2, 0x23a, 0x23b, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x23e, 0x7, 
    0x83, 0x2, 0x2, 0x23c, 0x23d, 0x7, 0x45, 0x2, 0x2, 0x23d, 0x23f, 0x7, 
    0x33, 0x2, 0x2, 0x23e, 0x23c, 0x3, 0x2, 0x2, 0x2, 0x23e, 0x23f, 0x3, 
    0x2, 0x2, 0x2, 0x23f, 0x240, 0x3, 0x2, 0x2, 0x2, 0x240, 0x243, 0x5, 
    0x92, 0x4a, 0x2, 0x241, 0x242, 0x7, 0x5e, 0x2, 0x2, 0x242, 0x244, 0x7, 
    0x26, 0x2, 0x2, 0x243, 0x241, 0x3, 0x2, 0x2, 0x2, 0x243, 0x244, 0x3, 
    0x2, 0x2, 0x2, 0x244, 0x246, 0x3, 0x2, 0x2, 0x2, 0x245, 0x230, 0x3, 
    0x2, 0x2, 0x2, 0x245, 0x237, 0x3, 0x2, 0x2, 0x2, 0x246, 0x31, 0x3, 0x2, 
    0x2, 0x2, 0x247, 0x249, 0x7, 0x33, 0x2, 0x2, 0x248, 0x24a, 0x7, 0x85, 
    0x2, 0x2, 0x249, 0x248, 0x3, 0x2, 0x2, 0x2, 0x249, 0x24a, 0x3, 0x2, 
    0x2, 0x2, 0x24a, 0x24b, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24c, 0x7, 0x83, 
    0x2, 0x2, 0x24c, 0x24d, 0x5, 0x92, 0x4a, 0x2, 0x24d, 0x33, 0x3, 0x2, 
    0x2, 0x2, 0x24e, 0x24f, 0x7, 0x34, 0x2, 0x2, 0x24f, 0x250, 0x7, 0x81, 
    0x2, 0x2, 0x250, 0x251, 0x5, 0x4, 0x3, 0x2, 0x251, 0x35, 0x3, 0x2, 0x2, 
    0x2, 0x252, 0x253, 0x7, 0x4b, 0x2, 0x2, 0x253, 0x255, 0x7, 0x4d, 0x2, 
    0x2, 0x254, 0x256, 0x7, 0x83, 0x2, 0x2, 0x255, 0x254, 0x3, 0x2, 0x2, 
    0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 0x2, 0x256, 0x25a, 0x3, 0x2, 0x2, 
    0x2, 0x257, 0x25b, 0x5, 0x92, 0x4a, 0x2, 0x258, 0x259, 0x7, 0x3e, 0x2, 
    0x2, 0x259, 0x25b, 0x5, 0x90, 0x49, 0x2, 0x25a, 0x257, 0x3, 0x2, 0x2, 
    0x2, 0x25a, 0x258, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x25d, 0x3, 0x2, 0x2, 
    0x2, 0x25c, 0x25e, 0x5, 0x38, 0x1d, 0x2, 0x25d, 0x25c, 0x3, 0x2, 0x2, 
    0x2, 0x25d, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x25f, 0x3, 0x2, 0x2, 
    0x2, 0x25f, 0x260, 0x5, 0x3a, 0x1e, 0x2, 0x260, 0x37, 0x3, 0x2, 0x2, 
    0x2, 0x261, 0x262, 0x7, 0xb3, 0x2, 0x2, 0x262, 0x267, 0x5, 0x8c, 0x47, 
    0x2, 0x263, 0x264, 0x7, 0xa8, 0x2, 0x2, 0x264, 0x266, 0x5, 0x8c, 0x47, 
    0x2, 0x265, 0x263, 0x3, 0x2, 0x2, 0x2, 0x266, 0x269, 0x3, 0x2, 0x2, 
    0x2, 0x267, 0x265, 0x3, 0x2, 0x2, 0x2, 0x267, 0x268, 0x3, 0x2, 0x2, 
    0x2, 0x268, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x269, 0x267, 0x3, 0x2, 0x2, 
    0x2, 0x26a, 0x26b, 0x7, 0xbd, 0x2, 0x2, 0x26b, 0x39, 0x3, 0x2, 0x2, 
    0x2, 0x26c, 0x26d, 0x7, 0x3b, 0x2, 0x2, 0x26d, 0x271, 0x5, 0xa8, 0x55, 
    0x2, 0x26e, 0x271, 0x7, 0x93, 0x2, 0x2, 0x26f, 0x271, 0x5, 0x40, 0x21, 
    0x2, 0x270, 0x26c, 0x3, 0x2, 0x2, 0x2, 0x270, 0x26e, 0x3, 0x2, 0x2, 
    0x2, 0x270, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x271, 0x3b, 0x3, 0x2, 0x2, 0x2, 
    0x272, 0x273, 0x7, 0x64, 0x2, 0x2, 0x273, 0x274, 0x7, 0x83, 0x2, 0x2, 
    0x274, 0x276, 0x5, 0x92, 0x4a, 0x2, 0x275, 0x277, 0x5, 0xc, 0x7, 0x2, 
    0x276, 0x275, 0x3, 0x2, 0x2, 0x2, 0x276, 0x277, 0x3, 0x2, 0x2, 0x2, 
    0x277, 0x279, 0x3, 0x2, 0x2, 0x2, 0x278, 0x27a, 0x7, 0x37, 0x2, 0x2, 
    0x279, 0x278, 0x3, 0x2, 0x2, 0x2, 0x279, 0x27a, 0x3, 0x2, 0x2, 0x2, 
    0x27a, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x27d, 0x7, 0x24, 0x2, 0x2, 
    0x27c, 0x27b, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x27d, 0x3, 0x2, 0x2, 0x2, 
    0x27d, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x27e, 0x27f, 0x7, 0x6f, 0x2, 0x2, 
    0x27f, 0x280, 0x7, 0x83, 0x2, 0x2, 0x280, 0x281, 0x5, 0x92, 0x4a, 0x2, 
    0x281, 0x282, 0x7, 0x89, 0x2, 0x2, 0x282, 0x28a, 0x5, 0x92, 0x4a, 0x2, 
    0x283, 0x284, 0x7, 0xa8, 0x2, 0x2, 0x284, 0x285, 0x5, 0x92, 0x4a, 0x2, 
    0x285, 0x286, 0x7, 0x89, 0x2, 0x2, 0x286, 0x287, 0x5, 0x92, 0x4a, 0x2, 
    0x287, 0x289, 0x3, 0x2, 0x2, 0x2, 0x288, 0x283, 0x3, 0x2, 0x2, 0x2, 
    0x289, 0x28c, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x288, 0x3, 0x2, 0x2, 0x2, 
    0x28a, 0x28b, 0x3, 0x2, 0x2, 0x2, 0x28b, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x28c, 
    0x28a, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x293, 0x5, 0x42, 0x22, 0x2, 0x28e, 
    0x28f, 0x7, 0x90, 0x2, 0x2, 0x28f, 0x290, 0x7, 0x6, 0x2, 0x2, 0x290, 
    0x292, 0x5, 0x42, 0x22, 0x2, 0x291, 0x28e, 0x3, 0x2, 0x2, 0x2, 0x292, 
    0x295, 0x3, 0x2, 0x2, 0x2, 0x293, 0x291, 0x3, 0x2, 0x2, 0x2, 0x293, 
    0x294, 0x3, 0x2, 0x2, 0x2, 0x294, 0x41, 0x3, 0x2, 0x2, 0x2, 0x295, 0x293, 
    0x3, 0x2, 0x2, 0x2, 0x296, 0x29c, 0x5, 0x44, 0x23, 0x2, 0x297, 0x298, 
    0x7, 0xb3, 0x2, 0x2, 0x298, 0x299, 0x5, 0x40, 0x21, 0x2, 0x299, 0x29a, 
    0x7, 0xbd, 0x2, 0x2, 0x29a, 0x29c, 0x3, 0x2, 0x2, 0x2, 0x29b, 0x296, 
    0x3, 0x2, 0x2, 0x2, 0x29b, 0x297, 0x3, 0x2, 0x2, 0x2, 0x29c, 0x43, 0x3, 
    0x2, 0x2, 0x2, 0x29d, 0x29f, 0x5, 0x46, 0x24, 0x2, 0x29e, 0x29d, 0x3, 
    0x2, 0x2, 0x2, 0x29e, 0x29f, 0x3, 0x2, 0x2, 0x2, 0x29f, 0x2a0, 0x3, 
    0x2, 0x2, 0x2, 0x2a0, 0x2a2, 0x7, 0x77, 0x2, 0x2, 0x2a1, 0x2a3, 0x7, 
    0x2d, 0x2, 0x2, 0x2a2, 0x2a1, 0x3, 0x2, 0x2, 0x2, 0x2a2, 0x2a3, 0x3, 
    0x2, 0x2, 0x2, 0x2a3, 0x2a4, 0x3, 0x2, 0x2, 0x2, 0x2a4, 0x2a6, 0x5, 
    0x7e, 0x40, 0x2, 0x2a5, 0x2a7, 0x5, 0x48, 0x25, 0x2, 0x2a6, 0x2a5, 0x3, 
    0x2, 0x2, 0x2, 0x2a6, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x2a7, 0x2a9, 0x3, 
    0x2, 0x2, 0x2, 0x2a8, 0x2aa, 0x5, 0x4a, 0x26, 0x2, 0x2a9, 0x2a8, 0x3, 
    0x2, 0x2, 0x2, 0x2a9, 0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2aa, 0x2ac, 0x3, 
    0x2, 0x2, 0x2, 0x2ab, 0x2ad, 0x5, 0x4c, 0x27, 0x2, 0x2ac, 0x2ab, 0x3, 
    0x2, 0x2, 0x2, 0x2ac, 0x2ad, 0x3, 0x2, 0x2, 0x2, 0x2ad, 0x2af, 0x3, 
    0x2, 0x2, 0x2, 0x2ae, 0x2b0, 0x5, 0x4e, 0x28, 0x2, 0x2af, 0x2ae, 0x3, 
    0x2, 0x2, 0x2, 0x2af, 0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b0, 0x2b2, 0x3, 
    0x2, 0x2, 0x2, 0x2b1, 0x2b3, 0x5, 0x50, 0x29, 0x2, 0x2b2, 0x2b1, 0x3, 
    0x2, 0x2, 0x2, 0x2b2, 0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2b6, 0x3, 
    0x2, 0x2, 0x2, 0x2b4, 0x2b5, 0x7, 0x99, 0x2, 0x2, 0x2b5, 0x2b7, 0x9, 
    0x7, 0x2, 0x2, 0x2b6, 0x2b4, 0x3, 0x2, 0x2, 0x2, 0x2b6, 0x2b7, 0x3, 
    0x2, 0x2, 0x2, 0x2b7, 0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b9, 0x7, 
    0x99, 0x2, 0x2, 0x2b9, 0x2bb, 0x7, 0x8a, 0x2, 0x2, 0x2ba, 0x2b8, 0x3, 
    0x2, 0x2, 0x2, 0x2ba, 0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2bb, 0x2bd, 0x3, 
    0x2, 0x2, 0x2, 0x2bc, 0x2be, 0x5, 0x52, 0x2a, 0x2, 0x2bd, 0x2bc, 0x3, 
    0x2, 0x2, 0x2, 0x2bd, 0x2be, 0x3, 0x2, 0x2, 0x2, 0x2be, 0x2c0, 0x3, 
    0x2, 0x2, 0x2, 0x2bf, 0x2c1, 0x5, 0x54, 0x2b, 0x2, 0x2c0, 0x2bf, 0x3, 
    0x2, 0x2, 0x2, 0x2c0, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2c1, 0x2c3, 0x3, 
    0x2, 0x2, 0x2, 0x2c2, 0x2c4, 0x5, 0x56, 0x2c, 0x2, 0x2c3, 0x2c2, 0x3, 
    0x2, 0x2, 0x2, 0x2c3, 0x2c4, 0x3, 0x2, 0x2, 0x2, 0x2c4, 0x2c6, 0x3, 
    0x2, 0x2, 0x2, 0x2c5, 0x2c7, 0x5, 0x58, 0x2d, 0x2, 0x2c6, 0x2c5, 0x3, 
    0x2, 0x2, 0x2, 0x2c6, 0x2c7, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c9, 0x3, 
    0x2, 0x2, 0x2, 0x2c8, 0x2ca, 0x5, 0x5a, 0x2e, 0x2, 0x2c9, 0x2c8, 0x3, 
    0x2, 0x2, 0x2, 0x2c9, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2ca, 0x45, 0x3, 0x2, 
    0x2, 0x2, 0x2cb, 0x2cc, 0x7, 0x99, 0x2, 0x2, 0x2cc, 0x2cd, 0x5, 0x7e, 
    0x40, 0x2, 0x2cd, 0x47, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2cf, 0x7, 0x3c, 
    0x2, 0x2, 0x2cf, 0x2d0, 0x5, 0x5c, 0x2f, 0x2, 0x2d0, 0x49, 0x3, 0x2, 
    0x2, 0x2, 0x2d1, 0x2d3, 0x9, 0x8, 0x2, 0x2, 0x2d2, 0x2d1, 0x3, 0x2, 
    0x2, 0x2, 0x2d2, 0x2d3, 0x3, 0x2, 0x2, 0x2, 0x2d3, 0x2d4, 0x3, 0x2, 
    0x2, 0x2, 0x2d4, 0x2d5, 0x7, 0xb, 0x2, 0x2, 0x2d5, 0x2d6, 0x7, 0x4f, 
    0x2, 0x2, 0x2d6, 0x2d7, 0x5, 0x7e, 0x40, 0x2, 0x2d7, 0x4b, 0x3, 0x2, 
    0x2, 0x2, 0x2d8, 0x2d9, 0x7, 0x6b, 0x2, 0x2, 0x2d9, 0x2da, 0x5, 0x82, 
    0x42, 0x2, 0x2da, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x2db, 0x2dc, 0x7, 0x98, 
    0x2, 0x2, 0x2dc, 0x2dd, 0x5, 0x82, 0x42, 0x2, 0x2dd, 0x4f, 0x3, 0x2, 
    0x2, 0x2, 0x2de, 0x2df, 0x7, 0x41, 0x2, 0x2, 0x2df, 0x2e6, 0x7, 0x12, 
    0x2, 0x2, 0x2e0, 0x2e1, 0x7, 0x74, 0x2, 0x2, 0x2e1, 0x2e2, 0x7, 0xb3, 
    0x2, 0x2, 0x2e2, 0x2e3, 0x5, 0x7e, 0x40, 0x2, 0x2e3, 0x2e4, 0x7, 0xbd, 
    0x2, 0x2, 0x2e4, 0x2e7, 0x3, 0x2, 0x2, 0x2, 0x2e5, 0x2e7, 0x5, 0x7e, 
    0x40, 0x2, 0x2e6, 0x2e0, 0x3, 0x2, 0x2, 0x2, 0x2e6, 0x2e5, 0x3, 0x2, 
    0x2, 0x2, 0x2e7, 0x51, 0x3, 0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x7, 0x42, 
    0x2, 0x2, 0x2e9, 0x2ea, 0x5, 0x82, 0x42, 0x2, 0x2ea, 0x53, 0x3, 0x2, 
    0x2, 0x2, 0x2eb, 0x2ec, 0x7, 0x66, 0x2, 0x2, 0x2ec, 0x2ed, 0x7, 0x12, 
    0x2, 0x2, 0x2ed, 0x2ee, 0x5, 0x68, 0x35, 0x2, 0x2ee, 0x55, 0x3, 0x2, 
    0x2, 0x2, 0x2ef, 0x2f0, 0x7, 0x55, 0x2, 0x2, 0x2f0, 0x2f1, 0x5, 0x66, 
    0x34, 0x2, 0x2f1, 0x2f2, 0x7, 0x12, 0x2, 0x2, 0x2f2, 0x2f3, 0x5, 0x7e, 
    0x40, 0x2, 0x2f3, 0x57, 0x3, 0x2, 0x2, 0x2, 0x2f4, 0x2f5, 0x7, 0x55, 
    0x2, 0x2, 0x2f5, 0x2f8, 0x5, 0x66, 0x34, 0x2, 0x2f6, 0x2f7, 0x7, 0x99, 
    0x2, 0x2, 0x2f7, 0x2f9, 0x7, 0x87, 0x2, 0x2, 0x2f8, 0x2f6, 0x3, 0x2, 
    0x2, 0x2, 0x2f8, 0x2f9, 0x3, 0x2, 0x2, 0x2, 0x2f9, 0x59, 0x3, 0x2, 0x2, 
    0x2, 0x2fa, 0x2fb, 0x7, 0x7b, 0x2, 0x2, 0x2fb, 0x2fc, 0x5, 0x6e, 0x38, 
    0x2, 0x2fc, 0x5b, 0x3, 0x2, 0x2, 0x2, 0x2fd, 0x2fe, 0x8, 0x2f, 0x1, 
    0x2, 0x2fe, 0x300, 0x5, 0x8e, 0x48, 0x2, 0x2ff, 0x301, 0x7, 0x37, 0x2, 
    0x2, 0x300, 0x2ff, 0x3, 0x2, 0x2, 0x2, 0x300, 0x301, 0x3, 0x2, 0x2, 
    0x2, 0x301, 0x303, 0x3, 0x2, 0x2, 0x2, 0x302, 0x304, 0x5, 0x64, 0x33, 
    0x2, 0x303, 0x302, 0x3, 0x2, 0x2, 0x2, 0x303, 0x304, 0x3, 0x2, 0x2, 
    0x2, 0x304, 0x30a, 0x3, 0x2, 0x2, 0x2, 0x305, 0x306, 0x7, 0xb3, 0x2, 
    0x2, 0x306, 0x307, 0x5, 0x5c, 0x2f, 0x2, 0x307, 0x308, 0x7, 0xbd, 0x2, 
    0x2, 0x308, 0x30a, 0x3, 0x2, 0x2, 0x2, 0x309, 0x2fd, 0x3, 0x2, 0x2, 
    0x2, 0x309, 0x305, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x31c, 0x3, 0x2, 0x2, 
    0x2, 0x30b, 0x30c, 0xc, 0x5, 0x2, 0x2, 0x30c, 0x30d, 0x5, 0x60, 0x31, 
    0x2, 0x30d, 0x30e, 0x5, 0x5c, 0x2f, 0x6, 0x30e, 0x31b, 0x3, 0x2, 0x2, 
    0x2, 0x30f, 0x311, 0xc, 0x6, 0x2, 0x2, 0x310, 0x312, 0x9, 0x9, 0x2, 
    0x2, 0x311, 0x310, 0x3, 0x2, 0x2, 0x2, 0x311, 0x312, 0x3, 0x2, 0x2, 
    0x2, 0x312, 0x314, 0x3, 0x2, 0x2, 0x2, 0x313, 0x315, 0x5, 0x5e, 0x30, 
    0x2, 0x314, 0x313, 0x3, 0x2, 0x2, 0x2, 0x314, 0x315, 0x3, 0x2, 0x2, 
    0x2, 0x315, 0x316, 0x3, 0x2, 0x2, 0x2, 0x316, 0x317, 0x7, 0x4f, 0x2, 
    0x2, 0x317, 0x318, 0x5, 0x5c, 0x2f, 0x2, 0x318, 0x319, 0x5, 0x62, 0x32, 
    0x2, 0x319, 0x31b, 0x3, 0x2, 0x2, 0x2, 0x31a, 0x30b, 0x3, 0x2, 0x2, 
    0x2, 0x31a, 0x30f, 0x3, 0x2, 0x2, 0x2, 0x31b, 0x31e, 0x3, 0x2, 0x2, 
    0x2, 0x31c, 0x31a, 0x3, 0x2, 0x2, 0x2, 0x31c, 0x31d, 0x3, 0x2, 0x2, 
    0x2, 0x31d, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x31e, 0x31c, 0x3, 0x2, 0x2, 0x2, 
    0x31f, 0x321, 0x9, 0xa, 0x2, 0x2, 0x320, 0x31f, 0x3, 0x2, 0x2, 0x2, 
    0x320, 0x321, 0x3, 0x2, 0x2, 0x2, 0x321, 0x322, 0x3, 0x2, 0x2, 0x2, 
    0x322, 0x329, 0x7, 0x4a, 0x2, 0x2, 0x323, 0x325, 0x7, 0x4a, 0x2, 0x2, 
    0x324, 0x326, 0x9, 0xa, 0x2, 0x2, 0x325, 0x324, 0x3, 0x2, 0x2, 0x2, 
    0x325, 0x326, 0x3, 0x2, 0x2, 0x2, 0x326, 0x329, 0x3, 0x2, 0x2, 0x2, 
    0x327, 0x329, 0x9, 0xa, 0x2, 0x2, 0x328, 0x320, 0x3, 0x2, 0x2, 0x2, 
    0x328, 0x323, 0x3, 0x2, 0x2, 0x2, 0x328, 0x327, 0x3, 0x2, 0x2, 0x2, 
    0x329, 0x34b, 0x3, 0x2, 0x2, 0x2, 0x32a, 0x32c, 0x9, 0xb, 0x2, 0x2, 
    0x32b, 0x32a, 0x3, 0x2, 0x2, 0x2, 0x32b, 0x32c, 0x3, 0x2, 0x2, 0x2, 
    0x32c, 0x32d, 0x3, 0x2, 0x2, 0x2, 0x32d, 0x32f, 0x9, 0xc, 0x2, 0x2, 
    0x32e, 0x330, 0x7, 0x67, 0x2, 0x2, 0x32f, 0x32e, 0x3, 0x2, 0x2, 0x2, 
    0x32f, 0x330, 0x3, 0x2, 0x2, 0x2, 0x330, 0x339, 0x3, 0x2, 0x2, 0x2, 
    0x331, 0x333, 0x9, 0xc, 0x2, 0x2, 0x332, 0x334, 0x7, 0x67, 0x2, 0x2, 
    0x333, 0x332, 0x3, 0x2, 0x2, 0x2, 0x333, 0x334, 0x3, 0x2, 0x2, 0x2, 
    0x334, 0x336, 0x3, 0x2, 0x2, 0x2, 0x335, 0x337, 0x9, 0xb, 0x2, 0x2, 
    0x336, 0x335, 0x3, 0x2, 0x2, 0x2, 0x336, 0x337, 0x3, 0x2, 0x2, 0x2, 
    0x337, 0x339, 0x3, 0x2, 0x2, 0x2, 0x338, 0x32b, 0x3, 0x2, 0x2, 0x2, 
    0x338, 0x331, 0x3, 0x2, 0x2, 0x2, 0x339, 0x34b, 0x3, 0x2, 0x2, 0x2, 
    0x33a, 0x33c, 0x9, 0xd, 0x2, 0x2, 0x33b, 0x33a, 0x3, 0x2, 0x2, 0x2, 
    0x33b, 0x33c, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x33d, 0x3, 0x2, 0x2, 0x2, 
    0x33d, 0x33f, 0x7, 0x3d, 0x2, 0x2, 0x33e, 0x340, 0x7, 0x67, 0x2, 0x2, 
    0x33f, 0x33e, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x340, 0x3, 0x2, 0x2, 0x2, 
    0x340, 0x349, 0x3, 0x2, 0x2, 0x2, 0x341, 0x343, 0x7, 0x3d, 0x2, 0x2, 
    0x342, 0x344, 0x7, 0x67, 0x2, 0x2, 0x343, 0x342, 0x3, 0x2, 0x2, 0x2, 
    0x343, 0x344, 0x3, 0x2, 0x2, 0x2, 0x344, 0x346, 0x3, 0x2, 0x2, 0x2, 
    0x345, 0x347, 0x9, 0xd, 0x2, 0x2, 0x346, 0x345, 0x3, 0x2, 0x2, 0x2, 
    0x346, 0x347, 0x3, 0x2, 0x2, 0x2, 0x347, 0x349, 0x3, 0x2, 0x2, 0x2, 
    0x348, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x348, 0x341, 0x3, 0x2, 0x2, 0x2, 
    0x349, 0x34b, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x328, 0x3, 0x2, 0x2, 0x2, 
    0x34a, 0x338, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x348, 0x3, 0x2, 0x2, 0x2, 
    0x34b, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x34c, 0x34e, 0x9, 0x9, 0x2, 0x2, 0x34d, 
    0x34c, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x34e, 0x3, 0x2, 0x2, 0x2, 0x34e, 
    0x34f, 0x3, 0x2, 0x2, 0x2, 0x34f, 0x350, 0x7, 0x1e, 0x2, 0x2, 0x350, 
    0x353, 0x7, 0x4f, 0x2, 0x2, 0x351, 0x353, 0x7, 0xa8, 0x2, 0x2, 0x352, 
    0x34d, 0x3, 0x2, 0x2, 0x2, 0x352, 0x351, 0x3, 0x2, 0x2, 0x2, 0x353, 
    0x61, 0x3, 0x2, 0x2, 0x2, 0x354, 0x355, 0x7, 0x63, 0x2, 0x2, 0x355, 
    0x35e, 0x5, 0x7e, 0x40, 0x2, 0x356, 0x357, 0x7, 0x92, 0x2, 0x2, 0x357, 
    0x358, 0x7, 0xb3, 0x2, 0x2, 0x358, 0x359, 0x5, 0x7e, 0x40, 0x2, 0x359, 
    0x35a, 0x7, 0xbd, 0x2, 0x2, 0x35a, 0x35e, 0x3, 0x2, 0x2, 0x2, 0x35b, 
    0x35c, 0x7, 0x92, 0x2, 0x2, 0x35c, 0x35e, 0x5, 0x7e, 0x40, 0x2, 0x35d, 
    0x354, 0x3, 0x2, 0x2, 0x2, 0x35d, 0x356, 0x3, 0x2, 0x2, 0x2, 0x35d, 
    0x35b, 0x3, 0x2, 0x2, 0x2, 0x35e, 0x63, 0x3, 0x2, 0x2, 0x2, 0x35f, 0x360, 
    0x7, 0x75, 0x2, 0x2, 0x360, 0x363, 0x5, 0x6c, 0x37, 0x2, 0x361, 0x362, 
    0x7, 0x62, 0x2, 0x2, 0x362, 0x364, 0x5, 0x6c, 0x37, 0x2, 0x363, 0x361, 
    0x3, 0x2, 0x2, 0x2, 0x363, 0x364, 0x3, 0x2, 0x2, 0x2, 0x364, 0x65, 0x3, 
    0x2, 0x2, 0x2, 0x365, 0x368, 0x7, 0xa0, 0x2, 0x2, 0x366, 0x367, 0x9, 
    0xe, 0x2, 0x2, 0x367, 0x369, 0x7, 0xa0, 0x2, 0x2, 0x368, 0x366, 0x3, 
    0x2, 0x2, 0x2, 0x368, 0x369, 0x3, 0x2, 0x2, 0x2, 0x369, 0x67, 0x3, 0x2, 
    0x2, 0x2, 0x36a, 0x36f, 0x5, 0x6a, 0x36, 0x2, 0x36b, 0x36c, 0x7, 0xa8, 
    0x2, 0x2, 0x36c, 0x36e, 0x5, 0x6a, 0x36, 0x2, 0x36d, 0x36b, 0x3, 0x2, 
    0x2, 0x2, 0x36e, 0x371, 0x3, 0x2, 0x2, 0x2, 0x36f, 0x36d, 0x3, 0x2, 
    0x2, 0x2, 0x36f, 0x370, 0x3, 0x2, 0x2, 0x2, 0x370, 0x69, 0x3, 0x2, 0x2, 
    0x2, 0x371, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x372, 0x374, 0x5, 0x82, 0x42, 
    0x2, 0x373, 0x375, 0x9, 0xf, 0x2, 0x2, 0x374, 0x373, 0x3, 0x2, 0x2, 
    0x2, 0x374, 0x375, 0x3, 0x2, 0x2, 0x2, 0x375, 0x378, 0x3, 0x2, 0x2, 
    0x2, 0x376, 0x377, 0x7, 0x61, 0x2, 0x2, 0x377, 0x379, 0x9, 0x10, 0x2, 
    0x2, 0x378, 0x376, 0x3, 0x2, 0x2, 0x2, 0x378, 0x379, 0x3, 0x2, 0x2, 
    0x2, 0x379, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x37a, 0x37b, 0x7, 0x19, 0x2, 
    0x2, 0x37b, 0x37d, 0x7, 0xa2, 0x2, 0x2, 0x37c, 0x37a, 0x3, 0x2, 0x2, 
    0x2, 0x37c, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x6b, 0x3, 0x2, 0x2, 0x2, 
    0x37e, 0x381, 0x5, 0x9c, 0x4f, 0x2, 0x37f, 0x380, 0x7, 0xbf, 0x2, 0x2, 
    0x380, 0x382, 0x5, 0x9c, 0x4f, 0x2, 0x381, 0x37f, 0x3, 0x2, 0x2, 0x2, 
    0x381, 0x382, 0x3, 0x2, 0x2, 0x2, 0x382, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x383, 
    0x388, 0x5, 0x70, 0x39, 0x2, 0x384, 0x385, 0x7, 0xa8, 0x2, 0x2, 0x385, 
    0x387, 0x5, 0x70, 0x39, 0x2, 0x386, 0x384, 0x3, 0x2, 0x2, 0x2, 0x387, 
    0x38a, 0x3, 0x2, 0x2, 0x2, 0x388, 0x386, 0x3, 0x2, 0x2, 0x2, 0x388, 
    0x389, 0x3, 0x2, 0x2, 0x2, 0x389, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x38a, 0x388, 
    0x3, 0x2, 0x2, 0x2, 0x38b, 0x38c, 0x5, 0xa8, 0x55, 0x2, 0x38c, 0x38d, 
    0x7, 0xad, 0x2, 0x2, 0x38d, 0x38e, 0x5, 0x9e, 0x50, 0x2, 0x38e, 0x71, 
    0x3, 0x2, 0x2, 0x2, 0x38f, 0x390, 0x7, 0x7a, 0x2, 0x2, 0x390, 0x391, 
    0x5, 0x6e, 0x38, 0x2, 0x391, 0x73, 0x3, 0x2, 0x2, 0x2, 0x392, 0x393, 
    0x7, 0x7c, 0x2, 0x2, 0x393, 0x394, 0x7, 0x1d, 0x2, 0x2, 0x394, 0x395, 
    0x7, 0x20, 0x2, 0x2, 0x395, 0x3b3, 0x5, 0x98, 0x4d, 0x2, 0x396, 0x397, 
    0x7, 0x7c, 0x2, 0x2, 0x397, 0x399, 0x7, 0x1d, 0x2, 0x2, 0x398, 0x39a, 
    0x7, 0x85, 0x2, 0x2, 0x399, 0x398, 0x3, 0x2, 0x2, 0x2, 0x399, 0x39a, 
    0x3, 0x2, 0x2, 0x2, 0x39a, 0x39c, 0x3, 0x2, 0x2, 0x2, 0x39b, 0x39d, 
    0x7, 0x83, 0x2, 0x2, 0x39c, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x39d, 
    0x3, 0x2, 0x2, 0x2, 0x39d, 0x39e, 0x3, 0x2, 0x2, 0x2, 0x39e, 0x3b3, 
    0x5, 0x92, 0x4a, 0x2, 0x39f, 0x3a1, 0x7, 0x7c, 0x2, 0x2, 0x3a0, 0x3a2, 
    0x7, 0x85, 0x2, 0x2, 0x3a1, 0x3a0, 0x3, 0x2, 0x2, 0x2, 0x3a1, 0x3a2, 
    0x3, 0x2, 0x2, 0x2, 0x3a2, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x3a3, 0x3a6, 
    0x7, 0x84, 0x2, 0x2, 0x3a4, 0x3a5, 0x9, 0x11, 0x2, 0x2, 0x3a5, 0x3a7, 
    0x5, 0x98, 0x4d, 0x2, 0x3a6, 0x3a4, 0x3, 0x2, 0x2, 0x2, 0x3a6, 0x3a7, 
    0x3, 0x2, 0x2, 0x2, 0x3a7, 0x3ab, 0x3, 0x2, 0x2, 0x2, 0x3a8, 0x3a9, 
    0x7, 0x54, 0x2, 0x2, 0x3a9, 0x3ac, 0x7, 0xa2, 0x2, 0x2, 0x3aa, 0x3ac, 
    0x5, 0x4e, 0x28, 0x2, 0x3ab, 0x3a8, 0x3, 0x2, 0x2, 0x2, 0x3ab, 0x3aa, 
    0x3, 0x2, 0x2, 0x2, 0x3ab, 0x3ac, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3ae, 
    0x3, 0x2, 0x2, 0x2, 0x3ad, 0x3af, 0x5, 0x58, 0x2d, 0x2, 0x3ae, 0x3ad, 
    0x3, 0x2, 0x2, 0x2, 0x3ae, 0x3af, 0x3, 0x2, 0x2, 0x2, 0x3af, 0x3b3, 
    0x3, 0x2, 0x2, 0x2, 0x3b0, 0x3b1, 0x7, 0x7c, 0x2, 0x2, 0x3b1, 0x3b3, 
    0x7, 0x21, 0x2, 0x2, 0x3b2, 0x392, 0x3, 0x2, 0x2, 0x2, 0x3b2, 0x396, 
    0x3, 0x2, 0x2, 0x2, 0x3b2, 0x39f, 0x3, 0x2, 0x2, 0x2, 0x3b2, 0x3b0, 
    0x3, 0x2, 0x2, 0x2, 0x3b3, 0x75, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b5, 0x7, 
    0x82, 0x2, 0x2, 0x3b5, 0x3b6, 0x7, 0x39, 0x2, 0x2, 0x3b6, 0x3b7, 0x7, 
    0x2e, 0x2, 0x2, 0x3b7, 0x3cd, 0x5, 0x92, 0x4a, 0x2, 0x3b8, 0x3b9, 0x7, 
    0x82, 0x2, 0x2, 0x3b9, 0x3ba, 0x7, 0x39, 0x2, 0x2, 0x3ba, 0x3cd, 0x7, 
    0x57, 0x2, 0x2, 0x3bb, 0x3bc, 0x7, 0x82, 0x2, 0x2, 0x3bc, 0x3c1, 0x9, 
    0x12, 0x2, 0x2, 0x3bd, 0x3be, 0x7, 0x2e, 0x2, 0x2, 0x3be, 0x3c2, 0x7, 
    0x79, 0x2, 0x2, 0x3bf, 0x3c2, 0x7, 0x36, 0x2, 0x2, 0x3c0, 0x3c2, 0x7, 
    0x59, 0x2, 0x2, 0x3c1, 0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3bf, 0x3, 
    0x2, 0x2, 0x2, 0x3c1, 0x3c0, 0x3, 0x2, 0x2, 0x2, 0x3c2, 0x3c3, 0x3, 
    0x2, 0x2, 0x2, 0x3c3, 0x3cd, 0x5, 0x92, 0x4a, 0x2, 0x3c4, 0x3c5, 0x7, 
    0x82, 0x2, 0x2, 0x3c5, 0x3c6, 0x9, 0x12, 0x2, 0x2, 0x3c6, 0x3c7, 0x7, 
    0x72, 0x2, 0x2, 0x3c7, 0x3cd, 0x7, 0x79, 0x2, 0x2, 0x3c8, 0x3c9, 0x7, 
    0x82, 0x2, 0x2, 0x3c9, 0x3ca, 0x7, 0x80, 0x2, 0x2, 0x3ca, 0x3cb, 0x7, 
    0x71, 0x2, 0x2, 0x3cb, 0x3cd, 0x5, 0x92, 0x4a, 0x2, 0x3cc, 0x3b4, 0x3, 
    0x2, 0x2, 0x2, 0x3cc, 0x3b8, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3bb, 0x3, 
    0x2, 0x2, 0x2, 0x3cc, 0x3c4, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3c8, 0x3, 
    0x2, 0x2, 0x2, 0x3cd, 0x77, 0x3, 0x2, 0x2, 0x2, 0x3ce, 0x3d0, 0x7, 0x8d, 
    0x2, 0x2, 0x3cf, 0x3d1, 0x7, 0x85, 0x2, 0x2, 0x3d0, 0x3cf, 0x3, 0x2, 
    0x2, 0x2, 0x3d0, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3d2, 0x3, 0x2, 
    0x2, 0x2, 0x3d2, 0x3d5, 0x7, 0x83, 0x2, 0x2, 0x3d3, 0x3d4, 0x7, 0x45, 
    0x2, 0x2, 0x3d4, 0x3d6, 0x7, 0x33, 0x2, 0x2, 0x3d5, 0x3d3, 0x3, 0x2, 
    0x2, 0x2, 0x3d5, 0x3d6, 0x3, 0x2, 0x2, 0x2, 0x3d6, 0x3d7, 0x3, 0x2, 
    0x2, 0x2, 0x3d7, 0x3d8, 0x5, 0x92, 0x4a, 0x2, 0x3d8, 0x79, 0x3, 0x2, 
    0x2, 0x2, 0x3d9, 0x3da, 0x7, 0x91, 0x2, 0x2, 0x3da, 0x3db, 0x5, 0x98, 
    0x4d, 0x2, 0x3db, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x3dc, 0x40c, 0x5, 0xa8, 
    0x55, 0x2, 0x3dd, 0x3de, 0x5, 0xa8, 0x55, 0x2, 0x3de, 0x3df, 0x7, 0xb3, 
    0x2, 0x2, 0x3df, 0x3e0, 0x5, 0xa8, 0x55, 0x2, 0x3e0, 0x3e7, 0x5, 0x7c, 
    0x3f, 0x2, 0x3e1, 0x3e2, 0x7, 0xa8, 0x2, 0x2, 0x3e2, 0x3e3, 0x5, 0xa8, 
    0x55, 0x2, 0x3e3, 0x3e4, 0x5, 0x7c, 0x3f, 0x2, 0x3e4, 0x3e6, 0x3, 0x2, 
    0x2, 0x2, 0x3e5, 0x3e1, 0x3, 0x2, 0x2, 0x2, 0x3e6, 0x3e9, 0x3, 0x2, 
    0x2, 0x2, 0x3e7, 0x3e5, 0x3, 0x2, 0x2, 0x2, 0x3e7, 0x3e8, 0x3, 0x2, 
    0x2, 0x2, 0x3e8, 0x3ea, 0x3, 0x2, 0x2, 0x2, 0x3e9, 0x3e7, 0x3, 0x2, 
    0x2, 0x2, 0x3ea, 0x3eb, 0x7, 0xbd, 0x2, 0x2, 0x3eb, 0x40c, 0x3, 0x2, 
    0x2, 0x2, 0x3ec, 0x3ed, 0x5, 0xa8, 0x55, 0x2, 0x3ed, 0x3ee, 0x7, 0xb3, 
    0x2, 0x2, 0x3ee, 0x3f3, 0x5, 0xac, 0x57, 0x2, 0x3ef, 0x3f0, 0x7, 0xa8, 
    0x2, 0x2, 0x3f0, 0x3f2, 0x5, 0xac, 0x57, 0x2, 0x3f1, 0x3ef, 0x3, 0x2, 
    0x2, 0x2, 0x3f2, 0x3f5, 0x3, 0x2, 0x2, 0x2, 0x3f3, 0x3f1, 0x3, 0x2, 
    0x2, 0x2, 0x3f3, 0x3f4, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3f6, 0x3, 0x2, 
    0x2, 0x2, 0x3f5, 0x3f3, 0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3f7, 0x7, 0xbd, 
    0x2, 0x2, 0x3f7, 0x40c, 0x3, 0x2, 0x2, 0x2, 0x3f8, 0x3f9, 0x5, 0xa8, 
    0x55, 0x2, 0x3f9, 0x3fa, 0x7, 0xb3, 0x2, 0x2, 0x3fa, 0x3ff, 0x5, 0x7c, 
    0x3f, 0x2, 0x3fb, 0x3fc, 0x7, 0xa8, 0x2, 0x2, 0x3fc, 0x3fe, 0x5, 0x7c, 
    0x3f, 0x2, 0x3fd, 0x3fb, 0x3, 0x2, 0x2, 0x2, 0x3fe, 0x401, 0x3, 0x2, 
    0x2, 0x2, 0x3ff, 0x3fd, 0x3, 0x2, 0x2, 0x2, 0x3ff, 0x400, 0x3, 0x2, 
    0x2, 0x2, 0x400, 0x402, 0x3, 0x2, 0x2, 0x2, 0x401, 0x3ff, 0x3, 0x2, 
    0x2, 0x2, 0x402, 0x403, 0x7, 0xbd, 0x2, 0x2, 0x403, 0x40c, 0x3, 0x2, 
    0x2, 0x2, 0x404, 0x405, 0x5, 0xa8, 0x55, 0x2, 0x405, 0x407, 0x7, 0xb3, 
    0x2, 0x2, 0x406, 0x408, 0x5, 0x7e, 0x40, 0x2, 0x407, 0x406, 0x3, 0x2, 
    0x2, 0x2, 0x407, 0x408, 0x3, 0x2, 0x2, 0x2, 0x408, 0x409, 0x3, 0x2, 
    0x2, 0x2, 0x409, 0x40a, 0x7, 0xbd, 0x2, 0x2, 0x40a, 0x40c, 0x3, 0x2, 
    0x2, 0x2, 0x40b, 0x3dc, 0x3, 0x2, 0x2, 0x2, 0x40b, 0x3dd, 0x3, 0x2, 
    0x2, 0x2, 0x40b, 0x3ec, 0x3, 0x2, 0x2, 0x2, 0x40b, 0x3f8, 0x3, 0x2, 
    0x2, 0x2, 0x40b, 0x404, 0x3, 0x2, 0x2, 0x2, 0x40c, 0x7d, 0x3, 0x2, 0x2, 
    0x2, 0x40d, 0x412, 0x5, 0x80, 0x41, 0x2, 0x40e, 0x40f, 0x7, 0xa8, 0x2, 
    0x2, 0x40f, 0x411, 0x5, 0x80, 0x41, 0x2, 0x410, 0x40e, 0x3, 0x2, 0x2, 
    0x2, 0x411, 0x414, 0x3, 0x2, 0x2, 0x2, 0x412, 0x410, 0x3, 0x2, 0x2, 
    0x2, 0x412, 0x413, 0x3, 0x2, 0x2, 0x2, 0x413, 0x7f, 0x3, 0x2, 0x2, 0x2, 
    0x414, 0x412, 0x3, 0x2, 0x2, 0x2, 0x415, 0x416, 0x5, 0x92, 0x4a, 0x2, 
    0x416, 0x417, 0x7, 0xab, 0x2, 0x2, 0x417, 0x419, 0x3, 0x2, 0x2, 0x2, 
    0x418, 0x415, 0x3, 0x2, 0x2, 0x2, 0x418, 0x419, 0x3, 0x2, 0x2, 0x2, 
    0x419, 0x41a, 0x3, 0x2, 0x2, 0x2, 0x41a, 0x421, 0x7, 0xa4, 0x2, 0x2, 
    0x41b, 0x41c, 0x7, 0xb3, 0x2, 0x2, 0x41c, 0x41d, 0x5, 0x40, 0x21, 0x2, 
    0x41d, 0x41e, 0x7, 0xbd, 0x2, 0x2, 0x41e, 0x421, 0x3, 0x2, 0x2, 0x2, 
    0x41f, 0x421, 0x5, 0x82, 0x42, 0x2, 0x420, 0x418, 0x3, 0x2, 0x2, 0x2, 
    0x420, 0x41b, 0x3, 0x2, 0x2, 0x2, 0x420, 0x41f, 0x3, 0x2, 0x2, 0x2, 
    0x421, 0x81, 0x3, 0x2, 0x2, 0x2, 0x422, 0x423, 0x8, 0x42, 0x1, 0x2, 
    0x423, 0x425, 0x7, 0x13, 0x2, 0x2, 0x424, 0x426, 0x5, 0x82, 0x42, 0x2, 
    0x425, 0x424, 0x3, 0x2, 0x2, 0x2, 0x425, 0x426, 0x3, 0x2, 0x2, 0x2, 
    0x426, 0x42c, 0x3, 0x2, 0x2, 0x2, 0x427, 0x428, 0x7, 0x97, 0x2, 0x2, 
    0x428, 0x429, 0x5, 0x82, 0x42, 0x2, 0x429, 0x42a, 0x7, 0x86, 0x2, 0x2, 
    0x42a, 0x42b, 0x5, 0x82, 0x42, 0x2, 0x42b, 0x42d, 0x3, 0x2, 0x2, 0x2, 
    0x42c, 0x427, 0x3, 0x2, 0x2, 0x2, 0x42d, 0x42e, 0x3, 0x2, 0x2, 0x2, 
    0x42e, 0x42c, 0x3, 0x2, 0x2, 0x2, 0x42e, 0x42f, 0x3, 0x2, 0x2, 0x2, 
    0x42f, 0x432, 0x3, 0x2, 0x2, 0x2, 0x430, 0x431, 0x7, 0x30, 0x2, 0x2, 
    0x431, 0x433, 0x5, 0x82, 0x42, 0x2, 0x432, 0x430, 0x3, 0x2, 0x2, 0x2, 
    0x432, 0x433, 0x3, 0x2, 0x2, 0x2, 0x433, 0x434, 0x3, 0x2, 0x2, 0x2, 
    0x434, 0x435, 0x7, 0x31, 0x2, 0x2, 0x435, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x436, 0x437, 0x7, 0x14, 0x2, 0x2, 0x437, 0x438, 0x7, 0xb3, 0x2, 0x2, 
    0x438, 0x439, 0x5, 0x82, 0x42, 0x2, 0x439, 0x43a, 0x7, 0xc, 0x2, 0x2, 
    0x43a, 0x43b, 0x5, 0x7c, 0x3f, 0x2, 0x43b, 0x43c, 0x7, 0xbd, 0x2, 0x2, 
    0x43c, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x43d, 0x43e, 0x7, 0x22, 0x2, 0x2, 
    0x43e, 0x48e, 0x7, 0xa2, 0x2, 0x2, 0x43f, 0x440, 0x7, 0x35, 0x2, 0x2, 
    0x440, 0x441, 0x7, 0xb3, 0x2, 0x2, 0x441, 0x442, 0x5, 0xa0, 0x51, 0x2, 
    0x442, 0x443, 0x7, 0x3c, 0x2, 0x2, 0x443, 0x444, 0x5, 0x82, 0x42, 0x2, 
    0x444, 0x445, 0x7, 0xbd, 0x2, 0x2, 0x445, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x446, 0x447, 0x7, 0x4c, 0x2, 0x2, 0x447, 0x448, 0x5, 0x82, 0x42, 0x2, 
    0x448, 0x449, 0x5, 0xa0, 0x51, 0x2, 0x449, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x44a, 0x44b, 0x7, 0x7f, 0x2, 0x2, 0x44b, 0x44c, 0x7, 0xb3, 0x2, 0x2, 
    0x44c, 0x44d, 0x5, 0x82, 0x42, 0x2, 0x44d, 0x44e, 0x7, 0x3c, 0x2, 0x2, 
    0x44e, 0x451, 0x5, 0x82, 0x42, 0x2, 0x44f, 0x450, 0x7, 0x3a, 0x2, 0x2, 
    0x450, 0x452, 0x5, 0x82, 0x42, 0x2, 0x451, 0x44f, 0x3, 0x2, 0x2, 0x2, 
    0x451, 0x452, 0x3, 0x2, 0x2, 0x2, 0x452, 0x453, 0x3, 0x2, 0x2, 0x2, 
    0x453, 0x454, 0x7, 0xbd, 0x2, 0x2, 0x454, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x455, 0x456, 0x7, 0x88, 0x2, 0x2, 0x456, 0x48e, 0x7, 0xa2, 0x2, 0x2, 
    0x457, 0x458, 0x7, 0x8c, 0x2, 0x2, 0x458, 0x459, 0x7, 0xb3, 0x2, 0x2, 
    0x459, 0x45a, 0x9, 0x13, 0x2, 0x2, 0x45a, 0x45b, 0x7, 0xa2, 0x2, 0x2, 
    0x45b, 0x45c, 0x7, 0x3c, 0x2, 0x2, 0x45c, 0x45d, 0x5, 0x82, 0x42, 0x2, 
    0x45d, 0x45e, 0x7, 0xbd, 0x2, 0x2, 0x45e, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x45f, 0x465, 0x5, 0xa8, 0x55, 0x2, 0x460, 0x462, 0x7, 0xb3, 0x2, 0x2, 
    0x461, 0x463, 0x5, 0x7e, 0x40, 0x2, 0x462, 0x461, 0x3, 0x2, 0x2, 0x2, 
    0x462, 0x463, 0x3, 0x2, 0x2, 0x2, 0x463, 0x464, 0x3, 0x2, 0x2, 0x2, 
    0x464, 0x466, 0x7, 0xbd, 0x2, 0x2, 0x465, 0x460, 0x3, 0x2, 0x2, 0x2, 
    0x465, 0x466, 0x3, 0x2, 0x2, 0x2, 0x466, 0x467, 0x3, 0x2, 0x2, 0x2, 
    0x467, 0x469, 0x7, 0xb3, 0x2, 0x2, 0x468, 0x46a, 0x7, 0x2d, 0x2, 0x2, 
    0x469, 0x468, 0x3, 0x2, 0x2, 0x2, 0x469, 0x46a, 0x3, 0x2, 0x2, 0x2, 
    0x46a, 0x46c, 0x3, 0x2, 0x2, 0x2, 0x46b, 0x46d, 0x5, 0x84, 0x43, 0x2, 
    0x46c, 0x46b, 0x3, 0x2, 0x2, 0x2, 0x46c, 0x46d, 0x3, 0x2, 0x2, 0x2, 
    0x46d, 0x46e, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x46f, 0x7, 0xbd, 0x2, 0x2, 
    0x46f, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x470, 0x48e, 0x5, 0x9e, 0x50, 0x2, 
    0x471, 0x472, 0x7, 0xaa, 0x2, 0x2, 0x472, 0x48e, 0x5, 0x82, 0x42, 0x13, 
    0x473, 0x474, 0x7, 0x5f, 0x2, 0x2, 0x474, 0x48e, 0x5, 0x82, 0x42, 0xe, 
    0x475, 0x476, 0x5, 0x92, 0x4a, 0x2, 0x476, 0x477, 0x7, 0xab, 0x2, 0x2, 
    0x477, 0x479, 0x3, 0x2, 0x2, 0x2, 0x478, 0x475, 0x3, 0x2, 0x2, 0x2, 
    0x478, 0x479, 0x3, 0x2, 0x2, 0x2, 0x479, 0x47a, 0x3, 0x2, 0x2, 0x2, 
    0x47a, 0x48e, 0x7, 0xa4, 0x2, 0x2, 0x47b, 0x47c, 0x7, 0xb3, 0x2, 0x2, 
    0x47c, 0x47d, 0x5, 0x40, 0x21, 0x2, 0x47d, 0x47e, 0x7, 0xbd, 0x2, 0x2, 
    0x47e, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x47f, 0x480, 0x7, 0xb3, 0x2, 0x2, 
    0x480, 0x481, 0x5, 0x82, 0x42, 0x2, 0x481, 0x482, 0x7, 0xbd, 0x2, 0x2, 
    0x482, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x483, 0x484, 0x7, 0xb3, 0x2, 0x2, 
    0x484, 0x485, 0x5, 0x7e, 0x40, 0x2, 0x485, 0x486, 0x7, 0xbd, 0x2, 0x2, 
    0x486, 0x48e, 0x3, 0x2, 0x2, 0x2, 0x487, 0x489, 0x7, 0xb1, 0x2, 0x2, 
    0x488, 0x48a, 0x5, 0x7e, 0x40, 0x2, 0x489, 0x488, 0x3, 0x2, 0x2, 0x2, 
    0x489, 0x48a, 0x3, 0x2, 0x2, 0x2, 0x48a, 0x48b, 0x3, 0x2, 0x2, 0x2, 
    0x48b, 0x48e, 0x7, 0xbc, 0x2, 0x2, 0x48c, 0x48e, 0x5, 0x8a, 0x46, 0x2, 
    0x48d, 0x422, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x436, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x43d, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x43f, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x446, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x44a, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x455, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x457, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x45f, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x470, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x471, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x473, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x478, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x47b, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x47f, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x483, 0x3, 0x2, 0x2, 0x2, 
    0x48d, 0x487, 0x3, 0x2, 0x2, 0x2, 0x48d, 0x48c, 0x3, 0x2, 0x2, 0x2, 
    0x48e, 0x4d6, 0x3, 0x2, 0x2, 0x2, 0x48f, 0x490, 0xc, 0x12, 0x2, 0x2, 
    0x490, 0x491, 0x9, 0x14, 0x2, 0x2, 0x491, 0x4d5, 0x5, 0x82, 0x42, 0x13, 
    0x492, 0x493, 0xc, 0x11, 0x2, 0x2, 0x493, 0x494, 0x9, 0x15, 0x2, 0x2, 
    0x494, 0x4d5, 0x5, 0x82, 0x42, 0x12, 0x495, 0x4a8, 0xc, 0x10, 0x2, 0x2, 
    0x496, 0x4a9, 0x7, 0xac, 0x2, 0x2, 0x497, 0x4a9, 0x7, 0xad, 0x2, 0x2, 
    0x498, 0x4a9, 0x7, 0xb5, 0x2, 0x2, 0x499, 0x4a9, 0x7, 0xb2, 0x2, 0x2, 
    0x49a, 0x4a9, 0x7, 0xae, 0x2, 0x2, 0x49b, 0x4a9, 0x7, 0xb4, 0x2, 0x2, 
    0x49c, 0x4a9, 0x7, 0xaf, 0x2, 0x2, 0x49d, 0x49f, 0x7, 0x3f, 0x2, 0x2, 
    0x49e, 0x49d, 0x3, 0x2, 0x2, 0x2, 0x49e, 0x49f, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x4a1, 0x3, 0x2, 0x2, 0x2, 0x4a0, 0x4a2, 0x7, 0x5f, 0x2, 0x2, 
    0x4a1, 0x4a0, 0x3, 0x2, 0x2, 0x2, 0x4a1, 0x4a2, 0x3, 0x2, 0x2, 0x2, 
    0x4a2, 0x4a3, 0x3, 0x2, 0x2, 0x2, 0x4a3, 0x4a9, 0x7, 0x47, 0x2, 0x2, 
    0x4a4, 0x4a6, 0x7, 0x5f, 0x2, 0x2, 0x4a5, 0x4a4, 0x3, 0x2, 0x2, 0x2, 
    0x4a5, 0x4a6, 0x3, 0x2, 0x2, 0x2, 0x4a6, 0x4a7, 0x3, 0x2, 0x2, 0x2, 
    0x4a7, 0x4a9, 0x9, 0x16, 0x2, 0x2, 0x4a8, 0x496, 0x3, 0x2, 0x2, 0x2, 
    0x4a8, 0x497, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x498, 0x3, 0x2, 0x2, 0x2, 
    0x4a8, 0x499, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x49a, 0x3, 0x2, 0x2, 0x2, 
    0x4a8, 0x49b, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x49c, 0x3, 0x2, 0x2, 0x2, 
    0x4a8, 0x49e, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x4a5, 0x3, 0x2, 0x2, 0x2, 
    0x4a9, 0x4aa, 0x3, 0x2, 0x2, 0x2, 0x4aa, 0x4d5, 0x5, 0x82, 0x42, 0x11, 
    0x4ab, 0x4ac, 0xc, 0xd, 0x2, 0x2, 0x4ac, 0x4ad, 0x7, 0x8, 0x2, 0x2, 
    0x4ad, 0x4d5, 0x5, 0x82, 0x42, 0xe, 0x4ae, 0x4af, 0xc, 0xc, 0x2, 0x2, 
    0x4af, 0x4b0, 0x7, 0x65, 0x2, 0x2, 0x4b0, 0x4d5, 0x5, 0x82, 0x42, 0xd, 
    0x4b1, 0x4b3, 0xc, 0xb, 0x2, 0x2, 0x4b2, 0x4b4, 0x7, 0x5f, 0x2, 0x2, 
    0x4b3, 0x4b2, 0x3, 0x2, 0x2, 0x2, 0x4b3, 0x4b4, 0x3, 0x2, 0x2, 0x2, 
    0x4b4, 0x4b5, 0x3, 0x2, 0x2, 0x2, 0x4b5, 0x4b6, 0x7, 0x10, 0x2, 0x2, 
    0x4b6, 0x4b7, 0x5, 0x82, 0x42, 0x2, 0x4b7, 0x4b8, 0x7, 0x8, 0x2, 0x2, 
    0x4b8, 0x4b9, 0x5, 0x82, 0x42, 0xc, 0x4b9, 0x4d5, 0x3, 0x2, 0x2, 0x2, 
    0x4ba, 0x4bb, 0xc, 0xa, 0x2, 0x2, 0x4bb, 0x4bc, 0x7, 0xb8, 0x2, 0x2, 
    0x4bc, 0x4bd, 0x5, 0x82, 0x42, 0x2, 0x4bd, 0x4be, 0x7, 0xa7, 0x2, 0x2, 
    0x4be, 0x4bf, 0x5, 0x82, 0x42, 0xa, 0x4bf, 0x4d5, 0x3, 0x2, 0x2, 0x2, 
    0x4c0, 0x4c1, 0xc, 0x15, 0x2, 0x2, 0x4c1, 0x4c2, 0x7, 0xb1, 0x2, 0x2, 
    0x4c2, 0x4c3, 0x5, 0x82, 0x42, 0x2, 0x4c3, 0x4c4, 0x7, 0xbc, 0x2, 0x2, 
    0x4c4, 0x4d5, 0x3, 0x2, 0x2, 0x2, 0x4c5, 0x4c6, 0xc, 0x14, 0x2, 0x2, 
    0x4c6, 0x4c7, 0x7, 0xab, 0x2, 0x2, 0x4c7, 0x4d5, 0x7, 0xa0, 0x2, 0x2, 
    0x4c8, 0x4c9, 0xc, 0xf, 0x2, 0x2, 0x4c9, 0x4cb, 0x7, 0x4e, 0x2, 0x2, 
    0x4ca, 0x4cc, 0x7, 0x5f, 0x2, 0x2, 0x4cb, 0x4ca, 0x3, 0x2, 0x2, 0x2, 
    0x4cb, 0x4cc, 0x3, 0x2, 0x2, 0x2, 0x4cc, 0x4cd, 0x3, 0x2, 0x2, 0x2, 
    0x4cd, 0x4d5, 0x7, 0x60, 0x2, 0x2, 0x4ce, 0x4d2, 0xc, 0x9, 0x2, 0x2, 
    0x4cf, 0x4d3, 0x5, 0xa6, 0x54, 0x2, 0x4d0, 0x4d1, 0x7, 0xc, 0x2, 0x2, 
    0x4d1, 0x4d3, 0x5, 0xa8, 0x55, 0x2, 0x4d2, 0x4cf, 0x3, 0x2, 0x2, 0x2, 
    0x4d2, 0x4d0, 0x3, 0x2, 0x2, 0x2, 0x4d3, 0x4d5, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x48f, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x492, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x495, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4ab, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x4ae, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4b1, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4c0, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x4c5, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4c8, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x4ce, 0x3, 0x2, 0x2, 0x2, 0x4d5, 0x4d8, 0x3, 0x2, 0x2, 0x2, 
    0x4d6, 0x4d4, 0x3, 0x2, 0x2, 0x2, 0x4d6, 0x4d7, 0x3, 0x2, 0x2, 0x2, 
    0x4d7, 0x83, 0x3, 0x2, 0x2, 0x2, 0x4d8, 0x4d6, 0x3, 0x2, 0x2, 0x2, 0x4d9, 
    0x4de, 0x5, 0x86, 0x44, 0x2, 0x4da, 0x4db, 0x7, 0xa8, 0x2, 0x2, 0x4db, 
    0x4dd, 0x5, 0x86, 0x44, 0x2, 0x4dc, 0x4da, 0x3, 0x2, 0x2, 0x2, 0x4dd, 
    0x4e0, 0x3, 0x2, 0x2, 0x2, 0x4de, 0x4dc, 0x3, 0x2, 0x2, 0x2, 0x4de, 
    0x4df, 0x3, 0x2, 0x2, 0x2, 0x4df, 0x85, 0x3, 0x2, 0x2, 0x2, 0x4e0, 0x4de, 
    0x3, 0x2, 0x2, 0x2, 0x4e1, 0x4e4, 0x5, 0x88, 0x45, 0x2, 0x4e2, 0x4e4, 
    0x5, 0x82, 0x42, 0x2, 0x4e3, 0x4e1, 0x3, 0x2, 0x2, 0x2, 0x4e3, 0x4e2, 
    0x3, 0x2, 0x2, 0x2, 0x4e4, 0x87, 0x3, 0x2, 0x2, 0x2, 0x4e5, 0x4e6, 0x7, 
    0xb3, 0x2, 0x2, 0x4e6, 0x4eb, 0x5, 0xa8, 0x55, 0x2, 0x4e7, 0x4e8, 0x7, 
    0xa8, 0x2, 0x2, 0x4e8, 0x4ea, 0x5, 0xa8, 0x55, 0x2, 0x4e9, 0x4e7, 0x3, 
    0x2, 0x2, 0x2, 0x4ea, 0x4ed, 0x3, 0x2, 0x2, 0x2, 0x4eb, 0x4e9, 0x3, 
    0x2, 0x2, 0x2, 0x4eb, 0x4ec, 0x3, 0x2, 0x2, 0x2, 0x4ec, 0x4ee, 0x3, 
    0x2, 0x2, 0x2, 0x4ed, 0x4eb, 0x3, 0x2, 0x2, 0x2, 0x4ee, 0x4ef, 0x7, 
    0xbd, 0x2, 0x2, 0x4ef, 0x4f9, 0x3, 0x2, 0x2, 0x2, 0x4f0, 0x4f5, 0x5, 
    0xa8, 0x55, 0x2, 0x4f1, 0x4f2, 0x7, 0xa8, 0x2, 0x2, 0x4f2, 0x4f4, 0x5, 
    0xa8, 0x55, 0x2, 0x4f3, 0x4f1, 0x3, 0x2, 0x2, 0x2, 0x4f4, 0x4f7, 0x3, 
    0x2, 0x2, 0x2, 0x4f5, 0x4f3, 0x3, 0x2, 0x2, 0x2, 0x4f5, 0x4f6, 0x3, 
    0x2, 0x2, 0x2, 0x4f6, 0x4f9, 0x3, 0x2, 0x2, 0x2, 0x4f7, 0x4f5, 0x3, 
    0x2, 0x2, 0x2, 0x4f8, 0x4e5, 0x3, 0x2, 0x2, 0x2, 0x4f8, 0x4f0, 0x3, 
    0x2, 0x2, 0x2, 0x4f9, 0x4fa, 0x3, 0x2, 0x2, 0x2, 0x4fa, 0x4fb, 0x7, 
    0xa3, 0x2, 0x2, 0x4fb, 0x4fc, 0x5, 0x82, 0x42, 0x2, 0x4fc, 0x89, 0x3, 
    0x2, 0x2, 0x2, 0x4fd, 0x4fe, 0x5, 0x92, 0x4a, 0x2, 0x4fe, 0x4ff, 0x7, 
    0xab, 0x2, 0x2, 0x4ff, 0x501, 0x3, 0x2, 0x2, 0x2, 0x500, 0x4fd, 0x3, 
    0x2, 0x2, 0x2, 0x500, 0x501, 0x3, 0x2, 0x2, 0x2, 0x501, 0x502, 0x3, 
    0x2, 0x2, 0x2, 0x502, 0x503, 0x5, 0x8c, 0x47, 0x2, 0x503, 0x8b, 0x3, 
    0x2, 0x2, 0x2, 0x504, 0x507, 0x5, 0xa8, 0x55, 0x2, 0x505, 0x506, 0x7, 
    0xab, 0x2, 0x2, 0x506, 0x508, 0x5, 0xa8, 0x55, 0x2, 0x507, 0x505, 0x3, 
    0x2, 0x2, 0x2, 0x507, 0x508, 0x3, 0x2, 0x2, 0x2, 0x508, 0x8d, 0x3, 0x2, 
    0x2, 0x2, 0x509, 0x50a, 0x8, 0x48, 0x1, 0x2, 0x50a, 0x511, 0x5, 0x92, 
    0x4a, 0x2, 0x50b, 0x511, 0x5, 0x90, 0x49, 0x2, 0x50c, 0x50d, 0x7, 0xb3, 
    0x2, 0x2, 0x50d, 0x50e, 0x5, 0x40, 0x21, 0x2, 0x50e, 0x50f, 0x7, 0xbd, 
    0x2, 0x2, 0x50f, 0x511, 0x3, 0x2, 0x2, 0x2, 0x510, 0x509, 0x3, 0x2, 
    0x2, 0x2, 0x510, 0x50b, 0x3, 0x2, 0x2, 0x2, 0x510, 0x50c, 0x3, 0x2, 
    0x2, 0x2, 0x511, 0x51a, 0x3, 0x2, 0x2, 0x2, 0x512, 0x516, 0xc, 0x3, 
    0x2, 0x2, 0x513, 0x517, 0x5, 0xa6, 0x54, 0x2, 0x514, 0x515, 0x7, 0xc, 
    0x2, 0x2, 0x515, 0x517, 0x5, 0xa8, 0x55, 0x2, 0x516, 0x513, 0x3, 0x2, 
    0x2, 0x2, 0x516, 0x514, 0x3, 0x2, 0x2, 0x2, 0x517, 0x519, 0x3, 0x2, 
    0x2, 0x2, 0x518, 0x512, 0x3, 0x2, 0x2, 0x2, 0x519, 0x51c, 0x3, 0x2, 
    0x2, 0x2, 0x51a, 0x518, 0x3, 0x2, 0x2, 0x2, 0x51a, 0x51b, 0x3, 0x2, 
    0x2, 0x2, 0x51b, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x51c, 0x51a, 0x3, 0x2, 0x2, 
    0x2, 0x51d, 0x51e, 0x5, 0xa8, 0x55, 0x2, 0x51e, 0x520, 0x7, 0xb3, 0x2, 
    0x2, 0x51f, 0x521, 0x5, 0x94, 0x4b, 0x2, 0x520, 0x51f, 0x3, 0x2, 0x2, 
    0x2, 0x520, 0x521, 0x3, 0x2, 0x2, 0x2, 0x521, 0x522, 0x3, 0x2, 0x2, 
    0x2, 0x522, 0x523, 0x7, 0xbd, 0x2, 0x2, 0x523, 0x91, 0x3, 0x2, 0x2, 
    0x2, 0x524, 0x525, 0x5, 0x98, 0x4d, 0x2, 0x525, 0x526, 0x7, 0xab, 0x2, 
    0x2, 0x526, 0x528, 0x3, 0x2, 0x2, 0x2, 0x527, 0x524, 0x3, 0x2, 0x2, 
    0x2, 0x527, 0x528, 0x3, 0x2, 0x2, 0x2, 0x528, 0x529, 0x3, 0x2, 0x2, 
    0x2, 0x529, 0x52a, 0x5, 0xa8, 0x55, 0x2, 0x52a, 0x93, 0x3, 0x2, 0x2, 
    0x2, 0x52b, 0x530, 0x5, 0x96, 0x4c, 0x2, 0x52c, 0x52d, 0x7, 0xa8, 0x2, 
    0x2, 0x52d, 0x52f, 0x5, 0x96, 0x4c, 0x2, 0x52e, 0x52c, 0x3, 0x2, 0x2, 
    0x2, 0x52f, 0x532, 0x3, 0x2, 0x2, 0x2, 0x530, 0x52e, 0x3, 0x2, 0x2, 
    0x2, 0x530, 0x531, 0x3, 0x2, 0x2, 0x2, 0x531, 0x95, 0x3, 0x2, 0x2, 0x2, 
    0x532, 0x530, 0x3, 0x2, 0x2, 0x2, 0x533, 0x537, 0x5, 0x92, 0x4a, 0x2, 
    0x534, 0x537, 0x5, 0x90, 0x49, 0x2, 0x535, 0x537, 0x5, 0x9e, 0x50, 0x2, 
    0x536, 0x533, 0x3, 0x2, 0x2, 0x2, 0x536, 0x534, 0x3, 0x2, 0x2, 0x2, 
    0x536, 0x535, 0x3, 0x2, 0x2, 0x2, 0x537, 0x97, 0x3, 0x2, 0x2, 0x2, 0x538, 
    0x539, 0x5, 0xa8, 0x55, 0x2, 0x539, 0x99, 0x3, 0x2, 0x2, 0x2, 0x53a, 
    0x543, 0x7, 0x9e, 0x2, 0x2, 0x53b, 0x53c, 0x7, 0xab, 0x2, 0x2, 0x53c, 
    0x543, 0x9, 0x17, 0x2, 0x2, 0x53d, 0x53e, 0x7, 0xa0, 0x2, 0x2, 0x53e, 
    0x540, 0x7, 0xab, 0x2, 0x2, 0x53f, 0x541, 0x9, 0x17, 0x2, 0x2, 0x540, 
    0x53f, 0x3, 0x2, 0x2, 0x2, 0x540, 0x541, 0x3, 0x2, 0x2, 0x2, 0x541, 
    0x543, 0x3, 0x2, 0x2, 0x2, 0x542, 0x53a, 0x3, 0x2, 0x2, 0x2, 0x542, 
    0x53b, 0x3, 0x2, 0x2, 0x2, 0x542, 0x53d, 0x3, 0x2, 0x2, 0x2, 0x543, 
    0x9b, 0x3, 0x2, 0x2, 0x2, 0x544, 0x546, 0x9, 0x18, 0x2, 0x2, 0x545, 
    0x544, 0x3, 0x2, 0x2, 0x2, 0x545, 0x546, 0x3, 0x2, 0x2, 0x2, 0x546, 
    0x54d, 0x3, 0x2, 0x2, 0x2, 0x547, 0x54e, 0x5, 0x9a, 0x4e, 0x2, 0x548, 
    0x54e, 0x7, 0x9f, 0x2, 0x2, 0x549, 0x54e, 0x7, 0xa0, 0x2, 0x2, 0x54a, 
    0x54e, 0x7, 0xa1, 0x2, 0x2, 0x54b, 0x54e, 0x7, 0x49, 0x2, 0x2, 0x54c, 
    0x54e, 0x7, 0x5d, 0x2, 0x2, 0x54d, 0x547, 0x3, 0x2, 0x2, 0x2, 0x54d, 
    0x548, 0x3, 0x2, 0x2, 0x2, 0x54d, 0x549, 0x3, 0x2, 0x2, 0x2, 0x54d, 
    0x54a, 0x3, 0x2, 0x2, 0x2, 0x54d, 0x54b, 0x3, 0x2, 0x2, 0x2, 0x54d, 
    0x54c, 0x3, 0x2, 0x2, 0x2, 0x54e, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x54f, 0x553, 
    0x5, 0x9c, 0x4f, 0x2, 0x550, 0x553, 0x7, 0xa2, 0x2, 0x2, 0x551, 0x553, 
    0x7, 0x60, 0x2, 0x2, 0x552, 0x54f, 0x3, 0x2, 0x2, 0x2, 0x552, 0x550, 
    0x3, 0x2, 0x2, 0x2, 0x552, 0x551, 0x3, 0x2, 0x2, 0x2, 0x553, 0x9f, 0x3, 
    0x2, 0x2, 0x2, 0x554, 0x555, 0x9, 0x19, 0x2, 0x2, 0x555, 0xa1, 0x3, 
    0x2, 0x2, 0x2, 0x556, 0x557, 0x9, 0x1a, 0x2, 0x2, 0x557, 0xa3, 0x3, 
    0x2, 0x2, 0x2, 0x558, 0x559, 0x9, 0x1b, 0x2, 0x2, 0x559, 0xa5, 0x3, 
    0x2, 0x2, 0x2, 0x55a, 0x55d, 0x7, 0x9d, 0x2, 0x2, 0x55b, 0x55d, 0x5, 
    0xa4, 0x53, 0x2, 0x55c, 0x55a, 0x3, 0x2, 0x2, 0x2, 0x55c, 0x55b, 0x3, 
    0x2, 0x2, 0x2, 0x55d, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x55e, 0x562, 0x7, 0x9d, 
    0x2, 0x2, 0x55f, 0x562, 0x5, 0xa0, 0x51, 0x2, 0x560, 0x562, 0x5, 0xa2, 
    0x52, 0x2, 0x561, 0x55e, 0x3, 0x2, 0x2, 0x2, 0x561, 0x55f, 0x3, 0x2, 
    0x2, 0x2, 0x561, 0x560, 0x3, 0x2, 0x2, 0x2, 0x562, 0xa9, 0x3, 0x2, 0x2, 
    0x2, 0x563, 0x566, 0x5, 0xa8, 0x55, 0x2, 0x564, 0x566, 0x7, 0x60, 0x2, 
    0x2, 0x565, 0x563, 0x3, 0x2, 0x2, 0x2, 0x565, 0x564, 0x3, 0x2, 0x2, 
    0x2, 0x566, 0xab, 0x3, 0x2, 0x2, 0x2, 0x567, 0x568, 0x7, 0xa2, 0x2, 
    0x2, 0x568, 0x569, 0x7, 0xad, 0x2, 0x2, 0x569, 0x56a, 0x5, 0x9c, 0x4f, 
    0x2, 0x56a, 0xad, 0x3, 0x2, 0x2, 0x2, 0xb7, 0xb2, 0xb6, 0xb9, 0xbc, 
    0xcd, 0xd7, 0xdf, 0xe4, 0xea, 0xf0, 0xf5, 0xfb, 0x109, 0x112, 0x11d, 
    0x12c, 0x137, 0x140, 0x14b, 0x14f, 0x157, 0x15b, 0x160, 0x162, 0x168, 
    0x16e, 0x172, 0x175, 0x178, 0x17f, 0x183, 0x187, 0x195, 0x19e, 0x1b9, 
    0x1bb, 0x1d0, 0x1d5, 0x1da, 0x1dd, 0x1ed, 0x1f2, 0x1f6, 0x1f9, 0x1fd, 
    0x201, 0x206, 0x209, 0x20d, 0x20f, 0x219, 0x21c, 0x228, 0x22c, 0x234, 
    0x239, 0x23e, 0x243, 0x245, 0x249, 0x255, 0x25a, 0x25d, 0x267, 0x270, 
    0x276, 0x279, 0x27c, 0x28a, 0x293, 0x29b, 0x29e, 0x2a2, 0x2a6, 0x2a9, 
    0x2ac, 0x2af, 0x2b2, 0x2b6, 0x2ba, 0x2bd, 0x2c0, 0x2c3, 0x2c6, 0x2c9, 
    0x2d2, 0x2e6, 0x2f8, 0x300, 0x303, 0x309, 0x311, 0x314, 0x31a, 0x31c, 
    0x320, 0x325, 0x328, 0x32b, 0x32f, 0x333, 0x336, 0x338, 0x33b, 0x33f, 
    0x343, 0x346, 0x348, 0x34a, 0x34d, 0x352, 0x35d, 0x363, 0x368, 0x36f, 
    0x374, 0x378, 0x37c, 0x381, 0x388, 0x399, 0x39c, 0x3a1, 0x3a6, 0x3ab, 
    0x3ae, 0x3b2, 0x3c1, 0x3cc, 0x3d0, 0x3d5, 0x3e7, 0x3f3, 0x3ff, 0x407, 
    0x40b, 0x412, 0x418, 0x420, 0x425, 0x42e, 0x432, 0x451, 0x462, 0x465, 
    0x469, 0x46c, 0x478, 0x489, 0x48d, 0x49e, 0x4a1, 0x4a5, 0x4a8, 0x4b3, 
    0x4cb, 0x4d2, 0x4d4, 0x4d6, 0x4de, 0x4e3, 0x4eb, 0x4f5, 0x4f8, 0x500, 
    0x507, 0x510, 0x516, 0x51a, 0x520, 0x527, 0x530, 0x536, 0x540, 0x542, 
    0x545, 0x54d, 0x552, 0x55c, 0x561, 0x565, 
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
