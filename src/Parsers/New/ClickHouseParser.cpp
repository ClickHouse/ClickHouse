
// Generated from ClickHouseParser.g4 by ANTLR 4.8


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


//----------------- QueryListContext ------------------------------------------------------------------

ClickHouseParser::QueryListContext::QueryListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::QueryStmtContext *> ClickHouseParser::QueryListContext::queryStmt() {
  return getRuleContexts<ClickHouseParser::QueryStmtContext>();
}

ClickHouseParser::QueryStmtContext* ClickHouseParser::QueryListContext::queryStmt(size_t i) {
  return getRuleContext<ClickHouseParser::QueryStmtContext>(i);
}

tree::TerminalNode* ClickHouseParser::QueryListContext::EOF() {
  return getToken(ClickHouseParser::EOF, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::QueryListContext::SEMICOLON() {
  return getTokens(ClickHouseParser::SEMICOLON);
}

tree::TerminalNode* ClickHouseParser::QueryListContext::SEMICOLON(size_t i) {
  return getToken(ClickHouseParser::SEMICOLON, i);
}


size_t ClickHouseParser::QueryListContext::getRuleIndex() const {
  return ClickHouseParser::RuleQueryList;
}


antlrcpp::Any ClickHouseParser::QueryListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitQueryList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::QueryListContext* ClickHouseParser::queryList() {
  QueryListContext *_localctx = _tracker.createInstance<QueryListContext>(_ctx, getState());
  enterRule(_localctx, 0, ClickHouseParser::RuleQueryList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(160);
    queryStmt();
    setState(165);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(161);
        match(ClickHouseParser::SEMICOLON);
        setState(162);
        queryStmt(); 
      }
      setState(167);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    }
    setState(169);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SEMICOLON) {
      setState(168);
      match(ClickHouseParser::SEMICOLON);
    }
    setState(171);
    match(ClickHouseParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
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
  enterRule(_localctx, 2, ClickHouseParser::RuleQueryStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(173);
    query();
    setState(177);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INTO) {
      setState(174);
      match(ClickHouseParser::INTO);
      setState(175);
      match(ClickHouseParser::OUTFILE);
      setState(176);
      match(ClickHouseParser::STRING_LITERAL);
    }
    setState(181);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FORMAT) {
      setState(179);
      match(ClickHouseParser::FORMAT);
      setState(180);
      identifierOrNull();
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

ClickHouseParser::InsertStmtContext* ClickHouseParser::QueryContext::insertStmt() {
  return getRuleContext<ClickHouseParser::InsertStmtContext>(0);
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
  enterRule(_localctx, 4, ClickHouseParser::RuleQuery);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(197);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::ALTER: {
        enterOuterAlt(_localctx, 1);
        setState(183);
        alterStmt();
        break;
      }

      case ClickHouseParser::CHECK: {
        enterOuterAlt(_localctx, 2);
        setState(184);
        checkStmt();
        break;
      }

      case ClickHouseParser::ATTACH:
      case ClickHouseParser::CREATE: {
        enterOuterAlt(_localctx, 3);
        setState(185);
        createStmt();
        break;
      }

      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCRIBE: {
        enterOuterAlt(_localctx, 4);
        setState(186);
        describeStmt();
        break;
      }

      case ClickHouseParser::DETACH:
      case ClickHouseParser::DROP: {
        enterOuterAlt(_localctx, 5);
        setState(187);
        dropStmt();
        break;
      }

      case ClickHouseParser::EXISTS: {
        enterOuterAlt(_localctx, 6);
        setState(188);
        existsStmt();
        break;
      }

      case ClickHouseParser::INSERT: {
        enterOuterAlt(_localctx, 7);
        setState(189);
        insertStmt();
        break;
      }

      case ClickHouseParser::OPTIMIZE: {
        enterOuterAlt(_localctx, 8);
        setState(190);
        optimizeStmt();
        break;
      }

      case ClickHouseParser::RENAME: {
        enterOuterAlt(_localctx, 9);
        setState(191);
        renameStmt();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 10);
        setState(192);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::SET: {
        enterOuterAlt(_localctx, 11);
        setState(193);
        setStmt();
        break;
      }

      case ClickHouseParser::SHOW: {
        enterOuterAlt(_localctx, 12);
        setState(194);
        showStmt();
        break;
      }

      case ClickHouseParser::SYSTEM: {
        enterOuterAlt(_localctx, 13);
        setState(195);
        systemStmt();
        break;
      }

      case ClickHouseParser::USE: {
        enterOuterAlt(_localctx, 14);
        setState(196);
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
//----------------- AlterPartitionStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterPartitionStmtContext::ALTER() {
  return getToken(ClickHouseParser::ALTER, 0);
}

tree::TerminalNode* ClickHouseParser::AlterPartitionStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::AlterPartitionStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

std::vector<ClickHouseParser::AlterPartitionClauseContext *> ClickHouseParser::AlterPartitionStmtContext::alterPartitionClause() {
  return getRuleContexts<ClickHouseParser::AlterPartitionClauseContext>();
}

ClickHouseParser::AlterPartitionClauseContext* ClickHouseParser::AlterPartitionStmtContext::alterPartitionClause(size_t i) {
  return getRuleContext<ClickHouseParser::AlterPartitionClauseContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::AlterPartitionStmtContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::AlterPartitionStmtContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::AlterPartitionStmtContext::AlterPartitionStmtContext(AlterStmtContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterPartitionStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterPartitionStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AlterStmtContext* ClickHouseParser::alterStmt() {
  AlterStmtContext *_localctx = _tracker.createInstance<AlterStmtContext>(_ctx, getState());
  enterRule(_localctx, 6, ClickHouseParser::RuleAlterStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(221);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<AlterStmtContext *>(_tracker.createInstance<ClickHouseParser::AlterTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(199);
      match(ClickHouseParser::ALTER);
      setState(200);
      match(ClickHouseParser::TABLE);
      setState(201);
      tableIdentifier();
      setState(202);
      alterTableClause();
      setState(207);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(203);
        match(ClickHouseParser::COMMA);
        setState(204);
        alterTableClause();
        setState(209);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<AlterStmtContext *>(_tracker.createInstance<ClickHouseParser::AlterPartitionStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(210);
      match(ClickHouseParser::ALTER);
      setState(211);
      match(ClickHouseParser::TABLE);
      setState(212);
      tableIdentifier();
      setState(213);
      alterPartitionClause();
      setState(218);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(214);
        match(ClickHouseParser::COMMA);
        setState(215);
        alterPartitionClause();
        setState(220);
        _errHandler->sync(this);
        _la = _input->LA(1);
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

//----------------- AlterTableDropClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableDropClauseContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableDropClauseContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableDropClauseContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableDropClauseContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableDropClauseContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableDropClauseContext::AlterTableDropClauseContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterTableDropClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableDropClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableModifyClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableModifyClauseContext::MODIFY() {
  return getToken(ClickHouseParser::MODIFY, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableModifyClauseContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::AlterTableModifyClauseContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableModifyClauseContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableModifyClauseContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableModifyClauseContext::AlterTableModifyClauseContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterTableModifyClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableModifyClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableAddClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::ADD() {
  return getToken(ClickHouseParser::ADD, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::TableColumnDfntContext* ClickHouseParser::AlterTableAddClauseContext::tableColumnDfnt() {
  return getRuleContext<ClickHouseParser::TableColumnDfntContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableAddClauseContext::AFTER() {
  return getToken(ClickHouseParser::AFTER, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableAddClauseContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

ClickHouseParser::AlterTableAddClauseContext::AlterTableAddClauseContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterTableAddClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableAddClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableCommentClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableCommentClauseContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableCommentClauseContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableCommentClauseContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableCommentClauseContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableCommentClauseContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableCommentClauseContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableCommentClauseContext::AlterTableCommentClauseContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterTableCommentClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableCommentClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterTableClearClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterTableClearClauseContext::CLEAR() {
  return getToken(ClickHouseParser::CLEAR, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClearClauseContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::AlterTableClearClauseContext::nestedIdentifier() {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClearClauseContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClearClauseContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClearClauseContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClearClauseContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
}

ClickHouseParser::AlterTableClearClauseContext::AlterTableClearClauseContext(AlterTableClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterTableClearClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterTableClearClause(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AlterTableClauseContext* ClickHouseParser::alterTableClause() {
  AlterTableClauseContext *_localctx = _tracker.createInstance<AlterTableClauseContext>(_ctx, getState());
  enterRule(_localctx, 8, ClickHouseParser::RuleAlterTableClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(268);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::ADD: {
        _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableAddClauseContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(223);
        match(ClickHouseParser::ADD);
        setState(224);
        match(ClickHouseParser::COLUMN);
        setState(228);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
        case 1: {
          setState(225);
          match(ClickHouseParser::IF);
          setState(226);
          match(ClickHouseParser::NOT);
          setState(227);
          match(ClickHouseParser::EXISTS);
          break;
        }

        }
        setState(230);
        tableColumnDfnt();
        setState(233);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::AFTER) {
          setState(231);
          match(ClickHouseParser::AFTER);
          setState(232);
          nestedIdentifier();
        }
        break;
      }

      case ClickHouseParser::CLEAR: {
        _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClearClauseContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(235);
        match(ClickHouseParser::CLEAR);
        setState(236);
        match(ClickHouseParser::COLUMN);
        setState(239);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
        case 1: {
          setState(237);
          match(ClickHouseParser::IF);
          setState(238);
          match(ClickHouseParser::EXISTS);
          break;
        }

        }
        setState(241);
        nestedIdentifier();
        setState(242);
        match(ClickHouseParser::IN);
        setState(243);
        partitionClause();
        break;
      }

      case ClickHouseParser::COMMENT: {
        _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableCommentClauseContext>(_localctx));
        enterOuterAlt(_localctx, 3);
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

      case ClickHouseParser::DROP: {
        _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableDropClauseContext>(_localctx));
        enterOuterAlt(_localctx, 4);
        setState(254);
        match(ClickHouseParser::DROP);
        setState(255);
        match(ClickHouseParser::COLUMN);
        setState(258);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
        case 1: {
          setState(256);
          match(ClickHouseParser::IF);
          setState(257);
          match(ClickHouseParser::EXISTS);
          break;
        }

        }
        setState(260);
        nestedIdentifier();
        break;
      }

      case ClickHouseParser::MODIFY: {
        _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableModifyClauseContext>(_localctx));
        enterOuterAlt(_localctx, 5);
        setState(261);
        match(ClickHouseParser::MODIFY);
        setState(262);
        match(ClickHouseParser::COLUMN);
        setState(265);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx)) {
        case 1: {
          setState(263);
          match(ClickHouseParser::IF);
          setState(264);
          match(ClickHouseParser::EXISTS);
          break;
        }

        }
        setState(267);
        tableColumnDfnt();
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

//----------------- AlterPartitionClauseContext ------------------------------------------------------------------

ClickHouseParser::AlterPartitionClauseContext::AlterPartitionClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::AlterPartitionClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleAlterPartitionClause;
}

void ClickHouseParser::AlterPartitionClauseContext::copyFrom(AlterPartitionClauseContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- AlterPartitionDropClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AlterPartitionDropClauseContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterPartitionDropClauseContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

ClickHouseParser::AlterPartitionDropClauseContext::AlterPartitionDropClauseContext(AlterPartitionClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AlterPartitionDropClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlterPartitionDropClause(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::AlterPartitionClauseContext* ClickHouseParser::alterPartitionClause() {
  AlterPartitionClauseContext *_localctx = _tracker.createInstance<AlterPartitionClauseContext>(_ctx, getState());
  enterRule(_localctx, 10, ClickHouseParser::RuleAlterPartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<AlterPartitionClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterPartitionDropClauseContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(270);
    match(ClickHouseParser::DROP);
    setState(271);
    partitionClause();
   
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
    setState(273);
    match(ClickHouseParser::CHECK);
    setState(274);
    match(ClickHouseParser::TABLE);
    setState(275);
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

ClickHouseParser::EngineClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
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
    setState(338);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(277);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(278);
      match(ClickHouseParser::DATABASE);
      setState(282);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 15, _ctx)) {
      case 1: {
        setState(279);
        match(ClickHouseParser::IF);
        setState(280);
        match(ClickHouseParser::NOT);
        setState(281);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(284);
      databaseIdentifier();
      setState(286);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(285);
        engineExpr();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateMaterializedViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(288);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(289);
      match(ClickHouseParser::MATERIALIZED);
      setState(290);
      match(ClickHouseParser::VIEW);
      setState(294);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
      case 1: {
        setState(291);
        match(ClickHouseParser::IF);
        setState(292);
        match(ClickHouseParser::NOT);
        setState(293);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(296);
      tableIdentifier();
      setState(298);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
      case 1: {
        setState(297);
        schemaClause();
        break;
      }

      }
      setState(301);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(300);
        engineClause();
      }
      setState(304);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::POPULATE) {
        setState(303);
        match(ClickHouseParser::POPULATE);
      }
      setState(306);
      subqueryClause();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(308);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(310);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(309);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(312);
      match(ClickHouseParser::TABLE);
      setState(316);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx)) {
      case 1: {
        setState(313);
        match(ClickHouseParser::IF);
        setState(314);
        match(ClickHouseParser::NOT);
        setState(315);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(318);
      tableIdentifier();
      setState(320);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx)) {
      case 1: {
        setState(319);
        schemaClause();
        break;
      }

      }
      setState(323);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(322);
        engineClause();
      }
      setState(326);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS) {
        setState(325);
        subqueryClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(328);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(329);
      match(ClickHouseParser::VIEW);
      setState(333);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
      case 1: {
        setState(330);
        match(ClickHouseParser::IF);
        setState(331);
        match(ClickHouseParser::NOT);
        setState(332);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(335);
      tableIdentifier();
      setState(336);
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
  enterRule(_localctx, 16, ClickHouseParser::RuleSubqueryClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(340);
    match(ClickHouseParser::AS);
    setState(341);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::SchemaAsFunctionClauseContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::SchemaAsFunctionClauseContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::SchemaAsFunctionClauseContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::TableArgListContext* ClickHouseParser::SchemaAsFunctionClauseContext::tableArgList() {
  return getRuleContext<ClickHouseParser::TableArgListContext>(0);
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
  enterRule(_localctx, 18, ClickHouseParser::RuleSchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(364);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaDescriptionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(343);
      match(ClickHouseParser::LPAREN);
      setState(344);
      tableElementExpr();
      setState(349);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(345);
        match(ClickHouseParser::COMMA);
        setState(346);
        tableElementExpr();
        setState(351);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(352);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsTableClauseContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(354);
      match(ClickHouseParser::AS);
      setState(355);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsFunctionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(356);
      match(ClickHouseParser::AS);
      setState(357);
      identifier();
      setState(358);
      match(ClickHouseParser::LPAREN);
      setState(360);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(359);
        tableArgList();
      }
      setState(362);
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

//----------------- EngineClauseContext ------------------------------------------------------------------

ClickHouseParser::EngineClauseContext::EngineClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::EngineExprContext* ClickHouseParser::EngineClauseContext::engineExpr() {
  return getRuleContext<ClickHouseParser::EngineExprContext>(0);
}

ClickHouseParser::OrderByClauseContext* ClickHouseParser::EngineClauseContext::orderByClause() {
  return getRuleContext<ClickHouseParser::OrderByClauseContext>(0);
}

ClickHouseParser::PartitionByClauseContext* ClickHouseParser::EngineClauseContext::partitionByClause() {
  return getRuleContext<ClickHouseParser::PartitionByClauseContext>(0);
}

ClickHouseParser::PrimaryKeyClauseContext* ClickHouseParser::EngineClauseContext::primaryKeyClause() {
  return getRuleContext<ClickHouseParser::PrimaryKeyClauseContext>(0);
}

ClickHouseParser::SampleByClauseContext* ClickHouseParser::EngineClauseContext::sampleByClause() {
  return getRuleContext<ClickHouseParser::SampleByClauseContext>(0);
}

ClickHouseParser::TtlClauseContext* ClickHouseParser::EngineClauseContext::ttlClause() {
  return getRuleContext<ClickHouseParser::TtlClauseContext>(0);
}

ClickHouseParser::SettingsClauseContext* ClickHouseParser::EngineClauseContext::settingsClause() {
  return getRuleContext<ClickHouseParser::SettingsClauseContext>(0);
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
  enterRule(_localctx, 20, ClickHouseParser::RuleEngineClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(366);
    engineExpr();
    setState(368);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(367);
      orderByClause();
    }
    setState(371);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(370);
      partitionByClause();
    }
    setState(374);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PRIMARY) {
      setState(373);
      primaryKeyClause();
    }
    setState(377);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(376);
      sampleByClause();
    }
    setState(380);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TTL) {
      setState(379);
      ttlClause();
    }
    setState(383);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(382);
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
  enterRule(_localctx, 22, ClickHouseParser::RulePartitionByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(385);
    match(ClickHouseParser::PARTITION);
    setState(386);
    match(ClickHouseParser::BY);
    setState(387);
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
  enterRule(_localctx, 24, ClickHouseParser::RulePrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(389);
    match(ClickHouseParser::PRIMARY);
    setState(390);
    match(ClickHouseParser::KEY);
    setState(391);
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
  enterRule(_localctx, 26, ClickHouseParser::RuleSampleByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(393);
    match(ClickHouseParser::SAMPLE);
    setState(394);
    match(ClickHouseParser::BY);
    setState(395);
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
  enterRule(_localctx, 28, ClickHouseParser::RuleTtlClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(397);
    match(ClickHouseParser::TTL);
    setState(398);
    ttlExpr();
    setState(403);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(399);
      match(ClickHouseParser::COMMA);
      setState(400);
      ttlExpr();
      setState(405);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleEngineExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(406);
    match(ClickHouseParser::ENGINE);
    setState(408);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EQ_SINGLE) {
      setState(407);
      match(ClickHouseParser::EQ_SINGLE);
    }
    setState(410);
    identifierOrNull();
    setState(416);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(411);
      match(ClickHouseParser::LPAREN);
      setState(413);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(412);
        columnExprList();
      }
      setState(415);
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
ClickHouseParser::TableElementExprContext* ClickHouseParser::tableElementExpr() {
  TableElementExprContext *_localctx = _tracker.createInstance<TableElementExprContext>(_ctx, getState());
  enterRule(_localctx, 32, ClickHouseParser::RuleTableElementExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(418);
    tableColumnDfnt();
   
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
  enterRule(_localctx, 34, ClickHouseParser::RuleTableColumnDfnt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(438);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(420);
      nestedIdentifier();
      setState(421);
      columnTypeExpr();
      setState(423);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ALIAS

      || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED) {
        setState(422);
        tableColumnPropertyExpr();
      }
      setState(427);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(425);
        match(ClickHouseParser::TTL);
        setState(426);
        columnExpr(0);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(429);
      nestedIdentifier();
      setState(431);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
      case 1: {
        setState(430);
        columnTypeExpr();
        break;
      }

      }
      setState(433);
      tableColumnPropertyExpr();
      setState(436);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(434);
        match(ClickHouseParser::TTL);
        setState(435);
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
  enterRule(_localctx, 36, ClickHouseParser::RuleTableColumnPropertyExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(440);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::ALIAS

    || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(441);
    columnExpr(0);
   
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
  enterRule(_localctx, 38, ClickHouseParser::RuleTtlExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(443);
    columnExpr(0);
    setState(451);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
    case 1: {
      setState(444);
      match(ClickHouseParser::DELETE);
      break;
    }

    case 2: {
      setState(445);
      match(ClickHouseParser::TO);
      setState(446);
      match(ClickHouseParser::DISK);
      setState(447);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 3: {
      setState(448);
      match(ClickHouseParser::TO);
      setState(449);
      match(ClickHouseParser::VOLUME);
      setState(450);
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

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::DescribeStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::DESCRIBE() {
  return getToken(ClickHouseParser::DESCRIBE, 0);
}

tree::TerminalNode* ClickHouseParser::DescribeStmtContext::DESC() {
  return getToken(ClickHouseParser::DESC, 0);
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
  enterRule(_localctx, 40, ClickHouseParser::RuleDescribeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(453);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DESC

    || _la == ClickHouseParser::DESCRIBE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(454);
    match(ClickHouseParser::TABLE);
    setState(455);
    tableIdentifier();
   
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
  enterRule(_localctx, 42, ClickHouseParser::RuleDropStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(478);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(457);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(458);
      match(ClickHouseParser::DATABASE);
      setState(461);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx)) {
      case 1: {
        setState(459);
        match(ClickHouseParser::IF);
        setState(460);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(463);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(464);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(466);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(465);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(468);
      match(ClickHouseParser::TABLE);
      setState(471);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
      case 1: {
        setState(469);
        match(ClickHouseParser::IF);
        setState(470);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(473);
      tableIdentifier();
      setState(476);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NO) {
        setState(474);
        match(ClickHouseParser::NO);
        setState(475);
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
  enterRule(_localctx, 44, ClickHouseParser::RuleExistsStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(480);
    match(ClickHouseParser::EXISTS);
    setState(482);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TEMPORARY) {
      setState(481);
      match(ClickHouseParser::TEMPORARY);
    }
    setState(484);
    match(ClickHouseParser::TABLE);
    setState(485);
    tableIdentifier();
   
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

ClickHouseParser::TableIdentifierContext* ClickHouseParser::InsertStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::ValuesClauseContext* ClickHouseParser::InsertStmtContext::valuesClause() {
  return getRuleContext<ClickHouseParser::ValuesClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

std::vector<ClickHouseParser::NestedIdentifierContext *> ClickHouseParser::InsertStmtContext::nestedIdentifier() {
  return getRuleContexts<ClickHouseParser::NestedIdentifierContext>();
}

ClickHouseParser::NestedIdentifierContext* ClickHouseParser::InsertStmtContext::nestedIdentifier(size_t i) {
  return getRuleContext<ClickHouseParser::NestedIdentifierContext>(i);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::InsertStmtContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::InsertStmtContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
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
  enterRule(_localctx, 46, ClickHouseParser::RuleInsertStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(487);
    match(ClickHouseParser::INSERT);
    setState(488);
    match(ClickHouseParser::INTO);
    setState(489);
    tableIdentifier();
    setState(501);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(490);
      match(ClickHouseParser::LPAREN);
      setState(491);
      nestedIdentifier();
      setState(496);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(492);
        match(ClickHouseParser::COMMA);
        setState(493);
        nestedIdentifier();
        setState(498);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(499);
      match(ClickHouseParser::RPAREN);
    }
    setState(503);
    valuesClause();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValuesClauseContext ------------------------------------------------------------------

ClickHouseParser::ValuesClauseContext::ValuesClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ValuesClauseContext::VALUES() {
  return getToken(ClickHouseParser::VALUES, 0);
}

std::vector<ClickHouseParser::ValueTupleExprContext *> ClickHouseParser::ValuesClauseContext::valueTupleExpr() {
  return getRuleContexts<ClickHouseParser::ValueTupleExprContext>();
}

ClickHouseParser::ValueTupleExprContext* ClickHouseParser::ValuesClauseContext::valueTupleExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ValueTupleExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ValuesClauseContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ValuesClauseContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::ValuesClauseContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}


size_t ClickHouseParser::ValuesClauseContext::getRuleIndex() const {
  return ClickHouseParser::RuleValuesClause;
}


antlrcpp::Any ClickHouseParser::ValuesClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValuesClause(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ValuesClauseContext* ClickHouseParser::valuesClause() {
  ValuesClauseContext *_localctx = _tracker.createInstance<ValuesClauseContext>(_ctx, getState());
  enterRule(_localctx, 48, ClickHouseParser::RuleValuesClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(517);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::VALUES: {
        enterOuterAlt(_localctx, 1);
        setState(505);
        match(ClickHouseParser::VALUES);
        setState(506);
        valueTupleExpr();
        setState(513);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA

        || _la == ClickHouseParser::LPAREN) {
          setState(508);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::COMMA) {
            setState(507);
            match(ClickHouseParser::COMMA);
          }
          setState(510);
          valueTupleExpr();
          setState(515);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 2);
        setState(516);
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

//----------------- ValueTupleExprContext ------------------------------------------------------------------

ClickHouseParser::ValueTupleExprContext::ValueTupleExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::ValueTupleExprContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ValueTupleExprContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::ValueTupleExprContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}


size_t ClickHouseParser::ValueTupleExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleValueTupleExpr;
}


antlrcpp::Any ClickHouseParser::ValueTupleExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValueTupleExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ValueTupleExprContext* ClickHouseParser::valueTupleExpr() {
  ValueTupleExprContext *_localctx = _tracker.createInstance<ValueTupleExprContext>(_ctx, getState());
  enterRule(_localctx, 50, ClickHouseParser::RuleValueTupleExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(519);
    match(ClickHouseParser::LPAREN);
    setState(520);
    columnExprList();
    setState(521);
    match(ClickHouseParser::RPAREN);
   
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
  enterRule(_localctx, 52, ClickHouseParser::RuleOptimizeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(523);
    match(ClickHouseParser::OPTIMIZE);
    setState(524);
    match(ClickHouseParser::TABLE);
    setState(525);
    tableIdentifier();
    setState(527);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(526);
      partitionClause();
    }
    setState(530);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(529);
      match(ClickHouseParser::FINAL);
    }
    setState(533);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DEDUPLICATE) {
      setState(532);
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
  enterRule(_localctx, 54, ClickHouseParser::RulePartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(540);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 61, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(535);
      match(ClickHouseParser::PARTITION);
      setState(536);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(537);
      match(ClickHouseParser::PARTITION);
      setState(538);
      match(ClickHouseParser::ID);
      setState(539);
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
  enterRule(_localctx, 56, ClickHouseParser::RuleRenameStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(542);
    match(ClickHouseParser::RENAME);
    setState(543);
    match(ClickHouseParser::TABLE);
    setState(544);
    tableIdentifier();
    setState(545);
    match(ClickHouseParser::TO);
    setState(546);
    tableIdentifier();
    setState(554);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(547);
      match(ClickHouseParser::COMMA);
      setState(548);
      tableIdentifier();
      setState(549);
      match(ClickHouseParser::TO);
      setState(550);
      tableIdentifier();
      setState(556);
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

std::vector<ClickHouseParser::SelectStmtContext *> ClickHouseParser::SelectUnionStmtContext::selectStmt() {
  return getRuleContexts<ClickHouseParser::SelectStmtContext>();
}

ClickHouseParser::SelectStmtContext* ClickHouseParser::SelectUnionStmtContext::selectStmt(size_t i) {
  return getRuleContext<ClickHouseParser::SelectStmtContext>(i);
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
  enterRule(_localctx, 58, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(557);
    selectStmt();
    setState(563);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(558);
      match(ClickHouseParser::UNION);
      setState(559);
      match(ClickHouseParser::ALL);
      setState(560);
      selectStmt();
      setState(565);
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

ClickHouseParser::SampleClauseContext* ClickHouseParser::SelectStmtContext::sampleClause() {
  return getRuleContext<ClickHouseParser::SampleClauseContext>(0);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(567);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(566);
      withClause();
    }
    setState(569);
    match(ClickHouseParser::SELECT);
    setState(571);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 65, _ctx)) {
    case 1: {
      setState(570);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(573);
    columnExprList();
    setState(575);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(574);
      fromClause();
    }
    setState(578);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(577);
      sampleClause();
    }
    setState(581);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY

    || _la == ClickHouseParser::LEFT) {
      setState(580);
      arrayJoinClause();
    }
    setState(584);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(583);
      prewhereClause();
    }
    setState(587);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(586);
      whereClause();
    }
    setState(590);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(589);
      groupByClause();
    }
    setState(593);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(592);
      havingClause();
    }
    setState(596);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(595);
      orderByClause();
    }
    setState(599);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
    case 1: {
      setState(598);
      limitByClause();
      break;
    }

    }
    setState(602);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(601);
      limitClause();
    }
    setState(605);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(604);
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
  enterRule(_localctx, 62, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(607);
    match(ClickHouseParser::WITH);
    setState(608);
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

tree::TerminalNode* ClickHouseParser::FromClauseContext::FINAL() {
  return getToken(ClickHouseParser::FINAL, 0);
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
  enterRule(_localctx, 64, ClickHouseParser::RuleFromClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(610);
    match(ClickHouseParser::FROM);
    setState(611);
    joinExpr(0);
    setState(613);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(612);
      match(ClickHouseParser::FINAL);
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
  enterRule(_localctx, 66, ClickHouseParser::RuleSampleClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(615);
    match(ClickHouseParser::SAMPLE);
    setState(616);
    ratioExpr();
    setState(619);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET) {
      setState(617);
      match(ClickHouseParser::OFFSET);
      setState(618);
      ratioExpr();
    }
   
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
  enterRule(_localctx, 68, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(622);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LEFT) {
      setState(621);
      match(ClickHouseParser::LEFT);
    }
    setState(624);
    match(ClickHouseParser::ARRAY);
    setState(625);
    match(ClickHouseParser::JOIN);
    setState(626);
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
  enterRule(_localctx, 70, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(628);
    match(ClickHouseParser::PREWHERE);
    setState(629);
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
  enterRule(_localctx, 72, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(631);
    match(ClickHouseParser::WHERE);
    setState(632);
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

ClickHouseParser::ColumnExprListContext* ClickHouseParser::GroupByClauseContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::WITH() {
  return getToken(ClickHouseParser::WITH, 0);
}

tree::TerminalNode* ClickHouseParser::GroupByClauseContext::TOTALS() {
  return getToken(ClickHouseParser::TOTALS, 0);
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
  enterRule(_localctx, 74, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(634);
    match(ClickHouseParser::GROUP);
    setState(635);
    match(ClickHouseParser::BY);
    setState(636);
    columnExprList();
    setState(639);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(637);
      match(ClickHouseParser::WITH);
      setState(638);
      match(ClickHouseParser::TOTALS);
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
  enterRule(_localctx, 76, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(641);
    match(ClickHouseParser::HAVING);
    setState(642);
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
  enterRule(_localctx, 78, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(644);
    match(ClickHouseParser::ORDER);
    setState(645);
    match(ClickHouseParser::BY);
    setState(646);
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
  enterRule(_localctx, 80, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(648);
    match(ClickHouseParser::LIMIT);
    setState(649);
    limitExpr();
    setState(650);
    match(ClickHouseParser::BY);
    setState(651);
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
  enterRule(_localctx, 82, ClickHouseParser::RuleLimitClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(653);
    match(ClickHouseParser::LIMIT);
    setState(654);
    limitExpr();
    setState(657);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(655);
      match(ClickHouseParser::WITH);
      setState(656);
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
  enterRule(_localctx, 84, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(659);
    match(ClickHouseParser::SETTINGS);
    setState(660);
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

ClickHouseParser::JoinOpContext* ClickHouseParser::JoinExprOpContext::joinOp() {
  return getRuleContext<ClickHouseParser::JoinOpContext>(0);
}

tree::TerminalNode* ClickHouseParser::JoinExprOpContext::JOIN() {
  return getToken(ClickHouseParser::JOIN, 0);
}

ClickHouseParser::JoinConstraintClauseContext* ClickHouseParser::JoinExprOpContext::joinConstraintClause() {
  return getRuleContext<ClickHouseParser::JoinConstraintClauseContext>(0);
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
  size_t startState = 86;
  enterRecursionRule(_localctx, 86, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(668);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 82, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(663);
      tableExpr(0);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<JoinExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(664);
      match(ClickHouseParser::LPAREN);
      setState(665);
      joinExpr(0);
      setState(666);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(685);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(683);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 84, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(670);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(671);
          joinOpCross();
          setState(672);
          joinExpr(2);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(674);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(676);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(675);
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
          setState(678);
          joinOp();
          setState(679);
          match(ClickHouseParser::JOIN);
          setState(680);
          joinExpr(0);
          setState(681);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(687);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx);
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

tree::TerminalNode* ClickHouseParser::JoinOpInnerContext::ANY() {
  return getToken(ClickHouseParser::ANY, 0);
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
  enterRule(_localctx, 88, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(718);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(696);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx)) {
      case 1: {
        setState(689);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(688);
          match(ClickHouseParser::ANY);
        }
        setState(691);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(692);
        match(ClickHouseParser::INNER);
        setState(694);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(693);
          match(ClickHouseParser::ANY);
        }
        break;
      }

      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpLeftRightContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(706);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx)) {
      case 1: {
        setState(699);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(698);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

          || _la == ClickHouseParser::SEMI)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(701);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case 2: {
        setState(702);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(704);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(703);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

          || _la == ClickHouseParser::SEMI)) {
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
      setState(716);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 94, _ctx)) {
      case 1: {
        setState(709);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(708);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(711);
        match(ClickHouseParser::FULL);
        break;
      }

      case 2: {
        setState(712);
        match(ClickHouseParser::FULL);
        setState(714);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(713);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER)) {
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
  enterRule(_localctx, 90, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(726);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(721);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(720);
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
        setState(723);
        match(ClickHouseParser::CROSS);
        setState(724);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(725);
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
  enterRule(_localctx, 92, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(737);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 98, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(728);
      match(ClickHouseParser::ON);
      setState(729);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(730);
      match(ClickHouseParser::USING);
      setState(731);
      match(ClickHouseParser::LPAREN);
      setState(732);
      columnExprList();
      setState(733);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(735);
      match(ClickHouseParser::USING);
      setState(736);
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

//----------------- LimitExprContext ------------------------------------------------------------------

ClickHouseParser::LimitExprContext::LimitExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> ClickHouseParser::LimitExprContext::INTEGER_LITERAL() {
  return getTokens(ClickHouseParser::INTEGER_LITERAL);
}

tree::TerminalNode* ClickHouseParser::LimitExprContext::INTEGER_LITERAL(size_t i) {
  return getToken(ClickHouseParser::INTEGER_LITERAL, i);
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
  enterRule(_localctx, 94, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(739);
    match(ClickHouseParser::INTEGER_LITERAL);
    setState(742);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET

    || _la == ClickHouseParser::COMMA) {
      setState(740);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET

      || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(741);
      match(ClickHouseParser::INTEGER_LITERAL);
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
  enterRule(_localctx, 96, ClickHouseParser::RuleOrderExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(744);
    orderExpr();
    setState(749);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(745);
      match(ClickHouseParser::COMMA);
      setState(746);
      orderExpr();
      setState(751);
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
  enterRule(_localctx, 98, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(752);
    columnExpr(0);
    setState(754);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::DESC)
      | (1ULL << ClickHouseParser::DESCENDING))) != 0)) {
      setState(753);
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
    }
    setState(758);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::NULLS) {
      setState(756);
      match(ClickHouseParser::NULLS);
      setState(757);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::FIRST

      || _la == ClickHouseParser::LAST)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(762);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::COLLATE) {
      setState(760);
      match(ClickHouseParser::COLLATE);
      setState(761);
      match(ClickHouseParser::STRING_LITERAL);
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
  enterRule(_localctx, 100, ClickHouseParser::RuleRatioExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(764);
    numberLiteral();
    setState(767);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SLASH) {
      setState(765);
      match(ClickHouseParser::SLASH);
      setState(766);
      numberLiteral();
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
  enterRule(_localctx, 102, ClickHouseParser::RuleSettingExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(769);
    settingExpr();
    setState(774);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(770);
      match(ClickHouseParser::COMMA);
      setState(771);
      settingExpr();
      setState(776);
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
  enterRule(_localctx, 104, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(777);
    identifier();
    setState(778);
    match(ClickHouseParser::EQ_SINGLE);
    setState(779);
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
  enterRule(_localctx, 106, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(781);
    match(ClickHouseParser::SET);
    setState(782);
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

//----------------- ShowCreateTableStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::SHOW() {
  return getToken(ClickHouseParser::SHOW, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ShowCreateTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ShowCreateTableStmtContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
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
  enterRule(_localctx, 108, ClickHouseParser::RuleShowStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(808);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 111, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(784);
      match(ClickHouseParser::SHOW);
      setState(785);
      match(ClickHouseParser::CREATE);
      setState(787);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(786);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(789);
      match(ClickHouseParser::TABLE);
      setState(790);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowTablesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(791);
      match(ClickHouseParser::SHOW);
      setState(793);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(792);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(795);
      match(ClickHouseParser::TABLES);
      setState(798);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM

      || _la == ClickHouseParser::IN) {
        setState(796);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::FROM

        || _la == ClickHouseParser::IN)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(797);
        databaseIdentifier();
      }
      setState(803);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::LIKE: {
          setState(800);
          match(ClickHouseParser::LIKE);
          setState(801);
          match(ClickHouseParser::STRING_LITERAL);
          break;
        }

        case ClickHouseParser::WHERE: {
          setState(802);
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
      setState(806);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::LIMIT) {
        setState(805);
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


size_t ClickHouseParser::SystemStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleSystemStmt;
}

void ClickHouseParser::SystemStmtContext::copyFrom(SystemStmtContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SystemSyncStmtContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SystemSyncStmtContext::SYSTEM() {
  return getToken(ClickHouseParser::SYSTEM, 0);
}

tree::TerminalNode* ClickHouseParser::SystemSyncStmtContext::SYNC() {
  return getToken(ClickHouseParser::SYNC, 0);
}

tree::TerminalNode* ClickHouseParser::SystemSyncStmtContext::REPLICA() {
  return getToken(ClickHouseParser::REPLICA, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SystemSyncStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SystemSyncStmtContext::SystemSyncStmtContext(SystemStmtContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::SystemSyncStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSystemSyncStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::SystemStmtContext* ClickHouseParser::systemStmt() {
  SystemStmtContext *_localctx = _tracker.createInstance<SystemStmtContext>(_ctx, getState());
  enterRule(_localctx, 110, ClickHouseParser::RuleSystemStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<SystemStmtContext *>(_tracker.createInstance<ClickHouseParser::SystemSyncStmtContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(810);
    match(ClickHouseParser::SYSTEM);
    setState(811);
    match(ClickHouseParser::SYNC);
    setState(812);
    match(ClickHouseParser::REPLICA);
    setState(813);
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
  enterRule(_localctx, 112, ClickHouseParser::RuleUseStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(815);
    match(ClickHouseParser::USE);
    setState(816);
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

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ColumnTypeExprParamContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnTypeExprParamContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
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
  enterRule(_localctx, 114, ClickHouseParser::RuleColumnTypeExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(863);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprSimpleContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(818);
      identifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprParamContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(819);
      identifier();
      setState(820);
      match(ClickHouseParser::LPAREN);
      setState(821);
      columnExprList();
      setState(822);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprEnumContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(824);
      identifier();
      setState(825);
      match(ClickHouseParser::LPAREN);
      setState(826);
      enumValue();
      setState(831);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(827);
        match(ClickHouseParser::COMMA);
        setState(828);
        enumValue();
        setState(833);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(834);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprComplexContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(836);
      identifier();
      setState(837);
      match(ClickHouseParser::LPAREN);
      setState(838);
      columnTypeExpr();
      setState(843);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(839);
        match(ClickHouseParser::COMMA);
        setState(840);
        columnTypeExpr();
        setState(845);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(846);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprNestedContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(848);
      identifier();
      setState(849);
      match(ClickHouseParser::LPAREN);
      setState(850);
      identifier();
      setState(851);
      columnTypeExpr();
      setState(858);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(852);
        match(ClickHouseParser::COMMA);
        setState(853);
        identifier();
        setState(854);
        columnTypeExpr();
        setState(860);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(861);
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
  enterRule(_localctx, 116, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(865);
    columnsExpr();
    setState(870);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(866);
        match(ClickHouseParser::COMMA);
        setState(867);
        columnsExpr(); 
      }
      setState(872);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx);
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
  enterRule(_localctx, 118, ClickHouseParser::RuleColumnsExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(884);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 118, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprAsteriskContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(876);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64)))) != 0)) {
        setState(873);
        tableIdentifier();
        setState(874);
        match(ClickHouseParser::DOT);
      }
      setState(878);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprSubqueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(879);
      match(ClickHouseParser::LPAREN);
      setState(880);
      selectUnionStmt();
      setState(881);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(883);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnExprAliasContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprAliasContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
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

tree::TerminalNode* ClickHouseParser::ColumnExprExtractContext::INTERVAL_TYPE() {
  return getToken(ClickHouseParser::INTERVAL_TYPE, 0);
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
//----------------- ColumnExprUnaryOpContext ------------------------------------------------------------------

ClickHouseParser::UnaryOpContext* ClickHouseParser::ColumnExprUnaryOpContext::unaryOp() {
  return getRuleContext<ClickHouseParser::UnaryOpContext>(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprUnaryOpContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::ColumnExprUnaryOpContext::ColumnExprUnaryOpContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ColumnExprUnaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprUnaryOp(this);
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

tree::TerminalNode* ClickHouseParser::ColumnExprTupleAccessContext::INTEGER_LITERAL() {
  return getToken(ClickHouseParser::INTEGER_LITERAL, 0);
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
//----------------- ColumnExprIntervalContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprIntervalContext::INTERVAL() {
  return getToken(ClickHouseParser::INTERVAL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprIntervalContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnExprIntervalContext::INTERVAL_TYPE() {
  return getToken(ClickHouseParser::INTERVAL_TYPE, 0);
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
//----------------- ColumnExprBinaryOpContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprBinaryOpContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprBinaryOpContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

ClickHouseParser::BinaryOpContext* ClickHouseParser::ColumnExprBinaryOpContext::binaryOp() {
  return getRuleContext<ClickHouseParser::BinaryOpContext>(0);
}

ClickHouseParser::ColumnExprBinaryOpContext::ColumnExprBinaryOpContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ColumnExprBinaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprBinaryOp(this);
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
  size_t startState = 120;
  enterRecursionRule(_localctx, 120, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(974);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 127, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(887);
      match(ClickHouseParser::CASE);
      setState(889);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 119, _ctx)) {
      case 1: {
        setState(888);
        columnExpr(0);
        break;
      }

      }
      setState(896); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(891);
        match(ClickHouseParser::WHEN);
        setState(892);
        columnExpr(0);
        setState(893);
        match(ClickHouseParser::THEN);
        setState(894);
        columnExpr(0);
        setState(898); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(902);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(900);
        match(ClickHouseParser::ELSE);
        setState(901);
        columnExpr(0);
      }
      setState(904);
      match(ClickHouseParser::END);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprCastContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(906);
      match(ClickHouseParser::CAST);
      setState(907);
      match(ClickHouseParser::LPAREN);
      setState(908);
      columnExpr(0);
      setState(909);
      match(ClickHouseParser::AS);
      setState(910);
      columnTypeExpr();
      setState(911);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(913);
      match(ClickHouseParser::EXTRACT);
      setState(914);
      match(ClickHouseParser::LPAREN);
      setState(915);
      match(ClickHouseParser::INTERVAL_TYPE);
      setState(916);
      match(ClickHouseParser::FROM);
      setState(917);
      columnExpr(0);
      setState(918);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(920);
      match(ClickHouseParser::INTERVAL);
      setState(921);
      columnExpr(0);
      setState(922);
      match(ClickHouseParser::INTERVAL_TYPE);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(924);
      match(ClickHouseParser::TRIM);
      setState(925);
      match(ClickHouseParser::LPAREN);
      setState(926);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::BOTH || _la == ClickHouseParser::LEADING

      || _la == ClickHouseParser::TRAILING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(927);
      match(ClickHouseParser::STRING_LITERAL);
      setState(928);
      match(ClickHouseParser::FROM);
      setState(929);
      columnExpr(0);
      setState(930);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(932);
      identifier();
      setState(938);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 123, _ctx)) {
      case 1: {
        setState(933);
        match(ClickHouseParser::LPAREN);
        setState(935);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
          | (1ULL << ClickHouseParser::AFTER)
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
          | (1ULL << ClickHouseParser::COLLATE)
          | (1ULL << ClickHouseParser::COMMENT)
          | (1ULL << ClickHouseParser::CREATE)
          | (1ULL << ClickHouseParser::CROSS)
          | (1ULL << ClickHouseParser::DATABASE)
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
          | (1ULL << ClickHouseParser::DROP)
          | (1ULL << ClickHouseParser::ELSE)
          | (1ULL << ClickHouseParser::END)
          | (1ULL << ClickHouseParser::ENGINE)
          | (1ULL << ClickHouseParser::EXISTS)
          | (1ULL << ClickHouseParser::EXTRACT)
          | (1ULL << ClickHouseParser::FINAL)
          | (1ULL << ClickHouseParser::FIRST)
          | (1ULL << ClickHouseParser::FORMAT)
          | (1ULL << ClickHouseParser::FULL)
          | (1ULL << ClickHouseParser::GLOBAL)
          | (1ULL << ClickHouseParser::GROUP)
          | (1ULL << ClickHouseParser::HAVING)
          | (1ULL << ClickHouseParser::HOUR)
          | (1ULL << ClickHouseParser::ID)
          | (1ULL << ClickHouseParser::IF)
          | (1ULL << ClickHouseParser::IN)
          | (1ULL << ClickHouseParser::INF)
          | (1ULL << ClickHouseParser::INNER)
          | (1ULL << ClickHouseParser::INSERT)
          | (1ULL << ClickHouseParser::INTERVAL)
          | (1ULL << ClickHouseParser::INTO)
          | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
          | (1ULL << (ClickHouseParser::KEY - 64))
          | (1ULL << (ClickHouseParser::LAST - 64))
          | (1ULL << (ClickHouseParser::LEADING - 64))
          | (1ULL << (ClickHouseParser::LEFT - 64))
          | (1ULL << (ClickHouseParser::LIKE - 64))
          | (1ULL << (ClickHouseParser::LIMIT - 64))
          | (1ULL << (ClickHouseParser::LOCAL - 64))
          | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
          | (1ULL << (ClickHouseParser::PREWHERE - 64))
          | (1ULL << (ClickHouseParser::PRIMARY - 64))
          | (1ULL << (ClickHouseParser::QUARTER - 64))
          | (1ULL << (ClickHouseParser::RENAME - 64))
          | (1ULL << (ClickHouseParser::REPLICA - 64))
          | (1ULL << (ClickHouseParser::RIGHT - 64))
          | (1ULL << (ClickHouseParser::SAMPLE - 64))
          | (1ULL << (ClickHouseParser::SECOND - 64))
          | (1ULL << (ClickHouseParser::SEMI - 64))
          | (1ULL << (ClickHouseParser::SET - 64))
          | (1ULL << (ClickHouseParser::SETTINGS - 64))
          | (1ULL << (ClickHouseParser::SHOW - 64))
          | (1ULL << (ClickHouseParser::SYNC - 64))
          | (1ULL << (ClickHouseParser::SYSTEM - 64))
          | (1ULL << (ClickHouseParser::TABLE - 64))
          | (1ULL << (ClickHouseParser::TABLES - 64))
          | (1ULL << (ClickHouseParser::TEMPORARY - 64))
          | (1ULL << (ClickHouseParser::THEN - 64))
          | (1ULL << (ClickHouseParser::TIES - 64))
          | (1ULL << (ClickHouseParser::TO - 64))
          | (1ULL << (ClickHouseParser::TOTALS - 64))
          | (1ULL << (ClickHouseParser::TRAILING - 64))
          | (1ULL << (ClickHouseParser::TRIM - 64))
          | (1ULL << (ClickHouseParser::TTL - 64))
          | (1ULL << (ClickHouseParser::UNION - 64))
          | (1ULL << (ClickHouseParser::USE - 64))
          | (1ULL << (ClickHouseParser::VALUES - 64))
          | (1ULL << (ClickHouseParser::VIEW - 64))
          | (1ULL << (ClickHouseParser::VOLUME - 64))
          | (1ULL << (ClickHouseParser::WEEK - 64))
          | (1ULL << (ClickHouseParser::WHEN - 64))
          | (1ULL << (ClickHouseParser::WITH - 64))
          | (1ULL << (ClickHouseParser::YEAR - 64))
          | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
          | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::ASTERISK - 128))
          | (1ULL << (ClickHouseParser::DASH - 128))
          | (1ULL << (ClickHouseParser::DOT - 128))
          | (1ULL << (ClickHouseParser::LBRACKET - 128))
          | (1ULL << (ClickHouseParser::LPAREN - 128))
          | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
          setState(934);
          columnExprList();
        }
        setState(937);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(940);
      match(ClickHouseParser::LPAREN);
      setState(942);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(941);
        columnArgList();
      }
      setState(944);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprUnaryOpContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(946);
      unaryOp();
      setState(947);
      columnExpr(13);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(952);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64)))) != 0)) {
        setState(949);
        tableIdentifier();
        setState(950);
        match(ClickHouseParser::DOT);
      }
      setState(954);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(955);
      match(ClickHouseParser::LPAREN);
      setState(956);
      selectUnionStmt();
      setState(957);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(959);
      match(ClickHouseParser::LPAREN);
      setState(960);
      columnExpr(0);
      setState(961);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(963);
      match(ClickHouseParser::LPAREN);
      setState(964);
      columnExprList();
      setState(965);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 12: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(967);
      match(ClickHouseParser::LBRACKET);
      setState(969);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(968);
        columnExprList();
      }
      setState(971);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 13: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(972);
      columnIdentifier();
      break;
    }

    case 14: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(973);
      literal();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1016);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 132, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1014);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 131, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprBinaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(976);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(977);
          binaryOp();
          setState(978);
          columnExpr(12);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(980);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(981);
          match(ClickHouseParser::QUERY);
          setState(982);
          columnExpr(0);
          setState(983);
          match(ClickHouseParser::COLON);
          setState(984);
          columnExpr(11);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(986);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(988);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(987);
            match(ClickHouseParser::NOT);
          }
          setState(990);
          match(ClickHouseParser::BETWEEN);
          setState(991);
          columnExpr(0);
          setState(992);
          match(ClickHouseParser::AND);
          setState(993);
          columnExpr(10);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(995);

          if (!(precpred(_ctx, 15))) throw FailedPredicateException(this, "precpred(_ctx, 15)");
          setState(996);
          match(ClickHouseParser::LBRACKET);
          setState(997);
          columnExpr(0);
          setState(998);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1000);

          if (!(precpred(_ctx, 14))) throw FailedPredicateException(this, "precpred(_ctx, 14)");
          setState(1001);
          match(ClickHouseParser::DOT);
          setState(1002);
          match(ClickHouseParser::INTEGER_LITERAL);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1003);

          if (!(precpred(_ctx, 12))) throw FailedPredicateException(this, "precpred(_ctx, 12)");
          setState(1004);
          match(ClickHouseParser::IS);
          setState(1006);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1005);
            match(ClickHouseParser::NOT);
          }
          setState(1008);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1009);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(1011);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 130, _ctx)) {
          case 1: {
            setState(1010);
            match(ClickHouseParser::AS);
            break;
          }

          }
          setState(1013);
          identifier();
          break;
        }

        } 
      }
      setState(1018);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 132, _ctx);
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
  enterRule(_localctx, 122, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1019);
    columnArgExpr();
    setState(1024);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1020);
      match(ClickHouseParser::COMMA);
      setState(1021);
      columnArgExpr();
      setState(1026);
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
  enterRule(_localctx, 124, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1029);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 134, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1027);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1028);
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
  enterRule(_localctx, 126, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1050);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(1031);
        match(ClickHouseParser::LPAREN);
        setState(1032);
        identifier();
        setState(1037);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1033);
          match(ClickHouseParser::COMMA);
          setState(1034);
          identifier();
          setState(1039);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1040);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
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
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
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
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
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
      case ClickHouseParser::MATERIALIZED:
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
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        setState(1042);
        identifier();
        setState(1047);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1043);
          match(ClickHouseParser::COMMA);
          setState(1044);
          identifier();
          setState(1049);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(1052);
    match(ClickHouseParser::ARROW);
    setState(1053);
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
  enterRule(_localctx, 128, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1058);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 138, _ctx)) {
    case 1: {
      setState(1055);
      tableIdentifier();
      setState(1056);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1060);
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
  enterRule(_localctx, 130, ClickHouseParser::RuleNestedIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1062);
    identifier();
    setState(1065);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 139, _ctx)) {
    case 1: {
      setState(1063);
      match(ClickHouseParser::DOT);
      setState(1064);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::TableExprAliasContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableExprAliasContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::TableExprAliasContext::TableExprAliasContext(TableExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TableExprAliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableExprAlias(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableExprFunctionContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::TableExprFunctionContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableExprFunctionContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::TableExprFunctionContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::TableArgListContext* ClickHouseParser::TableExprFunctionContext::tableArgList() {
  return getRuleContext<ClickHouseParser::TableArgListContext>(0);
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
  size_t startState = 132;
  enterRecursionRule(_localctx, 132, ClickHouseParser::RuleTableExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1080);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 141, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1068);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1069);
      identifier();
      setState(1070);
      match(ClickHouseParser::LPAREN);
      setState(1072);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
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
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::COMMENT)
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
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
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::ENGINE)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
        | (1ULL << (ClickHouseParser::KEY - 64))
        | (1ULL << (ClickHouseParser::LAST - 64))
        | (1ULL << (ClickHouseParser::LEADING - 64))
        | (1ULL << (ClickHouseParser::LEFT - 64))
        | (1ULL << (ClickHouseParser::LIKE - 64))
        | (1ULL << (ClickHouseParser::LIMIT - 64))
        | (1ULL << (ClickHouseParser::LOCAL - 64))
        | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::SYNC - 64))
        | (1ULL << (ClickHouseParser::SYSTEM - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TABLES - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TIES - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VIEW - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1071);
        tableArgList();
      }
      setState(1074);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1076);
      match(ClickHouseParser::LPAREN);
      setState(1077);
      selectUnionStmt();
      setState(1078);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1089);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 143, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(1082);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(1084);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 142, _ctx)) {
        case 1: {
          setState(1083);
          match(ClickHouseParser::AS);
          break;
        }

        }
        setState(1086);
        identifier(); 
      }
      setState(1091);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 143, _ctx);
    }
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
  enterRule(_localctx, 134, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1095);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 144, _ctx)) {
    case 1: {
      setState(1092);
      databaseIdentifier();
      setState(1093);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1097);
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
  enterRule(_localctx, 136, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1099);
    tableArgExpr();
    setState(1104);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1100);
      match(ClickHouseParser::COMMA);
      setState(1101);
      tableArgExpr();
      setState(1106);
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
  enterRule(_localctx, 138, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1109);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 146, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1107);
      tableIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1108);
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
  enterRule(_localctx, 140, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1111);
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

std::vector<tree::TerminalNode *> ClickHouseParser::FloatingLiteralContext::INTEGER_LITERAL() {
  return getTokens(ClickHouseParser::INTEGER_LITERAL);
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::INTEGER_LITERAL(size_t i) {
  return getToken(ClickHouseParser::INTEGER_LITERAL, i);
}

tree::TerminalNode* ClickHouseParser::FloatingLiteralContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
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
  enterRule(_localctx, 142, ClickHouseParser::RuleFloatingLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1121);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FLOATING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(1113);
        match(ClickHouseParser::FLOATING_LITERAL);
        break;
      }

      case ClickHouseParser::INTEGER_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1114);
        match(ClickHouseParser::INTEGER_LITERAL);
        setState(1115);
        match(ClickHouseParser::DOT);
        setState(1117);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 147, _ctx)) {
        case 1: {
          setState(1116);
          match(ClickHouseParser::INTEGER_LITERAL);
          break;
        }

        }
        break;
      }

      case ClickHouseParser::DOT: {
        enterOuterAlt(_localctx, 3);
        setState(1119);
        match(ClickHouseParser::DOT);
        setState(1120);
        match(ClickHouseParser::INTEGER_LITERAL);
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

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::HEXADECIMAL_LITERAL() {
  return getToken(ClickHouseParser::HEXADECIMAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::NumberLiteralContext::INTEGER_LITERAL() {
  return getToken(ClickHouseParser::INTEGER_LITERAL, 0);
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
  enterRule(_localctx, 144, ClickHouseParser::RuleNumberLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1124);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DASH

    || _la == ClickHouseParser::PLUS) {
      setState(1123);
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
    setState(1131);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 150, _ctx)) {
    case 1: {
      setState(1126);
      floatingLiteral();
      break;
    }

    case 2: {
      setState(1127);
      match(ClickHouseParser::HEXADECIMAL_LITERAL);
      break;
    }

    case 3: {
      setState(1128);
      match(ClickHouseParser::INTEGER_LITERAL);
      break;
    }

    case 4: {
      setState(1129);
      match(ClickHouseParser::INF);
      break;
    }

    case 5: {
      setState(1130);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::LiteralContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
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
  enterRule(_localctx, 146, ClickHouseParser::RuleLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1140);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::INTEGER_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::DOT:
      case ClickHouseParser::PLUS: {
        enterOuterAlt(_localctx, 1);
        setState(1133);
        numberLiteral();
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1134);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 3);
        setState(1135);
        match(ClickHouseParser::NULL_SQL);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
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
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
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
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
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
      case ClickHouseParser::MATERIALIZED:
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
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(1136);
        identifier();
        setState(1137);
        match(ClickHouseParser::LPAREN);
        setState(1138);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::COLLATE() {
  return getToken(ClickHouseParser::COLLATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CROSS() {
  return getToken(ClickHouseParser::CROSS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::DESC() {
  return getToken(ClickHouseParser::DESC, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCENDING() {
  return getToken(ClickHouseParser::DESCENDING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCRIBE() {
  return getToken(ClickHouseParser::DESCRIBE, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::EXTRACT() {
  return getToken(ClickHouseParser::EXTRACT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FINAL() {
  return getToken(ClickHouseParser::FINAL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FIRST() {
  return getToken(ClickHouseParser::FIRST, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FORMAT() {
  return getToken(ClickHouseParser::FORMAT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::FULL() {
  return getToken(ClickHouseParser::FULL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::MATERIALIZED() {
  return getToken(ClickHouseParser::MATERIALIZED, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::PREWHERE() {
  return getToken(ClickHouseParser::PREWHERE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::PRIMARY() {
  return getToken(ClickHouseParser::PRIMARY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::QUARTER() {
  return getToken(ClickHouseParser::QUARTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RENAME() {
  return getToken(ClickHouseParser::RENAME, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::REPLICA() {
  return getToken(ClickHouseParser::REPLICA, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::RIGHT() {
  return getToken(ClickHouseParser::RIGHT, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SAMPLE() {
  return getToken(ClickHouseParser::SAMPLE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SECOND() {
  return getToken(ClickHouseParser::SECOND, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::SEMI() {
  return getToken(ClickHouseParser::SEMI, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::SYNC() {
  return getToken(ClickHouseParser::SYNC, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::TOTALS() {
  return getToken(ClickHouseParser::TOTALS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TRAILING() {
  return getToken(ClickHouseParser::TRAILING, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TRIM() {
  return getToken(ClickHouseParser::TRIM, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TO() {
  return getToken(ClickHouseParser::TO, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::UNION() {
  return getToken(ClickHouseParser::UNION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USE() {
  return getToken(ClickHouseParser::USE, 0);
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
  enterRule(_localctx, 148, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1142);
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
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::COMMENT)
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::DATABASE)
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
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::ENGINE)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GROUP)
      | (1ULL << ClickHouseParser::HAVING)
      | (1ULL << ClickHouseParser::HOUR)
      | (1ULL << ClickHouseParser::ID)
      | (1ULL << ClickHouseParser::IF)
      | (1ULL << ClickHouseParser::IN)
      | (1ULL << ClickHouseParser::INNER)
      | (1ULL << ClickHouseParser::INSERT)
      | (1ULL << ClickHouseParser::INTERVAL)
      | (1ULL << ClickHouseParser::INTO)
      | (1ULL << ClickHouseParser::IS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::JOIN - 64))
      | (1ULL << (ClickHouseParser::KEY - 64))
      | (1ULL << (ClickHouseParser::LAST - 64))
      | (1ULL << (ClickHouseParser::LEADING - 64))
      | (1ULL << (ClickHouseParser::LEFT - 64))
      | (1ULL << (ClickHouseParser::LIKE - 64))
      | (1ULL << (ClickHouseParser::LIMIT - 64))
      | (1ULL << (ClickHouseParser::LOCAL - 64))
      | (1ULL << (ClickHouseParser::MATERIALIZED - 64))
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
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64))
      | (1ULL << (ClickHouseParser::RENAME - 64))
      | (1ULL << (ClickHouseParser::REPLICA - 64))
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::SHOW - 64))
      | (1ULL << (ClickHouseParser::SYNC - 64))
      | (1ULL << (ClickHouseParser::SYSTEM - 64))
      | (1ULL << (ClickHouseParser::TABLE - 64))
      | (1ULL << (ClickHouseParser::TABLES - 64))
      | (1ULL << (ClickHouseParser::TEMPORARY - 64))
      | (1ULL << (ClickHouseParser::THEN - 64))
      | (1ULL << (ClickHouseParser::TIES - 64))
      | (1ULL << (ClickHouseParser::TO - 64))
      | (1ULL << (ClickHouseParser::TOTALS - 64))
      | (1ULL << (ClickHouseParser::TRAILING - 64))
      | (1ULL << (ClickHouseParser::TRIM - 64))
      | (1ULL << (ClickHouseParser::TTL - 64))
      | (1ULL << (ClickHouseParser::UNION - 64))
      | (1ULL << (ClickHouseParser::USE - 64))
      | (1ULL << (ClickHouseParser::VALUES - 64))
      | (1ULL << (ClickHouseParser::VIEW - 64))
      | (1ULL << (ClickHouseParser::VOLUME - 64))
      | (1ULL << (ClickHouseParser::WEEK - 64))
      | (1ULL << (ClickHouseParser::WHEN - 64))
      | (1ULL << (ClickHouseParser::WITH - 64))
      | (1ULL << (ClickHouseParser::YEAR - 64)))) != 0))) {
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

//----------------- IdentifierContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext::IdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::IdentifierContext::IDENTIFIER() {
  return getToken(ClickHouseParser::IDENTIFIER, 0);
}

tree::TerminalNode* ClickHouseParser::IdentifierContext::INTERVAL_TYPE() {
  return getToken(ClickHouseParser::INTERVAL_TYPE, 0);
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
  enterRule(_localctx, 150, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1147);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1144);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE: {
        enterOuterAlt(_localctx, 2);
        setState(1145);
        match(ClickHouseParser::INTERVAL_TYPE);
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
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
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
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
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
      case ClickHouseParser::MATERIALIZED:
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
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR: {
        enterOuterAlt(_localctx, 3);
        setState(1146);
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
  enterRule(_localctx, 152, ClickHouseParser::RuleIdentifierOrNull);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1151);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INTERVAL_TYPE:
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
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::COMMENT:
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
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
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::ENGINE:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::ID:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
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
      case ClickHouseParser::MATERIALIZED:
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
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::SYNC:
      case ClickHouseParser::SYSTEM:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TABLES:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TIES:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VIEW:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1149);
        identifier();
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 2);
        setState(1150);
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

//----------------- UnaryOpContext ------------------------------------------------------------------

ClickHouseParser::UnaryOpContext::UnaryOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::UnaryOpContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}

tree::TerminalNode* ClickHouseParser::UnaryOpContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}


size_t ClickHouseParser::UnaryOpContext::getRuleIndex() const {
  return ClickHouseParser::RuleUnaryOp;
}


antlrcpp::Any ClickHouseParser::UnaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitUnaryOp(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::UnaryOpContext* ClickHouseParser::unaryOp() {
  UnaryOpContext *_localctx = _tracker.createInstance<UnaryOpContext>(_ctx, getState());
  enterRule(_localctx, 154, ClickHouseParser::RuleUnaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1153);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::NOT

    || _la == ClickHouseParser::DASH)) {
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

//----------------- BinaryOpContext ------------------------------------------------------------------

ClickHouseParser::BinaryOpContext::BinaryOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::CONCAT() {
  return getToken(ClickHouseParser::CONCAT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::SLASH() {
  return getToken(ClickHouseParser::SLASH, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::PLUS() {
  return getToken(ClickHouseParser::PLUS, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::PERCENT() {
  return getToken(ClickHouseParser::PERCENT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::EQ_DOUBLE() {
  return getToken(ClickHouseParser::EQ_DOUBLE, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::EQ_SINGLE() {
  return getToken(ClickHouseParser::EQ_SINGLE, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::NOT_EQ() {
  return getToken(ClickHouseParser::NOT_EQ, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::LE() {
  return getToken(ClickHouseParser::LE, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::GE() {
  return getToken(ClickHouseParser::GE, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::LT() {
  return getToken(ClickHouseParser::LT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::GT() {
  return getToken(ClickHouseParser::GT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::AND() {
  return getToken(ClickHouseParser::AND, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::OR() {
  return getToken(ClickHouseParser::OR, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::LIKE() {
  return getToken(ClickHouseParser::LIKE, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::GLOBAL() {
  return getToken(ClickHouseParser::GLOBAL, 0);
}


size_t ClickHouseParser::BinaryOpContext::getRuleIndex() const {
  return ClickHouseParser::RuleBinaryOp;
}


antlrcpp::Any ClickHouseParser::BinaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitBinaryOp(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::BinaryOpContext* ClickHouseParser::binaryOp() {
  BinaryOpContext *_localctx = _tracker.createInstance<BinaryOpContext>(_ctx, getState());
  enterRule(_localctx, 156, ClickHouseParser::RuleBinaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1181);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 157, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1155);
      match(ClickHouseParser::CONCAT);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1156);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1157);
      match(ClickHouseParser::SLASH);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(1158);
      match(ClickHouseParser::PLUS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(1159);
      match(ClickHouseParser::DASH);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(1160);
      match(ClickHouseParser::PERCENT);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(1161);
      match(ClickHouseParser::EQ_DOUBLE);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(1162);
      match(ClickHouseParser::EQ_SINGLE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(1163);
      match(ClickHouseParser::NOT_EQ);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(1164);
      match(ClickHouseParser::LE);
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(1165);
      match(ClickHouseParser::GE);
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(1166);
      match(ClickHouseParser::LT);
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(1167);
      match(ClickHouseParser::GT);
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(1168);
      match(ClickHouseParser::AND);
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(1169);
      match(ClickHouseParser::OR);
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(1171);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(1170);
        match(ClickHouseParser::NOT);
      }
      setState(1173);
      match(ClickHouseParser::LIKE);
      break;
    }

    case 17: {
      enterOuterAlt(_localctx, 17);
      setState(1175);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::GLOBAL) {
        setState(1174);
        match(ClickHouseParser::GLOBAL);
      }
      setState(1178);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(1177);
        match(ClickHouseParser::NOT);
      }
      setState(1180);
      match(ClickHouseParser::IN);
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
  enterRule(_localctx, 158, ClickHouseParser::RuleEnumValue);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1183);
    match(ClickHouseParser::STRING_LITERAL);
    setState(1184);
    match(ClickHouseParser::EQ_SINGLE);
    setState(1185);
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
    case 43: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 60: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 66: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::joinExprSempred(JoinExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 1);
    case 1: return precpred(_ctx, 2);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 2: return precpred(_ctx, 11);
    case 3: return precpred(_ctx, 10);
    case 4: return precpred(_ctx, 9);
    case 5: return precpred(_ctx, 15);
    case 6: return precpred(_ctx, 14);
    case 7: return precpred(_ctx, 12);
    case 8: return precpred(_ctx, 8);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::tableExprSempred(TableExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 9: return precpred(_ctx, 1);

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
  "queryList", "queryStmt", "query", "alterStmt", "alterTableClause", "alterPartitionClause", 
  "checkStmt", "createStmt", "subqueryClause", "schemaClause", "engineClause", 
  "partitionByClause", "primaryKeyClause", "sampleByClause", "ttlClause", 
  "engineExpr", "tableElementExpr", "tableColumnDfnt", "tableColumnPropertyExpr", 
  "ttlExpr", "describeStmt", "dropStmt", "existsStmt", "insertStmt", "valuesClause", 
  "valueTupleExpr", "optimizeStmt", "partitionClause", "renameStmt", "selectUnionStmt", 
  "selectStmt", "withClause", "fromClause", "sampleClause", "arrayJoinClause", 
  "prewhereClause", "whereClause", "groupByClause", "havingClause", "orderByClause", 
  "limitByClause", "limitClause", "settingsClause", "joinExpr", "joinOp", 
  "joinOpCross", "joinConstraintClause", "limitExpr", "orderExprList", "orderExpr", 
  "ratioExpr", "settingExprList", "settingExpr", "setStmt", "showStmt", 
  "systemStmt", "useStmt", "columnTypeExpr", "columnExprList", "columnsExpr", 
  "columnExpr", "columnArgList", "columnArgExpr", "columnLambdaExpr", "columnIdentifier", 
  "nestedIdentifier", "tableExpr", "tableIdentifier", "tableArgList", "tableArgExpr", 
  "databaseIdentifier", "floatingLiteral", "numberLiteral", "literal", "keyword", 
  "identifier", "identifierOrNull", "unaryOp", "binaryOp", "enumValue"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "'->'", "'*'", "'`'", "'\\'", "':'", "','", "'||'", 
  "'-'", "'.'", "'=='", "'='", "'>='", "'>'", "'['", "'<='", "'('", "'<'", 
  "", "'%'", "'+'", "'?'", "'\"'", "'''", "']'", "')'", "';'", "'/'", "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "INTERVAL_TYPE", "ADD", "AFTER", "ALIAS", "ALL", "ALTER", "AND", "ANTI", 
  "ANY", "ARRAY", "AS", "ASCENDING", "ASOF", "ATTACH", "BETWEEN", "BOTH", 
  "BY", "CASE", "CAST", "CHECK", "CLEAR", "CLUSTER", "COLLATE", "COLUMN", 
  "COMMENT", "CREATE", "CROSS", "DATABASE", "DAY", "DEDUPLICATE", "DEFAULT", 
  "DELAY", "DELETE", "DESC", "DESCENDING", "DESCRIBE", "DETACH", "DISK", 
  "DISTINCT", "DROP", "ELSE", "END", "ENGINE", "EXISTS", "EXTRACT", "FINAL", 
  "FIRST", "FORMAT", "FROM", "FULL", "GLOBAL", "GROUP", "HAVING", "HOUR", 
  "ID", "IF", "IN", "INF", "INNER", "INSERT", "INTERVAL", "INTO", "IS", 
  "JOIN", "KEY", "LAST", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCAL", "MATERIALIZED", 
  "MINUTE", "MODIFY", "MONTH", "NAN_SQL", "NO", "NOT", "NULL_SQL", "NULLS", 
  "OFFSET", "ON", "OPTIMIZE", "OR", "ORDER", "OUTER", "OUTFILE", "PARTITION", 
  "POPULATE", "PREWHERE", "PRIMARY", "QUARTER", "RENAME", "REPLICA", "RIGHT", 
  "SAMPLE", "SECOND", "SELECT", "SEMI", "SET", "SETTINGS", "SHOW", "SYNC", 
  "SYSTEM", "TABLE", "TABLES", "TEMPORARY", "THEN", "TIES", "TO", "TOTALS", 
  "TRAILING", "TRIM", "TTL", "UNION", "USE", "USING", "VALUES", "VIEW", 
  "VOLUME", "WEEK", "WHEN", "WHERE", "WITH", "YEAR", "IDENTIFIER", "FLOATING_LITERAL", 
  "HEXADECIMAL_LITERAL", "INTEGER_LITERAL", "STRING_LITERAL", "ARROW", "ASTERISK", 
  "BACKQUOTE", "BACKSLASH", "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ_DOUBLE", 
  "EQ_SINGLE", "GE", "GT", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", 
  "PLUS", "QUERY", "QUOTE_DOUBLE", "QUOTE_SINGLE", "RBRACKET", "RPAREN", 
  "SEMICOLON", "SLASH", "UNDERSCORE", "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", 
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
    0x3, 0xa3, 0x4a6, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x4, 0x50, 0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x3, 0x2, 0x3, 0x2, 0x3, 
    0x2, 0x7, 0x2, 0xa6, 0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 0xa9, 0xb, 0x2, 0x3, 
    0x2, 0x5, 0x2, 0xac, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0xb4, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 
    0x3, 0xb8, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0xc8, 0xa, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x7, 0x5, 0xd0, 0xa, 0x5, 0xc, 0x5, 
    0xe, 0x5, 0xd3, 0xb, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x3, 0x5, 0x7, 0x5, 0xdb, 0xa, 0x5, 0xc, 0x5, 0xe, 0x5, 0xde, 0xb, 
    0x5, 0x5, 0x5, 0xe0, 0xa, 0x5, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 
    0x3, 0x6, 0x5, 0x6, 0xe7, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 
    0x6, 0xec, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 
    0xf2, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 
    0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0xfc, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 
    0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x105, 0xa, 
    0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x10c, 
    0xa, 0x6, 0x3, 0x6, 0x5, 0x6, 0x10f, 0xa, 0x6, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x11d, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0x121, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x5, 0x9, 0x129, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 
    0x12d, 0xa, 0x9, 0x3, 0x9, 0x5, 0x9, 0x130, 0xa, 0x9, 0x3, 0x9, 0x5, 
    0x9, 0x133, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 
    0x139, 0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x13f, 
    0xa, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x143, 0xa, 0x9, 0x3, 0x9, 0x5, 
    0x9, 0x146, 0xa, 0x9, 0x3, 0x9, 0x5, 0x9, 0x149, 0xa, 0x9, 0x3, 0x9, 
    0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x150, 0xa, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0x155, 0xa, 0x9, 0x3, 0xa, 0x3, 0xa, 
    0x3, 0xa, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x7, 0xb, 0x15e, 0xa, 
    0xb, 0xc, 0xb, 0xe, 0xb, 0x161, 0xb, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 
    0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x16b, 0xa, 
    0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x16f, 0xa, 0xb, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x173, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x176, 0xa, 0xc, 0x3, 
    0xc, 0x5, 0xc, 0x179, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x17c, 0xa, 0xc, 
    0x3, 0xc, 0x5, 0xc, 0x17f, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x182, 0xa, 
    0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0x10, 0x3, 
    0x10, 0x3, 0x10, 0x3, 0x10, 0x7, 0x10, 0x194, 0xa, 0x10, 0xc, 0x10, 
    0xe, 0x10, 0x197, 0xb, 0x10, 0x3, 0x11, 0x3, 0x11, 0x5, 0x11, 0x19b, 
    0xa, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x5, 0x11, 0x1a0, 0xa, 0x11, 
    0x3, 0x11, 0x5, 0x11, 0x1a3, 0xa, 0x11, 0x3, 0x12, 0x3, 0x12, 0x3, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1aa, 0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 
    0x5, 0x13, 0x1ae, 0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1b2, 
    0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1b7, 0xa, 0x13, 
    0x5, 0x13, 0x1b9, 0xa, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x15, 
    0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 
    0x15, 0x5, 0x15, 0x1c6, 0xa, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x1d0, 
    0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x1d5, 0xa, 0x17, 
    0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x1da, 0xa, 0x17, 0x3, 0x17, 
    0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x1df, 0xa, 0x17, 0x5, 0x17, 0x1e1, 
    0xa, 0x17, 0x3, 0x18, 0x3, 0x18, 0x5, 0x18, 0x1e5, 0xa, 0x18, 0x3, 0x18, 
    0x3, 0x18, 0x3, 0x18, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 
    0x19, 0x3, 0x19, 0x3, 0x19, 0x7, 0x19, 0x1f1, 0xa, 0x19, 0xc, 0x19, 
    0xe, 0x19, 0x1f4, 0xb, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1f8, 
    0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x5, 
    0x1a, 0x1ff, 0xa, 0x1a, 0x3, 0x1a, 0x7, 0x1a, 0x202, 0xa, 0x1a, 0xc, 
    0x1a, 0xe, 0x1a, 0x205, 0xb, 0x1a, 0x3, 0x1a, 0x5, 0x1a, 0x208, 0xa, 
    0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x212, 0xa, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 
    0x215, 0xa, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x218, 0xa, 0x1c, 0x3, 0x1d, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x5, 0x1d, 0x21f, 0xa, 0x1d, 
    0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 
    0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x7, 0x1e, 0x22b, 0xa, 0x1e, 
    0xc, 0x1e, 0xe, 0x1e, 0x22e, 0xb, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x7, 0x1f, 0x234, 0xa, 0x1f, 0xc, 0x1f, 0xe, 0x1f, 0x237, 
    0xb, 0x1f, 0x3, 0x20, 0x5, 0x20, 0x23a, 0xa, 0x20, 0x3, 0x20, 0x3, 0x20, 
    0x5, 0x20, 0x23e, 0xa, 0x20, 0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x242, 
    0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x245, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 
    0x248, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x24b, 0xa, 0x20, 0x3, 0x20, 
    0x5, 0x20, 0x24e, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x251, 0xa, 0x20, 
    0x3, 0x20, 0x5, 0x20, 0x254, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x257, 
    0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x25a, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 
    0x25d, 0xa, 0x20, 0x3, 0x20, 0x5, 0x20, 0x260, 0xa, 0x20, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x5, 0x22, 0x268, 
    0xa, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x26e, 
    0xa, 0x23, 0x3, 0x24, 0x5, 0x24, 0x271, 0xa, 0x24, 0x3, 0x24, 0x3, 0x24, 
    0x3, 0x24, 0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 0x3, 
    0x26, 0x3, 0x26, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 
    0x5, 0x27, 0x282, 0xa, 0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 0x29, 
    0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x3, 
    0x2a, 0x3, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 
    0x294, 0xa, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 0x3, 0x2d, 
    0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 0x2d, 0x29f, 0xa, 0x2d, 
    0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x5, 
    0x2d, 0x2a7, 0xa, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 
    0x3, 0x2d, 0x7, 0x2d, 0x2ae, 0xa, 0x2d, 0xc, 0x2d, 0xe, 0x2d, 0x2b1, 
    0xb, 0x2d, 0x3, 0x2e, 0x5, 0x2e, 0x2b4, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 
    0x3, 0x2e, 0x5, 0x2e, 0x2b9, 0xa, 0x2e, 0x5, 0x2e, 0x2bb, 0xa, 0x2e, 
    0x3, 0x2e, 0x5, 0x2e, 0x2be, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 
    0x5, 0x2e, 0x2c3, 0xa, 0x2e, 0x5, 0x2e, 0x2c5, 0xa, 0x2e, 0x3, 0x2e, 
    0x5, 0x2e, 0x2c8, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 
    0x2cd, 0xa, 0x2e, 0x5, 0x2e, 0x2cf, 0xa, 0x2e, 0x5, 0x2e, 0x2d1, 0xa, 
    0x2e, 0x3, 0x2f, 0x5, 0x2f, 0x2d4, 0xa, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 
    0x3, 0x2f, 0x5, 0x2f, 0x2d9, 0xa, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 
    0x30, 0x2e4, 0xa, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x5, 0x31, 
    0x2e9, 0xa, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x7, 0x32, 0x2ee, 
    0xa, 0x32, 0xc, 0x32, 0xe, 0x32, 0x2f1, 0xb, 0x32, 0x3, 0x33, 0x3, 0x33, 
    0x5, 0x33, 0x2f5, 0xa, 0x33, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x2f9, 
    0xa, 0x33, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x2fd, 0xa, 0x33, 0x3, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x5, 0x34, 0x302, 0xa, 0x34, 0x3, 0x35, 0x3, 0x35, 
    0x3, 0x35, 0x7, 0x35, 0x307, 0xa, 0x35, 0xc, 0x35, 0xe, 0x35, 0x30a, 
    0xb, 0x35, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x316, 
    0xa, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x31c, 
    0xa, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x321, 0xa, 0x38, 
    0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x326, 0xa, 0x38, 0x3, 0x38, 
    0x5, 0x38, 0x329, 0xa, 0x38, 0x5, 0x38, 0x32b, 0xa, 0x38, 0x3, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x3, 0x39, 0x3, 0x39, 0x3, 0x3a, 0x3, 0x3a, 0x3, 
    0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x7, 0x3b, 0x340, 
    0xa, 0x3b, 0xc, 0x3b, 0xe, 0x3b, 0x343, 0xb, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x7, 0x3b, 0x34c, 
    0xa, 0x3b, 0xc, 0x3b, 0xe, 0x3b, 0x34f, 0xb, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 
    0x3b, 0x3, 0x3b, 0x7, 0x3b, 0x35b, 0xa, 0x3b, 0xc, 0x3b, 0xe, 0x3b, 
    0x35e, 0xb, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x362, 0xa, 0x3b, 
    0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x7, 0x3c, 0x367, 0xa, 0x3c, 0xc, 0x3c, 
    0xe, 0x3c, 0x36a, 0xb, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 
    0x36f, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 
    0x3, 0x3d, 0x5, 0x3d, 0x377, 0xa, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x5, 0x3e, 0x37c, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x6, 0x3e, 0x383, 0xa, 0x3e, 0xd, 0x3e, 0xe, 0x3e, 0x384, 
    0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x389, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3aa, 0xa, 0x3e, 0x3, 0x3e, 
    0x5, 0x3e, 0x3ad, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3b1, 
    0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3bb, 0xa, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x5, 0x3e, 0x3cc, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x5, 0x3e, 0x3d1, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3df, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
    0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x5, 0x3e, 0x3f1, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x5, 0x3e, 0x3f6, 0xa, 0x3e, 0x3, 0x3e, 0x7, 0x3e, 0x3f9, 0xa, 0x3e, 
    0xc, 0x3e, 0xe, 0x3e, 0x3fc, 0xb, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 
    0x7, 0x3f, 0x401, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 0x404, 0xb, 0x3f, 
    0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 0x408, 0xa, 0x40, 0x3, 0x41, 0x3, 0x41, 
    0x3, 0x41, 0x3, 0x41, 0x7, 0x41, 0x40e, 0xa, 0x41, 0xc, 0x41, 0xe, 0x41, 
    0x411, 0xb, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 
    0x7, 0x41, 0x418, 0xa, 0x41, 0xc, 0x41, 0xe, 0x41, 0x41b, 0xb, 0x41, 
    0x5, 0x41, 0x41d, 0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x425, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x5, 0x43, 0x42c, 0xa, 0x43, 0x3, 0x44, 
    0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x5, 0x44, 0x433, 0xa, 0x44, 
    0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x5, 
    0x44, 0x43b, 0xa, 0x44, 0x3, 0x44, 0x3, 0x44, 0x5, 0x44, 0x43f, 0xa, 
    0x44, 0x3, 0x44, 0x7, 0x44, 0x442, 0xa, 0x44, 0xc, 0x44, 0xe, 0x44, 
    0x445, 0xb, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x5, 0x45, 0x44a, 
    0xa, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x7, 
    0x46, 0x451, 0xa, 0x46, 0xc, 0x46, 0xe, 0x46, 0x454, 0xb, 0x46, 0x3, 
    0x47, 0x3, 0x47, 0x5, 0x47, 0x458, 0xa, 0x47, 0x3, 0x48, 0x3, 0x48, 
    0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x460, 0xa, 0x49, 
    0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x464, 0xa, 0x49, 0x3, 0x4a, 0x5, 0x4a, 
    0x467, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
    0x5, 0x4a, 0x46e, 0xa, 0x4a, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 
    0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x5, 0x4b, 0x477, 0xa, 0x4b, 0x3, 0x4c, 
    0x3, 0x4c, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x47e, 0xa, 0x4d, 
    0x3, 0x4e, 0x3, 0x4e, 0x5, 0x4e, 0x482, 0xa, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 
    0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 
    0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 
    0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x496, 0xa, 0x50, 0x3, 0x50, 
    0x3, 0x50, 0x5, 0x50, 0x49a, 0xa, 0x50, 0x3, 0x50, 0x5, 0x50, 0x49d, 
    0xa, 0x50, 0x3, 0x50, 0x5, 0x50, 0x4a0, 0xa, 0x50, 0x3, 0x51, 0x3, 0x51, 
    0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x2, 0x5, 0x58, 0x7a, 0x86, 0x52, 0x2, 
    0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 
    0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 
    0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 
    0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 
    0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 
    0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 
    0x96, 0x98, 0x9a, 0x9c, 0x9e, 0xa0, 0x2, 0x12, 0x4, 0x2, 0x10, 0x10, 
    0x1c, 0x1c, 0x5, 0x2, 0x6, 0x6, 0x21, 0x21, 0x4a, 0x4a, 0x4, 0x2, 0x24, 
    0x24, 0x26, 0x26, 0x4, 0x2, 0x27, 0x27, 0x2a, 0x2a, 0x4, 0x2, 0x35, 
    0x35, 0x49, 0x49, 0x6, 0x2, 0xa, 0xb, 0xf, 0xf, 0x58, 0x58, 0x65, 0x65, 
    0x4, 0x2, 0x46, 0x46, 0x61, 0x61, 0x4, 0x2, 0xb, 0xb, 0x58, 0x58, 0x4, 
    0x2, 0x53, 0x53, 0x8a, 0x8a, 0x4, 0x2, 0xe, 0xe, 0x24, 0x25, 0x4, 0x2, 
    0x31, 0x31, 0x44, 0x44, 0x4, 0x2, 0x33, 0x33, 0x3b, 0x3b, 0x5, 0x2, 
    0x12, 0x12, 0x45, 0x45, 0x72, 0x72, 0x4, 0x2, 0x8c, 0x8c, 0x98, 0x98, 
    0xc, 0x2, 0x5, 0x19, 0x1b, 0x32, 0x34, 0x3b, 0x3d, 0x4d, 0x4f, 0x50, 
    0x52, 0x5a, 0x5c, 0x63, 0x65, 0x76, 0x78, 0x7c, 0x7e, 0x7f, 0x4, 0x2, 
    0x50, 0x50, 0x8c, 0x8c, 0x2, 0x536, 0x2, 0xa2, 0x3, 0x2, 0x2, 0x2, 0x4, 
    0xaf, 0x3, 0x2, 0x2, 0x2, 0x6, 0xc7, 0x3, 0x2, 0x2, 0x2, 0x8, 0xdf, 
    0x3, 0x2, 0x2, 0x2, 0xa, 0x10e, 0x3, 0x2, 0x2, 0x2, 0xc, 0x110, 0x3, 
    0x2, 0x2, 0x2, 0xe, 0x113, 0x3, 0x2, 0x2, 0x2, 0x10, 0x154, 0x3, 0x2, 
    0x2, 0x2, 0x12, 0x156, 0x3, 0x2, 0x2, 0x2, 0x14, 0x16e, 0x3, 0x2, 0x2, 
    0x2, 0x16, 0x170, 0x3, 0x2, 0x2, 0x2, 0x18, 0x183, 0x3, 0x2, 0x2, 0x2, 
    0x1a, 0x187, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x1e, 
    0x18f, 0x3, 0x2, 0x2, 0x2, 0x20, 0x198, 0x3, 0x2, 0x2, 0x2, 0x22, 0x1a4, 
    0x3, 0x2, 0x2, 0x2, 0x24, 0x1b8, 0x3, 0x2, 0x2, 0x2, 0x26, 0x1ba, 0x3, 
    0x2, 0x2, 0x2, 0x28, 0x1bd, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x1c7, 0x3, 0x2, 
    0x2, 0x2, 0x2c, 0x1e0, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x1e2, 0x3, 0x2, 0x2, 
    0x2, 0x30, 0x1e9, 0x3, 0x2, 0x2, 0x2, 0x32, 0x207, 0x3, 0x2, 0x2, 0x2, 
    0x34, 0x209, 0x3, 0x2, 0x2, 0x2, 0x36, 0x20d, 0x3, 0x2, 0x2, 0x2, 0x38, 
    0x21e, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x220, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x22f, 
    0x3, 0x2, 0x2, 0x2, 0x3e, 0x239, 0x3, 0x2, 0x2, 0x2, 0x40, 0x261, 0x3, 
    0x2, 0x2, 0x2, 0x42, 0x264, 0x3, 0x2, 0x2, 0x2, 0x44, 0x269, 0x3, 0x2, 
    0x2, 0x2, 0x46, 0x270, 0x3, 0x2, 0x2, 0x2, 0x48, 0x276, 0x3, 0x2, 0x2, 
    0x2, 0x4a, 0x279, 0x3, 0x2, 0x2, 0x2, 0x4c, 0x27c, 0x3, 0x2, 0x2, 0x2, 
    0x4e, 0x283, 0x3, 0x2, 0x2, 0x2, 0x50, 0x286, 0x3, 0x2, 0x2, 0x2, 0x52, 
    0x28a, 0x3, 0x2, 0x2, 0x2, 0x54, 0x28f, 0x3, 0x2, 0x2, 0x2, 0x56, 0x295, 
    0x3, 0x2, 0x2, 0x2, 0x58, 0x29e, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x2d0, 0x3, 
    0x2, 0x2, 0x2, 0x5c, 0x2d8, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x2e3, 0x3, 0x2, 
    0x2, 0x2, 0x60, 0x2e5, 0x3, 0x2, 0x2, 0x2, 0x62, 0x2ea, 0x3, 0x2, 0x2, 
    0x2, 0x64, 0x2f2, 0x3, 0x2, 0x2, 0x2, 0x66, 0x2fe, 0x3, 0x2, 0x2, 0x2, 
    0x68, 0x303, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x30b, 0x3, 0x2, 0x2, 0x2, 0x6c, 
    0x30f, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x32a, 0x3, 0x2, 0x2, 0x2, 0x70, 0x32c, 
    0x3, 0x2, 0x2, 0x2, 0x72, 0x331, 0x3, 0x2, 0x2, 0x2, 0x74, 0x361, 0x3, 
    0x2, 0x2, 0x2, 0x76, 0x363, 0x3, 0x2, 0x2, 0x2, 0x78, 0x376, 0x3, 0x2, 
    0x2, 0x2, 0x7a, 0x3d0, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x3fd, 0x3, 0x2, 0x2, 
    0x2, 0x7e, 0x407, 0x3, 0x2, 0x2, 0x2, 0x80, 0x41c, 0x3, 0x2, 0x2, 0x2, 
    0x82, 0x424, 0x3, 0x2, 0x2, 0x2, 0x84, 0x428, 0x3, 0x2, 0x2, 0x2, 0x86, 
    0x43a, 0x3, 0x2, 0x2, 0x2, 0x88, 0x449, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x44d, 
    0x3, 0x2, 0x2, 0x2, 0x8c, 0x457, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x459, 0x3, 
    0x2, 0x2, 0x2, 0x90, 0x463, 0x3, 0x2, 0x2, 0x2, 0x92, 0x466, 0x3, 0x2, 
    0x2, 0x2, 0x94, 0x476, 0x3, 0x2, 0x2, 0x2, 0x96, 0x478, 0x3, 0x2, 0x2, 
    0x2, 0x98, 0x47d, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x481, 0x3, 0x2, 0x2, 0x2, 
    0x9c, 0x483, 0x3, 0x2, 0x2, 0x2, 0x9e, 0x49f, 0x3, 0x2, 0x2, 0x2, 0xa0, 
    0x4a1, 0x3, 0x2, 0x2, 0x2, 0xa2, 0xa7, 0x5, 0x4, 0x3, 0x2, 0xa3, 0xa4, 
    0x7, 0x9e, 0x2, 0x2, 0xa4, 0xa6, 0x5, 0x4, 0x3, 0x2, 0xa5, 0xa3, 0x3, 
    0x2, 0x2, 0x2, 0xa6, 0xa9, 0x3, 0x2, 0x2, 0x2, 0xa7, 0xa5, 0x3, 0x2, 
    0x2, 0x2, 0xa7, 0xa8, 0x3, 0x2, 0x2, 0x2, 0xa8, 0xab, 0x3, 0x2, 0x2, 
    0x2, 0xa9, 0xa7, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xac, 0x7, 0x9e, 0x2, 0x2, 
    0xab, 0xaa, 0x3, 0x2, 0x2, 0x2, 0xab, 0xac, 0x3, 0x2, 0x2, 0x2, 0xac, 
    0xad, 0x3, 0x2, 0x2, 0x2, 0xad, 0xae, 0x7, 0x2, 0x2, 0x3, 0xae, 0x3, 
    0x3, 0x2, 0x2, 0x2, 0xaf, 0xb3, 0x5, 0x6, 0x4, 0x2, 0xb0, 0xb1, 0x7, 
    0x40, 0x2, 0x2, 0xb1, 0xb2, 0x7, 0x59, 0x2, 0x2, 0xb2, 0xb4, 0x7, 0x84, 
    0x2, 0x2, 0xb3, 0xb0, 0x3, 0x2, 0x2, 0x2, 0xb3, 0xb4, 0x3, 0x2, 0x2, 
    0x2, 0xb4, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xb5, 0xb6, 0x7, 0x32, 0x2, 0x2, 
    0xb6, 0xb8, 0x5, 0x9a, 0x4e, 0x2, 0xb7, 0xb5, 0x3, 0x2, 0x2, 0x2, 0xb7, 
    0xb8, 0x3, 0x2, 0x2, 0x2, 0xb8, 0x5, 0x3, 0x2, 0x2, 0x2, 0xb9, 0xc8, 
    0x5, 0x8, 0x5, 0x2, 0xba, 0xc8, 0x5, 0xe, 0x8, 0x2, 0xbb, 0xc8, 0x5, 
    0x10, 0x9, 0x2, 0xbc, 0xc8, 0x5, 0x2a, 0x16, 0x2, 0xbd, 0xc8, 0x5, 0x2c, 
    0x17, 0x2, 0xbe, 0xc8, 0x5, 0x2e, 0x18, 0x2, 0xbf, 0xc8, 0x5, 0x30, 
    0x19, 0x2, 0xc0, 0xc8, 0x5, 0x36, 0x1c, 0x2, 0xc1, 0xc8, 0x5, 0x3a, 
    0x1e, 0x2, 0xc2, 0xc8, 0x5, 0x3c, 0x1f, 0x2, 0xc3, 0xc8, 0x5, 0x6c, 
    0x37, 0x2, 0xc4, 0xc8, 0x5, 0x6e, 0x38, 0x2, 0xc5, 0xc8, 0x5, 0x70, 
    0x39, 0x2, 0xc6, 0xc8, 0x5, 0x72, 0x3a, 0x2, 0xc7, 0xb9, 0x3, 0x2, 0x2, 
    0x2, 0xc7, 0xba, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xbb, 0x3, 0x2, 0x2, 0x2, 
    0xc7, 0xbc, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xbd, 0x3, 0x2, 0x2, 0x2, 0xc7, 
    0xbe, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xbf, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc0, 
    0x3, 0x2, 0x2, 0x2, 0xc7, 0xc1, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc2, 0x3, 
    0x2, 0x2, 0x2, 0xc7, 0xc3, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc4, 0x3, 0x2, 
    0x2, 0x2, 0xc7, 0xc5, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc6, 0x3, 0x2, 0x2, 
    0x2, 0xc8, 0x7, 0x3, 0x2, 0x2, 0x2, 0xc9, 0xca, 0x7, 0x8, 0x2, 0x2, 
    0xca, 0xcb, 0x7, 0x6b, 0x2, 0x2, 0xcb, 0xcc, 0x5, 0x88, 0x45, 0x2, 0xcc, 
    0xd1, 0x5, 0xa, 0x6, 0x2, 0xcd, 0xce, 0x7, 0x8a, 0x2, 0x2, 0xce, 0xd0, 
    0x5, 0xa, 0x6, 0x2, 0xcf, 0xcd, 0x3, 0x2, 0x2, 0x2, 0xd0, 0xd3, 0x3, 
    0x2, 0x2, 0x2, 0xd1, 0xcf, 0x3, 0x2, 0x2, 0x2, 0xd1, 0xd2, 0x3, 0x2, 
    0x2, 0x2, 0xd2, 0xe0, 0x3, 0x2, 0x2, 0x2, 0xd3, 0xd1, 0x3, 0x2, 0x2, 
    0x2, 0xd4, 0xd5, 0x7, 0x8, 0x2, 0x2, 0xd5, 0xd6, 0x7, 0x6b, 0x2, 0x2, 
    0xd6, 0xd7, 0x5, 0x88, 0x45, 0x2, 0xd7, 0xdc, 0x5, 0xc, 0x7, 0x2, 0xd8, 
    0xd9, 0x7, 0x8a, 0x2, 0x2, 0xd9, 0xdb, 0x5, 0xc, 0x7, 0x2, 0xda, 0xd8, 
    0x3, 0x2, 0x2, 0x2, 0xdb, 0xde, 0x3, 0x2, 0x2, 0x2, 0xdc, 0xda, 0x3, 
    0x2, 0x2, 0x2, 0xdc, 0xdd, 0x3, 0x2, 0x2, 0x2, 0xdd, 0xe0, 0x3, 0x2, 
    0x2, 0x2, 0xde, 0xdc, 0x3, 0x2, 0x2, 0x2, 0xdf, 0xc9, 0x3, 0x2, 0x2, 
    0x2, 0xdf, 0xd4, 0x3, 0x2, 0x2, 0x2, 0xe0, 0x9, 0x3, 0x2, 0x2, 0x2, 
    0xe1, 0xe2, 0x7, 0x4, 0x2, 0x2, 0xe2, 0xe6, 0x7, 0x1a, 0x2, 0x2, 0xe3, 
    0xe4, 0x7, 0x3a, 0x2, 0x2, 0xe4, 0xe5, 0x7, 0x50, 0x2, 0x2, 0xe5, 0xe7, 
    0x7, 0x2e, 0x2, 0x2, 0xe6, 0xe3, 0x3, 0x2, 0x2, 0x2, 0xe6, 0xe7, 0x3, 
    0x2, 0x2, 0x2, 0xe7, 0xe8, 0x3, 0x2, 0x2, 0x2, 0xe8, 0xeb, 0x5, 0x24, 
    0x13, 0x2, 0xe9, 0xea, 0x7, 0x5, 0x2, 0x2, 0xea, 0xec, 0x5, 0x84, 0x43, 
    0x2, 0xeb, 0xe9, 0x3, 0x2, 0x2, 0x2, 0xeb, 0xec, 0x3, 0x2, 0x2, 0x2, 
    0xec, 0x10f, 0x3, 0x2, 0x2, 0x2, 0xed, 0xee, 0x7, 0x17, 0x2, 0x2, 0xee, 
    0xf1, 0x7, 0x1a, 0x2, 0x2, 0xef, 0xf0, 0x7, 0x3a, 0x2, 0x2, 0xf0, 0xf2, 
    0x7, 0x2e, 0x2, 0x2, 0xf1, 0xef, 0x3, 0x2, 0x2, 0x2, 0xf1, 0xf2, 0x3, 
    0x2, 0x2, 0x2, 0xf2, 0xf3, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf4, 0x5, 0x84, 
    0x43, 0x2, 0xf4, 0xf5, 0x7, 0x3b, 0x2, 0x2, 0xf5, 0xf6, 0x5, 0x38, 0x1d, 
    0x2, 0xf6, 0x10f, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xf8, 0x7, 0x1b, 0x2, 0x2, 
    0xf8, 0xfb, 0x7, 0x1a, 0x2, 0x2, 0xf9, 0xfa, 0x7, 0x3a, 0x2, 0x2, 0xfa, 
    0xfc, 0x7, 0x2e, 0x2, 0x2, 0xfb, 0xf9, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 
    0x3, 0x2, 0x2, 0x2, 0xfc, 0xfd, 0x3, 0x2, 0x2, 0x2, 0xfd, 0xfe, 0x5, 
    0x84, 0x43, 0x2, 0xfe, 0xff, 0x7, 0x84, 0x2, 0x2, 0xff, 0x10f, 0x3, 
    0x2, 0x2, 0x2, 0x100, 0x101, 0x7, 0x2a, 0x2, 0x2, 0x101, 0x104, 0x7, 
    0x1a, 0x2, 0x2, 0x102, 0x103, 0x7, 0x3a, 0x2, 0x2, 0x103, 0x105, 0x7, 
    0x2e, 0x2, 0x2, 0x104, 0x102, 0x3, 0x2, 0x2, 0x2, 0x104, 0x105, 0x3, 
    0x2, 0x2, 0x2, 0x105, 0x106, 0x3, 0x2, 0x2, 0x2, 0x106, 0x10f, 0x5, 
    0x84, 0x43, 0x2, 0x107, 0x108, 0x7, 0x4c, 0x2, 0x2, 0x108, 0x10b, 0x7, 
    0x1a, 0x2, 0x2, 0x109, 0x10a, 0x7, 0x3a, 0x2, 0x2, 0x10a, 0x10c, 0x7, 
    0x2e, 0x2, 0x2, 0x10b, 0x109, 0x3, 0x2, 0x2, 0x2, 0x10b, 0x10c, 0x3, 
    0x2, 0x2, 0x2, 0x10c, 0x10d, 0x3, 0x2, 0x2, 0x2, 0x10d, 0x10f, 0x5, 
    0x24, 0x13, 0x2, 0x10e, 0xe1, 0x3, 0x2, 0x2, 0x2, 0x10e, 0xed, 0x3, 
    0x2, 0x2, 0x2, 0x10e, 0xf7, 0x3, 0x2, 0x2, 0x2, 0x10e, 0x100, 0x3, 0x2, 
    0x2, 0x2, 0x10e, 0x107, 0x3, 0x2, 0x2, 0x2, 0x10f, 0xb, 0x3, 0x2, 0x2, 
    0x2, 0x110, 0x111, 0x7, 0x2a, 0x2, 0x2, 0x111, 0x112, 0x5, 0x38, 0x1d, 
    0x2, 0x112, 0xd, 0x3, 0x2, 0x2, 0x2, 0x113, 0x114, 0x7, 0x16, 0x2, 0x2, 
    0x114, 0x115, 0x7, 0x6b, 0x2, 0x2, 0x115, 0x116, 0x5, 0x88, 0x45, 0x2, 
    0x116, 0xf, 0x3, 0x2, 0x2, 0x2, 0x117, 0x118, 0x9, 0x2, 0x2, 0x2, 0x118, 
    0x11c, 0x7, 0x1e, 0x2, 0x2, 0x119, 0x11a, 0x7, 0x3a, 0x2, 0x2, 0x11a, 
    0x11b, 0x7, 0x50, 0x2, 0x2, 0x11b, 0x11d, 0x7, 0x2e, 0x2, 0x2, 0x11c, 
    0x119, 0x3, 0x2, 0x2, 0x2, 0x11c, 0x11d, 0x3, 0x2, 0x2, 0x2, 0x11d, 
    0x11e, 0x3, 0x2, 0x2, 0x2, 0x11e, 0x120, 0x5, 0x8e, 0x48, 0x2, 0x11f, 
    0x121, 0x5, 0x20, 0x11, 0x2, 0x120, 0x11f, 0x3, 0x2, 0x2, 0x2, 0x120, 
    0x121, 0x3, 0x2, 0x2, 0x2, 0x121, 0x155, 0x3, 0x2, 0x2, 0x2, 0x122, 
    0x123, 0x9, 0x2, 0x2, 0x2, 0x123, 0x124, 0x7, 0x4a, 0x2, 0x2, 0x124, 
    0x128, 0x7, 0x79, 0x2, 0x2, 0x125, 0x126, 0x7, 0x3a, 0x2, 0x2, 0x126, 
    0x127, 0x7, 0x50, 0x2, 0x2, 0x127, 0x129, 0x7, 0x2e, 0x2, 0x2, 0x128, 
    0x125, 0x3, 0x2, 0x2, 0x2, 0x128, 0x129, 0x3, 0x2, 0x2, 0x2, 0x129, 
    0x12a, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x12c, 0x5, 0x88, 0x45, 0x2, 0x12b, 
    0x12d, 0x5, 0x14, 0xb, 0x2, 0x12c, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x12c, 
    0x12d, 0x3, 0x2, 0x2, 0x2, 0x12d, 0x12f, 0x3, 0x2, 0x2, 0x2, 0x12e, 
    0x130, 0x5, 0x16, 0xc, 0x2, 0x12f, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x12f, 
    0x130, 0x3, 0x2, 0x2, 0x2, 0x130, 0x132, 0x3, 0x2, 0x2, 0x2, 0x131, 
    0x133, 0x7, 0x5b, 0x2, 0x2, 0x132, 0x131, 0x3, 0x2, 0x2, 0x2, 0x132, 
    0x133, 0x3, 0x2, 0x2, 0x2, 0x133, 0x134, 0x3, 0x2, 0x2, 0x2, 0x134, 
    0x135, 0x5, 0x12, 0xa, 0x2, 0x135, 0x155, 0x3, 0x2, 0x2, 0x2, 0x136, 
    0x138, 0x9, 0x2, 0x2, 0x2, 0x137, 0x139, 0x7, 0x6d, 0x2, 0x2, 0x138, 
    0x137, 0x3, 0x2, 0x2, 0x2, 0x138, 0x139, 0x3, 0x2, 0x2, 0x2, 0x139, 
    0x13a, 0x3, 0x2, 0x2, 0x2, 0x13a, 0x13e, 0x7, 0x6b, 0x2, 0x2, 0x13b, 
    0x13c, 0x7, 0x3a, 0x2, 0x2, 0x13c, 0x13d, 0x7, 0x50, 0x2, 0x2, 0x13d, 
    0x13f, 0x7, 0x2e, 0x2, 0x2, 0x13e, 0x13b, 0x3, 0x2, 0x2, 0x2, 0x13e, 
    0x13f, 0x3, 0x2, 0x2, 0x2, 0x13f, 0x140, 0x3, 0x2, 0x2, 0x2, 0x140, 
    0x142, 0x5, 0x88, 0x45, 0x2, 0x141, 0x143, 0x5, 0x14, 0xb, 0x2, 0x142, 
    0x141, 0x3, 0x2, 0x2, 0x2, 0x142, 0x143, 0x3, 0x2, 0x2, 0x2, 0x143, 
    0x145, 0x3, 0x2, 0x2, 0x2, 0x144, 0x146, 0x5, 0x16, 0xc, 0x2, 0x145, 
    0x144, 0x3, 0x2, 0x2, 0x2, 0x145, 0x146, 0x3, 0x2, 0x2, 0x2, 0x146, 
    0x148, 0x3, 0x2, 0x2, 0x2, 0x147, 0x149, 0x5, 0x12, 0xa, 0x2, 0x148, 
    0x147, 0x3, 0x2, 0x2, 0x2, 0x148, 0x149, 0x3, 0x2, 0x2, 0x2, 0x149, 
    0x155, 0x3, 0x2, 0x2, 0x2, 0x14a, 0x14b, 0x9, 0x2, 0x2, 0x2, 0x14b, 
    0x14f, 0x7, 0x79, 0x2, 0x2, 0x14c, 0x14d, 0x7, 0x3a, 0x2, 0x2, 0x14d, 
    0x14e, 0x7, 0x50, 0x2, 0x2, 0x14e, 0x150, 0x7, 0x2e, 0x2, 0x2, 0x14f, 
    0x14c, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x150, 0x3, 0x2, 0x2, 0x2, 0x150, 
    0x151, 0x3, 0x2, 0x2, 0x2, 0x151, 0x152, 0x5, 0x88, 0x45, 0x2, 0x152, 
    0x153, 0x5, 0x12, 0xa, 0x2, 0x153, 0x155, 0x3, 0x2, 0x2, 0x2, 0x154, 
    0x117, 0x3, 0x2, 0x2, 0x2, 0x154, 0x122, 0x3, 0x2, 0x2, 0x2, 0x154, 
    0x136, 0x3, 0x2, 0x2, 0x2, 0x154, 0x14a, 0x3, 0x2, 0x2, 0x2, 0x155, 
    0x11, 0x3, 0x2, 0x2, 0x2, 0x156, 0x157, 0x7, 0xd, 0x2, 0x2, 0x157, 0x158, 
    0x5, 0x3c, 0x1f, 0x2, 0x158, 0x13, 0x3, 0x2, 0x2, 0x2, 0x159, 0x15a, 
    0x7, 0x94, 0x2, 0x2, 0x15a, 0x15f, 0x5, 0x22, 0x12, 0x2, 0x15b, 0x15c, 
    0x7, 0x8a, 0x2, 0x2, 0x15c, 0x15e, 0x5, 0x22, 0x12, 0x2, 0x15d, 0x15b, 
    0x3, 0x2, 0x2, 0x2, 0x15e, 0x161, 0x3, 0x2, 0x2, 0x2, 0x15f, 0x15d, 
    0x3, 0x2, 0x2, 0x2, 0x15f, 0x160, 0x3, 0x2, 0x2, 0x2, 0x160, 0x162, 
    0x3, 0x2, 0x2, 0x2, 0x161, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x162, 0x163, 
    0x7, 0x9d, 0x2, 0x2, 0x163, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x164, 0x165, 
    0x7, 0xd, 0x2, 0x2, 0x165, 0x16f, 0x5, 0x88, 0x45, 0x2, 0x166, 0x167, 
    0x7, 0xd, 0x2, 0x2, 0x167, 0x168, 0x5, 0x98, 0x4d, 0x2, 0x168, 0x16a, 
    0x7, 0x94, 0x2, 0x2, 0x169, 0x16b, 0x5, 0x8a, 0x46, 0x2, 0x16a, 0x169, 
    0x3, 0x2, 0x2, 0x2, 0x16a, 0x16b, 0x3, 0x2, 0x2, 0x2, 0x16b, 0x16c, 
    0x3, 0x2, 0x2, 0x2, 0x16c, 0x16d, 0x7, 0x9d, 0x2, 0x2, 0x16d, 0x16f, 
    0x3, 0x2, 0x2, 0x2, 0x16e, 0x159, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x164, 
    0x3, 0x2, 0x2, 0x2, 0x16e, 0x166, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x15, 0x3, 
    0x2, 0x2, 0x2, 0x170, 0x172, 0x5, 0x20, 0x11, 0x2, 0x171, 0x173, 0x5, 
    0x50, 0x29, 0x2, 0x172, 0x171, 0x3, 0x2, 0x2, 0x2, 0x172, 0x173, 0x3, 
    0x2, 0x2, 0x2, 0x173, 0x175, 0x3, 0x2, 0x2, 0x2, 0x174, 0x176, 0x5, 
    0x18, 0xd, 0x2, 0x175, 0x174, 0x3, 0x2, 0x2, 0x2, 0x175, 0x176, 0x3, 
    0x2, 0x2, 0x2, 0x176, 0x178, 0x3, 0x2, 0x2, 0x2, 0x177, 0x179, 0x5, 
    0x1a, 0xe, 0x2, 0x178, 0x177, 0x3, 0x2, 0x2, 0x2, 0x178, 0x179, 0x3, 
    0x2, 0x2, 0x2, 0x179, 0x17b, 0x3, 0x2, 0x2, 0x2, 0x17a, 0x17c, 0x5, 
    0x1c, 0xf, 0x2, 0x17b, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17b, 0x17c, 0x3, 
    0x2, 0x2, 0x2, 0x17c, 0x17e, 0x3, 0x2, 0x2, 0x2, 0x17d, 0x17f, 0x5, 
    0x1e, 0x10, 0x2, 0x17e, 0x17d, 0x3, 0x2, 0x2, 0x2, 0x17e, 0x17f, 0x3, 
    0x2, 0x2, 0x2, 0x17f, 0x181, 0x3, 0x2, 0x2, 0x2, 0x180, 0x182, 0x5, 
    0x56, 0x2c, 0x2, 0x181, 0x180, 0x3, 0x2, 0x2, 0x2, 0x181, 0x182, 0x3, 
    0x2, 0x2, 0x2, 0x182, 0x17, 0x3, 0x2, 0x2, 0x2, 0x183, 0x184, 0x7, 0x5a, 
    0x2, 0x2, 0x184, 0x185, 0x7, 0x13, 0x2, 0x2, 0x185, 0x186, 0x5, 0x7a, 
    0x3e, 0x2, 0x186, 0x19, 0x3, 0x2, 0x2, 0x2, 0x187, 0x188, 0x7, 0x5d, 
    0x2, 0x2, 0x188, 0x189, 0x7, 0x43, 0x2, 0x2, 0x189, 0x18a, 0x5, 0x7a, 
    0x3e, 0x2, 0x18a, 0x1b, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x18c, 0x7, 0x62, 
    0x2, 0x2, 0x18c, 0x18d, 0x7, 0x13, 0x2, 0x2, 0x18d, 0x18e, 0x5, 0x7a, 
    0x3e, 0x2, 0x18e, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x18f, 0x190, 0x7, 0x74, 
    0x2, 0x2, 0x190, 0x195, 0x5, 0x28, 0x15, 0x2, 0x191, 0x192, 0x7, 0x8a, 
    0x2, 0x2, 0x192, 0x194, 0x5, 0x28, 0x15, 0x2, 0x193, 0x191, 0x3, 0x2, 
    0x2, 0x2, 0x194, 0x197, 0x3, 0x2, 0x2, 0x2, 0x195, 0x193, 0x3, 0x2, 
    0x2, 0x2, 0x195, 0x196, 0x3, 0x2, 0x2, 0x2, 0x196, 0x1f, 0x3, 0x2, 0x2, 
    0x2, 0x197, 0x195, 0x3, 0x2, 0x2, 0x2, 0x198, 0x19a, 0x7, 0x2d, 0x2, 
    0x2, 0x199, 0x19b, 0x7, 0x8f, 0x2, 0x2, 0x19a, 0x199, 0x3, 0x2, 0x2, 
    0x2, 0x19a, 0x19b, 0x3, 0x2, 0x2, 0x2, 0x19b, 0x19c, 0x3, 0x2, 0x2, 
    0x2, 0x19c, 0x1a2, 0x5, 0x9a, 0x4e, 0x2, 0x19d, 0x19f, 0x7, 0x94, 0x2, 
    0x2, 0x19e, 0x1a0, 0x5, 0x76, 0x3c, 0x2, 0x19f, 0x19e, 0x3, 0x2, 0x2, 
    0x2, 0x19f, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x1a0, 0x1a1, 0x3, 0x2, 0x2, 
    0x2, 0x1a1, 0x1a3, 0x7, 0x9d, 0x2, 0x2, 0x1a2, 0x19d, 0x3, 0x2, 0x2, 
    0x2, 0x1a2, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x1a3, 0x21, 0x3, 0x2, 0x2, 0x2, 
    0x1a4, 0x1a5, 0x5, 0x24, 0x13, 0x2, 0x1a5, 0x23, 0x3, 0x2, 0x2, 0x2, 
    0x1a6, 0x1a7, 0x5, 0x84, 0x43, 0x2, 0x1a7, 0x1a9, 0x5, 0x74, 0x3b, 0x2, 
    0x1a8, 0x1aa, 0x5, 0x26, 0x14, 0x2, 0x1a9, 0x1a8, 0x3, 0x2, 0x2, 0x2, 
    0x1a9, 0x1aa, 0x3, 0x2, 0x2, 0x2, 0x1aa, 0x1ad, 0x3, 0x2, 0x2, 0x2, 
    0x1ab, 0x1ac, 0x7, 0x74, 0x2, 0x2, 0x1ac, 0x1ae, 0x5, 0x7a, 0x3e, 0x2, 
    0x1ad, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1ad, 0x1ae, 0x3, 0x2, 0x2, 0x2, 
    0x1ae, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1af, 0x1b1, 0x5, 0x84, 0x43, 0x2, 
    0x1b0, 0x1b2, 0x5, 0x74, 0x3b, 0x2, 0x1b1, 0x1b0, 0x3, 0x2, 0x2, 0x2, 
    0x1b1, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b2, 0x1b3, 0x3, 0x2, 0x2, 0x2, 
    0x1b3, 0x1b6, 0x5, 0x26, 0x14, 0x2, 0x1b4, 0x1b5, 0x7, 0x74, 0x2, 0x2, 
    0x1b5, 0x1b7, 0x5, 0x7a, 0x3e, 0x2, 0x1b6, 0x1b4, 0x3, 0x2, 0x2, 0x2, 
    0x1b6, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x1b7, 0x1b9, 0x3, 0x2, 0x2, 0x2, 
    0x1b8, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1b8, 0x1af, 0x3, 0x2, 0x2, 0x2, 
    0x1b9, 0x25, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1bb, 0x9, 0x3, 0x2, 0x2, 0x1bb, 
    0x1bc, 0x5, 0x7a, 0x3e, 0x2, 0x1bc, 0x27, 0x3, 0x2, 0x2, 0x2, 0x1bd, 
    0x1c5, 0x5, 0x7a, 0x3e, 0x2, 0x1be, 0x1c6, 0x7, 0x23, 0x2, 0x2, 0x1bf, 
    0x1c0, 0x7, 0x70, 0x2, 0x2, 0x1c0, 0x1c1, 0x7, 0x28, 0x2, 0x2, 0x1c1, 
    0x1c6, 0x7, 0x84, 0x2, 0x2, 0x1c2, 0x1c3, 0x7, 0x70, 0x2, 0x2, 0x1c3, 
    0x1c4, 0x7, 0x7a, 0x2, 0x2, 0x1c4, 0x1c6, 0x7, 0x84, 0x2, 0x2, 0x1c5, 
    0x1be, 0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1bf, 0x3, 0x2, 0x2, 0x2, 0x1c5, 
    0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 
    0x29, 0x3, 0x2, 0x2, 0x2, 0x1c7, 0x1c8, 0x9, 0x4, 0x2, 0x2, 0x1c8, 0x1c9, 
    0x7, 0x6b, 0x2, 0x2, 0x1c9, 0x1ca, 0x5, 0x88, 0x45, 0x2, 0x1ca, 0x2b, 
    0x3, 0x2, 0x2, 0x2, 0x1cb, 0x1cc, 0x9, 0x5, 0x2, 0x2, 0x1cc, 0x1cf, 
    0x7, 0x1e, 0x2, 0x2, 0x1cd, 0x1ce, 0x7, 0x3a, 0x2, 0x2, 0x1ce, 0x1d0, 
    0x7, 0x2e, 0x2, 0x2, 0x1cf, 0x1cd, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d0, 
    0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x3, 0x2, 0x2, 0x2, 0x1d1, 0x1e1, 
    0x5, 0x8e, 0x48, 0x2, 0x1d2, 0x1d4, 0x9, 0x5, 0x2, 0x2, 0x1d3, 0x1d5, 
    0x7, 0x6d, 0x2, 0x2, 0x1d4, 0x1d3, 0x3, 0x2, 0x2, 0x2, 0x1d4, 0x1d5, 
    0x3, 0x2, 0x2, 0x2, 0x1d5, 0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d6, 0x1d9, 
    0x7, 0x6b, 0x2, 0x2, 0x1d7, 0x1d8, 0x7, 0x3a, 0x2, 0x2, 0x1d8, 0x1da, 
    0x7, 0x2e, 0x2, 0x2, 0x1d9, 0x1d7, 0x3, 0x2, 0x2, 0x2, 0x1d9, 0x1da, 
    0x3, 0x2, 0x2, 0x2, 0x1da, 0x1db, 0x3, 0x2, 0x2, 0x2, 0x1db, 0x1de, 
    0x5, 0x88, 0x45, 0x2, 0x1dc, 0x1dd, 0x7, 0x4f, 0x2, 0x2, 0x1dd, 0x1df, 
    0x7, 0x22, 0x2, 0x2, 0x1de, 0x1dc, 0x3, 0x2, 0x2, 0x2, 0x1de, 0x1df, 
    0x3, 0x2, 0x2, 0x2, 0x1df, 0x1e1, 0x3, 0x2, 0x2, 0x2, 0x1e0, 0x1cb, 
    0x3, 0x2, 0x2, 0x2, 0x1e0, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x1e1, 0x2d, 0x3, 
    0x2, 0x2, 0x2, 0x1e2, 0x1e4, 0x7, 0x2e, 0x2, 0x2, 0x1e3, 0x1e5, 0x7, 
    0x6d, 0x2, 0x2, 0x1e4, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1e5, 0x3, 
    0x2, 0x2, 0x2, 0x1e5, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1e7, 0x7, 
    0x6b, 0x2, 0x2, 0x1e7, 0x1e8, 0x5, 0x88, 0x45, 0x2, 0x1e8, 0x2f, 0x3, 
    0x2, 0x2, 0x2, 0x1e9, 0x1ea, 0x7, 0x3e, 0x2, 0x2, 0x1ea, 0x1eb, 0x7, 
    0x40, 0x2, 0x2, 0x1eb, 0x1f7, 0x5, 0x88, 0x45, 0x2, 0x1ec, 0x1ed, 0x7, 
    0x94, 0x2, 0x2, 0x1ed, 0x1f2, 0x5, 0x84, 0x43, 0x2, 0x1ee, 0x1ef, 0x7, 
    0x8a, 0x2, 0x2, 0x1ef, 0x1f1, 0x5, 0x84, 0x43, 0x2, 0x1f0, 0x1ee, 0x3, 
    0x2, 0x2, 0x2, 0x1f1, 0x1f4, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1f0, 0x3, 
    0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1f5, 0x3, 
    0x2, 0x2, 0x2, 0x1f4, 0x1f2, 0x3, 0x2, 0x2, 0x2, 0x1f5, 0x1f6, 0x7, 
    0x9d, 0x2, 0x2, 0x1f6, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1ec, 0x3, 
    0x2, 0x2, 0x2, 0x1f7, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1f9, 0x3, 
    0x2, 0x2, 0x2, 0x1f9, 0x1fa, 0x5, 0x32, 0x1a, 0x2, 0x1fa, 0x31, 0x3, 
    0x2, 0x2, 0x2, 0x1fb, 0x1fc, 0x7, 0x78, 0x2, 0x2, 0x1fc, 0x203, 0x5, 
    0x34, 0x1b, 0x2, 0x1fd, 0x1ff, 0x7, 0x8a, 0x2, 0x2, 0x1fe, 0x1fd, 0x3, 
    0x2, 0x2, 0x2, 0x1fe, 0x1ff, 0x3, 0x2, 0x2, 0x2, 0x1ff, 0x200, 0x3, 
    0x2, 0x2, 0x2, 0x200, 0x202, 0x5, 0x34, 0x1b, 0x2, 0x201, 0x1fe, 0x3, 
    0x2, 0x2, 0x2, 0x202, 0x205, 0x3, 0x2, 0x2, 0x2, 0x203, 0x201, 0x3, 
    0x2, 0x2, 0x2, 0x203, 0x204, 0x3, 0x2, 0x2, 0x2, 0x204, 0x208, 0x3, 
    0x2, 0x2, 0x2, 0x205, 0x203, 0x3, 0x2, 0x2, 0x2, 0x206, 0x208, 0x5, 
    0x3c, 0x1f, 0x2, 0x207, 0x1fb, 0x3, 0x2, 0x2, 0x2, 0x207, 0x206, 0x3, 
    0x2, 0x2, 0x2, 0x208, 0x33, 0x3, 0x2, 0x2, 0x2, 0x209, 0x20a, 0x7, 0x94, 
    0x2, 0x2, 0x20a, 0x20b, 0x5, 0x76, 0x3c, 0x2, 0x20b, 0x20c, 0x7, 0x9d, 
    0x2, 0x2, 0x20c, 0x35, 0x3, 0x2, 0x2, 0x2, 0x20d, 0x20e, 0x7, 0x55, 
    0x2, 0x2, 0x20e, 0x20f, 0x7, 0x6b, 0x2, 0x2, 0x20f, 0x211, 0x5, 0x88, 
    0x45, 0x2, 0x210, 0x212, 0x5, 0x38, 0x1d, 0x2, 0x211, 0x210, 0x3, 0x2, 
    0x2, 0x2, 0x211, 0x212, 0x3, 0x2, 0x2, 0x2, 0x212, 0x214, 0x3, 0x2, 
    0x2, 0x2, 0x213, 0x215, 0x7, 0x30, 0x2, 0x2, 0x214, 0x213, 0x3, 0x2, 
    0x2, 0x2, 0x214, 0x215, 0x3, 0x2, 0x2, 0x2, 0x215, 0x217, 0x3, 0x2, 
    0x2, 0x2, 0x216, 0x218, 0x7, 0x20, 0x2, 0x2, 0x217, 0x216, 0x3, 0x2, 
    0x2, 0x2, 0x217, 0x218, 0x3, 0x2, 0x2, 0x2, 0x218, 0x37, 0x3, 0x2, 0x2, 
    0x2, 0x219, 0x21a, 0x7, 0x5a, 0x2, 0x2, 0x21a, 0x21f, 0x5, 0x7a, 0x3e, 
    0x2, 0x21b, 0x21c, 0x7, 0x5a, 0x2, 0x2, 0x21c, 0x21d, 0x7, 0x39, 0x2, 
    0x2, 0x21d, 0x21f, 0x7, 0x84, 0x2, 0x2, 0x21e, 0x219, 0x3, 0x2, 0x2, 
    0x2, 0x21e, 0x21b, 0x3, 0x2, 0x2, 0x2, 0x21f, 0x39, 0x3, 0x2, 0x2, 0x2, 
    0x220, 0x221, 0x7, 0x5f, 0x2, 0x2, 0x221, 0x222, 0x7, 0x6b, 0x2, 0x2, 
    0x222, 0x223, 0x5, 0x88, 0x45, 0x2, 0x223, 0x224, 0x7, 0x70, 0x2, 0x2, 
    0x224, 0x22c, 0x5, 0x88, 0x45, 0x2, 0x225, 0x226, 0x7, 0x8a, 0x2, 0x2, 
    0x226, 0x227, 0x5, 0x88, 0x45, 0x2, 0x227, 0x228, 0x7, 0x70, 0x2, 0x2, 
    0x228, 0x229, 0x5, 0x88, 0x45, 0x2, 0x229, 0x22b, 0x3, 0x2, 0x2, 0x2, 
    0x22a, 0x225, 0x3, 0x2, 0x2, 0x2, 0x22b, 0x22e, 0x3, 0x2, 0x2, 0x2, 
    0x22c, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x22c, 0x22d, 0x3, 0x2, 0x2, 0x2, 
    0x22d, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x22e, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22f, 
    0x235, 0x5, 0x3e, 0x20, 0x2, 0x230, 0x231, 0x7, 0x75, 0x2, 0x2, 0x231, 
    0x232, 0x7, 0x7, 0x2, 0x2, 0x232, 0x234, 0x5, 0x3e, 0x20, 0x2, 0x233, 
    0x230, 0x3, 0x2, 0x2, 0x2, 0x234, 0x237, 0x3, 0x2, 0x2, 0x2, 0x235, 
    0x233, 0x3, 0x2, 0x2, 0x2, 0x235, 0x236, 0x3, 0x2, 0x2, 0x2, 0x236, 
    0x3d, 0x3, 0x2, 0x2, 0x2, 0x237, 0x235, 0x3, 0x2, 0x2, 0x2, 0x238, 0x23a, 
    0x5, 0x40, 0x21, 0x2, 0x239, 0x238, 0x3, 0x2, 0x2, 0x2, 0x239, 0x23a, 
    0x3, 0x2, 0x2, 0x2, 0x23a, 0x23b, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x23d, 
    0x7, 0x64, 0x2, 0x2, 0x23c, 0x23e, 0x7, 0x29, 0x2, 0x2, 0x23d, 0x23c, 
    0x3, 0x2, 0x2, 0x2, 0x23d, 0x23e, 0x3, 0x2, 0x2, 0x2, 0x23e, 0x23f, 
    0x3, 0x2, 0x2, 0x2, 0x23f, 0x241, 0x5, 0x76, 0x3c, 0x2, 0x240, 0x242, 
    0x5, 0x42, 0x22, 0x2, 0x241, 0x240, 0x3, 0x2, 0x2, 0x2, 0x241, 0x242, 
    0x3, 0x2, 0x2, 0x2, 0x242, 0x244, 0x3, 0x2, 0x2, 0x2, 0x243, 0x245, 
    0x5, 0x44, 0x23, 0x2, 0x244, 0x243, 0x3, 0x2, 0x2, 0x2, 0x244, 0x245, 
    0x3, 0x2, 0x2, 0x2, 0x245, 0x247, 0x3, 0x2, 0x2, 0x2, 0x246, 0x248, 
    0x5, 0x46, 0x24, 0x2, 0x247, 0x246, 0x3, 0x2, 0x2, 0x2, 0x247, 0x248, 
    0x3, 0x2, 0x2, 0x2, 0x248, 0x24a, 0x3, 0x2, 0x2, 0x2, 0x249, 0x24b, 
    0x5, 0x48, 0x25, 0x2, 0x24a, 0x249, 0x3, 0x2, 0x2, 0x2, 0x24a, 0x24b, 
    0x3, 0x2, 0x2, 0x2, 0x24b, 0x24d, 0x3, 0x2, 0x2, 0x2, 0x24c, 0x24e, 
    0x5, 0x4a, 0x26, 0x2, 0x24d, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24d, 0x24e, 
    0x3, 0x2, 0x2, 0x2, 0x24e, 0x250, 0x3, 0x2, 0x2, 0x2, 0x24f, 0x251, 
    0x5, 0x4c, 0x27, 0x2, 0x250, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x250, 0x251, 
    0x3, 0x2, 0x2, 0x2, 0x251, 0x253, 0x3, 0x2, 0x2, 0x2, 0x252, 0x254, 
    0x5, 0x4e, 0x28, 0x2, 0x253, 0x252, 0x3, 0x2, 0x2, 0x2, 0x253, 0x254, 
    0x3, 0x2, 0x2, 0x2, 0x254, 0x256, 0x3, 0x2, 0x2, 0x2, 0x255, 0x257, 
    0x5, 0x50, 0x29, 0x2, 0x256, 0x255, 0x3, 0x2, 0x2, 0x2, 0x256, 0x257, 
    0x3, 0x2, 0x2, 0x2, 0x257, 0x259, 0x3, 0x2, 0x2, 0x2, 0x258, 0x25a, 
    0x5, 0x52, 0x2a, 0x2, 0x259, 0x258, 0x3, 0x2, 0x2, 0x2, 0x259, 0x25a, 
    0x3, 0x2, 0x2, 0x2, 0x25a, 0x25c, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x25d, 
    0x5, 0x54, 0x2b, 0x2, 0x25c, 0x25b, 0x3, 0x2, 0x2, 0x2, 0x25c, 0x25d, 
    0x3, 0x2, 0x2, 0x2, 0x25d, 0x25f, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x260, 
    0x5, 0x56, 0x2c, 0x2, 0x25f, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x25f, 0x260, 
    0x3, 0x2, 0x2, 0x2, 0x260, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x261, 0x262, 0x7, 
    0x7e, 0x2, 0x2, 0x262, 0x263, 0x5, 0x76, 0x3c, 0x2, 0x263, 0x41, 0x3, 
    0x2, 0x2, 0x2, 0x264, 0x265, 0x7, 0x33, 0x2, 0x2, 0x265, 0x267, 0x5, 
    0x58, 0x2d, 0x2, 0x266, 0x268, 0x7, 0x30, 0x2, 0x2, 0x267, 0x266, 0x3, 
    0x2, 0x2, 0x2, 0x267, 0x268, 0x3, 0x2, 0x2, 0x2, 0x268, 0x43, 0x3, 0x2, 
    0x2, 0x2, 0x269, 0x26a, 0x7, 0x62, 0x2, 0x2, 0x26a, 0x26d, 0x5, 0x66, 
    0x34, 0x2, 0x26b, 0x26c, 0x7, 0x53, 0x2, 0x2, 0x26c, 0x26e, 0x5, 0x66, 
    0x34, 0x2, 0x26d, 0x26b, 0x3, 0x2, 0x2, 0x2, 0x26d, 0x26e, 0x3, 0x2, 
    0x2, 0x2, 0x26e, 0x45, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x271, 0x7, 0x46, 
    0x2, 0x2, 0x270, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x270, 0x271, 0x3, 0x2, 
    0x2, 0x2, 0x271, 0x272, 0x3, 0x2, 0x2, 0x2, 0x272, 0x273, 0x7, 0xc, 
    0x2, 0x2, 0x273, 0x274, 0x7, 0x42, 0x2, 0x2, 0x274, 0x275, 0x5, 0x76, 
    0x3c, 0x2, 0x275, 0x47, 0x3, 0x2, 0x2, 0x2, 0x276, 0x277, 0x7, 0x5c, 
    0x2, 0x2, 0x277, 0x278, 0x5, 0x7a, 0x3e, 0x2, 0x278, 0x49, 0x3, 0x2, 
    0x2, 0x2, 0x279, 0x27a, 0x7, 0x7d, 0x2, 0x2, 0x27a, 0x27b, 0x5, 0x7a, 
    0x3e, 0x2, 0x27b, 0x4b, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x27d, 0x7, 0x36, 
    0x2, 0x2, 0x27d, 0x27e, 0x7, 0x13, 0x2, 0x2, 0x27e, 0x281, 0x5, 0x76, 
    0x3c, 0x2, 0x27f, 0x280, 0x7, 0x7e, 0x2, 0x2, 0x280, 0x282, 0x7, 0x71, 
    0x2, 0x2, 0x281, 0x27f, 0x3, 0x2, 0x2, 0x2, 0x281, 0x282, 0x3, 0x2, 
    0x2, 0x2, 0x282, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x283, 0x284, 0x7, 0x37, 
    0x2, 0x2, 0x284, 0x285, 0x5, 0x7a, 0x3e, 0x2, 0x285, 0x4f, 0x3, 0x2, 
    0x2, 0x2, 0x286, 0x287, 0x7, 0x57, 0x2, 0x2, 0x287, 0x288, 0x7, 0x13, 
    0x2, 0x2, 0x288, 0x289, 0x5, 0x62, 0x32, 0x2, 0x289, 0x51, 0x3, 0x2, 
    0x2, 0x2, 0x28a, 0x28b, 0x7, 0x48, 0x2, 0x2, 0x28b, 0x28c, 0x5, 0x60, 
    0x31, 0x2, 0x28c, 0x28d, 0x7, 0x13, 0x2, 0x2, 0x28d, 0x28e, 0x5, 0x76, 
    0x3c, 0x2, 0x28e, 0x53, 0x3, 0x2, 0x2, 0x2, 0x28f, 0x290, 0x7, 0x48, 
    0x2, 0x2, 0x290, 0x293, 0x5, 0x60, 0x31, 0x2, 0x291, 0x292, 0x7, 0x7e, 
    0x2, 0x2, 0x292, 0x294, 0x7, 0x6f, 0x2, 0x2, 0x293, 0x291, 0x3, 0x2, 
    0x2, 0x2, 0x293, 0x294, 0x3, 0x2, 0x2, 0x2, 0x294, 0x55, 0x3, 0x2, 0x2, 
    0x2, 0x295, 0x296, 0x7, 0x67, 0x2, 0x2, 0x296, 0x297, 0x5, 0x68, 0x35, 
    0x2, 0x297, 0x57, 0x3, 0x2, 0x2, 0x2, 0x298, 0x299, 0x8, 0x2d, 0x1, 
    0x2, 0x299, 0x29f, 0x5, 0x86, 0x44, 0x2, 0x29a, 0x29b, 0x7, 0x94, 0x2, 
    0x2, 0x29b, 0x29c, 0x5, 0x58, 0x2d, 0x2, 0x29c, 0x29d, 0x7, 0x9d, 0x2, 
    0x2, 0x29d, 0x29f, 0x3, 0x2, 0x2, 0x2, 0x29e, 0x298, 0x3, 0x2, 0x2, 
    0x2, 0x29e, 0x29a, 0x3, 0x2, 0x2, 0x2, 0x29f, 0x2af, 0x3, 0x2, 0x2, 
    0x2, 0x2a0, 0x2a1, 0xc, 0x3, 0x2, 0x2, 0x2a1, 0x2a2, 0x5, 0x5c, 0x2f, 
    0x2, 0x2a2, 0x2a3, 0x5, 0x58, 0x2d, 0x4, 0x2a3, 0x2ae, 0x3, 0x2, 0x2, 
    0x2, 0x2a4, 0x2a6, 0xc, 0x4, 0x2, 0x2, 0x2a5, 0x2a7, 0x9, 0x6, 0x2, 
    0x2, 0x2a6, 0x2a5, 0x3, 0x2, 0x2, 0x2, 0x2a6, 0x2a7, 0x3, 0x2, 0x2, 
    0x2, 0x2a7, 0x2a8, 0x3, 0x2, 0x2, 0x2, 0x2a8, 0x2a9, 0x5, 0x5a, 0x2e, 
    0x2, 0x2a9, 0x2aa, 0x7, 0x42, 0x2, 0x2, 0x2aa, 0x2ab, 0x5, 0x58, 0x2d, 
    0x2, 0x2ab, 0x2ac, 0x5, 0x5e, 0x30, 0x2, 0x2ac, 0x2ae, 0x3, 0x2, 0x2, 
    0x2, 0x2ad, 0x2a0, 0x3, 0x2, 0x2, 0x2, 0x2ad, 0x2a4, 0x3, 0x2, 0x2, 
    0x2, 0x2ae, 0x2b1, 0x3, 0x2, 0x2, 0x2, 0x2af, 0x2ad, 0x3, 0x2, 0x2, 
    0x2, 0x2af, 0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b0, 0x59, 0x3, 0x2, 0x2, 0x2, 
    0x2b1, 0x2af, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x2b4, 0x7, 0xb, 0x2, 0x2, 
    0x2b3, 0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2b4, 0x3, 0x2, 0x2, 0x2, 
    0x2b4, 0x2b5, 0x3, 0x2, 0x2, 0x2, 0x2b5, 0x2bb, 0x7, 0x3d, 0x2, 0x2, 
    0x2b6, 0x2b8, 0x7, 0x3d, 0x2, 0x2, 0x2b7, 0x2b9, 0x7, 0xb, 0x2, 0x2, 
    0x2b8, 0x2b7, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b9, 0x3, 0x2, 0x2, 0x2, 
    0x2b9, 0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2ba, 0x2b3, 0x3, 0x2, 0x2, 0x2, 
    0x2ba, 0x2b6, 0x3, 0x2, 0x2, 0x2, 0x2bb, 0x2d1, 0x3, 0x2, 0x2, 0x2, 
    0x2bc, 0x2be, 0x9, 0x7, 0x2, 0x2, 0x2bd, 0x2bc, 0x3, 0x2, 0x2, 0x2, 
    0x2bd, 0x2be, 0x3, 0x2, 0x2, 0x2, 0x2be, 0x2bf, 0x3, 0x2, 0x2, 0x2, 
    0x2bf, 0x2c5, 0x9, 0x8, 0x2, 0x2, 0x2c0, 0x2c2, 0x9, 0x8, 0x2, 0x2, 
    0x2c1, 0x2c3, 0x9, 0x7, 0x2, 0x2, 0x2c2, 0x2c1, 0x3, 0x2, 0x2, 0x2, 
    0x2c2, 0x2c3, 0x3, 0x2, 0x2, 0x2, 0x2c3, 0x2c5, 0x3, 0x2, 0x2, 0x2, 
    0x2c4, 0x2bd, 0x3, 0x2, 0x2, 0x2, 0x2c4, 0x2c0, 0x3, 0x2, 0x2, 0x2, 
    0x2c5, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2c6, 0x2c8, 0x9, 0x9, 0x2, 0x2, 
    0x2c7, 0x2c6, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c8, 0x3, 0x2, 0x2, 0x2, 
    0x2c8, 0x2c9, 0x3, 0x2, 0x2, 0x2, 0x2c9, 0x2cf, 0x7, 0x34, 0x2, 0x2, 
    0x2ca, 0x2cc, 0x7, 0x34, 0x2, 0x2, 0x2cb, 0x2cd, 0x9, 0x9, 0x2, 0x2, 
    0x2cc, 0x2cb, 0x3, 0x2, 0x2, 0x2, 0x2cc, 0x2cd, 0x3, 0x2, 0x2, 0x2, 
    0x2cd, 0x2cf, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2c7, 0x3, 0x2, 0x2, 0x2, 
    0x2ce, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2cf, 0x2d1, 0x3, 0x2, 0x2, 0x2, 
    0x2d0, 0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2c4, 0x3, 0x2, 0x2, 0x2, 
    0x2d0, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x2d1, 0x5b, 0x3, 0x2, 0x2, 0x2, 0x2d2, 
    0x2d4, 0x9, 0x6, 0x2, 0x2, 0x2d3, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x2d3, 
    0x2d4, 0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d5, 0x3, 0x2, 0x2, 0x2, 0x2d5, 
    0x2d6, 0x7, 0x1d, 0x2, 0x2, 0x2d6, 0x2d9, 0x7, 0x42, 0x2, 0x2, 0x2d7, 
    0x2d9, 0x7, 0x8a, 0x2, 0x2, 0x2d8, 0x2d3, 0x3, 0x2, 0x2, 0x2, 0x2d8, 
    0x2d7, 0x3, 0x2, 0x2, 0x2, 0x2d9, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x2da, 0x2db, 
    0x7, 0x54, 0x2, 0x2, 0x2db, 0x2e4, 0x5, 0x76, 0x3c, 0x2, 0x2dc, 0x2dd, 
    0x7, 0x77, 0x2, 0x2, 0x2dd, 0x2de, 0x7, 0x94, 0x2, 0x2, 0x2de, 0x2df, 
    0x5, 0x76, 0x3c, 0x2, 0x2df, 0x2e0, 0x7, 0x9d, 0x2, 0x2, 0x2e0, 0x2e4, 
    0x3, 0x2, 0x2, 0x2, 0x2e1, 0x2e2, 0x7, 0x77, 0x2, 0x2, 0x2e2, 0x2e4, 
    0x5, 0x76, 0x3c, 0x2, 0x2e3, 0x2da, 0x3, 0x2, 0x2, 0x2, 0x2e3, 0x2dc, 
    0x3, 0x2, 0x2, 0x2, 0x2e3, 0x2e1, 0x3, 0x2, 0x2, 0x2, 0x2e4, 0x5f, 0x3, 
    0x2, 0x2, 0x2, 0x2e5, 0x2e8, 0x7, 0x83, 0x2, 0x2, 0x2e6, 0x2e7, 0x9, 
    0xa, 0x2, 0x2, 0x2e7, 0x2e9, 0x7, 0x83, 0x2, 0x2, 0x2e8, 0x2e6, 0x3, 
    0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x3, 0x2, 0x2, 0x2, 0x2e9, 0x61, 0x3, 0x2, 
    0x2, 0x2, 0x2ea, 0x2ef, 0x5, 0x64, 0x33, 0x2, 0x2eb, 0x2ec, 0x7, 0x8a, 
    0x2, 0x2, 0x2ec, 0x2ee, 0x5, 0x64, 0x33, 0x2, 0x2ed, 0x2eb, 0x3, 0x2, 
    0x2, 0x2, 0x2ee, 0x2f1, 0x3, 0x2, 0x2, 0x2, 0x2ef, 0x2ed, 0x3, 0x2, 
    0x2, 0x2, 0x2ef, 0x2f0, 0x3, 0x2, 0x2, 0x2, 0x2f0, 0x63, 0x3, 0x2, 0x2, 
    0x2, 0x2f1, 0x2ef, 0x3, 0x2, 0x2, 0x2, 0x2f2, 0x2f4, 0x5, 0x7a, 0x3e, 
    0x2, 0x2f3, 0x2f5, 0x9, 0xb, 0x2, 0x2, 0x2f4, 0x2f3, 0x3, 0x2, 0x2, 
    0x2, 0x2f4, 0x2f5, 0x3, 0x2, 0x2, 0x2, 0x2f5, 0x2f8, 0x3, 0x2, 0x2, 
    0x2, 0x2f6, 0x2f7, 0x7, 0x52, 0x2, 0x2, 0x2f7, 0x2f9, 0x9, 0xc, 0x2, 
    0x2, 0x2f8, 0x2f6, 0x3, 0x2, 0x2, 0x2, 0x2f8, 0x2f9, 0x3, 0x2, 0x2, 
    0x2, 0x2f9, 0x2fc, 0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2fb, 0x7, 0x19, 0x2, 
    0x2, 0x2fb, 0x2fd, 0x7, 0x84, 0x2, 0x2, 0x2fc, 0x2fa, 0x3, 0x2, 0x2, 
    0x2, 0x2fc, 0x2fd, 0x3, 0x2, 0x2, 0x2, 0x2fd, 0x65, 0x3, 0x2, 0x2, 0x2, 
    0x2fe, 0x301, 0x5, 0x92, 0x4a, 0x2, 0x2ff, 0x300, 0x7, 0x9f, 0x2, 0x2, 
    0x300, 0x302, 0x5, 0x92, 0x4a, 0x2, 0x301, 0x2ff, 0x3, 0x2, 0x2, 0x2, 
    0x301, 0x302, 0x3, 0x2, 0x2, 0x2, 0x302, 0x67, 0x3, 0x2, 0x2, 0x2, 0x303, 
    0x308, 0x5, 0x6a, 0x36, 0x2, 0x304, 0x305, 0x7, 0x8a, 0x2, 0x2, 0x305, 
    0x307, 0x5, 0x6a, 0x36, 0x2, 0x306, 0x304, 0x3, 0x2, 0x2, 0x2, 0x307, 
    0x30a, 0x3, 0x2, 0x2, 0x2, 0x308, 0x306, 0x3, 0x2, 0x2, 0x2, 0x308, 
    0x309, 0x3, 0x2, 0x2, 0x2, 0x309, 0x69, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x308, 
    0x3, 0x2, 0x2, 0x2, 0x30b, 0x30c, 0x5, 0x98, 0x4d, 0x2, 0x30c, 0x30d, 
    0x7, 0x8f, 0x2, 0x2, 0x30d, 0x30e, 0x5, 0x94, 0x4b, 0x2, 0x30e, 0x6b, 
    0x3, 0x2, 0x2, 0x2, 0x30f, 0x310, 0x7, 0x66, 0x2, 0x2, 0x310, 0x311, 
    0x5, 0x68, 0x35, 0x2, 0x311, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x312, 0x313, 
    0x7, 0x68, 0x2, 0x2, 0x313, 0x315, 0x7, 0x1c, 0x2, 0x2, 0x314, 0x316, 
    0x7, 0x6d, 0x2, 0x2, 0x315, 0x314, 0x3, 0x2, 0x2, 0x2, 0x315, 0x316, 
    0x3, 0x2, 0x2, 0x2, 0x316, 0x317, 0x3, 0x2, 0x2, 0x2, 0x317, 0x318, 
    0x7, 0x6b, 0x2, 0x2, 0x318, 0x32b, 0x5, 0x88, 0x45, 0x2, 0x319, 0x31b, 
    0x7, 0x68, 0x2, 0x2, 0x31a, 0x31c, 0x7, 0x6d, 0x2, 0x2, 0x31b, 0x31a, 
    0x3, 0x2, 0x2, 0x2, 0x31b, 0x31c, 0x3, 0x2, 0x2, 0x2, 0x31c, 0x31d, 
    0x3, 0x2, 0x2, 0x2, 0x31d, 0x320, 0x7, 0x6c, 0x2, 0x2, 0x31e, 0x31f, 
    0x9, 0xd, 0x2, 0x2, 0x31f, 0x321, 0x5, 0x8e, 0x48, 0x2, 0x320, 0x31e, 
    0x3, 0x2, 0x2, 0x2, 0x320, 0x321, 0x3, 0x2, 0x2, 0x2, 0x321, 0x325, 
    0x3, 0x2, 0x2, 0x2, 0x322, 0x323, 0x7, 0x47, 0x2, 0x2, 0x323, 0x326, 
    0x7, 0x84, 0x2, 0x2, 0x324, 0x326, 0x5, 0x4a, 0x26, 0x2, 0x325, 0x322, 
    0x3, 0x2, 0x2, 0x2, 0x325, 0x324, 0x3, 0x2, 0x2, 0x2, 0x325, 0x326, 
    0x3, 0x2, 0x2, 0x2, 0x326, 0x328, 0x3, 0x2, 0x2, 0x2, 0x327, 0x329, 
    0x5, 0x54, 0x2b, 0x2, 0x328, 0x327, 0x3, 0x2, 0x2, 0x2, 0x328, 0x329, 
    0x3, 0x2, 0x2, 0x2, 0x329, 0x32b, 0x3, 0x2, 0x2, 0x2, 0x32a, 0x312, 
    0x3, 0x2, 0x2, 0x2, 0x32a, 0x319, 0x3, 0x2, 0x2, 0x2, 0x32b, 0x6f, 0x3, 
    0x2, 0x2, 0x2, 0x32c, 0x32d, 0x7, 0x6a, 0x2, 0x2, 0x32d, 0x32e, 0x7, 
    0x69, 0x2, 0x2, 0x32e, 0x32f, 0x7, 0x60, 0x2, 0x2, 0x32f, 0x330, 0x5, 
    0x88, 0x45, 0x2, 0x330, 0x71, 0x3, 0x2, 0x2, 0x2, 0x331, 0x332, 0x7, 
    0x76, 0x2, 0x2, 0x332, 0x333, 0x5, 0x8e, 0x48, 0x2, 0x333, 0x73, 0x3, 
    0x2, 0x2, 0x2, 0x334, 0x362, 0x5, 0x98, 0x4d, 0x2, 0x335, 0x336, 0x5, 
    0x98, 0x4d, 0x2, 0x336, 0x337, 0x7, 0x94, 0x2, 0x2, 0x337, 0x338, 0x5, 
    0x76, 0x3c, 0x2, 0x338, 0x339, 0x7, 0x9d, 0x2, 0x2, 0x339, 0x362, 0x3, 
    0x2, 0x2, 0x2, 0x33a, 0x33b, 0x5, 0x98, 0x4d, 0x2, 0x33b, 0x33c, 0x7, 
    0x94, 0x2, 0x2, 0x33c, 0x341, 0x5, 0xa0, 0x51, 0x2, 0x33d, 0x33e, 0x7, 
    0x8a, 0x2, 0x2, 0x33e, 0x340, 0x5, 0xa0, 0x51, 0x2, 0x33f, 0x33d, 0x3, 
    0x2, 0x2, 0x2, 0x340, 0x343, 0x3, 0x2, 0x2, 0x2, 0x341, 0x33f, 0x3, 
    0x2, 0x2, 0x2, 0x341, 0x342, 0x3, 0x2, 0x2, 0x2, 0x342, 0x344, 0x3, 
    0x2, 0x2, 0x2, 0x343, 0x341, 0x3, 0x2, 0x2, 0x2, 0x344, 0x345, 0x7, 
    0x9d, 0x2, 0x2, 0x345, 0x362, 0x3, 0x2, 0x2, 0x2, 0x346, 0x347, 0x5, 
    0x98, 0x4d, 0x2, 0x347, 0x348, 0x7, 0x94, 0x2, 0x2, 0x348, 0x34d, 0x5, 
    0x74, 0x3b, 0x2, 0x349, 0x34a, 0x7, 0x8a, 0x2, 0x2, 0x34a, 0x34c, 0x5, 
    0x74, 0x3b, 0x2, 0x34b, 0x349, 0x3, 0x2, 0x2, 0x2, 0x34c, 0x34f, 0x3, 
    0x2, 0x2, 0x2, 0x34d, 0x34b, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x34e, 0x3, 
    0x2, 0x2, 0x2, 0x34e, 0x350, 0x3, 0x2, 0x2, 0x2, 0x34f, 0x34d, 0x3, 
    0x2, 0x2, 0x2, 0x350, 0x351, 0x7, 0x9d, 0x2, 0x2, 0x351, 0x362, 0x3, 
    0x2, 0x2, 0x2, 0x352, 0x353, 0x5, 0x98, 0x4d, 0x2, 0x353, 0x354, 0x7, 
    0x94, 0x2, 0x2, 0x354, 0x355, 0x5, 0x98, 0x4d, 0x2, 0x355, 0x35c, 0x5, 
    0x74, 0x3b, 0x2, 0x356, 0x357, 0x7, 0x8a, 0x2, 0x2, 0x357, 0x358, 0x5, 
    0x98, 0x4d, 0x2, 0x358, 0x359, 0x5, 0x74, 0x3b, 0x2, 0x359, 0x35b, 0x3, 
    0x2, 0x2, 0x2, 0x35a, 0x356, 0x3, 0x2, 0x2, 0x2, 0x35b, 0x35e, 0x3, 
    0x2, 0x2, 0x2, 0x35c, 0x35a, 0x3, 0x2, 0x2, 0x2, 0x35c, 0x35d, 0x3, 
    0x2, 0x2, 0x2, 0x35d, 0x35f, 0x3, 0x2, 0x2, 0x2, 0x35e, 0x35c, 0x3, 
    0x2, 0x2, 0x2, 0x35f, 0x360, 0x7, 0x9d, 0x2, 0x2, 0x360, 0x362, 0x3, 
    0x2, 0x2, 0x2, 0x361, 0x334, 0x3, 0x2, 0x2, 0x2, 0x361, 0x335, 0x3, 
    0x2, 0x2, 0x2, 0x361, 0x33a, 0x3, 0x2, 0x2, 0x2, 0x361, 0x346, 0x3, 
    0x2, 0x2, 0x2, 0x361, 0x352, 0x3, 0x2, 0x2, 0x2, 0x362, 0x75, 0x3, 0x2, 
    0x2, 0x2, 0x363, 0x368, 0x5, 0x78, 0x3d, 0x2, 0x364, 0x365, 0x7, 0x8a, 
    0x2, 0x2, 0x365, 0x367, 0x5, 0x78, 0x3d, 0x2, 0x366, 0x364, 0x3, 0x2, 
    0x2, 0x2, 0x367, 0x36a, 0x3, 0x2, 0x2, 0x2, 0x368, 0x366, 0x3, 0x2, 
    0x2, 0x2, 0x368, 0x369, 0x3, 0x2, 0x2, 0x2, 0x369, 0x77, 0x3, 0x2, 0x2, 
    0x2, 0x36a, 0x368, 0x3, 0x2, 0x2, 0x2, 0x36b, 0x36c, 0x5, 0x88, 0x45, 
    0x2, 0x36c, 0x36d, 0x7, 0x8d, 0x2, 0x2, 0x36d, 0x36f, 0x3, 0x2, 0x2, 
    0x2, 0x36e, 0x36b, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x36f, 0x3, 0x2, 0x2, 
    0x2, 0x36f, 0x370, 0x3, 0x2, 0x2, 0x2, 0x370, 0x377, 0x7, 0x86, 0x2, 
    0x2, 0x371, 0x372, 0x7, 0x94, 0x2, 0x2, 0x372, 0x373, 0x5, 0x3c, 0x1f, 
    0x2, 0x373, 0x374, 0x7, 0x9d, 0x2, 0x2, 0x374, 0x377, 0x3, 0x2, 0x2, 
    0x2, 0x375, 0x377, 0x5, 0x7a, 0x3e, 0x2, 0x376, 0x36e, 0x3, 0x2, 0x2, 
    0x2, 0x376, 0x371, 0x3, 0x2, 0x2, 0x2, 0x376, 0x375, 0x3, 0x2, 0x2, 
    0x2, 0x377, 0x79, 0x3, 0x2, 0x2, 0x2, 0x378, 0x379, 0x8, 0x3e, 0x1, 
    0x2, 0x379, 0x37b, 0x7, 0x14, 0x2, 0x2, 0x37a, 0x37c, 0x5, 0x7a, 0x3e, 
    0x2, 0x37b, 0x37a, 0x3, 0x2, 0x2, 0x2, 0x37b, 0x37c, 0x3, 0x2, 0x2, 
    0x2, 0x37c, 0x382, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x37e, 0x7, 0x7c, 0x2, 
    0x2, 0x37e, 0x37f, 0x5, 0x7a, 0x3e, 0x2, 0x37f, 0x380, 0x7, 0x6e, 0x2, 
    0x2, 0x380, 0x381, 0x5, 0x7a, 0x3e, 0x2, 0x381, 0x383, 0x3, 0x2, 0x2, 
    0x2, 0x382, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x383, 0x384, 0x3, 0x2, 0x2, 
    0x2, 0x384, 0x382, 0x3, 0x2, 0x2, 0x2, 0x384, 0x385, 0x3, 0x2, 0x2, 
    0x2, 0x385, 0x388, 0x3, 0x2, 0x2, 0x2, 0x386, 0x387, 0x7, 0x2b, 0x2, 
    0x2, 0x387, 0x389, 0x5, 0x7a, 0x3e, 0x2, 0x388, 0x386, 0x3, 0x2, 0x2, 
    0x2, 0x388, 0x389, 0x3, 0x2, 0x2, 0x2, 0x389, 0x38a, 0x3, 0x2, 0x2, 
    0x2, 0x38a, 0x38b, 0x7, 0x2c, 0x2, 0x2, 0x38b, 0x3d1, 0x3, 0x2, 0x2, 
    0x2, 0x38c, 0x38d, 0x7, 0x15, 0x2, 0x2, 0x38d, 0x38e, 0x7, 0x94, 0x2, 
    0x2, 0x38e, 0x38f, 0x5, 0x7a, 0x3e, 0x2, 0x38f, 0x390, 0x7, 0xd, 0x2, 
    0x2, 0x390, 0x391, 0x5, 0x74, 0x3b, 0x2, 0x391, 0x392, 0x7, 0x9d, 0x2, 
    0x2, 0x392, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x393, 0x394, 0x7, 0x2f, 0x2, 
    0x2, 0x394, 0x395, 0x7, 0x94, 0x2, 0x2, 0x395, 0x396, 0x7, 0x3, 0x2, 
    0x2, 0x396, 0x397, 0x7, 0x33, 0x2, 0x2, 0x397, 0x398, 0x5, 0x7a, 0x3e, 
    0x2, 0x398, 0x399, 0x7, 0x9d, 0x2, 0x2, 0x399, 0x3d1, 0x3, 0x2, 0x2, 
    0x2, 0x39a, 0x39b, 0x7, 0x3f, 0x2, 0x2, 0x39b, 0x39c, 0x5, 0x7a, 0x3e, 
    0x2, 0x39c, 0x39d, 0x7, 0x3, 0x2, 0x2, 0x39d, 0x3d1, 0x3, 0x2, 0x2, 
    0x2, 0x39e, 0x39f, 0x7, 0x73, 0x2, 0x2, 0x39f, 0x3a0, 0x7, 0x94, 0x2, 
    0x2, 0x3a0, 0x3a1, 0x9, 0xe, 0x2, 0x2, 0x3a1, 0x3a2, 0x7, 0x84, 0x2, 
    0x2, 0x3a2, 0x3a3, 0x7, 0x33, 0x2, 0x2, 0x3a3, 0x3a4, 0x5, 0x7a, 0x3e, 
    0x2, 0x3a4, 0x3a5, 0x7, 0x9d, 0x2, 0x2, 0x3a5, 0x3d1, 0x3, 0x2, 0x2, 
    0x2, 0x3a6, 0x3ac, 0x5, 0x98, 0x4d, 0x2, 0x3a7, 0x3a9, 0x7, 0x94, 0x2, 
    0x2, 0x3a8, 0x3aa, 0x5, 0x76, 0x3c, 0x2, 0x3a9, 0x3a8, 0x3, 0x2, 0x2, 
    0x2, 0x3a9, 0x3aa, 0x3, 0x2, 0x2, 0x2, 0x3aa, 0x3ab, 0x3, 0x2, 0x2, 
    0x2, 0x3ab, 0x3ad, 0x7, 0x9d, 0x2, 0x2, 0x3ac, 0x3a7, 0x3, 0x2, 0x2, 
    0x2, 0x3ac, 0x3ad, 0x3, 0x2, 0x2, 0x2, 0x3ad, 0x3ae, 0x3, 0x2, 0x2, 
    0x2, 0x3ae, 0x3b0, 0x7, 0x94, 0x2, 0x2, 0x3af, 0x3b1, 0x5, 0x7c, 0x3f, 
    0x2, 0x3b0, 0x3af, 0x3, 0x2, 0x2, 0x2, 0x3b0, 0x3b1, 0x3, 0x2, 0x2, 
    0x2, 0x3b1, 0x3b2, 0x3, 0x2, 0x2, 0x2, 0x3b2, 0x3b3, 0x7, 0x9d, 0x2, 
    0x2, 0x3b3, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b5, 0x5, 0x9c, 0x4f, 
    0x2, 0x3b5, 0x3b6, 0x5, 0x7a, 0x3e, 0xf, 0x3b6, 0x3d1, 0x3, 0x2, 0x2, 
    0x2, 0x3b7, 0x3b8, 0x5, 0x88, 0x45, 0x2, 0x3b8, 0x3b9, 0x7, 0x8d, 0x2, 
    0x2, 0x3b9, 0x3bb, 0x3, 0x2, 0x2, 0x2, 0x3ba, 0x3b7, 0x3, 0x2, 0x2, 
    0x2, 0x3ba, 0x3bb, 0x3, 0x2, 0x2, 0x2, 0x3bb, 0x3bc, 0x3, 0x2, 0x2, 
    0x2, 0x3bc, 0x3d1, 0x7, 0x86, 0x2, 0x2, 0x3bd, 0x3be, 0x7, 0x94, 0x2, 
    0x2, 0x3be, 0x3bf, 0x5, 0x3c, 0x1f, 0x2, 0x3bf, 0x3c0, 0x7, 0x9d, 0x2, 
    0x2, 0x3c0, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3c2, 0x7, 0x94, 0x2, 
    0x2, 0x3c2, 0x3c3, 0x5, 0x7a, 0x3e, 0x2, 0x3c3, 0x3c4, 0x7, 0x9d, 0x2, 
    0x2, 0x3c4, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3c5, 0x3c6, 0x7, 0x94, 0x2, 
    0x2, 0x3c6, 0x3c7, 0x5, 0x76, 0x3c, 0x2, 0x3c7, 0x3c8, 0x7, 0x9d, 0x2, 
    0x2, 0x3c8, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3c9, 0x3cb, 0x7, 0x92, 0x2, 
    0x2, 0x3ca, 0x3cc, 0x5, 0x76, 0x3c, 0x2, 0x3cb, 0x3ca, 0x3, 0x2, 0x2, 
    0x2, 0x3cb, 0x3cc, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3cd, 0x3, 0x2, 0x2, 
    0x2, 0x3cd, 0x3d1, 0x7, 0x9c, 0x2, 0x2, 0x3ce, 0x3d1, 0x5, 0x82, 0x42, 
    0x2, 0x3cf, 0x3d1, 0x5, 0x94, 0x4b, 0x2, 0x3d0, 0x378, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x393, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x39a, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x39e, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x3a6, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x3b4, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x3ba, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x3bd, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x3c1, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x3c5, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x3c9, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x3ce, 0x3, 0x2, 0x2, 
    0x2, 0x3d0, 0x3cf, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3fa, 0x3, 0x2, 0x2, 
    0x2, 0x3d2, 0x3d3, 0xc, 0xd, 0x2, 0x2, 0x3d3, 0x3d4, 0x5, 0x9e, 0x50, 
    0x2, 0x3d4, 0x3d5, 0x5, 0x7a, 0x3e, 0xe, 0x3d5, 0x3f9, 0x3, 0x2, 0x2, 
    0x2, 0x3d6, 0x3d7, 0xc, 0xc, 0x2, 0x2, 0x3d7, 0x3d8, 0x7, 0x99, 0x2, 
    0x2, 0x3d8, 0x3d9, 0x5, 0x7a, 0x3e, 0x2, 0x3d9, 0x3da, 0x7, 0x89, 0x2, 
    0x2, 0x3da, 0x3db, 0x5, 0x7a, 0x3e, 0xd, 0x3db, 0x3f9, 0x3, 0x2, 0x2, 
    0x2, 0x3dc, 0x3de, 0xc, 0xb, 0x2, 0x2, 0x3dd, 0x3df, 0x7, 0x50, 0x2, 
    0x2, 0x3de, 0x3dd, 0x3, 0x2, 0x2, 0x2, 0x3de, 0x3df, 0x3, 0x2, 0x2, 
    0x2, 0x3df, 0x3e0, 0x3, 0x2, 0x2, 0x2, 0x3e0, 0x3e1, 0x7, 0x11, 0x2, 
    0x2, 0x3e1, 0x3e2, 0x5, 0x7a, 0x3e, 0x2, 0x3e2, 0x3e3, 0x7, 0x9, 0x2, 
    0x2, 0x3e3, 0x3e4, 0x5, 0x7a, 0x3e, 0xc, 0x3e4, 0x3f9, 0x3, 0x2, 0x2, 
    0x2, 0x3e5, 0x3e6, 0xc, 0x11, 0x2, 0x2, 0x3e6, 0x3e7, 0x7, 0x92, 0x2, 
    0x2, 0x3e7, 0x3e8, 0x5, 0x7a, 0x3e, 0x2, 0x3e8, 0x3e9, 0x7, 0x9c, 0x2, 
    0x2, 0x3e9, 0x3f9, 0x3, 0x2, 0x2, 0x2, 0x3ea, 0x3eb, 0xc, 0x10, 0x2, 
    0x2, 0x3eb, 0x3ec, 0x7, 0x8d, 0x2, 0x2, 0x3ec, 0x3f9, 0x7, 0x83, 0x2, 
    0x2, 0x3ed, 0x3ee, 0xc, 0xe, 0x2, 0x2, 0x3ee, 0x3f0, 0x7, 0x41, 0x2, 
    0x2, 0x3ef, 0x3f1, 0x7, 0x50, 0x2, 0x2, 0x3f0, 0x3ef, 0x3, 0x2, 0x2, 
    0x2, 0x3f0, 0x3f1, 0x3, 0x2, 0x2, 0x2, 0x3f1, 0x3f2, 0x3, 0x2, 0x2, 
    0x2, 0x3f2, 0x3f9, 0x7, 0x51, 0x2, 0x2, 0x3f3, 0x3f5, 0xc, 0xa, 0x2, 
    0x2, 0x3f4, 0x3f6, 0x7, 0xd, 0x2, 0x2, 0x3f5, 0x3f4, 0x3, 0x2, 0x2, 
    0x2, 0x3f5, 0x3f6, 0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3f7, 0x3, 0x2, 0x2, 
    0x2, 0x3f7, 0x3f9, 0x5, 0x98, 0x4d, 0x2, 0x3f8, 0x3d2, 0x3, 0x2, 0x2, 
    0x2, 0x3f8, 0x3d6, 0x3, 0x2, 0x2, 0x2, 0x3f8, 0x3dc, 0x3, 0x2, 0x2, 
    0x2, 0x3f8, 0x3e5, 0x3, 0x2, 0x2, 0x2, 0x3f8, 0x3ea, 0x3, 0x2, 0x2, 
    0x2, 0x3f8, 0x3ed, 0x3, 0x2, 0x2, 0x2, 0x3f8, 0x3f3, 0x3, 0x2, 0x2, 
    0x2, 0x3f9, 0x3fc, 0x3, 0x2, 0x2, 0x2, 0x3fa, 0x3f8, 0x3, 0x2, 0x2, 
    0x2, 0x3fa, 0x3fb, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x7b, 0x3, 0x2, 0x2, 0x2, 
    0x3fc, 0x3fa, 0x3, 0x2, 0x2, 0x2, 0x3fd, 0x402, 0x5, 0x7e, 0x40, 0x2, 
    0x3fe, 0x3ff, 0x7, 0x8a, 0x2, 0x2, 0x3ff, 0x401, 0x5, 0x7e, 0x40, 0x2, 
    0x400, 0x3fe, 0x3, 0x2, 0x2, 0x2, 0x401, 0x404, 0x3, 0x2, 0x2, 0x2, 
    0x402, 0x400, 0x3, 0x2, 0x2, 0x2, 0x402, 0x403, 0x3, 0x2, 0x2, 0x2, 
    0x403, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x404, 0x402, 0x3, 0x2, 0x2, 0x2, 0x405, 
    0x408, 0x5, 0x80, 0x41, 0x2, 0x406, 0x408, 0x5, 0x7a, 0x3e, 0x2, 0x407, 
    0x405, 0x3, 0x2, 0x2, 0x2, 0x407, 0x406, 0x3, 0x2, 0x2, 0x2, 0x408, 
    0x7f, 0x3, 0x2, 0x2, 0x2, 0x409, 0x40a, 0x7, 0x94, 0x2, 0x2, 0x40a, 
    0x40f, 0x5, 0x98, 0x4d, 0x2, 0x40b, 0x40c, 0x7, 0x8a, 0x2, 0x2, 0x40c, 
    0x40e, 0x5, 0x98, 0x4d, 0x2, 0x40d, 0x40b, 0x3, 0x2, 0x2, 0x2, 0x40e, 
    0x411, 0x3, 0x2, 0x2, 0x2, 0x40f, 0x40d, 0x3, 0x2, 0x2, 0x2, 0x40f, 
    0x410, 0x3, 0x2, 0x2, 0x2, 0x410, 0x412, 0x3, 0x2, 0x2, 0x2, 0x411, 
    0x40f, 0x3, 0x2, 0x2, 0x2, 0x412, 0x413, 0x7, 0x9d, 0x2, 0x2, 0x413, 
    0x41d, 0x3, 0x2, 0x2, 0x2, 0x414, 0x419, 0x5, 0x98, 0x4d, 0x2, 0x415, 
    0x416, 0x7, 0x8a, 0x2, 0x2, 0x416, 0x418, 0x5, 0x98, 0x4d, 0x2, 0x417, 
    0x415, 0x3, 0x2, 0x2, 0x2, 0x418, 0x41b, 0x3, 0x2, 0x2, 0x2, 0x419, 
    0x417, 0x3, 0x2, 0x2, 0x2, 0x419, 0x41a, 0x3, 0x2, 0x2, 0x2, 0x41a, 
    0x41d, 0x3, 0x2, 0x2, 0x2, 0x41b, 0x419, 0x3, 0x2, 0x2, 0x2, 0x41c, 
    0x409, 0x3, 0x2, 0x2, 0x2, 0x41c, 0x414, 0x3, 0x2, 0x2, 0x2, 0x41d, 
    0x41e, 0x3, 0x2, 0x2, 0x2, 0x41e, 0x41f, 0x7, 0x85, 0x2, 0x2, 0x41f, 
    0x420, 0x5, 0x7a, 0x3e, 0x2, 0x420, 0x81, 0x3, 0x2, 0x2, 0x2, 0x421, 
    0x422, 0x5, 0x88, 0x45, 0x2, 0x422, 0x423, 0x7, 0x8d, 0x2, 0x2, 0x423, 
    0x425, 0x3, 0x2, 0x2, 0x2, 0x424, 0x421, 0x3, 0x2, 0x2, 0x2, 0x424, 
    0x425, 0x3, 0x2, 0x2, 0x2, 0x425, 0x426, 0x3, 0x2, 0x2, 0x2, 0x426, 
    0x427, 0x5, 0x84, 0x43, 0x2, 0x427, 0x83, 0x3, 0x2, 0x2, 0x2, 0x428, 
    0x42b, 0x5, 0x98, 0x4d, 0x2, 0x429, 0x42a, 0x7, 0x8d, 0x2, 0x2, 0x42a, 
    0x42c, 0x5, 0x98, 0x4d, 0x2, 0x42b, 0x429, 0x3, 0x2, 0x2, 0x2, 0x42b, 
    0x42c, 0x3, 0x2, 0x2, 0x2, 0x42c, 0x85, 0x3, 0x2, 0x2, 0x2, 0x42d, 0x42e, 
    0x8, 0x44, 0x1, 0x2, 0x42e, 0x43b, 0x5, 0x88, 0x45, 0x2, 0x42f, 0x430, 
    0x5, 0x98, 0x4d, 0x2, 0x430, 0x432, 0x7, 0x94, 0x2, 0x2, 0x431, 0x433, 
    0x5, 0x8a, 0x46, 0x2, 0x432, 0x431, 0x3, 0x2, 0x2, 0x2, 0x432, 0x433, 
    0x3, 0x2, 0x2, 0x2, 0x433, 0x434, 0x3, 0x2, 0x2, 0x2, 0x434, 0x435, 
    0x7, 0x9d, 0x2, 0x2, 0x435, 0x43b, 0x3, 0x2, 0x2, 0x2, 0x436, 0x437, 
    0x7, 0x94, 0x2, 0x2, 0x437, 0x438, 0x5, 0x3c, 0x1f, 0x2, 0x438, 0x439, 
    0x7, 0x9d, 0x2, 0x2, 0x439, 0x43b, 0x3, 0x2, 0x2, 0x2, 0x43a, 0x42d, 
    0x3, 0x2, 0x2, 0x2, 0x43a, 0x42f, 0x3, 0x2, 0x2, 0x2, 0x43a, 0x436, 
    0x3, 0x2, 0x2, 0x2, 0x43b, 0x443, 0x3, 0x2, 0x2, 0x2, 0x43c, 0x43e, 
    0xc, 0x3, 0x2, 0x2, 0x43d, 0x43f, 0x7, 0xd, 0x2, 0x2, 0x43e, 0x43d, 
    0x3, 0x2, 0x2, 0x2, 0x43e, 0x43f, 0x3, 0x2, 0x2, 0x2, 0x43f, 0x440, 
    0x3, 0x2, 0x2, 0x2, 0x440, 0x442, 0x5, 0x98, 0x4d, 0x2, 0x441, 0x43c, 
    0x3, 0x2, 0x2, 0x2, 0x442, 0x445, 0x3, 0x2, 0x2, 0x2, 0x443, 0x441, 
    0x3, 0x2, 0x2, 0x2, 0x443, 0x444, 0x3, 0x2, 0x2, 0x2, 0x444, 0x87, 0x3, 
    0x2, 0x2, 0x2, 0x445, 0x443, 0x3, 0x2, 0x2, 0x2, 0x446, 0x447, 0x5, 
    0x8e, 0x48, 0x2, 0x447, 0x448, 0x7, 0x8d, 0x2, 0x2, 0x448, 0x44a, 0x3, 
    0x2, 0x2, 0x2, 0x449, 0x446, 0x3, 0x2, 0x2, 0x2, 0x449, 0x44a, 0x3, 
    0x2, 0x2, 0x2, 0x44a, 0x44b, 0x3, 0x2, 0x2, 0x2, 0x44b, 0x44c, 0x5, 
    0x98, 0x4d, 0x2, 0x44c, 0x89, 0x3, 0x2, 0x2, 0x2, 0x44d, 0x452, 0x5, 
    0x8c, 0x47, 0x2, 0x44e, 0x44f, 0x7, 0x8a, 0x2, 0x2, 0x44f, 0x451, 0x5, 
    0x8c, 0x47, 0x2, 0x450, 0x44e, 0x3, 0x2, 0x2, 0x2, 0x451, 0x454, 0x3, 
    0x2, 0x2, 0x2, 0x452, 0x450, 0x3, 0x2, 0x2, 0x2, 0x452, 0x453, 0x3, 
    0x2, 0x2, 0x2, 0x453, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x454, 0x452, 0x3, 0x2, 
    0x2, 0x2, 0x455, 0x458, 0x5, 0x88, 0x45, 0x2, 0x456, 0x458, 0x5, 0x94, 
    0x4b, 0x2, 0x457, 0x455, 0x3, 0x2, 0x2, 0x2, 0x457, 0x456, 0x3, 0x2, 
    0x2, 0x2, 0x458, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x459, 0x45a, 0x5, 0x98, 
    0x4d, 0x2, 0x45a, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x45b, 0x464, 0x7, 0x81, 
    0x2, 0x2, 0x45c, 0x45d, 0x7, 0x83, 0x2, 0x2, 0x45d, 0x45f, 0x7, 0x8d, 
    0x2, 0x2, 0x45e, 0x460, 0x7, 0x83, 0x2, 0x2, 0x45f, 0x45e, 0x3, 0x2, 
    0x2, 0x2, 0x45f, 0x460, 0x3, 0x2, 0x2, 0x2, 0x460, 0x464, 0x3, 0x2, 
    0x2, 0x2, 0x461, 0x462, 0x7, 0x8d, 0x2, 0x2, 0x462, 0x464, 0x7, 0x83, 
    0x2, 0x2, 0x463, 0x45b, 0x3, 0x2, 0x2, 0x2, 0x463, 0x45c, 0x3, 0x2, 
    0x2, 0x2, 0x463, 0x461, 0x3, 0x2, 0x2, 0x2, 0x464, 0x91, 0x3, 0x2, 0x2, 
    0x2, 0x465, 0x467, 0x9, 0xf, 0x2, 0x2, 0x466, 0x465, 0x3, 0x2, 0x2, 
    0x2, 0x466, 0x467, 0x3, 0x2, 0x2, 0x2, 0x467, 0x46d, 0x3, 0x2, 0x2, 
    0x2, 0x468, 0x46e, 0x5, 0x90, 0x49, 0x2, 0x469, 0x46e, 0x7, 0x82, 0x2, 
    0x2, 0x46a, 0x46e, 0x7, 0x83, 0x2, 0x2, 0x46b, 0x46e, 0x7, 0x3c, 0x2, 
    0x2, 0x46c, 0x46e, 0x7, 0x4e, 0x2, 0x2, 0x46d, 0x468, 0x3, 0x2, 0x2, 
    0x2, 0x46d, 0x469, 0x3, 0x2, 0x2, 0x2, 0x46d, 0x46a, 0x3, 0x2, 0x2, 
    0x2, 0x46d, 0x46b, 0x3, 0x2, 0x2, 0x2, 0x46d, 0x46c, 0x3, 0x2, 0x2, 
    0x2, 0x46e, 0x93, 0x3, 0x2, 0x2, 0x2, 0x46f, 0x477, 0x5, 0x92, 0x4a, 
    0x2, 0x470, 0x477, 0x7, 0x84, 0x2, 0x2, 0x471, 0x477, 0x7, 0x51, 0x2, 
    0x2, 0x472, 0x473, 0x5, 0x98, 0x4d, 0x2, 0x473, 0x474, 0x7, 0x94, 0x2, 
    0x2, 0x474, 0x475, 0x7, 0x9d, 0x2, 0x2, 0x475, 0x477, 0x3, 0x2, 0x2, 
    0x2, 0x476, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x476, 0x470, 0x3, 0x2, 0x2, 
    0x2, 0x476, 0x471, 0x3, 0x2, 0x2, 0x2, 0x476, 0x472, 0x3, 0x2, 0x2, 
    0x2, 0x477, 0x95, 0x3, 0x2, 0x2, 0x2, 0x478, 0x479, 0x9, 0x10, 0x2, 
    0x2, 0x479, 0x97, 0x3, 0x2, 0x2, 0x2, 0x47a, 0x47e, 0x7, 0x80, 0x2, 
    0x2, 0x47b, 0x47e, 0x7, 0x3, 0x2, 0x2, 0x47c, 0x47e, 0x5, 0x96, 0x4c, 
    0x2, 0x47d, 0x47a, 0x3, 0x2, 0x2, 0x2, 0x47d, 0x47b, 0x3, 0x2, 0x2, 
    0x2, 0x47d, 0x47c, 0x3, 0x2, 0x2, 0x2, 0x47e, 0x99, 0x3, 0x2, 0x2, 0x2, 
    0x47f, 0x482, 0x5, 0x98, 0x4d, 0x2, 0x480, 0x482, 0x7, 0x51, 0x2, 0x2, 
    0x481, 0x47f, 0x3, 0x2, 0x2, 0x2, 0x481, 0x480, 0x3, 0x2, 0x2, 0x2, 
    0x482, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x483, 0x484, 0x9, 0x11, 0x2, 0x2, 
    0x484, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x485, 0x4a0, 0x7, 0x8b, 0x2, 0x2, 
    0x486, 0x4a0, 0x7, 0x86, 0x2, 0x2, 0x487, 0x4a0, 0x7, 0x9f, 0x2, 0x2, 
    0x488, 0x4a0, 0x7, 0x98, 0x2, 0x2, 0x489, 0x4a0, 0x7, 0x8c, 0x2, 0x2, 
    0x48a, 0x4a0, 0x7, 0x97, 0x2, 0x2, 0x48b, 0x4a0, 0x7, 0x8e, 0x2, 0x2, 
    0x48c, 0x4a0, 0x7, 0x8f, 0x2, 0x2, 0x48d, 0x4a0, 0x7, 0x96, 0x2, 0x2, 
    0x48e, 0x4a0, 0x7, 0x93, 0x2, 0x2, 0x48f, 0x4a0, 0x7, 0x90, 0x2, 0x2, 
    0x490, 0x4a0, 0x7, 0x95, 0x2, 0x2, 0x491, 0x4a0, 0x7, 0x91, 0x2, 0x2, 
    0x492, 0x4a0, 0x7, 0x9, 0x2, 0x2, 0x493, 0x4a0, 0x7, 0x56, 0x2, 0x2, 
    0x494, 0x496, 0x7, 0x50, 0x2, 0x2, 0x495, 0x494, 0x3, 0x2, 0x2, 0x2, 
    0x495, 0x496, 0x3, 0x2, 0x2, 0x2, 0x496, 0x497, 0x3, 0x2, 0x2, 0x2, 
    0x497, 0x4a0, 0x7, 0x47, 0x2, 0x2, 0x498, 0x49a, 0x7, 0x35, 0x2, 0x2, 
    0x499, 0x498, 0x3, 0x2, 0x2, 0x2, 0x499, 0x49a, 0x3, 0x2, 0x2, 0x2, 
    0x49a, 0x49c, 0x3, 0x2, 0x2, 0x2, 0x49b, 0x49d, 0x7, 0x50, 0x2, 0x2, 
    0x49c, 0x49b, 0x3, 0x2, 0x2, 0x2, 0x49c, 0x49d, 0x3, 0x2, 0x2, 0x2, 
    0x49d, 0x49e, 0x3, 0x2, 0x2, 0x2, 0x49e, 0x4a0, 0x7, 0x3b, 0x2, 0x2, 
    0x49f, 0x485, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x486, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x487, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x488, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x489, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x48a, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x48b, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x48c, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x48d, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x48e, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x48f, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x490, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x491, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x492, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x493, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x495, 0x3, 0x2, 0x2, 0x2, 
    0x49f, 0x499, 0x3, 0x2, 0x2, 0x2, 0x4a0, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x4a1, 
    0x4a2, 0x7, 0x84, 0x2, 0x2, 0x4a2, 0x4a3, 0x7, 0x8f, 0x2, 0x2, 0x4a3, 
    0x4a4, 0x5, 0x92, 0x4a, 0x2, 0x4a4, 0xa1, 0x3, 0x2, 0x2, 0x2, 0xa0, 
    0xa7, 0xab, 0xb3, 0xb7, 0xc7, 0xd1, 0xdc, 0xdf, 0xe6, 0xeb, 0xf1, 0xfb, 
    0x104, 0x10b, 0x10e, 0x11c, 0x120, 0x128, 0x12c, 0x12f, 0x132, 0x138, 
    0x13e, 0x142, 0x145, 0x148, 0x14f, 0x154, 0x15f, 0x16a, 0x16e, 0x172, 
    0x175, 0x178, 0x17b, 0x17e, 0x181, 0x195, 0x19a, 0x19f, 0x1a2, 0x1a9, 
    0x1ad, 0x1b1, 0x1b6, 0x1b8, 0x1c5, 0x1cf, 0x1d4, 0x1d9, 0x1de, 0x1e0, 
    0x1e4, 0x1f2, 0x1f7, 0x1fe, 0x203, 0x207, 0x211, 0x214, 0x217, 0x21e, 
    0x22c, 0x235, 0x239, 0x23d, 0x241, 0x244, 0x247, 0x24a, 0x24d, 0x250, 
    0x253, 0x256, 0x259, 0x25c, 0x25f, 0x267, 0x26d, 0x270, 0x281, 0x293, 
    0x29e, 0x2a6, 0x2ad, 0x2af, 0x2b3, 0x2b8, 0x2ba, 0x2bd, 0x2c2, 0x2c4, 
    0x2c7, 0x2cc, 0x2ce, 0x2d0, 0x2d3, 0x2d8, 0x2e3, 0x2e8, 0x2ef, 0x2f4, 
    0x2f8, 0x2fc, 0x301, 0x308, 0x315, 0x31b, 0x320, 0x325, 0x328, 0x32a, 
    0x341, 0x34d, 0x35c, 0x361, 0x368, 0x36e, 0x376, 0x37b, 0x384, 0x388, 
    0x3a9, 0x3ac, 0x3b0, 0x3ba, 0x3cb, 0x3d0, 0x3de, 0x3f0, 0x3f5, 0x3f8, 
    0x3fa, 0x402, 0x407, 0x40f, 0x419, 0x41c, 0x424, 0x42b, 0x432, 0x43a, 
    0x43e, 0x443, 0x449, 0x452, 0x457, 0x45f, 0x463, 0x466, 0x46d, 0x476, 
    0x47d, 0x481, 0x495, 0x499, 0x49c, 0x49f, 
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
