
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


//----------------- InputContext ------------------------------------------------------------------

ClickHouseParser::InputContext::InputContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::InsertStmtContext* ClickHouseParser::InputContext::insertStmt() {
  return getRuleContext<ClickHouseParser::InsertStmtContext>(0);
}

ClickHouseParser::QueryListContext* ClickHouseParser::InputContext::queryList() {
  return getRuleContext<ClickHouseParser::QueryListContext>(0);
}


size_t ClickHouseParser::InputContext::getRuleIndex() const {
  return ClickHouseParser::RuleInput;
}

antlrcpp::Any ClickHouseParser::InputContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitInput(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::InputContext* ClickHouseParser::input() {
  InputContext *_localctx = _tracker.createInstance<InputContext>(_ctx, getState());
  enterRule(_localctx, 0, ClickHouseParser::RuleInput);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(184);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(182);
      insertStmt();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(183);
      queryList();
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
  enterRule(_localctx, 2, ClickHouseParser::RuleQueryList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(186);
    queryStmt();
    setState(191);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(187);
        match(ClickHouseParser::SEMICOLON);
        setState(188);
        queryStmt(); 
      }
      setState(193);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx);
    }
    setState(195);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SEMICOLON) {
      setState(194);
      match(ClickHouseParser::SEMICOLON);
    }
    setState(197);
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
  enterRule(_localctx, 4, ClickHouseParser::RuleQueryStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(199);
    query();
    setState(203);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
    case 1: {
      setState(200);
      match(ClickHouseParser::INTO);
      setState(201);
      match(ClickHouseParser::OUTFILE);
      setState(202);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    }
    setState(207);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      setState(205);
      match(ClickHouseParser::FORMAT);
      setState(206);
      identifierOrNull();
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

//----------------- QueryContext ------------------------------------------------------------------

ClickHouseParser::QueryContext::QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::AlterStmtContext* ClickHouseParser::QueryContext::alterStmt() {
  return getRuleContext<ClickHouseParser::AlterStmtContext>(0);
}

ClickHouseParser::AnalyzeStmtContext* ClickHouseParser::QueryContext::analyzeStmt() {
  return getRuleContext<ClickHouseParser::AnalyzeStmtContext>(0);
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
  enterRule(_localctx, 6, ClickHouseParser::RuleQuery);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(225);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::ALTER: {
        enterOuterAlt(_localctx, 1);
        setState(209);
        alterStmt();
        break;
      }

      case ClickHouseParser::ANALYZE: {
        enterOuterAlt(_localctx, 2);
        setState(210);
        analyzeStmt();
        break;
      }

      case ClickHouseParser::CHECK: {
        enterOuterAlt(_localctx, 3);
        setState(211);
        checkStmt();
        break;
      }

      case ClickHouseParser::ATTACH:
      case ClickHouseParser::CREATE: {
        enterOuterAlt(_localctx, 4);
        setState(212);
        createStmt();
        break;
      }

      case ClickHouseParser::DESC:
      case ClickHouseParser::DESCRIBE: {
        enterOuterAlt(_localctx, 5);
        setState(213);
        describeStmt();
        break;
      }

      case ClickHouseParser::DETACH:
      case ClickHouseParser::DROP: {
        enterOuterAlt(_localctx, 6);
        setState(214);
        dropStmt();
        break;
      }

      case ClickHouseParser::EXISTS: {
        enterOuterAlt(_localctx, 7);
        setState(215);
        existsStmt();
        break;
      }

      case ClickHouseParser::INSERT: {
        enterOuterAlt(_localctx, 8);
        setState(216);
        insertStmt();
        break;
      }

      case ClickHouseParser::OPTIMIZE: {
        enterOuterAlt(_localctx, 9);
        setState(217);
        optimizeStmt();
        break;
      }

      case ClickHouseParser::RENAME: {
        enterOuterAlt(_localctx, 10);
        setState(218);
        renameStmt();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 11);
        setState(219);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::SET: {
        enterOuterAlt(_localctx, 12);
        setState(220);
        setStmt();
        break;
      }

      case ClickHouseParser::SHOW: {
        enterOuterAlt(_localctx, 13);
        setState(221);
        showStmt();
        break;
      }

      case ClickHouseParser::SYSTEM: {
        enterOuterAlt(_localctx, 14);
        setState(222);
        systemStmt();
        break;
      }

      case ClickHouseParser::TRUNCATE: {
        enterOuterAlt(_localctx, 15);
        setState(223);
        truncateStmt();
        break;
      }

      case ClickHouseParser::USE: {
        enterOuterAlt(_localctx, 16);
        setState(224);
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
  enterRule(_localctx, 8, ClickHouseParser::RuleAlterStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<AlterStmtContext *>(_tracker.createInstance<ClickHouseParser::AlterTableStmtContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(227);
    match(ClickHouseParser::ALTER);
    setState(228);
    match(ClickHouseParser::TABLE);
    setState(229);
    tableIdentifier();
    setState(230);
    alterTableClause();
    setState(235);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(231);
      match(ClickHouseParser::COMMA);
      setState(232);
      alterTableClause();
      setState(237);
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

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::IN() {
  return getToken(ClickHouseParser::IN, 0);
}

ClickHouseParser::PartitionClauseContext* ClickHouseParser::AlterTableClauseClearContext::partitionClause() {
  return getRuleContext<ClickHouseParser::PartitionClauseContext>(0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::IF() {
  return getToken(ClickHouseParser::IF, 0);
}

tree::TerminalNode* ClickHouseParser::AlterTableClauseClearContext::EXISTS() {
  return getToken(ClickHouseParser::EXISTS, 0);
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
ClickHouseParser::AlterTableClauseContext* ClickHouseParser::alterTableClause() {
  AlterTableClauseContext *_localctx = _tracker.createInstance<AlterTableClauseContext>(_ctx, getState());
  enterRule(_localctx, 10, ClickHouseParser::RuleAlterTableClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(305);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAddContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(238);
      match(ClickHouseParser::ADD);
      setState(239);
      match(ClickHouseParser::COLUMN);
      setState(243);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
      case 1: {
        setState(240);
        match(ClickHouseParser::IF);
        setState(241);
        match(ClickHouseParser::NOT);
        setState(242);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(245);
      tableColumnDfnt();
      setState(248);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AFTER) {
        setState(246);
        match(ClickHouseParser::AFTER);
        setState(247);
        nestedIdentifier();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseAttachContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(250);
      match(ClickHouseParser::ATTACH);
      setState(251);
      partitionClause();
      setState(254);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM) {
        setState(252);
        match(ClickHouseParser::FROM);
        setState(253);
        tableIdentifier();
      }
      break;
    }

    case 3: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseClearContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(256);
      match(ClickHouseParser::CLEAR);
      setState(257);
      match(ClickHouseParser::COLUMN);
      setState(260);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
      case 1: {
        setState(258);
        match(ClickHouseParser::IF);
        setState(259);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(262);
      nestedIdentifier();
      setState(263);
      match(ClickHouseParser::IN);
      setState(264);
      partitionClause();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseCommentContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(266);
      match(ClickHouseParser::COMMENT);
      setState(267);
      match(ClickHouseParser::COLUMN);
      setState(270);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
      case 1: {
        setState(268);
        match(ClickHouseParser::IF);
        setState(269);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(272);
      nestedIdentifier();
      setState(273);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDeleteContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(275);
      match(ClickHouseParser::DELETE);
      setState(276);
      match(ClickHouseParser::WHERE);
      setState(277);
      columnExpr(0);
      break;
    }

    case 6: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDetachContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(278);
      match(ClickHouseParser::DETACH);
      setState(279);
      partitionClause();
      break;
    }

    case 7: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropColumnContext>(_localctx));
      enterOuterAlt(_localctx, 7);
      setState(280);
      match(ClickHouseParser::DROP);
      setState(281);
      match(ClickHouseParser::COLUMN);
      setState(284);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
      case 1: {
        setState(282);
        match(ClickHouseParser::IF);
        setState(283);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(286);
      nestedIdentifier();
      break;
    }

    case 8: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseDropPartitionContext>(_localctx));
      enterOuterAlt(_localctx, 8);
      setState(287);
      match(ClickHouseParser::DROP);
      setState(288);
      partitionClause();
      break;
    }

    case 9: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseModifyContext>(_localctx));
      enterOuterAlt(_localctx, 9);
      setState(289);
      match(ClickHouseParser::MODIFY);
      setState(290);
      match(ClickHouseParser::COLUMN);
      setState(293);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx)) {
      case 1: {
        setState(291);
        match(ClickHouseParser::IF);
        setState(292);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(295);
      tableColumnDfnt();
      break;
    }

    case 10: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseOrderByContext>(_localctx));
      enterOuterAlt(_localctx, 10);
      setState(296);
      match(ClickHouseParser::MODIFY);
      setState(297);
      match(ClickHouseParser::ORDER);
      setState(298);
      match(ClickHouseParser::BY);
      setState(299);
      columnExpr(0);
      break;
    }

    case 11: {
      _localctx = dynamic_cast<AlterTableClauseContext *>(_tracker.createInstance<ClickHouseParser::AlterTableClauseReplaceContext>(_localctx));
      enterOuterAlt(_localctx, 11);
      setState(300);
      match(ClickHouseParser::REPLACE);
      setState(301);
      partitionClause();
      setState(302);
      match(ClickHouseParser::FROM);
      setState(303);
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

//----------------- AnalyzeStmtContext ------------------------------------------------------------------

ClickHouseParser::AnalyzeStmtContext::AnalyzeStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::AnalyzeStmtContext::ANALYZE() {
  return getToken(ClickHouseParser::ANALYZE, 0);
}

ClickHouseParser::QueryStmtContext* ClickHouseParser::AnalyzeStmtContext::queryStmt() {
  return getRuleContext<ClickHouseParser::QueryStmtContext>(0);
}


size_t ClickHouseParser::AnalyzeStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleAnalyzeStmt;
}

antlrcpp::Any ClickHouseParser::AnalyzeStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAnalyzeStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::AnalyzeStmtContext* ClickHouseParser::analyzeStmt() {
  AnalyzeStmtContext *_localctx = _tracker.createInstance<AnalyzeStmtContext>(_ctx, getState());
  enterRule(_localctx, 12, ClickHouseParser::RuleAnalyzeStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(307);
    match(ClickHouseParser::ANALYZE);
    setState(308);
    queryStmt();
   
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
  enterRule(_localctx, 14, ClickHouseParser::RuleCheckStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(310);
    match(ClickHouseParser::CHECK);
    setState(311);
    match(ClickHouseParser::TABLE);
    setState(312);
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

ClickHouseParser::DestinationClauseContext* ClickHouseParser::CreateMaterializedViewStmtContext::destinationClause() {
  return getRuleContext<ClickHouseParser::DestinationClauseContext>(0);
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
  enterRule(_localctx, 16, ClickHouseParser::RuleCreateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(378);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 28, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(314);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(315);
      match(ClickHouseParser::DATABASE);
      setState(319);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 15, _ctx)) {
      case 1: {
        setState(316);
        match(ClickHouseParser::IF);
        setState(317);
        match(ClickHouseParser::NOT);
        setState(318);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(321);
      databaseIdentifier();
      setState(323);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(322);
        engineExpr();
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateMaterializedViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(325);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(326);
      match(ClickHouseParser::MATERIALIZED);
      setState(327);
      match(ClickHouseParser::VIEW);
      setState(331);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
      case 1: {
        setState(328);
        match(ClickHouseParser::IF);
        setState(329);
        match(ClickHouseParser::NOT);
        setState(330);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(333);
      tableIdentifier();
      setState(335);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
      case 1: {
        setState(334);
        schemaClause();
        break;
      }

      }
      setState(338);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TO) {
        setState(337);
        destinationClause();
      }
      setState(341);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(340);
        engineClause();
      }
      setState(344);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::POPULATE) {
        setState(343);
        match(ClickHouseParser::POPULATE);
      }
      setState(346);
      subqueryClause();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(348);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(350);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(349);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(352);
      match(ClickHouseParser::TABLE);
      setState(356);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx)) {
      case 1: {
        setState(353);
        match(ClickHouseParser::IF);
        setState(354);
        match(ClickHouseParser::NOT);
        setState(355);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(358);
      tableIdentifier();
      setState(360);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx)) {
      case 1: {
        setState(359);
        schemaClause();
        break;
      }

      }
      setState(363);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(362);
        engineClause();
      }
      setState(366);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::AS) {
        setState(365);
        subqueryClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<CreateStmtContext *>(_tracker.createInstance<ClickHouseParser::CreateViewStmtContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(368);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ATTACH

      || _la == ClickHouseParser::CREATE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(369);
      match(ClickHouseParser::VIEW);
      setState(373);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
      case 1: {
        setState(370);
        match(ClickHouseParser::IF);
        setState(371);
        match(ClickHouseParser::NOT);
        setState(372);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(375);
      tableIdentifier();
      setState(376);
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
  enterRule(_localctx, 18, ClickHouseParser::RuleDestinationClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(380);
    match(ClickHouseParser::TO);
    setState(381);
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
  enterRule(_localctx, 20, ClickHouseParser::RuleSubqueryClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(383);
    match(ClickHouseParser::AS);
    setState(384);
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
  enterRule(_localctx, 22, ClickHouseParser::RuleSchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(401);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaDescriptionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(386);
      match(ClickHouseParser::LPAREN);
      setState(387);
      tableElementExpr();
      setState(392);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(388);
        match(ClickHouseParser::COMMA);
        setState(389);
        tableElementExpr();
        setState(394);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(395);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsTableClauseContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(397);
      match(ClickHouseParser::AS);
      setState(398);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsFunctionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(399);
      match(ClickHouseParser::AS);
      setState(400);
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
  enterRule(_localctx, 24, ClickHouseParser::RuleEngineClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(403);
    engineExpr();
    setState(430);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(428);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
        case 1: {
          setState(404);

          if (!(!_localctx->clauses.count("orderByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"orderByClause\")");
          setState(405);
          orderByClause();
          _localctx->clauses.insert("orderByClause");
          break;
        }

        case 2: {
          setState(408);

          if (!(!_localctx->clauses.count("partitionByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"partitionByClause\")");
          setState(409);
          partitionByClause();
          _localctx->clauses.insert("partitionByClause");
          break;
        }

        case 3: {
          setState(412);

          if (!(!_localctx->clauses.count("primaryKeyClause"))) throw FailedPredicateException(this, "!$clauses.count(\"primaryKeyClause\")");
          setState(413);
          primaryKeyClause();
          _localctx->clauses.insert("primaryKeyClause");
          break;
        }

        case 4: {
          setState(416);

          if (!(!_localctx->clauses.count("sampleByClause"))) throw FailedPredicateException(this, "!$clauses.count(\"sampleByClause\")");
          setState(417);
          sampleByClause();
          _localctx->clauses.insert("sampleByClause");
          break;
        }

        case 5: {
          setState(420);

          if (!(!_localctx->clauses.count("ttlClause"))) throw FailedPredicateException(this, "!$clauses.count(\"ttlClause\")");
          setState(421);
          ttlClause();
          _localctx->clauses.insert("ttlClause");
          break;
        }

        case 6: {
          setState(424);

          if (!(!_localctx->clauses.count("settingsClause"))) throw FailedPredicateException(this, "!$clauses.count(\"settingsClause\")");
          setState(425);
          settingsClause();
          _localctx->clauses.insert("settingsClause");
          break;
        }

        } 
      }
      setState(432);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx);
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
  enterRule(_localctx, 26, ClickHouseParser::RulePartitionByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(433);
    match(ClickHouseParser::PARTITION);
    setState(434);
    match(ClickHouseParser::BY);
    setState(435);
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
  enterRule(_localctx, 28, ClickHouseParser::RulePrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(437);
    match(ClickHouseParser::PRIMARY);
    setState(438);
    match(ClickHouseParser::KEY);
    setState(439);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleSampleByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(441);
    match(ClickHouseParser::SAMPLE);
    setState(442);
    match(ClickHouseParser::BY);
    setState(443);
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
  enterRule(_localctx, 32, ClickHouseParser::RuleTtlClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(445);
    match(ClickHouseParser::TTL);
    setState(446);
    ttlExpr();
    setState(451);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(447);
        match(ClickHouseParser::COMMA);
        setState(448);
        ttlExpr(); 
      }
      setState(453);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx);
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
  enterRule(_localctx, 34, ClickHouseParser::RuleEngineExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(454);
    match(ClickHouseParser::ENGINE);
    setState(456);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EQ_SINGLE) {
      setState(455);
      match(ClickHouseParser::EQ_SINGLE);
    }
    setState(458);
    identifierOrNull();
    setState(464);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx)) {
    case 1: {
      setState(459);
      match(ClickHouseParser::LPAREN);
      setState(461);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::ANALYZE)
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
        | (1ULL << ClickHouseParser::COLUMN)
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
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
        | (1ULL << (ClickHouseParser::TRUNCATE - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(460);
        columnExprList();
      }
      setState(463);
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
  enterRule(_localctx, 36, ClickHouseParser::RuleTableElementExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(466);
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

tree::TerminalNode* ClickHouseParser::TableColumnDfntContext::COMMENT() {
  return getToken(ClickHouseParser::COMMENT, 0);
}

tree::TerminalNode* ClickHouseParser::TableColumnDfntContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
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
  enterRule(_localctx, 38, ClickHouseParser::RuleTableColumnDfnt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(494);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(468);
      nestedIdentifier();
      setState(469);
      columnTypeExpr();
      setState(471);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ALIAS

      || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED) {
        setState(470);
        tableColumnPropertyExpr();
      }
      setState(475);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(473);
        match(ClickHouseParser::COMMENT);
        setState(474);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(479);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(477);
        match(ClickHouseParser::TTL);
        setState(478);
        columnExpr(0);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(481);
      nestedIdentifier();
      setState(483);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
      case 1: {
        setState(482);
        columnTypeExpr();
        break;
      }

      }
      setState(485);
      tableColumnPropertyExpr();
      setState(488);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMENT) {
        setState(486);
        match(ClickHouseParser::COMMENT);
        setState(487);
        match(ClickHouseParser::STRING_LITERAL);
      }
      setState(492);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(490);
        match(ClickHouseParser::TTL);
        setState(491);
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
  enterRule(_localctx, 40, ClickHouseParser::RuleTableColumnPropertyExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(496);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::ALIAS

    || _la == ClickHouseParser::DEFAULT || _la == ClickHouseParser::MATERIALIZED)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(497);
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
  enterRule(_localctx, 42, ClickHouseParser::RuleTtlExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(499);
    columnExpr(0);
    setState(507);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
    case 1: {
      setState(500);
      match(ClickHouseParser::DELETE);
      break;
    }

    case 2: {
      setState(501);
      match(ClickHouseParser::TO);
      setState(502);
      match(ClickHouseParser::DISK);
      setState(503);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 3: {
      setState(504);
      match(ClickHouseParser::TO);
      setState(505);
      match(ClickHouseParser::VOLUME);
      setState(506);
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
    setState(509);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DESC

    || _la == ClickHouseParser::DESCRIBE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(511);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
    case 1: {
      setState(510);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(513);
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
    setState(536);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 50, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(515);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(516);
      match(ClickHouseParser::DATABASE);
      setState(519);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
      case 1: {
        setState(517);
        match(ClickHouseParser::IF);
        setState(518);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(521);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(522);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::DETACH

      || _la == ClickHouseParser::DROP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(524);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(523);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(526);
      match(ClickHouseParser::TABLE);
      setState(529);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 48, _ctx)) {
      case 1: {
        setState(527);
        match(ClickHouseParser::IF);
        setState(528);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(531);
      tableIdentifier();
      setState(534);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NO) {
        setState(532);
        match(ClickHouseParser::NO);
        setState(533);
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
    setState(538);
    match(ClickHouseParser::EXISTS);
    setState(540);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TEMPORARY) {
      setState(539);
      match(ClickHouseParser::TEMPORARY);
    }
    setState(542);
    match(ClickHouseParser::TABLE);
    setState(543);
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
  enterRule(_localctx, 50, ClickHouseParser::RuleInsertStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(545);
    match(ClickHouseParser::INSERT);
    setState(546);
    match(ClickHouseParser::INTO);
    setState(548);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
    case 1: {
      setState(547);
      match(ClickHouseParser::TABLE);
      break;
    }

    }
    setState(553);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
    case 1: {
      setState(550);
      tableIdentifier();
      break;
    }

    case 2: {
      setState(551);
      match(ClickHouseParser::FUNCTION);
      setState(552);
      tableFunctionExpr();
      break;
    }

    }
    setState(556);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
    case 1: {
      setState(555);
      columnsClause();
      break;
    }

    }
    setState(558);
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
  enterRule(_localctx, 52, ClickHouseParser::RuleColumnsClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(560);
    match(ClickHouseParser::LPAREN);
    setState(561);
    nestedIdentifier();
    setState(566);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(562);
      match(ClickHouseParser::COMMA);
      setState(563);
      nestedIdentifier();
      setState(568);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(569);
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

ClickHouseParser::ValuesExprContext* ClickHouseParser::DataClauseValuesContext::valuesExpr() {
  return getRuleContext<ClickHouseParser::ValuesExprContext>(0);
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

ClickHouseParser::DataExprContext* ClickHouseParser::DataClauseFormatContext::dataExpr() {
  return getRuleContext<ClickHouseParser::DataExprContext>(0);
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
  enterRule(_localctx, 54, ClickHouseParser::RuleDataClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(578);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FORMAT: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseFormatContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(571);
        match(ClickHouseParser::FORMAT);
        setState(572);
        identifier();
        setState(573);
        dataExpr();
        break;
      }

      case ClickHouseParser::VALUES: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseValuesContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(575);
        match(ClickHouseParser::VALUES);
        setState(576);
        valuesExpr();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH:
      case ClickHouseParser::LPAREN: {
        _localctx = dynamic_cast<DataClauseContext *>(_tracker.createInstance<ClickHouseParser::DataClauseSelectContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(577);
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

//----------------- DataExprContext ------------------------------------------------------------------

ClickHouseParser::DataExprContext::DataExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::DataExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleDataExpr;
}

void ClickHouseParser::DataExprContext::copyFrom(DataExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DataExprTSVContext ------------------------------------------------------------------

std::vector<ClickHouseParser::DataLiteralContext *> ClickHouseParser::DataExprTSVContext::dataLiteral() {
  return getRuleContexts<ClickHouseParser::DataLiteralContext>();
}

ClickHouseParser::DataLiteralContext* ClickHouseParser::DataExprTSVContext::dataLiteral(size_t i) {
  return getRuleContext<ClickHouseParser::DataLiteralContext>(i);
}

ClickHouseParser::DataExprTSVContext::DataExprTSVContext(DataExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataExprTSVContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataExprTSV(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DataExprJSONContext ------------------------------------------------------------------

std::vector<ClickHouseParser::JsonExprContext *> ClickHouseParser::DataExprJSONContext::jsonExpr() {
  return getRuleContexts<ClickHouseParser::JsonExprContext>();
}

ClickHouseParser::JsonExprContext* ClickHouseParser::DataExprJSONContext::jsonExpr(size_t i) {
  return getRuleContext<ClickHouseParser::JsonExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DataExprJSONContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::DataExprJSONContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::DataExprJSONContext::DataExprJSONContext(DataExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataExprJSONContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataExprJSON(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DataExprValuesContext ------------------------------------------------------------------

ClickHouseParser::ValuesExprContext* ClickHouseParser::DataExprValuesContext::valuesExpr() {
  return getRuleContext<ClickHouseParser::ValuesExprContext>(0);
}

ClickHouseParser::DataExprValuesContext::DataExprValuesContext(DataExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataExprValuesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataExprValues(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DataExprCSVContext ------------------------------------------------------------------

std::vector<ClickHouseParser::DataLiteralContext *> ClickHouseParser::DataExprCSVContext::dataLiteral() {
  return getRuleContexts<ClickHouseParser::DataLiteralContext>();
}

ClickHouseParser::DataLiteralContext* ClickHouseParser::DataExprCSVContext::dataLiteral(size_t i) {
  return getRuleContext<ClickHouseParser::DataLiteralContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::DataExprCSVContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::DataExprCSVContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::DataExprCSVContext::DataExprCSVContext(DataExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::DataExprCSVContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataExprCSV(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::DataExprContext* ClickHouseParser::dataExpr() {
  DataExprContext *_localctx = _tracker.createInstance<DataExprContext>(_ctx, getState());
  enterRule(_localctx, 56, ClickHouseParser::RuleDataExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(604);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 61, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DataExprContext *>(_tracker.createInstance<ClickHouseParser::DataExprCSVContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(580);
      dataLiteral();
      setState(585);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(581);
        match(ClickHouseParser::COMMA);
        setState(582);
        dataLiteral();
        setState(587);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DataExprContext *>(_tracker.createInstance<ClickHouseParser::DataExprJSONContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(588);
      jsonExpr();
      setState(595);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::INF

      || _la == ClickHouseParser::NAN_SQL || ((((_la - 136) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 136)) & ((1ULL << (ClickHouseParser::JSON_FALSE - 136))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 136))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 136))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 136))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 136))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 136))
        | (1ULL << (ClickHouseParser::COMMA - 136))
        | (1ULL << (ClickHouseParser::DASH - 136))
        | (1ULL << (ClickHouseParser::DOT - 136))
        | (1ULL << (ClickHouseParser::LBRACE - 136))
        | (1ULL << (ClickHouseParser::PLUS - 136)))) != 0)) {
        setState(590);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::COMMA) {
          setState(589);
          match(ClickHouseParser::COMMA);
        }
        setState(592);
        jsonExpr();
        setState(597);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    case 3: {
      _localctx = dynamic_cast<DataExprContext *>(_tracker.createInstance<ClickHouseParser::DataExprTSVContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(599); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(598);
        dataLiteral();
        setState(601); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::INF

      || _la == ClickHouseParser::NAN_SQL || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (ClickHouseParser::FLOATING_LITERAL - 139))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 139))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 139))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 139))
        | (1ULL << (ClickHouseParser::DASH - 139))
        | (1ULL << (ClickHouseParser::DOT - 139))
        | (1ULL << (ClickHouseParser::PLUS - 139)))) != 0));
      break;
    }

    case 4: {
      _localctx = dynamic_cast<DataExprContext *>(_tracker.createInstance<ClickHouseParser::DataExprValuesContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(603);
      valuesExpr();
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

//----------------- ValuesExprContext ------------------------------------------------------------------

ClickHouseParser::ValuesExprContext::ValuesExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::ValueTupleExprContext *> ClickHouseParser::ValuesExprContext::valueTupleExpr() {
  return getRuleContexts<ClickHouseParser::ValueTupleExprContext>();
}

ClickHouseParser::ValueTupleExprContext* ClickHouseParser::ValuesExprContext::valueTupleExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ValueTupleExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ValuesExprContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ValuesExprContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ValuesExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleValuesExpr;
}

antlrcpp::Any ClickHouseParser::ValuesExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValuesExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ValuesExprContext* ClickHouseParser::valuesExpr() {
  ValuesExprContext *_localctx = _tracker.createInstance<ValuesExprContext>(_ctx, getState());
  enterRule(_localctx, 58, ClickHouseParser::RuleValuesExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(606);
    valueTupleExpr();
    setState(613);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA

    || _la == ClickHouseParser::LPAREN) {
      setState(608);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::COMMA) {
        setState(607);
        match(ClickHouseParser::COMMA);
      }
      setState(610);
      valueTupleExpr();
      setState(615);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleValueTupleExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(616);
    match(ClickHouseParser::LPAREN);
    setState(617);
    columnExprList();
    setState(618);
    match(ClickHouseParser::RPAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JsonExprContext ------------------------------------------------------------------

ClickHouseParser::JsonExprContext::JsonExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::JsonExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleJsonExpr;
}

void ClickHouseParser::JsonExprContext::copyFrom(JsonExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- JsonExprLiteralContext ------------------------------------------------------------------

ClickHouseParser::DataLiteralContext* ClickHouseParser::JsonExprLiteralContext::dataLiteral() {
  return getRuleContext<ClickHouseParser::DataLiteralContext>(0);
}

ClickHouseParser::JsonExprLiteralContext::JsonExprLiteralContext(JsonExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JsonExprLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJsonExprLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JsonExprBooleanContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JsonExprBooleanContext::JSON_TRUE() {
  return getToken(ClickHouseParser::JSON_TRUE, 0);
}

tree::TerminalNode* ClickHouseParser::JsonExprBooleanContext::JSON_FALSE() {
  return getToken(ClickHouseParser::JSON_FALSE, 0);
}

ClickHouseParser::JsonExprBooleanContext::JsonExprBooleanContext(JsonExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JsonExprBooleanContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJsonExprBoolean(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JsonExprObjectContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::JsonExprObjectContext::LBRACE() {
  return getToken(ClickHouseParser::LBRACE, 0);
}

std::vector<ClickHouseParser::JsonValueExprContext *> ClickHouseParser::JsonExprObjectContext::jsonValueExpr() {
  return getRuleContexts<ClickHouseParser::JsonValueExprContext>();
}

ClickHouseParser::JsonValueExprContext* ClickHouseParser::JsonExprObjectContext::jsonValueExpr(size_t i) {
  return getRuleContext<ClickHouseParser::JsonValueExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::JsonExprObjectContext::RBRACE() {
  return getToken(ClickHouseParser::RBRACE, 0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::JsonExprObjectContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::JsonExprObjectContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

ClickHouseParser::JsonExprObjectContext::JsonExprObjectContext(JsonExprContext *ctx) { copyFrom(ctx); }

antlrcpp::Any ClickHouseParser::JsonExprObjectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJsonExprObject(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::JsonExprContext* ClickHouseParser::jsonExpr() {
  JsonExprContext *_localctx = _tracker.createInstance<JsonExprContext>(_ctx, getState());
  enterRule(_localctx, 62, ClickHouseParser::RuleJsonExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(633);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::INTEGER_LITERAL:
      case ClickHouseParser::DATA_STRING_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::DOT:
      case ClickHouseParser::PLUS: {
        _localctx = dynamic_cast<JsonExprContext *>(_tracker.createInstance<ClickHouseParser::JsonExprLiteralContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(620);
        dataLiteral();
        break;
      }

      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE: {
        _localctx = dynamic_cast<JsonExprContext *>(_tracker.createInstance<ClickHouseParser::JsonExprBooleanContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(621);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::JSON_FALSE

        || _la == ClickHouseParser::JSON_TRUE)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case ClickHouseParser::LBRACE: {
        _localctx = dynamic_cast<JsonExprContext *>(_tracker.createInstance<ClickHouseParser::JsonExprObjectContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(622);
        match(ClickHouseParser::LBRACE);
        setState(623);
        jsonValueExpr();
        setState(628);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(624);
          match(ClickHouseParser::COMMA);
          setState(625);
          jsonValueExpr();
          setState(630);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(631);
        match(ClickHouseParser::RBRACE);
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

//----------------- JsonValueExprContext ------------------------------------------------------------------

ClickHouseParser::JsonValueExprContext::JsonValueExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::JsonValueExprContext::DATA_STRING_LITERAL() {
  return getToken(ClickHouseParser::DATA_STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::JsonValueExprContext::COLON() {
  return getToken(ClickHouseParser::COLON, 0);
}

ClickHouseParser::JsonExprContext* ClickHouseParser::JsonValueExprContext::jsonExpr() {
  return getRuleContext<ClickHouseParser::JsonExprContext>(0);
}


size_t ClickHouseParser::JsonValueExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleJsonValueExpr;
}

antlrcpp::Any ClickHouseParser::JsonValueExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJsonValueExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::JsonValueExprContext* ClickHouseParser::jsonValueExpr() {
  JsonValueExprContext *_localctx = _tracker.createInstance<JsonValueExprContext>(_ctx, getState());
  enterRule(_localctx, 64, ClickHouseParser::RuleJsonValueExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(635);
    match(ClickHouseParser::DATA_STRING_LITERAL);
    setState(636);
    match(ClickHouseParser::COLON);
    setState(637);
    jsonExpr();
   
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
  enterRule(_localctx, 66, ClickHouseParser::RuleOptimizeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(639);
    match(ClickHouseParser::OPTIMIZE);
    setState(640);
    match(ClickHouseParser::TABLE);
    setState(641);
    tableIdentifier();
    setState(643);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(642);
      partitionClause();
    }
    setState(646);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(645);
      match(ClickHouseParser::FINAL);
    }
    setState(649);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DEDUPLICATE) {
      setState(648);
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
  enterRule(_localctx, 68, ClickHouseParser::RulePartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(656);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 69, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(651);
      match(ClickHouseParser::PARTITION);
      setState(652);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(653);
      match(ClickHouseParser::PARTITION);
      setState(654);
      match(ClickHouseParser::ID);
      setState(655);
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
  enterRule(_localctx, 70, ClickHouseParser::RuleRenameStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(658);
    match(ClickHouseParser::RENAME);
    setState(659);
    match(ClickHouseParser::TABLE);
    setState(660);
    tableIdentifier();
    setState(661);
    match(ClickHouseParser::TO);
    setState(662);
    tableIdentifier();
    setState(670);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(663);
      match(ClickHouseParser::COMMA);
      setState(664);
      tableIdentifier();
      setState(665);
      match(ClickHouseParser::TO);
      setState(666);
      tableIdentifier();
      setState(672);
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
  enterRule(_localctx, 72, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(673);
    selectStmtWithParens();
    setState(679);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(674);
      match(ClickHouseParser::UNION);
      setState(675);
      match(ClickHouseParser::ALL);
      setState(676);
      selectStmtWithParens();
      setState(681);
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
  enterRule(_localctx, 74, ClickHouseParser::RuleSelectStmtWithParens);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(687);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 1);
        setState(682);
        selectStmt();
        break;
      }

      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 2);
        setState(683);
        match(ClickHouseParser::LPAREN);
        setState(684);
        selectUnionStmt();
        setState(685);
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
  enterRule(_localctx, 76, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(690);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(689);
      withClause();
    }
    setState(692);
    match(ClickHouseParser::SELECT);
    setState(694);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
    case 1: {
      setState(693);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(696);
    columnExprList();
    setState(698);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(697);
      fromClause();
    }
    setState(701);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(700);
      sampleClause();
    }
    setState(704);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY

    || _la == ClickHouseParser::LEFT) {
      setState(703);
      arrayJoinClause();
    }
    setState(707);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(706);
      prewhereClause();
    }
    setState(710);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(709);
      whereClause();
    }
    setState(713);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(712);
      groupByClause();
    }
    setState(716);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(715);
      havingClause();
    }
    setState(719);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(718);
      orderByClause();
    }
    setState(722);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 83, _ctx)) {
    case 1: {
      setState(721);
      limitByClause();
      break;
    }

    }
    setState(725);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(724);
      limitClause();
    }
    setState(728);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(727);
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
  enterRule(_localctx, 78, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(730);
    match(ClickHouseParser::WITH);
    setState(731);
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
  enterRule(_localctx, 80, ClickHouseParser::RuleFromClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(733);
    match(ClickHouseParser::FROM);
    setState(734);
    joinExpr(0);
    setState(736);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(735);
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
  enterRule(_localctx, 82, ClickHouseParser::RuleSampleClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(738);
    match(ClickHouseParser::SAMPLE);
    setState(739);
    ratioExpr();
    setState(742);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET) {
      setState(740);
      match(ClickHouseParser::OFFSET);
      setState(741);
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
  enterRule(_localctx, 84, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(745);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LEFT) {
      setState(744);
      match(ClickHouseParser::LEFT);
    }
    setState(747);
    match(ClickHouseParser::ARRAY);
    setState(748);
    match(ClickHouseParser::JOIN);
    setState(749);
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
  enterRule(_localctx, 86, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(751);
    match(ClickHouseParser::PREWHERE);
    setState(752);
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
  enterRule(_localctx, 88, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(754);
    match(ClickHouseParser::WHERE);
    setState(755);
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
  enterRule(_localctx, 90, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(757);
    match(ClickHouseParser::GROUP);
    setState(758);
    match(ClickHouseParser::BY);
    setState(759);
    columnExprList();
    setState(762);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(760);
      match(ClickHouseParser::WITH);
      setState(761);
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
  enterRule(_localctx, 92, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(764);
    match(ClickHouseParser::HAVING);
    setState(765);
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
  enterRule(_localctx, 94, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(767);
    match(ClickHouseParser::ORDER);
    setState(768);
    match(ClickHouseParser::BY);
    setState(769);
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
  enterRule(_localctx, 96, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(771);
    match(ClickHouseParser::LIMIT);
    setState(772);
    limitExpr();
    setState(773);
    match(ClickHouseParser::BY);
    setState(774);
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
  enterRule(_localctx, 98, ClickHouseParser::RuleLimitClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(776);
    match(ClickHouseParser::LIMIT);
    setState(777);
    limitExpr();
    setState(780);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(778);
      match(ClickHouseParser::WITH);
      setState(779);
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
  enterRule(_localctx, 100, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(782);
    match(ClickHouseParser::SETTINGS);
    setState(783);
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
  size_t startState = 102;
  enterRecursionRule(_localctx, 102, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(791);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(786);
      match(ClickHouseParser::LPAREN);
      setState(787);
      joinExpr(0);
      setState(788);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(790);
      tableExpr(0);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(810);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(808);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 94, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(793);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(794);
          joinOpCross();
          setState(795);
          joinExpr(3);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(797);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(799);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(798);
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
          setState(802);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if ((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
            | (1ULL << ClickHouseParser::ANY)
            | (1ULL << ClickHouseParser::ASOF)
            | (1ULL << ClickHouseParser::FULL)
            | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 72) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 72)) & ((1ULL << (ClickHouseParser::LEFT - 72))
            | (1ULL << (ClickHouseParser::OUTER - 72))
            | (1ULL << (ClickHouseParser::RIGHT - 72))
            | (1ULL << (ClickHouseParser::SEMI - 72)))) != 0)) {
            setState(801);
            joinOp();
          }
          setState(804);
          match(ClickHouseParser::JOIN);
          setState(805);
          joinExpr(0);
          setState(806);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(812);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx);
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
  enterRule(_localctx, 104, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(843);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 105, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(821);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 98, _ctx)) {
      case 1: {
        setState(814);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(813);
          match(ClickHouseParser::ANY);
        }
        setState(816);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(817);
        match(ClickHouseParser::INNER);
        setState(819);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(818);
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
      setState(831);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx)) {
      case 1: {
        setState(824);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(823);
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
        setState(826);
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
        setState(827);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(829);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(828);
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
      setState(841);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 104, _ctx)) {
      case 1: {
        setState(834);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(833);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(836);
        match(ClickHouseParser::FULL);
        break;
      }

      case 2: {
        setState(837);
        match(ClickHouseParser::FULL);
        setState(839);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(838);
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
  enterRule(_localctx, 106, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(851);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(846);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(845);
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
        setState(848);
        match(ClickHouseParser::CROSS);
        setState(849);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(850);
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
  enterRule(_localctx, 108, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(862);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 108, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(853);
      match(ClickHouseParser::ON);
      setState(854);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(855);
      match(ClickHouseParser::USING);
      setState(856);
      match(ClickHouseParser::LPAREN);
      setState(857);
      columnExprList();
      setState(858);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(860);
      match(ClickHouseParser::USING);
      setState(861);
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
  enterRule(_localctx, 110, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(864);
    match(ClickHouseParser::INTEGER_LITERAL);
    setState(867);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET

    || _la == ClickHouseParser::COMMA) {
      setState(865);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET

      || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(866);
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
  enterRule(_localctx, 112, ClickHouseParser::RuleOrderExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(869);
    orderExpr();
    setState(874);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 110, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(870);
        match(ClickHouseParser::COMMA);
        setState(871);
        orderExpr(); 
      }
      setState(876);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 110, _ctx);
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
  enterRule(_localctx, 114, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(877);
    columnExpr(0);
    setState(879);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 111, _ctx)) {
    case 1: {
      setState(878);
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
    setState(883);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 112, _ctx)) {
    case 1: {
      setState(881);
      match(ClickHouseParser::NULLS);
      setState(882);
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
    setState(887);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 113, _ctx)) {
    case 1: {
      setState(885);
      match(ClickHouseParser::COLLATE);
      setState(886);
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
  enterRule(_localctx, 116, ClickHouseParser::RuleRatioExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(889);
    numberLiteral();
    setState(892);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SLASH) {
      setState(890);
      match(ClickHouseParser::SLASH);
      setState(891);
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
  enterRule(_localctx, 118, ClickHouseParser::RuleSettingExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(894);
    settingExpr();
    setState(899);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(895);
        match(ClickHouseParser::COMMA);
        setState(896);
        settingExpr(); 
      }
      setState(901);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx);
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
  enterRule(_localctx, 120, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(902);
    identifier();
    setState(903);
    match(ClickHouseParser::EQ_SINGLE);
    setState(904);
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
  enterRule(_localctx, 122, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(906);
    match(ClickHouseParser::SET);
    setState(907);
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
  enterRule(_localctx, 124, ClickHouseParser::RuleShowStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(939);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 122, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(909);
      match(ClickHouseParser::SHOW);
      setState(910);
      match(ClickHouseParser::CREATE);
      setState(911);
      match(ClickHouseParser::DATABASE);
      setState(912);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowCreateTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(913);
      match(ClickHouseParser::SHOW);
      setState(914);
      match(ClickHouseParser::CREATE);
      setState(916);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx)) {
      case 1: {
        setState(915);
        match(ClickHouseParser::TEMPORARY);
        break;
      }

      }
      setState(919);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 117, _ctx)) {
      case 1: {
        setState(918);
        match(ClickHouseParser::TABLE);
        break;
      }

      }
      setState(921);
      tableIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ShowStmtContext *>(_tracker.createInstance<ClickHouseParser::ShowTablesStmtContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(922);
      match(ClickHouseParser::SHOW);
      setState(924);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(923);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(926);
      match(ClickHouseParser::TABLES);
      setState(929);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FROM

      || _la == ClickHouseParser::IN) {
        setState(927);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::FROM

        || _la == ClickHouseParser::IN)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(928);
        databaseIdentifier();
      }
      setState(934);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case ClickHouseParser::LIKE: {
          setState(931);
          match(ClickHouseParser::LIKE);
          setState(932);
          match(ClickHouseParser::STRING_LITERAL);
          break;
        }

        case ClickHouseParser::WHERE: {
          setState(933);
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
      setState(937);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::LIMIT) {
        setState(936);
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

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SystemStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::START() {
  return getToken(ClickHouseParser::START, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::STOP() {
  return getToken(ClickHouseParser::STOP, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::FETCHES() {
  return getToken(ClickHouseParser::FETCHES, 0);
}

tree::TerminalNode* ClickHouseParser::SystemStmtContext::MERGES() {
  return getToken(ClickHouseParser::MERGES, 0);
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
  enterRule(_localctx, 126, ClickHouseParser::RuleSystemStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(949);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 123, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(941);
      match(ClickHouseParser::SYSTEM);
      setState(942);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::START

      || _la == ClickHouseParser::STOP)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(943);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::FETCHES

      || _la == ClickHouseParser::MERGES)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(944);
      tableIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(945);
      match(ClickHouseParser::SYSTEM);
      setState(946);
      match(ClickHouseParser::SYNC);
      setState(947);
      match(ClickHouseParser::REPLICA);
      setState(948);
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
  enterRule(_localctx, 128, ClickHouseParser::RuleTruncateStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(951);
    match(ClickHouseParser::TRUNCATE);
    setState(953);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TEMPORARY) {
      setState(952);
      match(ClickHouseParser::TEMPORARY);
    }
    setState(955);
    match(ClickHouseParser::TABLE);
    setState(958);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 125, _ctx)) {
    case 1: {
      setState(956);
      match(ClickHouseParser::IF);
      setState(957);
      match(ClickHouseParser::EXISTS);
      break;
    }

    }
    setState(960);
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
  enterRule(_localctx, 130, ClickHouseParser::RuleUseStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(962);
    match(ClickHouseParser::USE);
    setState(963);
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
  enterRule(_localctx, 132, ClickHouseParser::RuleColumnTypeExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1010);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 129, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprSimpleContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(965);
      identifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprParamContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(966);
      identifier();
      setState(967);
      match(ClickHouseParser::LPAREN);
      setState(968);
      columnExprList();
      setState(969);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprEnumContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(971);
      identifier();
      setState(972);
      match(ClickHouseParser::LPAREN);
      setState(973);
      enumValue();
      setState(978);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(974);
        match(ClickHouseParser::COMMA);
        setState(975);
        enumValue();
        setState(980);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(981);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprComplexContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(983);
      identifier();
      setState(984);
      match(ClickHouseParser::LPAREN);
      setState(985);
      columnTypeExpr();
      setState(990);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(986);
        match(ClickHouseParser::COMMA);
        setState(987);
        columnTypeExpr();
        setState(992);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(993);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprNestedContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(995);
      identifier();
      setState(996);
      match(ClickHouseParser::LPAREN);
      setState(997);
      identifier();
      setState(998);
      columnTypeExpr();
      setState(1005);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(999);
        match(ClickHouseParser::COMMA);
        setState(1000);
        identifier();
        setState(1001);
        columnTypeExpr();
        setState(1007);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(1008);
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
  enterRule(_localctx, 134, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1012);
    columnsExpr();
    setState(1017);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 130, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1013);
        match(ClickHouseParser::COMMA);
        setState(1014);
        columnsExpr(); 
      }
      setState(1019);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 130, _ctx);
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
  enterRule(_localctx, 136, ClickHouseParser::RuleColumnsExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1031);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 132, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprAsteriskContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(1023);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::ANALYZE)
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
        | (1ULL << ClickHouseParser::COLUMN)
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
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
        | (1ULL << (ClickHouseParser::TRUNCATE - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128)))) != 0)) {
        setState(1020);
        tableIdentifier();
        setState(1021);
        match(ClickHouseParser::DOT);
      }
      setState(1025);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprSubqueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(1026);
      match(ClickHouseParser::LPAREN);
      setState(1027);
      selectUnionStmt();
      setState(1028);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(1030);
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
  size_t startState = 138;
  enterRecursionRule(_localctx, 138, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1132);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 142, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1034);
      match(ClickHouseParser::CASE);
      setState(1036);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 133, _ctx)) {
      case 1: {
        setState(1035);
        columnExpr(0);
        break;
      }

      }
      setState(1043); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(1038);
        match(ClickHouseParser::WHEN);
        setState(1039);
        columnExpr(0);
        setState(1040);
        match(ClickHouseParser::THEN);
        setState(1041);
        columnExpr(0);
        setState(1045); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(1049);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(1047);
        match(ClickHouseParser::ELSE);
        setState(1048);
        columnExpr(0);
      }
      setState(1051);
      match(ClickHouseParser::END);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprCastContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1053);
      match(ClickHouseParser::CAST);
      setState(1054);
      match(ClickHouseParser::LPAREN);
      setState(1055);
      columnExpr(0);
      setState(1056);
      match(ClickHouseParser::AS);
      setState(1057);
      columnTypeExpr();
      setState(1058);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1060);
      match(ClickHouseParser::EXTRACT);
      setState(1061);
      match(ClickHouseParser::LPAREN);
      setState(1062);
      match(ClickHouseParser::INTERVAL_TYPE);
      setState(1063);
      match(ClickHouseParser::FROM);
      setState(1064);
      columnExpr(0);
      setState(1065);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1067);
      match(ClickHouseParser::INTERVAL);
      setState(1068);
      columnExpr(0);
      setState(1069);
      match(ClickHouseParser::INTERVAL_TYPE);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprSubstringContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1071);
      match(ClickHouseParser::SUBSTRING);
      setState(1072);
      match(ClickHouseParser::LPAREN);
      setState(1073);
      columnExpr(0);
      setState(1074);
      match(ClickHouseParser::FROM);
      setState(1075);
      columnExpr(0);
      setState(1078);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::FOR) {
        setState(1076);
        match(ClickHouseParser::FOR);
        setState(1077);
        columnExpr(0);
      }
      setState(1080);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1082);
      match(ClickHouseParser::TRIM);
      setState(1083);
      match(ClickHouseParser::LPAREN);
      setState(1084);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::BOTH || _la == ClickHouseParser::LEADING

      || _la == ClickHouseParser::TRAILING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(1085);
      match(ClickHouseParser::STRING_LITERAL);
      setState(1086);
      match(ClickHouseParser::FROM);
      setState(1087);
      columnExpr(0);
      setState(1088);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1090);
      identifier();
      setState(1096);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 138, _ctx)) {
      case 1: {
        setState(1091);
        match(ClickHouseParser::LPAREN);
        setState(1093);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
          | (1ULL << ClickHouseParser::AFTER)
          | (1ULL << ClickHouseParser::ALIAS)
          | (1ULL << ClickHouseParser::ALL)
          | (1ULL << ClickHouseParser::ALTER)
          | (1ULL << ClickHouseParser::ANALYZE)
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
          | (1ULL << ClickHouseParser::COLUMN)
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
          | (1ULL << ClickHouseParser::FETCHES)
          | (1ULL << ClickHouseParser::FINAL)
          | (1ULL << ClickHouseParser::FIRST)
          | (1ULL << ClickHouseParser::FORMAT)
          | (1ULL << ClickHouseParser::FULL)
          | (1ULL << ClickHouseParser::FUNCTION)
          | (1ULL << ClickHouseParser::GLOBAL)
          | (1ULL << ClickHouseParser::GROUP)
          | (1ULL << ClickHouseParser::HAVING)
          | (1ULL << ClickHouseParser::HOUR)
          | (1ULL << ClickHouseParser::ID)
          | (1ULL << ClickHouseParser::IF)
          | (1ULL << ClickHouseParser::IN)
          | (1ULL << ClickHouseParser::INF)
          | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
          | (1ULL << (ClickHouseParser::OUTER - 64))
          | (1ULL << (ClickHouseParser::OUTFILE - 64))
          | (1ULL << (ClickHouseParser::PARTITION - 64))
          | (1ULL << (ClickHouseParser::PREWHERE - 64))
          | (1ULL << (ClickHouseParser::PRIMARY - 64))
          | (1ULL << (ClickHouseParser::QUARTER - 64))
          | (1ULL << (ClickHouseParser::RENAME - 64))
          | (1ULL << (ClickHouseParser::REPLACE - 64))
          | (1ULL << (ClickHouseParser::REPLICA - 64))
          | (1ULL << (ClickHouseParser::RIGHT - 64))
          | (1ULL << (ClickHouseParser::SAMPLE - 64))
          | (1ULL << (ClickHouseParser::SECOND - 64))
          | (1ULL << (ClickHouseParser::SEMI - 64))
          | (1ULL << (ClickHouseParser::SET - 64))
          | (1ULL << (ClickHouseParser::SETTINGS - 64))
          | (1ULL << (ClickHouseParser::SHOW - 64))
          | (1ULL << (ClickHouseParser::START - 64))
          | (1ULL << (ClickHouseParser::STOP - 64))
          | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
          | (1ULL << (ClickHouseParser::TRUNCATE - 64))
          | (1ULL << (ClickHouseParser::TTL - 64))
          | (1ULL << (ClickHouseParser::UNION - 64))
          | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
          | (1ULL << (ClickHouseParser::VIEW - 128))
          | (1ULL << (ClickHouseParser::VOLUME - 128))
          | (1ULL << (ClickHouseParser::WEEK - 128))
          | (1ULL << (ClickHouseParser::WHEN - 128))
          | (1ULL << (ClickHouseParser::WITH - 128))
          | (1ULL << (ClickHouseParser::YEAR - 128))
          | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
          | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
          | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
          | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128))
          | (1ULL << (ClickHouseParser::ASTERISK - 128))
          | (1ULL << (ClickHouseParser::DASH - 128))
          | (1ULL << (ClickHouseParser::DOT - 128))
          | (1ULL << (ClickHouseParser::LBRACKET - 128))
          | (1ULL << (ClickHouseParser::LPAREN - 128))
          | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
          setState(1092);
          columnExprList();
        }
        setState(1095);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(1098);
      match(ClickHouseParser::LPAREN);
      setState(1100);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::ANALYZE)
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
        | (1ULL << ClickHouseParser::COLUMN)
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
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
        | (1ULL << (ClickHouseParser::TRUNCATE - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1099);
        columnArgList();
      }
      setState(1102);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprUnaryOpContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1104);
      unaryOp();
      setState(1105);
      columnExpr(13);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1110);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::ANALYZE)
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
        | (1ULL << ClickHouseParser::COLUMN)
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
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
        | (1ULL << (ClickHouseParser::TRUNCATE - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128)))) != 0)) {
        setState(1107);
        tableIdentifier();
        setState(1108);
        match(ClickHouseParser::DOT);
      }
      setState(1112);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1113);
      match(ClickHouseParser::LPAREN);
      setState(1114);
      selectUnionStmt();
      setState(1115);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1117);
      match(ClickHouseParser::LPAREN);
      setState(1118);
      columnExpr(0);
      setState(1119);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 12: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1121);
      match(ClickHouseParser::LPAREN);
      setState(1122);
      columnExprList();
      setState(1123);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 13: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1125);
      match(ClickHouseParser::LBRACKET);
      setState(1127);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::AFTER)
        | (1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::ALTER)
        | (1ULL << ClickHouseParser::ANALYZE)
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
        | (1ULL << ClickHouseParser::COLUMN)
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
        | (1ULL << ClickHouseParser::FETCHES)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::FUNCTION)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::ID)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INF)
        | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
        | (1ULL << (ClickHouseParser::OUTER - 64))
        | (1ULL << (ClickHouseParser::OUTFILE - 64))
        | (1ULL << (ClickHouseParser::PARTITION - 64))
        | (1ULL << (ClickHouseParser::PREWHERE - 64))
        | (1ULL << (ClickHouseParser::PRIMARY - 64))
        | (1ULL << (ClickHouseParser::QUARTER - 64))
        | (1ULL << (ClickHouseParser::RENAME - 64))
        | (1ULL << (ClickHouseParser::REPLACE - 64))
        | (1ULL << (ClickHouseParser::REPLICA - 64))
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::SHOW - 64))
        | (1ULL << (ClickHouseParser::START - 64))
        | (1ULL << (ClickHouseParser::STOP - 64))
        | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
        | (1ULL << (ClickHouseParser::TRUNCATE - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
        | (1ULL << (ClickHouseParser::VIEW - 128))
        | (1ULL << (ClickHouseParser::VOLUME - 128))
        | (1ULL << (ClickHouseParser::WEEK - 128))
        | (1ULL << (ClickHouseParser::WHEN - 128))
        | (1ULL << (ClickHouseParser::WITH - 128))
        | (1ULL << (ClickHouseParser::YEAR - 128))
        | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
        | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128))
        | (1ULL << (ClickHouseParser::ASTERISK - 128))
        | (1ULL << (ClickHouseParser::DASH - 128))
        | (1ULL << (ClickHouseParser::DOT - 128))
        | (1ULL << (ClickHouseParser::LBRACKET - 128))
        | (1ULL << (ClickHouseParser::LPAREN - 128))
        | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
        setState(1126);
        columnExprList();
      }
      setState(1129);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 14: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1130);
      columnIdentifier();
      break;
    }

    case 15: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1131);
      literal();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1174);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 147, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1172);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 146, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprBinaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1134);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(1135);
          binaryOp();
          setState(1136);
          columnExpr(12);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1138);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(1139);
          match(ClickHouseParser::QUERY);
          setState(1140);
          columnExpr(0);
          setState(1141);
          match(ClickHouseParser::COLON);
          setState(1142);
          columnExpr(11);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1144);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(1146);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1145);
            match(ClickHouseParser::NOT);
          }
          setState(1148);
          match(ClickHouseParser::BETWEEN);
          setState(1149);
          columnExpr(0);
          setState(1150);
          match(ClickHouseParser::AND);
          setState(1151);
          columnExpr(10);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1153);

          if (!(precpred(_ctx, 15))) throw FailedPredicateException(this, "precpred(_ctx, 15)");
          setState(1154);
          match(ClickHouseParser::LBRACKET);
          setState(1155);
          columnExpr(0);
          setState(1156);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1158);

          if (!(precpred(_ctx, 14))) throw FailedPredicateException(this, "precpred(_ctx, 14)");
          setState(1159);
          match(ClickHouseParser::DOT);
          setState(1160);
          match(ClickHouseParser::INTEGER_LITERAL);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1161);

          if (!(precpred(_ctx, 12))) throw FailedPredicateException(this, "precpred(_ctx, 12)");
          setState(1162);
          match(ClickHouseParser::IS);
          setState(1164);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(1163);
            match(ClickHouseParser::NOT);
          }
          setState(1166);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(1167);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(1169);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 145, _ctx)) {
          case 1: {
            setState(1168);
            match(ClickHouseParser::AS);
            break;
          }

          }
          setState(1171);
          identifier();
          break;
        }

        } 
      }
      setState(1176);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 147, _ctx);
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
  enterRule(_localctx, 140, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1177);
    columnArgExpr();
    setState(1182);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1178);
      match(ClickHouseParser::COMMA);
      setState(1179);
      columnArgExpr();
      setState(1184);
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
  enterRule(_localctx, 142, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1187);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 149, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1185);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1186);
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
  enterRule(_localctx, 144, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1208);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(1189);
        match(ClickHouseParser::LPAREN);
        setState(1190);
        identifier();
        setState(1195);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1191);
          match(ClickHouseParser::COMMA);
          setState(1192);
          identifier();
          setState(1197);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1198);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::ANALYZE:
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
      case ClickHouseParser::COLUMN:
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
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
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
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
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
      case ClickHouseParser::TRUNCATE:
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
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER:
      case ClickHouseParser::DATA_STRING_LITERAL: {
        setState(1200);
        identifier();
        setState(1205);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(1201);
          match(ClickHouseParser::COMMA);
          setState(1202);
          identifier();
          setState(1207);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(1210);
    match(ClickHouseParser::ARROW);
    setState(1211);
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
  enterRule(_localctx, 146, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1216);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 153, _ctx)) {
    case 1: {
      setState(1213);
      tableIdentifier();
      setState(1214);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1218);
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
  enterRule(_localctx, 148, ClickHouseParser::RuleNestedIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1220);
    identifier();
    setState(1223);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 154, _ctx)) {
    case 1: {
      setState(1221);
      match(ClickHouseParser::DOT);
      setState(1222);
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
  size_t startState = 150;
  enterRecursionRule(_localctx, 150, ClickHouseParser::RuleTableExpr, precedence);

    

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1232);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 155, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(1226);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1227);
      tableFunctionExpr();
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(1228);
      match(ClickHouseParser::LPAREN);
      setState(1229);
      selectUnionStmt();
      setState(1230);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(1241);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 157, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(1234);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(1236);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 156, _ctx)) {
        case 1: {
          setState(1235);
          match(ClickHouseParser::AS);
          break;
        }

        }
        setState(1238);
        identifier(); 
      }
      setState(1243);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 157, _ctx);
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
  enterRule(_localctx, 152, ClickHouseParser::RuleTableFunctionExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1244);
    identifier();
    setState(1245);
    match(ClickHouseParser::LPAREN);
    setState(1247);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
      | (1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::ANALYZE)
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
      | (1ULL << ClickHouseParser::COLUMN)
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
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::FUNCTION)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GROUP)
      | (1ULL << ClickHouseParser::HAVING)
      | (1ULL << ClickHouseParser::HOUR)
      | (1ULL << ClickHouseParser::ID)
      | (1ULL << ClickHouseParser::IF)
      | (1ULL << ClickHouseParser::IN)
      | (1ULL << ClickHouseParser::INF)
      | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64))
      | (1ULL << (ClickHouseParser::RENAME - 64))
      | (1ULL << (ClickHouseParser::REPLACE - 64))
      | (1ULL << (ClickHouseParser::REPLICA - 64))
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::SHOW - 64))
      | (1ULL << (ClickHouseParser::START - 64))
      | (1ULL << (ClickHouseParser::STOP - 64))
      | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
      | (1ULL << (ClickHouseParser::TRUNCATE - 64))
      | (1ULL << (ClickHouseParser::TTL - 64))
      | (1ULL << (ClickHouseParser::UNION - 64))
      | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
      | (1ULL << (ClickHouseParser::WITH - 128))
      | (1ULL << (ClickHouseParser::YEAR - 128))
      | (1ULL << (ClickHouseParser::JSON_FALSE - 128))
      | (1ULL << (ClickHouseParser::JSON_TRUE - 128))
      | (1ULL << (ClickHouseParser::IDENTIFIER - 128))
      | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 128))
      | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 128))
      | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 128))
      | (1ULL << (ClickHouseParser::STRING_LITERAL - 128))
      | (1ULL << (ClickHouseParser::DATA_STRING_LITERAL - 128))
      | (1ULL << (ClickHouseParser::DASH - 128))
      | (1ULL << (ClickHouseParser::DOT - 128))
      | (1ULL << (ClickHouseParser::LPAREN - 128))
      | (1ULL << (ClickHouseParser::PLUS - 128)))) != 0)) {
      setState(1246);
      tableArgList();
    }
    setState(1249);
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
  enterRule(_localctx, 154, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1254);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 159, _ctx)) {
    case 1: {
      setState(1251);
      databaseIdentifier();
      setState(1252);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(1256);
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
  enterRule(_localctx, 156, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1258);
    tableArgExpr();
    setState(1263);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(1259);
      match(ClickHouseParser::COMMA);
      setState(1260);
      tableArgExpr();
      setState(1265);
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

ClickHouseParser::TableExprContext* ClickHouseParser::TableArgExprContext::tableExpr() {
  return getRuleContext<ClickHouseParser::TableExprContext>(0);
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
  enterRule(_localctx, 158, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1268);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::ANALYZE:
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
      case ClickHouseParser::COLUMN:
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
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
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
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
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
      case ClickHouseParser::TRUNCATE:
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
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER:
      case ClickHouseParser::DATA_STRING_LITERAL:
      case ClickHouseParser::LPAREN: {
        enterOuterAlt(_localctx, 1);
        setState(1266);
        tableExpr(0);
        break;
      }

      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::NULL_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::INTEGER_LITERAL:
      case ClickHouseParser::STRING_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::DOT:
      case ClickHouseParser::PLUS: {
        enterOuterAlt(_localctx, 2);
        setState(1267);
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
  enterRule(_localctx, 160, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1270);
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
  enterRule(_localctx, 162, ClickHouseParser::RuleFloatingLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1280);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::FLOATING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(1272);
        match(ClickHouseParser::FLOATING_LITERAL);
        break;
      }

      case ClickHouseParser::INTEGER_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1273);
        match(ClickHouseParser::INTEGER_LITERAL);
        setState(1274);
        match(ClickHouseParser::DOT);
        setState(1276);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 162, _ctx)) {
        case 1: {
          setState(1275);
          match(ClickHouseParser::INTEGER_LITERAL);
          break;
        }

        }
        break;
      }

      case ClickHouseParser::DOT: {
        enterOuterAlt(_localctx, 3);
        setState(1278);
        match(ClickHouseParser::DOT);
        setState(1279);
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
  enterRule(_localctx, 164, ClickHouseParser::RuleNumberLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1283);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DASH

    || _la == ClickHouseParser::PLUS) {
      setState(1282);
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
    setState(1290);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 165, _ctx)) {
    case 1: {
      setState(1285);
      floatingLiteral();
      break;
    }

    case 2: {
      setState(1286);
      match(ClickHouseParser::HEXADECIMAL_LITERAL);
      break;
    }

    case 3: {
      setState(1287);
      match(ClickHouseParser::INTEGER_LITERAL);
      break;
    }

    case 4: {
      setState(1288);
      match(ClickHouseParser::INF);
      break;
    }

    case 5: {
      setState(1289);
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
  enterRule(_localctx, 166, ClickHouseParser::RuleLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1295);
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
        setState(1292);
        numberLiteral();
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1293);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 3);
        setState(1294);
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

//----------------- DataLiteralContext ------------------------------------------------------------------

ClickHouseParser::DataLiteralContext::DataLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::NumberLiteralContext* ClickHouseParser::DataLiteralContext::numberLiteral() {
  return getRuleContext<ClickHouseParser::NumberLiteralContext>(0);
}

tree::TerminalNode* ClickHouseParser::DataLiteralContext::DATA_STRING_LITERAL() {
  return getToken(ClickHouseParser::DATA_STRING_LITERAL, 0);
}


size_t ClickHouseParser::DataLiteralContext::getRuleIndex() const {
  return ClickHouseParser::RuleDataLiteral;
}

antlrcpp::Any ClickHouseParser::DataLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDataLiteral(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DataLiteralContext* ClickHouseParser::dataLiteral() {
  DataLiteralContext *_localctx = _tracker.createInstance<DataLiteralContext>(_ctx, getState());
  enterRule(_localctx, 168, ClickHouseParser::RuleDataLiteral);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1299);
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
        setState(1297);
        numberLiteral();
        break;
      }

      case ClickHouseParser::DATA_STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1298);
        match(ClickHouseParser::DATA_STRING_LITERAL);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::ANALYZE() {
  return getToken(ClickHouseParser::ANALYZE, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::COLUMN() {
  return getToken(ClickHouseParser::COLUMN, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::FETCHES() {
  return getToken(ClickHouseParser::FETCHES, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::FUNCTION() {
  return getToken(ClickHouseParser::FUNCTION, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::REPLACE() {
  return getToken(ClickHouseParser::REPLACE, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::TRUNCATE() {
  return getToken(ClickHouseParser::TRUNCATE, 0);
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
  enterRule(_localctx, 170, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1301);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::AFTER)
      | (1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::ALTER)
      | (1ULL << ClickHouseParser::ANALYZE)
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
      | (1ULL << ClickHouseParser::COLUMN)
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
      | (1ULL << ClickHouseParser::FETCHES)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::FUNCTION)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GROUP)
      | (1ULL << ClickHouseParser::HAVING)
      | (1ULL << ClickHouseParser::HOUR)
      | (1ULL << ClickHouseParser::ID)
      | (1ULL << ClickHouseParser::IF)
      | (1ULL << ClickHouseParser::IN)
      | (1ULL << ClickHouseParser::INNER))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::INSERT - 64))
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
      | (1ULL << (ClickHouseParser::OUTER - 64))
      | (1ULL << (ClickHouseParser::OUTFILE - 64))
      | (1ULL << (ClickHouseParser::PARTITION - 64))
      | (1ULL << (ClickHouseParser::PREWHERE - 64))
      | (1ULL << (ClickHouseParser::PRIMARY - 64))
      | (1ULL << (ClickHouseParser::QUARTER - 64))
      | (1ULL << (ClickHouseParser::RENAME - 64))
      | (1ULL << (ClickHouseParser::REPLACE - 64))
      | (1ULL << (ClickHouseParser::REPLICA - 64))
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::SHOW - 64))
      | (1ULL << (ClickHouseParser::START - 64))
      | (1ULL << (ClickHouseParser::STOP - 64))
      | (1ULL << (ClickHouseParser::SUBSTRING - 64))
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
      | (1ULL << (ClickHouseParser::TRUNCATE - 64))
      | (1ULL << (ClickHouseParser::TTL - 64))
      | (1ULL << (ClickHouseParser::UNION - 64))
      | (1ULL << (ClickHouseParser::USE - 64)))) != 0) || ((((_la - 128) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 128)) & ((1ULL << (ClickHouseParser::VALUES - 128))
      | (1ULL << (ClickHouseParser::VIEW - 128))
      | (1ULL << (ClickHouseParser::VOLUME - 128))
      | (1ULL << (ClickHouseParser::WEEK - 128))
      | (1ULL << (ClickHouseParser::WHEN - 128))
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

//----------------- IdentifierContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext::IdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::IdentifierContext::IDENTIFIER() {
  return getToken(ClickHouseParser::IDENTIFIER, 0);
}

tree::TerminalNode* ClickHouseParser::IdentifierContext::DATA_STRING_LITERAL() {
  return getToken(ClickHouseParser::DATA_STRING_LITERAL, 0);
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
  enterRule(_localctx, 172, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1307);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(1303);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::DATA_STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(1304);
        match(ClickHouseParser::DATA_STRING_LITERAL);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE: {
        enterOuterAlt(_localctx, 3);
        setState(1305);
        match(ClickHouseParser::INTERVAL_TYPE);
        break;
      }

      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::ANALYZE:
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
      case ClickHouseParser::COLUMN:
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
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
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
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
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
      case ClickHouseParser::TRUNCATE:
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
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE: {
        enterOuterAlt(_localctx, 4);
        setState(1306);
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
  enterRule(_localctx, 174, ClickHouseParser::RuleIdentifierOrNull);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1311);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::AFTER:
      case ClickHouseParser::ALIAS:
      case ClickHouseParser::ALL:
      case ClickHouseParser::ALTER:
      case ClickHouseParser::ANALYZE:
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
      case ClickHouseParser::COLUMN:
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
      case ClickHouseParser::FETCHES:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FULL:
      case ClickHouseParser::FUNCTION:
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
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PARTITION:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::PRIMARY:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RENAME:
      case ClickHouseParser::REPLACE:
      case ClickHouseParser::REPLICA:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::SHOW:
      case ClickHouseParser::START:
      case ClickHouseParser::STOP:
      case ClickHouseParser::SUBSTRING:
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
      case ClickHouseParser::TRUNCATE:
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
      case ClickHouseParser::JSON_FALSE:
      case ClickHouseParser::JSON_TRUE:
      case ClickHouseParser::IDENTIFIER:
      case ClickHouseParser::DATA_STRING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(1309);
        identifier();
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 2);
        setState(1310);
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
  enterRule(_localctx, 176, ClickHouseParser::RuleUnaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1313);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::NOT || _la == ClickHouseParser::DASH)) {
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
  enterRule(_localctx, 178, ClickHouseParser::RuleBinaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(1341);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 173, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1315);
      match(ClickHouseParser::CONCAT);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1316);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1317);
      match(ClickHouseParser::SLASH);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(1318);
      match(ClickHouseParser::PLUS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(1319);
      match(ClickHouseParser::DASH);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(1320);
      match(ClickHouseParser::PERCENT);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(1321);
      match(ClickHouseParser::EQ_DOUBLE);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(1322);
      match(ClickHouseParser::EQ_SINGLE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(1323);
      match(ClickHouseParser::NOT_EQ);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(1324);
      match(ClickHouseParser::LE);
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(1325);
      match(ClickHouseParser::GE);
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(1326);
      match(ClickHouseParser::LT);
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(1327);
      match(ClickHouseParser::GT);
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(1328);
      match(ClickHouseParser::AND);
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(1329);
      match(ClickHouseParser::OR);
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(1331);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(1330);
        match(ClickHouseParser::NOT);
      }
      setState(1333);
      match(ClickHouseParser::LIKE);
      break;
    }

    case 17: {
      enterOuterAlt(_localctx, 17);
      setState(1335);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::GLOBAL) {
        setState(1334);
        match(ClickHouseParser::GLOBAL);
      }
      setState(1338);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(1337);
        match(ClickHouseParser::NOT);
      }
      setState(1340);
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
  enterRule(_localctx, 180, ClickHouseParser::RuleEnumValue);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1343);
    match(ClickHouseParser::STRING_LITERAL);
    setState(1344);
    match(ClickHouseParser::EQ_SINGLE);
    setState(1345);
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
    case 12: return engineClauseSempred(dynamic_cast<EngineClauseContext *>(context), predicateIndex);
    case 51: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 69: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 75: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

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
    case 6: return precpred(_ctx, 2);
    case 7: return precpred(_ctx, 3);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 8: return precpred(_ctx, 11);
    case 9: return precpred(_ctx, 10);
    case 10: return precpred(_ctx, 9);
    case 11: return precpred(_ctx, 15);
    case 12: return precpred(_ctx, 14);
    case 13: return precpred(_ctx, 12);
    case 14: return precpred(_ctx, 8);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::tableExprSempred(TableExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 15: return precpred(_ctx, 1);

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
  "input", "queryList", "queryStmt", "query", "alterStmt", "alterTableClause", 
  "analyzeStmt", "checkStmt", "createStmt", "destinationClause", "subqueryClause", 
  "schemaClause", "engineClause", "partitionByClause", "primaryKeyClause", 
  "sampleByClause", "ttlClause", "engineExpr", "tableElementExpr", "tableColumnDfnt", 
  "tableColumnPropertyExpr", "ttlExpr", "describeStmt", "dropStmt", "existsStmt", 
  "insertStmt", "columnsClause", "dataClause", "dataExpr", "valuesExpr", 
  "valueTupleExpr", "jsonExpr", "jsonValueExpr", "optimizeStmt", "partitionClause", 
  "renameStmt", "selectUnionStmt", "selectStmtWithParens", "selectStmt", 
  "withClause", "fromClause", "sampleClause", "arrayJoinClause", "prewhereClause", 
  "whereClause", "groupByClause", "havingClause", "orderByClause", "limitByClause", 
  "limitClause", "settingsClause", "joinExpr", "joinOp", "joinOpCross", 
  "joinConstraintClause", "limitExpr", "orderExprList", "orderExpr", "ratioExpr", 
  "settingExprList", "settingExpr", "setStmt", "showStmt", "systemStmt", 
  "truncateStmt", "useStmt", "columnTypeExpr", "columnExprList", "columnsExpr", 
  "columnExpr", "columnArgList", "columnArgExpr", "columnLambdaExpr", "columnIdentifier", 
  "nestedIdentifier", "tableExpr", "tableFunctionExpr", "tableIdentifier", 
  "tableArgList", "tableArgExpr", "databaseIdentifier", "floatingLiteral", 
  "numberLiteral", "literal", "dataLiteral", "keyword", "identifier", "identifierOrNull", 
  "unaryOp", "binaryOp", "enumValue"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "'false'", "'true'", "", "", "", 
  "", "", "", "'->'", "'*'", "'`'", "'\\'", "':'", "','", "'||'", "'-'", 
  "'.'", "'=='", "'='", "'>='", "'>'", "'{'", "'['", "'<='", "'('", "'<'", 
  "", "'%'", "'+'", "'?'", "'\"'", "'''", "'}'", "']'", "')'", "';'", "'/'", 
  "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "INTERVAL_TYPE", "ADD", "AFTER", "ALIAS", "ALL", "ALTER", "ANALYZE", 
  "AND", "ANTI", "ANY", "ARRAY", "AS", "ASCENDING", "ASOF", "ATTACH", "BETWEEN", 
  "BOTH", "BY", "CASE", "CAST", "CHECK", "CLEAR", "CLUSTER", "COLLATE", 
  "COLUMN", "COMMENT", "CREATE", "CROSS", "DATABASE", "DAY", "DEDUPLICATE", 
  "DEFAULT", "DELAY", "DELETE", "DESC", "DESCENDING", "DESCRIBE", "DETACH", 
  "DISK", "DISTINCT", "DROP", "ELSE", "END", "ENGINE", "EXISTS", "EXTRACT", 
  "FETCHES", "FINAL", "FIRST", "FOR", "FORMAT", "FROM", "FULL", "FUNCTION", 
  "GLOBAL", "GROUP", "HAVING", "HOUR", "ID", "IF", "IN", "INF", "INNER", 
  "INSERT", "INTERVAL", "INTO", "IS", "JOIN", "KEY", "LAST", "LEADING", 
  "LEFT", "LIKE", "LIMIT", "LOCAL", "MATERIALIZED", "MERGES", "MINUTE", 
  "MODIFY", "MONTH", "NAN_SQL", "NO", "NOT", "NULL_SQL", "NULLS", "OFFSET", 
  "ON", "OPTIMIZE", "OR", "ORDER", "OUTER", "OUTFILE", "PARTITION", "POPULATE", 
  "PREWHERE", "PRIMARY", "QUARTER", "RENAME", "REPLACE", "REPLICA", "RIGHT", 
  "SAMPLE", "SECOND", "SELECT", "SEMI", "SET", "SETTINGS", "SHOW", "START", 
  "STOP", "SUBSTRING", "SYNC", "SYSTEM", "TABLE", "TABLES", "TEMPORARY", 
  "THEN", "TIES", "TO", "TOTALS", "TRAILING", "TRIM", "TRUNCATE", "TTL", 
  "UNION", "USE", "USING", "VALUES", "VIEW", "VOLUME", "WEEK", "WHEN", "WHERE", 
  "WITH", "YEAR", "JSON_FALSE", "JSON_TRUE", "IDENTIFIER", "FLOATING_LITERAL", 
  "HEXADECIMAL_LITERAL", "INTEGER_LITERAL", "STRING_LITERAL", "DATA_STRING_LITERAL", 
  "ARROW", "ASTERISK", "BACKQUOTE", "BACKSLASH", "COLON", "COMMA", "CONCAT", 
  "DASH", "DOT", "EQ_DOUBLE", "EQ_SINGLE", "GE", "GT", "LBRACE", "LBRACKET", 
  "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", "PLUS", "QUERY", "QUOTE_DOUBLE", 
  "QUOTE_SINGLE", "RBRACE", "RBRACKET", "RPAREN", "SEMICOLON", "SLASH", 
  "UNDERSCORE", "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", "WHITESPACE"
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
    0x3, 0xb2, 0x546, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x3, 0x2, 0x3, 0x2, 0x5, 0x2, 0xbb, 0xa, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x7, 0x3, 0xc0, 0xa, 0x3, 0xc, 0x3, 0xe, 0x3, 0xc3, 0xb, 0x3, 0x3, 
    0x3, 0x5, 0x3, 0xc6, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x4, 0x3, 0x4, 
    0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0xce, 0xa, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 
    0x4, 0xd2, 0xa, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xe4, 0xa, 0x5, 0x3, 
    0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 0xec, 
    0xa, 0x6, 0xc, 0x6, 0xe, 0x6, 0xef, 0xb, 0x6, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0xf6, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x5, 0x7, 0xfb, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x5, 0x7, 0x101, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x5, 0x7, 0x107, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x111, 0xa, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x11f, 0xa, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 
    0x7, 0x128, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x134, 0xa, 
    0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x142, 
    0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x146, 0xa, 0xa, 0x3, 0xa, 0x3, 
    0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x14e, 0xa, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x152, 0xa, 0xa, 0x3, 0xa, 0x5, 0xa, 0x155, 
    0xa, 0xa, 0x3, 0xa, 0x5, 0xa, 0x158, 0xa, 0xa, 0x3, 0xa, 0x5, 0xa, 0x15b, 
    0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x161, 0xa, 
    0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x167, 0xa, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x16b, 0xa, 0xa, 0x3, 0xa, 0x5, 0xa, 0x16e, 
    0xa, 0xa, 0x3, 0xa, 0x5, 0xa, 0x171, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 
    0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x178, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 
    0x3, 0xa, 0x5, 0xa, 0x17d, 0xa, 0xa, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x7, 
    0xd, 0x189, 0xa, 0xd, 0xc, 0xd, 0xe, 0xd, 0x18c, 0xb, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x5, 0xd, 0x194, 0xa, 
    0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x7, 0xe, 0x1af, 0xa, 0xe, 
    0xc, 0xe, 0xe, 0xe, 0x1b2, 0xb, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x7, 
    0x12, 0x1c4, 0xa, 0x12, 0xc, 0x12, 0xe, 0x12, 0x1c7, 0xb, 0x12, 0x3, 
    0x13, 0x3, 0x13, 0x5, 0x13, 0x1cb, 0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x13, 0x5, 0x13, 0x1d0, 0xa, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1d3, 
    0xa, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 
    0x15, 0x1da, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x1de, 0xa, 
    0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x1e2, 0xa, 0x15, 0x3, 0x15, 
    0x3, 0x15, 0x5, 0x15, 0x1e6, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 
    0x5, 0x15, 0x1eb, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x1ef, 
    0xa, 0x15, 0x5, 0x15, 0x1f1, 0xa, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 
    0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 
    0x17, 0x3, 0x17, 0x5, 0x17, 0x1fe, 0xa, 0x17, 0x3, 0x18, 0x3, 0x18, 
    0x5, 0x18, 0x202, 0xa, 0x18, 0x3, 0x18, 0x3, 0x18, 0x3, 0x19, 0x3, 0x19, 
    0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x20a, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x3, 0x19, 0x5, 0x19, 0x20f, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x5, 0x19, 0x214, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 
    0x219, 0xa, 0x19, 0x5, 0x19, 0x21b, 0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 
    0x5, 0x1a, 0x21f, 0xa, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x227, 0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x5, 0x1b, 0x22c, 0xa, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x22f, 
    0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1c, 0x7, 0x1c, 0x237, 0xa, 0x1c, 0xc, 0x1c, 0xe, 0x1c, 0x23a, 0xb, 
    0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x5, 0x1d, 0x245, 0xa, 0x1d, 0x3, 0x1e, 
    0x3, 0x1e, 0x3, 0x1e, 0x7, 0x1e, 0x24a, 0xa, 0x1e, 0xc, 0x1e, 0xe, 0x1e, 
    0x24d, 0xb, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x251, 0xa, 0x1e, 
    0x3, 0x1e, 0x7, 0x1e, 0x254, 0xa, 0x1e, 0xc, 0x1e, 0xe, 0x1e, 0x257, 
    0xb, 0x1e, 0x3, 0x1e, 0x6, 0x1e, 0x25a, 0xa, 0x1e, 0xd, 0x1e, 0xe, 0x1e, 
    0x25b, 0x3, 0x1e, 0x5, 0x1e, 0x25f, 0xa, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 
    0x5, 0x1f, 0x263, 0xa, 0x1f, 0x3, 0x1f, 0x7, 0x1f, 0x266, 0xa, 0x1f, 
    0xc, 0x1f, 0xe, 0x1f, 0x269, 0xb, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 
    0x3, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 
    0x21, 0x7, 0x21, 0x275, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x278, 0xb, 
    0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x27c, 0xa, 0x21, 0x3, 0x22, 
    0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 
    0x23, 0x5, 0x23, 0x286, 0xa, 0x23, 0x3, 0x23, 0x5, 0x23, 0x289, 0xa, 
    0x23, 0x3, 0x23, 0x5, 0x23, 0x28c, 0xa, 0x23, 0x3, 0x24, 0x3, 0x24, 
    0x3, 0x24, 0x3, 0x24, 0x3, 0x24, 0x5, 0x24, 0x293, 0xa, 0x24, 0x3, 0x25, 
    0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 
    0x25, 0x3, 0x25, 0x3, 0x25, 0x7, 0x25, 0x29f, 0xa, 0x25, 0xc, 0x25, 
    0xe, 0x25, 0x2a2, 0xb, 0x25, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 
    0x7, 0x26, 0x2a8, 0xa, 0x26, 0xc, 0x26, 0xe, 0x26, 0x2ab, 0xb, 0x26, 
    0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x5, 0x27, 0x2b2, 
    0xa, 0x27, 0x3, 0x28, 0x5, 0x28, 0x2b5, 0xa, 0x28, 0x3, 0x28, 0x3, 0x28, 
    0x5, 0x28, 0x2b9, 0xa, 0x28, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2bd, 
    0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2c0, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 
    0x2c3, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2c6, 0xa, 0x28, 0x3, 0x28, 
    0x5, 0x28, 0x2c9, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2cc, 0xa, 0x28, 
    0x3, 0x28, 0x5, 0x28, 0x2cf, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2d2, 
    0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2d5, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 
    0x2d8, 0xa, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2db, 0xa, 0x28, 0x3, 0x29, 
    0x3, 0x29, 0x3, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x2e3, 
    0xa, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x2e9, 
    0xa, 0x2b, 0x3, 0x2c, 0x5, 0x2c, 0x2ec, 0xa, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 
    0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2e, 0x3, 
    0x2e, 0x3, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 
    0x5, 0x2f, 0x2fd, 0xa, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x31, 
    0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 
    0x32, 0x3, 0x32, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 
    0x30f, 0xa, 0x33, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x35, 0x3, 0x35, 
    0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x5, 0x35, 0x31a, 0xa, 0x35, 
    0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x5, 
    0x35, 0x322, 0xa, 0x35, 0x3, 0x35, 0x5, 0x35, 0x325, 0xa, 0x35, 0x3, 
    0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x7, 0x35, 0x32b, 0xa, 0x35, 
    0xc, 0x35, 0xe, 0x35, 0x32e, 0xb, 0x35, 0x3, 0x36, 0x5, 0x36, 0x331, 
    0xa, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x336, 0xa, 0x36, 
    0x5, 0x36, 0x338, 0xa, 0x36, 0x3, 0x36, 0x5, 0x36, 0x33b, 0xa, 0x36, 
    0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x340, 0xa, 0x36, 0x5, 0x36, 
    0x342, 0xa, 0x36, 0x3, 0x36, 0x5, 0x36, 0x345, 0xa, 0x36, 0x3, 0x36, 
    0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x34a, 0xa, 0x36, 0x5, 0x36, 0x34c, 
    0xa, 0x36, 0x5, 0x36, 0x34e, 0xa, 0x36, 0x3, 0x37, 0x5, 0x37, 0x351, 
    0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x356, 0xa, 0x37, 
    0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 
    0x38, 0x3, 0x38, 0x3, 0x38, 0x5, 0x38, 0x361, 0xa, 0x38, 0x3, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x366, 0xa, 0x39, 0x3, 0x3a, 0x3, 0x3a, 
    0x3, 0x3a, 0x7, 0x3a, 0x36b, 0xa, 0x3a, 0xc, 0x3a, 0xe, 0x3a, 0x36e, 
    0xb, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x372, 0xa, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x5, 0x3b, 0x376, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 
    0x37a, 0xa, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x37f, 
    0xa, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x7, 0x3d, 0x384, 0xa, 0x3d, 
    0xc, 0x3d, 0xe, 0x3d, 0x387, 0xb, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x3, 
    0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 0x397, 
    0xa, 0x40, 0x3, 0x40, 0x5, 0x40, 0x39a, 0xa, 0x40, 0x3, 0x40, 0x3, 0x40, 
    0x3, 0x40, 0x5, 0x40, 0x39f, 0xa, 0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 
    0x5, 0x40, 0x3a4, 0xa, 0x40, 0x3, 0x40, 0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 
    0x3a9, 0xa, 0x40, 0x3, 0x40, 0x5, 0x40, 0x3ac, 0xa, 0x40, 0x5, 0x40, 
    0x3ae, 0xa, 0x40, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 
    0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x3b8, 0xa, 0x41, 0x3, 0x42, 
    0x3, 0x42, 0x5, 0x42, 0x3bc, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x5, 0x42, 0x3c1, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x43, 0x3, 0x43, 
    0x3, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 
    0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x7, 0x44, 
    0x3d3, 0xa, 0x44, 0xc, 0x44, 0xe, 0x44, 0x3d6, 0xb, 0x44, 0x3, 0x44, 
    0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x7, 
    0x44, 0x3df, 0xa, 0x44, 0xc, 0x44, 0xe, 0x44, 0x3e2, 0xb, 0x44, 0x3, 
    0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 
    0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x7, 0x44, 0x3ee, 0xa, 0x44, 0xc, 0x44, 
    0xe, 0x44, 0x3f1, 0xb, 0x44, 0x3, 0x44, 0x3, 0x44, 0x5, 0x44, 0x3f5, 
    0xa, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x3fa, 0xa, 0x45, 
    0xc, 0x45, 0xe, 0x45, 0x3fd, 0xb, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
    0x5, 0x46, 0x402, 0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
    0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x40a, 0xa, 0x46, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x5, 0x47, 0x40f, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x6, 0x47, 0x416, 0xa, 0x47, 0xd, 0x47, 0xe, 0x47, 
    0x417, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x41c, 0xa, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x5, 0x47, 0x439, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x448, 0xa, 0x47, 0x3, 0x47, 
    0x5, 0x47, 0x44b, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x44f, 
    0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x459, 0xa, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x5, 0x47, 0x46a, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x5, 0x47, 0x46f, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x5, 0x47, 0x47d, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
    0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x5, 0x47, 0x48f, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x5, 0x47, 0x494, 0xa, 0x47, 0x3, 0x47, 0x7, 0x47, 0x497, 0xa, 0x47, 
    0xc, 0x47, 0xe, 0x47, 0x49a, 0xb, 0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 
    0x7, 0x48, 0x49f, 0xa, 0x48, 0xc, 0x48, 0xe, 0x48, 0x4a2, 0xb, 0x48, 
    0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x4a6, 0xa, 0x49, 0x3, 0x4a, 0x3, 0x4a, 
    0x3, 0x4a, 0x3, 0x4a, 0x7, 0x4a, 0x4ac, 0xa, 0x4a, 0xc, 0x4a, 0xe, 0x4a, 
    0x4af, 0xb, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
    0x7, 0x4a, 0x4b6, 0xa, 0x4a, 0xc, 0x4a, 0xe, 0x4a, 0x4b9, 0xb, 0x4a, 
    0x5, 0x4a, 0x4bb, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4b, 
    0x3, 0x4b, 0x3, 0x4b, 0x5, 0x4b, 0x4c3, 0xa, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 
    0x3, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x4ca, 0xa, 0x4c, 0x3, 0x4d, 
    0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x5, 
    0x4d, 0x4d3, 0xa, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x4d7, 0xa, 
    0x4d, 0x3, 0x4d, 0x7, 0x4d, 0x4da, 0xa, 0x4d, 0xc, 0x4d, 0xe, 0x4d, 
    0x4dd, 0xb, 0x4d, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x5, 0x4e, 0x4e2, 
    0xa, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x5, 
    0x4f, 0x4e9, 0xa, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x50, 0x3, 0x50, 
    0x3, 0x50, 0x7, 0x50, 0x4f0, 0xa, 0x50, 0xc, 0x50, 0xe, 0x50, 0x4f3, 
    0xb, 0x50, 0x3, 0x51, 0x3, 0x51, 0x5, 0x51, 0x4f7, 0xa, 0x51, 0x3, 0x52, 
    0x3, 0x52, 0x3, 0x53, 0x3, 0x53, 0x3, 0x53, 0x3, 0x53, 0x5, 0x53, 0x4ff, 
    0xa, 0x53, 0x3, 0x53, 0x3, 0x53, 0x5, 0x53, 0x503, 0xa, 0x53, 0x3, 0x54, 
    0x5, 0x54, 0x506, 0xa, 0x54, 0x3, 0x54, 0x3, 0x54, 0x3, 0x54, 0x3, 0x54, 
    0x3, 0x54, 0x5, 0x54, 0x50d, 0xa, 0x54, 0x3, 0x55, 0x3, 0x55, 0x3, 0x55, 
    0x5, 0x55, 0x512, 0xa, 0x55, 0x3, 0x56, 0x3, 0x56, 0x5, 0x56, 0x516, 
    0xa, 0x56, 0x3, 0x57, 0x3, 0x57, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 
    0x58, 0x5, 0x58, 0x51e, 0xa, 0x58, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 
    0x522, 0xa, 0x59, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 
    0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 
    0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 
    0x5, 0x5b, 0x536, 0xa, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x5, 0x5b, 0x53a, 
    0xa, 0x5b, 0x3, 0x5b, 0x5, 0x5b, 0x53d, 0xa, 0x5b, 0x3, 0x5b, 0x5, 0x5b, 
    0x540, 0xa, 0x5b, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 0x3, 0x5c, 
    0x2, 0x5, 0x68, 0x8c, 0x98, 0x5d, 0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 
    0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 
    0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 
    0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 
    0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 
    0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 
    0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 
    0xa0, 0xa2, 0xa4, 0xa6, 0xa8, 0xaa, 0xac, 0xae, 0xb0, 0xb2, 0xb4, 0xb6, 
    0x2, 0x15, 0x4, 0x2, 0x11, 0x11, 0x1d, 0x1d, 0x5, 0x2, 0x6, 0x6, 0x22, 
    0x22, 0x4e, 0x4e, 0x4, 0x2, 0x25, 0x25, 0x27, 0x27, 0x4, 0x2, 0x28, 
    0x28, 0x2b, 0x2b, 0x3, 0x2, 0x8a, 0x8b, 0x4, 0x2, 0x39, 0x39, 0x4d, 
    0x4d, 0x6, 0x2, 0xb, 0xc, 0x10, 0x10, 0x5d, 0x5d, 0x6b, 0x6b, 0x4, 0x2, 
    0x4a, 0x4a, 0x67, 0x67, 0x4, 0x2, 0xc, 0xc, 0x5d, 0x5d, 0x4, 0x2, 0x58, 
    0x58, 0x97, 0x97, 0x4, 0x2, 0xf, 0xf, 0x25, 0x26, 0x4, 0x2, 0x33, 0x33, 
    0x48, 0x48, 0x4, 0x2, 0x36, 0x36, 0x3f, 0x3f, 0x3, 0x2, 0x6f, 0x70, 
    0x4, 0x2, 0x31, 0x31, 0x4f, 0x4f, 0x5, 0x2, 0x13, 0x13, 0x49, 0x49, 
    0x7b, 0x7b, 0x4, 0x2, 0x99, 0x99, 0xa6, 0xa6, 0xd, 0x2, 0x5, 0x33, 0x35, 
    0x35, 0x37, 0x3f, 0x41, 0x52, 0x54, 0x55, 0x57, 0x5b, 0x5d, 0x5f, 0x61, 
    0x69, 0x6b, 0x80, 0x82, 0x86, 0x88, 0x8b, 0x4, 0x2, 0x55, 0x55, 0x99, 
    0x99, 0x2, 0x5ed, 0x2, 0xba, 0x3, 0x2, 0x2, 0x2, 0x4, 0xbc, 0x3, 0x2, 
    0x2, 0x2, 0x6, 0xc9, 0x3, 0x2, 0x2, 0x2, 0x8, 0xe3, 0x3, 0x2, 0x2, 0x2, 
    0xa, 0xe5, 0x3, 0x2, 0x2, 0x2, 0xc, 0x133, 0x3, 0x2, 0x2, 0x2, 0xe, 
    0x135, 0x3, 0x2, 0x2, 0x2, 0x10, 0x138, 0x3, 0x2, 0x2, 0x2, 0x12, 0x17c, 
    0x3, 0x2, 0x2, 0x2, 0x14, 0x17e, 0x3, 0x2, 0x2, 0x2, 0x16, 0x181, 0x3, 
    0x2, 0x2, 0x2, 0x18, 0x193, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x195, 0x3, 0x2, 
    0x2, 0x2, 0x1c, 0x1b3, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x1b7, 0x3, 0x2, 0x2, 
    0x2, 0x20, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x22, 0x1bf, 0x3, 0x2, 0x2, 0x2, 
    0x24, 0x1c8, 0x3, 0x2, 0x2, 0x2, 0x26, 0x1d4, 0x3, 0x2, 0x2, 0x2, 0x28, 
    0x1f0, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x1f2, 0x3, 0x2, 0x2, 0x2, 0x2c, 0x1f5, 
    0x3, 0x2, 0x2, 0x2, 0x2e, 0x1ff, 0x3, 0x2, 0x2, 0x2, 0x30, 0x21a, 0x3, 
    0x2, 0x2, 0x2, 0x32, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x34, 0x223, 0x3, 0x2, 
    0x2, 0x2, 0x36, 0x232, 0x3, 0x2, 0x2, 0x2, 0x38, 0x244, 0x3, 0x2, 0x2, 
    0x2, 0x3a, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x260, 0x3, 0x2, 0x2, 0x2, 
    0x3e, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x40, 0x27b, 0x3, 0x2, 0x2, 0x2, 0x42, 
    0x27d, 0x3, 0x2, 0x2, 0x2, 0x44, 0x281, 0x3, 0x2, 0x2, 0x2, 0x46, 0x292, 
    0x3, 0x2, 0x2, 0x2, 0x48, 0x294, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x2a3, 0x3, 
    0x2, 0x2, 0x2, 0x4c, 0x2b1, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x2b4, 0x3, 0x2, 
    0x2, 0x2, 0x50, 0x2dc, 0x3, 0x2, 0x2, 0x2, 0x52, 0x2df, 0x3, 0x2, 0x2, 
    0x2, 0x54, 0x2e4, 0x3, 0x2, 0x2, 0x2, 0x56, 0x2eb, 0x3, 0x2, 0x2, 0x2, 
    0x58, 0x2f1, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x5c, 
    0x2f7, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x2fe, 0x3, 0x2, 0x2, 0x2, 0x60, 0x301, 
    0x3, 0x2, 0x2, 0x2, 0x62, 0x305, 0x3, 0x2, 0x2, 0x2, 0x64, 0x30a, 0x3, 
    0x2, 0x2, 0x2, 0x66, 0x310, 0x3, 0x2, 0x2, 0x2, 0x68, 0x319, 0x3, 0x2, 
    0x2, 0x2, 0x6a, 0x34d, 0x3, 0x2, 0x2, 0x2, 0x6c, 0x355, 0x3, 0x2, 0x2, 
    0x2, 0x6e, 0x360, 0x3, 0x2, 0x2, 0x2, 0x70, 0x362, 0x3, 0x2, 0x2, 0x2, 
    0x72, 0x367, 0x3, 0x2, 0x2, 0x2, 0x74, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x76, 
    0x37b, 0x3, 0x2, 0x2, 0x2, 0x78, 0x380, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x388, 
    0x3, 0x2, 0x2, 0x2, 0x7c, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x3ad, 0x3, 
    0x2, 0x2, 0x2, 0x80, 0x3b7, 0x3, 0x2, 0x2, 0x2, 0x82, 0x3b9, 0x3, 0x2, 
    0x2, 0x2, 0x84, 0x3c4, 0x3, 0x2, 0x2, 0x2, 0x86, 0x3f4, 0x3, 0x2, 0x2, 
    0x2, 0x88, 0x3f6, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x409, 0x3, 0x2, 0x2, 0x2, 
    0x8c, 0x46e, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x49b, 0x3, 0x2, 0x2, 0x2, 0x90, 
    0x4a5, 0x3, 0x2, 0x2, 0x2, 0x92, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x94, 0x4c2, 
    0x3, 0x2, 0x2, 0x2, 0x96, 0x4c6, 0x3, 0x2, 0x2, 0x2, 0x98, 0x4d2, 0x3, 
    0x2, 0x2, 0x2, 0x9a, 0x4de, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x4e8, 0x3, 0x2, 
    0x2, 0x2, 0x9e, 0x4ec, 0x3, 0x2, 0x2, 0x2, 0xa0, 0x4f6, 0x3, 0x2, 0x2, 
    0x2, 0xa2, 0x4f8, 0x3, 0x2, 0x2, 0x2, 0xa4, 0x502, 0x3, 0x2, 0x2, 0x2, 
    0xa6, 0x505, 0x3, 0x2, 0x2, 0x2, 0xa8, 0x511, 0x3, 0x2, 0x2, 0x2, 0xaa, 
    0x515, 0x3, 0x2, 0x2, 0x2, 0xac, 0x517, 0x3, 0x2, 0x2, 0x2, 0xae, 0x51d, 
    0x3, 0x2, 0x2, 0x2, 0xb0, 0x521, 0x3, 0x2, 0x2, 0x2, 0xb2, 0x523, 0x3, 
    0x2, 0x2, 0x2, 0xb4, 0x53f, 0x3, 0x2, 0x2, 0x2, 0xb6, 0x541, 0x3, 0x2, 
    0x2, 0x2, 0xb8, 0xbb, 0x5, 0x34, 0x1b, 0x2, 0xb9, 0xbb, 0x5, 0x4, 0x3, 
    0x2, 0xba, 0xb8, 0x3, 0x2, 0x2, 0x2, 0xba, 0xb9, 0x3, 0x2, 0x2, 0x2, 
    0xbb, 0x3, 0x3, 0x2, 0x2, 0x2, 0xbc, 0xc1, 0x5, 0x6, 0x4, 0x2, 0xbd, 
    0xbe, 0x7, 0xad, 0x2, 0x2, 0xbe, 0xc0, 0x5, 0x6, 0x4, 0x2, 0xbf, 0xbd, 
    0x3, 0x2, 0x2, 0x2, 0xc0, 0xc3, 0x3, 0x2, 0x2, 0x2, 0xc1, 0xbf, 0x3, 
    0x2, 0x2, 0x2, 0xc1, 0xc2, 0x3, 0x2, 0x2, 0x2, 0xc2, 0xc5, 0x3, 0x2, 
    0x2, 0x2, 0xc3, 0xc1, 0x3, 0x2, 0x2, 0x2, 0xc4, 0xc6, 0x7, 0xad, 0x2, 
    0x2, 0xc5, 0xc4, 0x3, 0x2, 0x2, 0x2, 0xc5, 0xc6, 0x3, 0x2, 0x2, 0x2, 
    0xc6, 0xc7, 0x3, 0x2, 0x2, 0x2, 0xc7, 0xc8, 0x7, 0x2, 0x2, 0x3, 0xc8, 
    0x5, 0x3, 0x2, 0x2, 0x2, 0xc9, 0xcd, 0x5, 0x8, 0x5, 0x2, 0xca, 0xcb, 
    0x7, 0x44, 0x2, 0x2, 0xcb, 0xcc, 0x7, 0x5e, 0x2, 0x2, 0xcc, 0xce, 0x7, 
    0x90, 0x2, 0x2, 0xcd, 0xca, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xce, 0x3, 0x2, 
    0x2, 0x2, 0xce, 0xd1, 0x3, 0x2, 0x2, 0x2, 0xcf, 0xd0, 0x7, 0x35, 0x2, 
    0x2, 0xd0, 0xd2, 0x5, 0xb0, 0x59, 0x2, 0xd1, 0xcf, 0x3, 0x2, 0x2, 0x2, 
    0xd1, 0xd2, 0x3, 0x2, 0x2, 0x2, 0xd2, 0x7, 0x3, 0x2, 0x2, 0x2, 0xd3, 
    0xe4, 0x5, 0xa, 0x6, 0x2, 0xd4, 0xe4, 0x5, 0xe, 0x8, 0x2, 0xd5, 0xe4, 
    0x5, 0x10, 0x9, 0x2, 0xd6, 0xe4, 0x5, 0x12, 0xa, 0x2, 0xd7, 0xe4, 0x5, 
    0x2e, 0x18, 0x2, 0xd8, 0xe4, 0x5, 0x30, 0x19, 0x2, 0xd9, 0xe4, 0x5, 
    0x32, 0x1a, 0x2, 0xda, 0xe4, 0x5, 0x34, 0x1b, 0x2, 0xdb, 0xe4, 0x5, 
    0x44, 0x23, 0x2, 0xdc, 0xe4, 0x5, 0x48, 0x25, 0x2, 0xdd, 0xe4, 0x5, 
    0x4a, 0x26, 0x2, 0xde, 0xe4, 0x5, 0x7c, 0x3f, 0x2, 0xdf, 0xe4, 0x5, 
    0x7e, 0x40, 0x2, 0xe0, 0xe4, 0x5, 0x80, 0x41, 0x2, 0xe1, 0xe4, 0x5, 
    0x82, 0x42, 0x2, 0xe2, 0xe4, 0x5, 0x84, 0x43, 0x2, 0xe3, 0xd3, 0x3, 
    0x2, 0x2, 0x2, 0xe3, 0xd4, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xd5, 0x3, 0x2, 
    0x2, 0x2, 0xe3, 0xd6, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xd7, 0x3, 0x2, 0x2, 
    0x2, 0xe3, 0xd8, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xd9, 0x3, 0x2, 0x2, 0x2, 
    0xe3, 0xda, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xdb, 0x3, 0x2, 0x2, 0x2, 0xe3, 
    0xdc, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xdd, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xde, 
    0x3, 0x2, 0x2, 0x2, 0xe3, 0xdf, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xe0, 0x3, 
    0x2, 0x2, 0x2, 0xe3, 0xe1, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xe2, 0x3, 0x2, 
    0x2, 0x2, 0xe4, 0x9, 0x3, 0x2, 0x2, 0x2, 0xe5, 0xe6, 0x7, 0x8, 0x2, 
    0x2, 0xe6, 0xe7, 0x7, 0x74, 0x2, 0x2, 0xe7, 0xe8, 0x5, 0x9c, 0x4f, 0x2, 
    0xe8, 0xed, 0x5, 0xc, 0x7, 0x2, 0xe9, 0xea, 0x7, 0x97, 0x2, 0x2, 0xea, 
    0xec, 0x5, 0xc, 0x7, 0x2, 0xeb, 0xe9, 0x3, 0x2, 0x2, 0x2, 0xec, 0xef, 
    0x3, 0x2, 0x2, 0x2, 0xed, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xed, 0xee, 0x3, 
    0x2, 0x2, 0x2, 0xee, 0xb, 0x3, 0x2, 0x2, 0x2, 0xef, 0xed, 0x3, 0x2, 
    0x2, 0x2, 0xf0, 0xf1, 0x7, 0x4, 0x2, 0x2, 0xf1, 0xf5, 0x7, 0x1b, 0x2, 
    0x2, 0xf2, 0xf3, 0x7, 0x3e, 0x2, 0x2, 0xf3, 0xf4, 0x7, 0x55, 0x2, 0x2, 
    0xf4, 0xf6, 0x7, 0x2f, 0x2, 0x2, 0xf5, 0xf2, 0x3, 0x2, 0x2, 0x2, 0xf5, 
    0xf6, 0x3, 0x2, 0x2, 0x2, 0xf6, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xfa, 
    0x5, 0x28, 0x15, 0x2, 0xf8, 0xf9, 0x7, 0x5, 0x2, 0x2, 0xf9, 0xfb, 0x5, 
    0x96, 0x4c, 0x2, 0xfa, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xfa, 0xfb, 0x3, 0x2, 
    0x2, 0x2, 0xfb, 0x134, 0x3, 0x2, 0x2, 0x2, 0xfc, 0xfd, 0x7, 0x11, 0x2, 
    0x2, 0xfd, 0x100, 0x5, 0x46, 0x24, 0x2, 0xfe, 0xff, 0x7, 0x36, 0x2, 
    0x2, 0xff, 0x101, 0x5, 0x9c, 0x4f, 0x2, 0x100, 0xfe, 0x3, 0x2, 0x2, 
    0x2, 0x100, 0x101, 0x3, 0x2, 0x2, 0x2, 0x101, 0x134, 0x3, 0x2, 0x2, 
    0x2, 0x102, 0x103, 0x7, 0x18, 0x2, 0x2, 0x103, 0x106, 0x7, 0x1b, 0x2, 
    0x2, 0x104, 0x105, 0x7, 0x3e, 0x2, 0x2, 0x105, 0x107, 0x7, 0x2f, 0x2, 
    0x2, 0x106, 0x104, 0x3, 0x2, 0x2, 0x2, 0x106, 0x107, 0x3, 0x2, 0x2, 
    0x2, 0x107, 0x108, 0x3, 0x2, 0x2, 0x2, 0x108, 0x109, 0x5, 0x96, 0x4c, 
    0x2, 0x109, 0x10a, 0x7, 0x3f, 0x2, 0x2, 0x10a, 0x10b, 0x5, 0x46, 0x24, 
    0x2, 0x10b, 0x134, 0x3, 0x2, 0x2, 0x2, 0x10c, 0x10d, 0x7, 0x1c, 0x2, 
    0x2, 0x10d, 0x110, 0x7, 0x1b, 0x2, 0x2, 0x10e, 0x10f, 0x7, 0x3e, 0x2, 
    0x2, 0x10f, 0x111, 0x7, 0x2f, 0x2, 0x2, 0x110, 0x10e, 0x3, 0x2, 0x2, 
    0x2, 0x110, 0x111, 0x3, 0x2, 0x2, 0x2, 0x111, 0x112, 0x3, 0x2, 0x2, 
    0x2, 0x112, 0x113, 0x5, 0x96, 0x4c, 0x2, 0x113, 0x114, 0x7, 0x90, 0x2, 
    0x2, 0x114, 0x134, 0x3, 0x2, 0x2, 0x2, 0x115, 0x116, 0x7, 0x24, 0x2, 
    0x2, 0x116, 0x117, 0x7, 0x87, 0x2, 0x2, 0x117, 0x134, 0x5, 0x8c, 0x47, 
    0x2, 0x118, 0x119, 0x7, 0x28, 0x2, 0x2, 0x119, 0x134, 0x5, 0x46, 0x24, 
    0x2, 0x11a, 0x11b, 0x7, 0x2b, 0x2, 0x2, 0x11b, 0x11e, 0x7, 0x1b, 0x2, 
    0x2, 0x11c, 0x11d, 0x7, 0x3e, 0x2, 0x2, 0x11d, 0x11f, 0x7, 0x2f, 0x2, 
    0x2, 0x11e, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x11e, 0x11f, 0x3, 0x2, 0x2, 
    0x2, 0x11f, 0x120, 0x3, 0x2, 0x2, 0x2, 0x120, 0x134, 0x5, 0x96, 0x4c, 
    0x2, 0x121, 0x122, 0x7, 0x2b, 0x2, 0x2, 0x122, 0x134, 0x5, 0x46, 0x24, 
    0x2, 0x123, 0x124, 0x7, 0x51, 0x2, 0x2, 0x124, 0x127, 0x7, 0x1b, 0x2, 
    0x2, 0x125, 0x126, 0x7, 0x3e, 0x2, 0x2, 0x126, 0x128, 0x7, 0x2f, 0x2, 
    0x2, 0x127, 0x125, 0x3, 0x2, 0x2, 0x2, 0x127, 0x128, 0x3, 0x2, 0x2, 
    0x2, 0x128, 0x129, 0x3, 0x2, 0x2, 0x2, 0x129, 0x134, 0x5, 0x28, 0x15, 
    0x2, 0x12a, 0x12b, 0x7, 0x51, 0x2, 0x2, 0x12b, 0x12c, 0x7, 0x5c, 0x2, 
    0x2, 0x12c, 0x12d, 0x7, 0x14, 0x2, 0x2, 0x12d, 0x134, 0x5, 0x8c, 0x47, 
    0x2, 0x12e, 0x12f, 0x7, 0x65, 0x2, 0x2, 0x12f, 0x130, 0x5, 0x46, 0x24, 
    0x2, 0x130, 0x131, 0x7, 0x36, 0x2, 0x2, 0x131, 0x132, 0x5, 0x9c, 0x4f, 
    0x2, 0x132, 0x134, 0x3, 0x2, 0x2, 0x2, 0x133, 0xf0, 0x3, 0x2, 0x2, 0x2, 
    0x133, 0xfc, 0x3, 0x2, 0x2, 0x2, 0x133, 0x102, 0x3, 0x2, 0x2, 0x2, 0x133, 
    0x10c, 0x3, 0x2, 0x2, 0x2, 0x133, 0x115, 0x3, 0x2, 0x2, 0x2, 0x133, 
    0x118, 0x3, 0x2, 0x2, 0x2, 0x133, 0x11a, 0x3, 0x2, 0x2, 0x2, 0x133, 
    0x121, 0x3, 0x2, 0x2, 0x2, 0x133, 0x123, 0x3, 0x2, 0x2, 0x2, 0x133, 
    0x12a, 0x3, 0x2, 0x2, 0x2, 0x133, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x134, 
    0xd, 0x3, 0x2, 0x2, 0x2, 0x135, 0x136, 0x7, 0x9, 0x2, 0x2, 0x136, 0x137, 
    0x5, 0x6, 0x4, 0x2, 0x137, 0xf, 0x3, 0x2, 0x2, 0x2, 0x138, 0x139, 0x7, 
    0x17, 0x2, 0x2, 0x139, 0x13a, 0x7, 0x74, 0x2, 0x2, 0x13a, 0x13b, 0x5, 
    0x9c, 0x4f, 0x2, 0x13b, 0x11, 0x3, 0x2, 0x2, 0x2, 0x13c, 0x13d, 0x9, 
    0x2, 0x2, 0x2, 0x13d, 0x141, 0x7, 0x1f, 0x2, 0x2, 0x13e, 0x13f, 0x7, 
    0x3e, 0x2, 0x2, 0x13f, 0x140, 0x7, 0x55, 0x2, 0x2, 0x140, 0x142, 0x7, 
    0x2f, 0x2, 0x2, 0x141, 0x13e, 0x3, 0x2, 0x2, 0x2, 0x141, 0x142, 0x3, 
    0x2, 0x2, 0x2, 0x142, 0x143, 0x3, 0x2, 0x2, 0x2, 0x143, 0x145, 0x5, 
    0xa2, 0x52, 0x2, 0x144, 0x146, 0x5, 0x24, 0x13, 0x2, 0x145, 0x144, 0x3, 
    0x2, 0x2, 0x2, 0x145, 0x146, 0x3, 0x2, 0x2, 0x2, 0x146, 0x17d, 0x3, 
    0x2, 0x2, 0x2, 0x147, 0x148, 0x9, 0x2, 0x2, 0x2, 0x148, 0x149, 0x7, 
    0x4e, 0x2, 0x2, 0x149, 0x14d, 0x7, 0x83, 0x2, 0x2, 0x14a, 0x14b, 0x7, 
    0x3e, 0x2, 0x2, 0x14b, 0x14c, 0x7, 0x55, 0x2, 0x2, 0x14c, 0x14e, 0x7, 
    0x2f, 0x2, 0x2, 0x14d, 0x14a, 0x3, 0x2, 0x2, 0x2, 0x14d, 0x14e, 0x3, 
    0x2, 0x2, 0x2, 0x14e, 0x14f, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x151, 0x5, 
    0x9c, 0x4f, 0x2, 0x150, 0x152, 0x5, 0x18, 0xd, 0x2, 0x151, 0x150, 0x3, 
    0x2, 0x2, 0x2, 0x151, 0x152, 0x3, 0x2, 0x2, 0x2, 0x152, 0x154, 0x3, 
    0x2, 0x2, 0x2, 0x153, 0x155, 0x5, 0x14, 0xb, 0x2, 0x154, 0x153, 0x3, 
    0x2, 0x2, 0x2, 0x154, 0x155, 0x3, 0x2, 0x2, 0x2, 0x155, 0x157, 0x3, 
    0x2, 0x2, 0x2, 0x156, 0x158, 0x5, 0x1a, 0xe, 0x2, 0x157, 0x156, 0x3, 
    0x2, 0x2, 0x2, 0x157, 0x158, 0x3, 0x2, 0x2, 0x2, 0x158, 0x15a, 0x3, 
    0x2, 0x2, 0x2, 0x159, 0x15b, 0x7, 0x60, 0x2, 0x2, 0x15a, 0x159, 0x3, 
    0x2, 0x2, 0x2, 0x15a, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15b, 0x15c, 0x3, 
    0x2, 0x2, 0x2, 0x15c, 0x15d, 0x5, 0x16, 0xc, 0x2, 0x15d, 0x17d, 0x3, 
    0x2, 0x2, 0x2, 0x15e, 0x160, 0x9, 0x2, 0x2, 0x2, 0x15f, 0x161, 0x7, 
    0x76, 0x2, 0x2, 0x160, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x160, 0x161, 0x3, 
    0x2, 0x2, 0x2, 0x161, 0x162, 0x3, 0x2, 0x2, 0x2, 0x162, 0x166, 0x7, 
    0x74, 0x2, 0x2, 0x163, 0x164, 0x7, 0x3e, 0x2, 0x2, 0x164, 0x165, 0x7, 
    0x55, 0x2, 0x2, 0x165, 0x167, 0x7, 0x2f, 0x2, 0x2, 0x166, 0x163, 0x3, 
    0x2, 0x2, 0x2, 0x166, 0x167, 0x3, 0x2, 0x2, 0x2, 0x167, 0x168, 0x3, 
    0x2, 0x2, 0x2, 0x168, 0x16a, 0x5, 0x9c, 0x4f, 0x2, 0x169, 0x16b, 0x5, 
    0x18, 0xd, 0x2, 0x16a, 0x169, 0x3, 0x2, 0x2, 0x2, 0x16a, 0x16b, 0x3, 
    0x2, 0x2, 0x2, 0x16b, 0x16d, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16e, 0x5, 
    0x1a, 0xe, 0x2, 0x16d, 0x16c, 0x3, 0x2, 0x2, 0x2, 0x16d, 0x16e, 0x3, 
    0x2, 0x2, 0x2, 0x16e, 0x170, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x171, 0x5, 
    0x16, 0xc, 0x2, 0x170, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x170, 0x171, 0x3, 
    0x2, 0x2, 0x2, 0x171, 0x17d, 0x3, 0x2, 0x2, 0x2, 0x172, 0x173, 0x9, 
    0x2, 0x2, 0x2, 0x173, 0x177, 0x7, 0x83, 0x2, 0x2, 0x174, 0x175, 0x7, 
    0x3e, 0x2, 0x2, 0x175, 0x176, 0x7, 0x55, 0x2, 0x2, 0x176, 0x178, 0x7, 
    0x2f, 0x2, 0x2, 0x177, 0x174, 0x3, 0x2, 0x2, 0x2, 0x177, 0x178, 0x3, 
    0x2, 0x2, 0x2, 0x178, 0x179, 0x3, 0x2, 0x2, 0x2, 0x179, 0x17a, 0x5, 
    0x9c, 0x4f, 0x2, 0x17a, 0x17b, 0x5, 0x16, 0xc, 0x2, 0x17b, 0x17d, 0x3, 
    0x2, 0x2, 0x2, 0x17c, 0x13c, 0x3, 0x2, 0x2, 0x2, 0x17c, 0x147, 0x3, 
    0x2, 0x2, 0x2, 0x17c, 0x15e, 0x3, 0x2, 0x2, 0x2, 0x17c, 0x172, 0x3, 
    0x2, 0x2, 0x2, 0x17d, 0x13, 0x3, 0x2, 0x2, 0x2, 0x17e, 0x17f, 0x7, 0x79, 
    0x2, 0x2, 0x17f, 0x180, 0x5, 0x9c, 0x4f, 0x2, 0x180, 0x15, 0x3, 0x2, 
    0x2, 0x2, 0x181, 0x182, 0x7, 0xe, 0x2, 0x2, 0x182, 0x183, 0x5, 0x4a, 
    0x26, 0x2, 0x183, 0x17, 0x3, 0x2, 0x2, 0x2, 0x184, 0x185, 0x7, 0xa2, 
    0x2, 0x2, 0x185, 0x18a, 0x5, 0x26, 0x14, 0x2, 0x186, 0x187, 0x7, 0x97, 
    0x2, 0x2, 0x187, 0x189, 0x5, 0x26, 0x14, 0x2, 0x188, 0x186, 0x3, 0x2, 
    0x2, 0x2, 0x189, 0x18c, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x188, 0x3, 0x2, 
    0x2, 0x2, 0x18a, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x18d, 0x3, 0x2, 
    0x2, 0x2, 0x18c, 0x18a, 0x3, 0x2, 0x2, 0x2, 0x18d, 0x18e, 0x7, 0xac, 
    0x2, 0x2, 0x18e, 0x194, 0x3, 0x2, 0x2, 0x2, 0x18f, 0x190, 0x7, 0xe, 
    0x2, 0x2, 0x190, 0x194, 0x5, 0x9c, 0x4f, 0x2, 0x191, 0x192, 0x7, 0xe, 
    0x2, 0x2, 0x192, 0x194, 0x5, 0x9a, 0x4e, 0x2, 0x193, 0x184, 0x3, 0x2, 
    0x2, 0x2, 0x193, 0x18f, 0x3, 0x2, 0x2, 0x2, 0x193, 0x191, 0x3, 0x2, 
    0x2, 0x2, 0x194, 0x19, 0x3, 0x2, 0x2, 0x2, 0x195, 0x1b0, 0x5, 0x24, 
    0x13, 0x2, 0x196, 0x197, 0x6, 0xe, 0x2, 0x3, 0x197, 0x198, 0x5, 0x60, 
    0x31, 0x2, 0x198, 0x199, 0x8, 0xe, 0x1, 0x2, 0x199, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x19a, 0x19b, 0x6, 0xe, 0x3, 0x3, 0x19b, 0x19c, 0x5, 0x1c, 
    0xf, 0x2, 0x19c, 0x19d, 0x8, 0xe, 0x1, 0x2, 0x19d, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x19e, 0x19f, 0x6, 0xe, 0x4, 0x3, 0x19f, 0x1a0, 0x5, 0x1e, 
    0x10, 0x2, 0x1a0, 0x1a1, 0x8, 0xe, 0x1, 0x2, 0x1a1, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x1a2, 0x1a3, 0x6, 0xe, 0x5, 0x3, 0x1a3, 0x1a4, 0x5, 0x20, 
    0x11, 0x2, 0x1a4, 0x1a5, 0x8, 0xe, 0x1, 0x2, 0x1a5, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x1a6, 0x1a7, 0x6, 0xe, 0x6, 0x3, 0x1a7, 0x1a8, 0x5, 0x22, 
    0x12, 0x2, 0x1a8, 0x1a9, 0x8, 0xe, 0x1, 0x2, 0x1a9, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x1aa, 0x1ab, 0x6, 0xe, 0x7, 0x3, 0x1ab, 0x1ac, 0x5, 0x66, 
    0x34, 0x2, 0x1ac, 0x1ad, 0x8, 0xe, 0x1, 0x2, 0x1ad, 0x1af, 0x3, 0x2, 
    0x2, 0x2, 0x1ae, 0x196, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x19a, 0x3, 0x2, 
    0x2, 0x2, 0x1ae, 0x19e, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1a2, 0x3, 0x2, 
    0x2, 0x2, 0x1ae, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1aa, 0x3, 0x2, 
    0x2, 0x2, 0x1af, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b0, 0x1ae, 0x3, 0x2, 
    0x2, 0x2, 0x1b0, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b, 0x3, 0x2, 0x2, 
    0x2, 0x1b2, 0x1b0, 0x3, 0x2, 0x2, 0x2, 0x1b3, 0x1b4, 0x7, 0x5f, 0x2, 
    0x2, 0x1b4, 0x1b5, 0x7, 0x14, 0x2, 0x2, 0x1b5, 0x1b6, 0x5, 0x8c, 0x47, 
    0x2, 0x1b6, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x1b7, 0x1b8, 0x7, 0x62, 0x2, 
    0x2, 0x1b8, 0x1b9, 0x7, 0x47, 0x2, 0x2, 0x1b9, 0x1ba, 0x5, 0x8c, 0x47, 
    0x2, 0x1ba, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x7, 0x68, 0x2, 
    0x2, 0x1bc, 0x1bd, 0x7, 0x14, 0x2, 0x2, 0x1bd, 0x1be, 0x5, 0x8c, 0x47, 
    0x2, 0x1be, 0x21, 0x3, 0x2, 0x2, 0x2, 0x1bf, 0x1c0, 0x7, 0x7e, 0x2, 
    0x2, 0x1c0, 0x1c5, 0x5, 0x2c, 0x17, 0x2, 0x1c1, 0x1c2, 0x7, 0x97, 0x2, 
    0x2, 0x1c2, 0x1c4, 0x5, 0x2c, 0x17, 0x2, 0x1c3, 0x1c1, 0x3, 0x2, 0x2, 
    0x2, 0x1c4, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1c3, 0x3, 0x2, 0x2, 
    0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x23, 0x3, 0x2, 0x2, 0x2, 
    0x1c7, 0x1c5, 0x3, 0x2, 0x2, 0x2, 0x1c8, 0x1ca, 0x7, 0x2e, 0x2, 0x2, 
    0x1c9, 0x1cb, 0x7, 0x9c, 0x2, 0x2, 0x1ca, 0x1c9, 0x3, 0x2, 0x2, 0x2, 
    0x1ca, 0x1cb, 0x3, 0x2, 0x2, 0x2, 0x1cb, 0x1cc, 0x3, 0x2, 0x2, 0x2, 
    0x1cc, 0x1d2, 0x5, 0xb0, 0x59, 0x2, 0x1cd, 0x1cf, 0x7, 0xa2, 0x2, 0x2, 
    0x1ce, 0x1d0, 0x5, 0x88, 0x45, 0x2, 0x1cf, 0x1ce, 0x3, 0x2, 0x2, 0x2, 
    0x1cf, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x3, 0x2, 0x2, 0x2, 
    0x1d1, 0x1d3, 0x7, 0xac, 0x2, 0x2, 0x1d2, 0x1cd, 0x3, 0x2, 0x2, 0x2, 
    0x1d2, 0x1d3, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x25, 0x3, 0x2, 0x2, 0x2, 0x1d4, 
    0x1d5, 0x5, 0x28, 0x15, 0x2, 0x1d5, 0x27, 0x3, 0x2, 0x2, 0x2, 0x1d6, 
    0x1d7, 0x5, 0x96, 0x4c, 0x2, 0x1d7, 0x1d9, 0x5, 0x86, 0x44, 0x2, 0x1d8, 
    0x1da, 0x5, 0x2a, 0x16, 0x2, 0x1d9, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1d9, 
    0x1da, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1dd, 0x3, 0x2, 0x2, 0x2, 0x1db, 
    0x1dc, 0x7, 0x1c, 0x2, 0x2, 0x1dc, 0x1de, 0x7, 0x90, 0x2, 0x2, 0x1dd, 
    0x1db, 0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1de, 0x3, 0x2, 0x2, 0x2, 0x1de, 
    0x1e1, 0x3, 0x2, 0x2, 0x2, 0x1df, 0x1e0, 0x7, 0x7e, 0x2, 0x2, 0x1e0, 
    0x1e2, 0x5, 0x8c, 0x47, 0x2, 0x1e1, 0x1df, 0x3, 0x2, 0x2, 0x2, 0x1e1, 
    0x1e2, 0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1f1, 0x3, 0x2, 0x2, 0x2, 0x1e3, 
    0x1e5, 0x5, 0x96, 0x4c, 0x2, 0x1e4, 0x1e6, 0x5, 0x86, 0x44, 0x2, 0x1e5, 
    0x1e4, 0x3, 0x2, 0x2, 0x2, 0x1e5, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0x1e6, 
    0x1e7, 0x3, 0x2, 0x2, 0x2, 0x1e7, 0x1ea, 0x5, 0x2a, 0x16, 0x2, 0x1e8, 
    0x1e9, 0x7, 0x1c, 0x2, 0x2, 0x1e9, 0x1eb, 0x7, 0x90, 0x2, 0x2, 0x1ea, 
    0x1e8, 0x3, 0x2, 0x2, 0x2, 0x1ea, 0x1eb, 0x3, 0x2, 0x2, 0x2, 0x1eb, 
    0x1ee, 0x3, 0x2, 0x2, 0x2, 0x1ec, 0x1ed, 0x7, 0x7e, 0x2, 0x2, 0x1ed, 
    0x1ef, 0x5, 0x8c, 0x47, 0x2, 0x1ee, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1ee, 
    0x1ef, 0x3, 0x2, 0x2, 0x2, 0x1ef, 0x1f1, 0x3, 0x2, 0x2, 0x2, 0x1f0, 
    0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1f0, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1f1, 
    0x29, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x9, 0x3, 0x2, 0x2, 0x1f3, 0x1f4, 
    0x5, 0x8c, 0x47, 0x2, 0x1f4, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x1f5, 0x1fd, 
    0x5, 0x8c, 0x47, 0x2, 0x1f6, 0x1fe, 0x7, 0x24, 0x2, 0x2, 0x1f7, 0x1f8, 
    0x7, 0x79, 0x2, 0x2, 0x1f8, 0x1f9, 0x7, 0x29, 0x2, 0x2, 0x1f9, 0x1fe, 
    0x7, 0x90, 0x2, 0x2, 0x1fa, 0x1fb, 0x7, 0x79, 0x2, 0x2, 0x1fb, 0x1fc, 
    0x7, 0x84, 0x2, 0x2, 0x1fc, 0x1fe, 0x7, 0x90, 0x2, 0x2, 0x1fd, 0x1f6, 
    0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1f7, 0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1fa, 
    0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1fe, 0x2d, 0x3, 
    0x2, 0x2, 0x2, 0x1ff, 0x201, 0x9, 0x4, 0x2, 0x2, 0x200, 0x202, 0x7, 
    0x74, 0x2, 0x2, 0x201, 0x200, 0x3, 0x2, 0x2, 0x2, 0x201, 0x202, 0x3, 
    0x2, 0x2, 0x2, 0x202, 0x203, 0x3, 0x2, 0x2, 0x2, 0x203, 0x204, 0x5, 
    0x98, 0x4d, 0x2, 0x204, 0x2f, 0x3, 0x2, 0x2, 0x2, 0x205, 0x206, 0x9, 
    0x5, 0x2, 0x2, 0x206, 0x209, 0x7, 0x1f, 0x2, 0x2, 0x207, 0x208, 0x7, 
    0x3e, 0x2, 0x2, 0x208, 0x20a, 0x7, 0x2f, 0x2, 0x2, 0x209, 0x207, 0x3, 
    0x2, 0x2, 0x2, 0x209, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x20a, 0x20b, 0x3, 
    0x2, 0x2, 0x2, 0x20b, 0x21b, 0x5, 0xa2, 0x52, 0x2, 0x20c, 0x20e, 0x9, 
    0x5, 0x2, 0x2, 0x20d, 0x20f, 0x7, 0x76, 0x2, 0x2, 0x20e, 0x20d, 0x3, 
    0x2, 0x2, 0x2, 0x20e, 0x20f, 0x3, 0x2, 0x2, 0x2, 0x20f, 0x210, 0x3, 
    0x2, 0x2, 0x2, 0x210, 0x213, 0x7, 0x74, 0x2, 0x2, 0x211, 0x212, 0x7, 
    0x3e, 0x2, 0x2, 0x212, 0x214, 0x7, 0x2f, 0x2, 0x2, 0x213, 0x211, 0x3, 
    0x2, 0x2, 0x2, 0x213, 0x214, 0x3, 0x2, 0x2, 0x2, 0x214, 0x215, 0x3, 
    0x2, 0x2, 0x2, 0x215, 0x218, 0x5, 0x9c, 0x4f, 0x2, 0x216, 0x217, 0x7, 
    0x54, 0x2, 0x2, 0x217, 0x219, 0x7, 0x23, 0x2, 0x2, 0x218, 0x216, 0x3, 
    0x2, 0x2, 0x2, 0x218, 0x219, 0x3, 0x2, 0x2, 0x2, 0x219, 0x21b, 0x3, 
    0x2, 0x2, 0x2, 0x21a, 0x205, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x20c, 0x3, 
    0x2, 0x2, 0x2, 0x21b, 0x31, 0x3, 0x2, 0x2, 0x2, 0x21c, 0x21e, 0x7, 0x2f, 
    0x2, 0x2, 0x21d, 0x21f, 0x7, 0x76, 0x2, 0x2, 0x21e, 0x21d, 0x3, 0x2, 
    0x2, 0x2, 0x21e, 0x21f, 0x3, 0x2, 0x2, 0x2, 0x21f, 0x220, 0x3, 0x2, 
    0x2, 0x2, 0x220, 0x221, 0x7, 0x74, 0x2, 0x2, 0x221, 0x222, 0x5, 0x9c, 
    0x4f, 0x2, 0x222, 0x33, 0x3, 0x2, 0x2, 0x2, 0x223, 0x224, 0x7, 0x42, 
    0x2, 0x2, 0x224, 0x226, 0x7, 0x44, 0x2, 0x2, 0x225, 0x227, 0x7, 0x74, 
    0x2, 0x2, 0x226, 0x225, 0x3, 0x2, 0x2, 0x2, 0x226, 0x227, 0x3, 0x2, 
    0x2, 0x2, 0x227, 0x22b, 0x3, 0x2, 0x2, 0x2, 0x228, 0x22c, 0x5, 0x9c, 
    0x4f, 0x2, 0x229, 0x22a, 0x7, 0x38, 0x2, 0x2, 0x22a, 0x22c, 0x5, 0x9a, 
    0x4e, 0x2, 0x22b, 0x228, 0x3, 0x2, 0x2, 0x2, 0x22b, 0x229, 0x3, 0x2, 
    0x2, 0x2, 0x22c, 0x22e, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22f, 0x5, 0x36, 
    0x1c, 0x2, 0x22e, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x22e, 0x22f, 0x3, 0x2, 
    0x2, 0x2, 0x22f, 0x230, 0x3, 0x2, 0x2, 0x2, 0x230, 0x231, 0x5, 0x38, 
    0x1d, 0x2, 0x231, 0x35, 0x3, 0x2, 0x2, 0x2, 0x232, 0x233, 0x7, 0xa2, 
    0x2, 0x2, 0x233, 0x238, 0x5, 0x96, 0x4c, 0x2, 0x234, 0x235, 0x7, 0x97, 
    0x2, 0x2, 0x235, 0x237, 0x5, 0x96, 0x4c, 0x2, 0x236, 0x234, 0x3, 0x2, 
    0x2, 0x2, 0x237, 0x23a, 0x3, 0x2, 0x2, 0x2, 0x238, 0x236, 0x3, 0x2, 
    0x2, 0x2, 0x238, 0x239, 0x3, 0x2, 0x2, 0x2, 0x239, 0x23b, 0x3, 0x2, 
    0x2, 0x2, 0x23a, 0x238, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x23c, 0x7, 0xac, 
    0x2, 0x2, 0x23c, 0x37, 0x3, 0x2, 0x2, 0x2, 0x23d, 0x23e, 0x7, 0x35, 
    0x2, 0x2, 0x23e, 0x23f, 0x5, 0xae, 0x58, 0x2, 0x23f, 0x240, 0x5, 0x3a, 
    0x1e, 0x2, 0x240, 0x245, 0x3, 0x2, 0x2, 0x2, 0x241, 0x242, 0x7, 0x82, 
    0x2, 0x2, 0x242, 0x245, 0x5, 0x3c, 0x1f, 0x2, 0x243, 0x245, 0x5, 0x4a, 
    0x26, 0x2, 0x244, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x244, 0x241, 0x3, 0x2, 
    0x2, 0x2, 0x244, 0x243, 0x3, 0x2, 0x2, 0x2, 0x245, 0x39, 0x3, 0x2, 0x2, 
    0x2, 0x246, 0x24b, 0x5, 0xaa, 0x56, 0x2, 0x247, 0x248, 0x7, 0x97, 0x2, 
    0x2, 0x248, 0x24a, 0x5, 0xaa, 0x56, 0x2, 0x249, 0x247, 0x3, 0x2, 0x2, 
    0x2, 0x24a, 0x24d, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x249, 0x3, 0x2, 0x2, 
    0x2, 0x24b, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24c, 0x25f, 0x3, 0x2, 0x2, 
    0x2, 0x24d, 0x24b, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x255, 0x5, 0x40, 0x21, 
    0x2, 0x24f, 0x251, 0x7, 0x97, 0x2, 0x2, 0x250, 0x24f, 0x3, 0x2, 0x2, 
    0x2, 0x250, 0x251, 0x3, 0x2, 0x2, 0x2, 0x251, 0x252, 0x3, 0x2, 0x2, 
    0x2, 0x252, 0x254, 0x5, 0x40, 0x21, 0x2, 0x253, 0x250, 0x3, 0x2, 0x2, 
    0x2, 0x254, 0x257, 0x3, 0x2, 0x2, 0x2, 0x255, 0x253, 0x3, 0x2, 0x2, 
    0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 0x2, 0x256, 0x25f, 0x3, 0x2, 0x2, 
    0x2, 0x257, 0x255, 0x3, 0x2, 0x2, 0x2, 0x258, 0x25a, 0x5, 0xaa, 0x56, 
    0x2, 0x259, 0x258, 0x3, 0x2, 0x2, 0x2, 0x25a, 0x25b, 0x3, 0x2, 0x2, 
    0x2, 0x25b, 0x259, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x25c, 0x3, 0x2, 0x2, 
    0x2, 0x25c, 0x25f, 0x3, 0x2, 0x2, 0x2, 0x25d, 0x25f, 0x5, 0x3c, 0x1f, 
    0x2, 0x25e, 0x246, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x24e, 0x3, 0x2, 0x2, 
    0x2, 0x25e, 0x259, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x25d, 0x3, 0x2, 0x2, 
    0x2, 0x25f, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x260, 0x267, 0x5, 0x3e, 0x20, 
    0x2, 0x261, 0x263, 0x7, 0x97, 0x2, 0x2, 0x262, 0x261, 0x3, 0x2, 0x2, 
    0x2, 0x262, 0x263, 0x3, 0x2, 0x2, 0x2, 0x263, 0x264, 0x3, 0x2, 0x2, 
    0x2, 0x264, 0x266, 0x5, 0x3e, 0x20, 0x2, 0x265, 0x262, 0x3, 0x2, 0x2, 
    0x2, 0x266, 0x269, 0x3, 0x2, 0x2, 0x2, 0x267, 0x265, 0x3, 0x2, 0x2, 
    0x2, 0x267, 0x268, 0x3, 0x2, 0x2, 0x2, 0x268, 0x3d, 0x3, 0x2, 0x2, 0x2, 
    0x269, 0x267, 0x3, 0x2, 0x2, 0x2, 0x26a, 0x26b, 0x7, 0xa2, 0x2, 0x2, 
    0x26b, 0x26c, 0x5, 0x88, 0x45, 0x2, 0x26c, 0x26d, 0x7, 0xac, 0x2, 0x2, 
    0x26d, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x26e, 0x27c, 0x5, 0xaa, 0x56, 0x2, 
    0x26f, 0x27c, 0x9, 0x6, 0x2, 0x2, 0x270, 0x271, 0x7, 0x9f, 0x2, 0x2, 
    0x271, 0x276, 0x5, 0x42, 0x22, 0x2, 0x272, 0x273, 0x7, 0x97, 0x2, 0x2, 
    0x273, 0x275, 0x5, 0x42, 0x22, 0x2, 0x274, 0x272, 0x3, 0x2, 0x2, 0x2, 
    0x275, 0x278, 0x3, 0x2, 0x2, 0x2, 0x276, 0x274, 0x3, 0x2, 0x2, 0x2, 
    0x276, 0x277, 0x3, 0x2, 0x2, 0x2, 0x277, 0x279, 0x3, 0x2, 0x2, 0x2, 
    0x278, 0x276, 0x3, 0x2, 0x2, 0x2, 0x279, 0x27a, 0x7, 0xaa, 0x2, 0x2, 
    0x27a, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x26e, 0x3, 0x2, 0x2, 0x2, 
    0x27b, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x270, 0x3, 0x2, 0x2, 0x2, 
    0x27c, 0x41, 0x3, 0x2, 0x2, 0x2, 0x27d, 0x27e, 0x7, 0x91, 0x2, 0x2, 
    0x27e, 0x27f, 0x7, 0x96, 0x2, 0x2, 0x27f, 0x280, 0x5, 0x40, 0x21, 0x2, 
    0x280, 0x43, 0x3, 0x2, 0x2, 0x2, 0x281, 0x282, 0x7, 0x5a, 0x2, 0x2, 
    0x282, 0x283, 0x7, 0x74, 0x2, 0x2, 0x283, 0x285, 0x5, 0x9c, 0x4f, 0x2, 
    0x284, 0x286, 0x5, 0x46, 0x24, 0x2, 0x285, 0x284, 0x3, 0x2, 0x2, 0x2, 
    0x285, 0x286, 0x3, 0x2, 0x2, 0x2, 0x286, 0x288, 0x3, 0x2, 0x2, 0x2, 
    0x287, 0x289, 0x7, 0x32, 0x2, 0x2, 0x288, 0x287, 0x3, 0x2, 0x2, 0x2, 
    0x288, 0x289, 0x3, 0x2, 0x2, 0x2, 0x289, 0x28b, 0x3, 0x2, 0x2, 0x2, 
    0x28a, 0x28c, 0x7, 0x21, 0x2, 0x2, 0x28b, 0x28a, 0x3, 0x2, 0x2, 0x2, 
    0x28b, 0x28c, 0x3, 0x2, 0x2, 0x2, 0x28c, 0x45, 0x3, 0x2, 0x2, 0x2, 0x28d, 
    0x28e, 0x7, 0x5f, 0x2, 0x2, 0x28e, 0x293, 0x5, 0x8c, 0x47, 0x2, 0x28f, 
    0x290, 0x7, 0x5f, 0x2, 0x2, 0x290, 0x291, 0x7, 0x3d, 0x2, 0x2, 0x291, 
    0x293, 0x7, 0x90, 0x2, 0x2, 0x292, 0x28d, 0x3, 0x2, 0x2, 0x2, 0x292, 
    0x28f, 0x3, 0x2, 0x2, 0x2, 0x293, 0x47, 0x3, 0x2, 0x2, 0x2, 0x294, 0x295, 
    0x7, 0x64, 0x2, 0x2, 0x295, 0x296, 0x7, 0x74, 0x2, 0x2, 0x296, 0x297, 
    0x5, 0x9c, 0x4f, 0x2, 0x297, 0x298, 0x7, 0x79, 0x2, 0x2, 0x298, 0x2a0, 
    0x5, 0x9c, 0x4f, 0x2, 0x299, 0x29a, 0x7, 0x97, 0x2, 0x2, 0x29a, 0x29b, 
    0x5, 0x9c, 0x4f, 0x2, 0x29b, 0x29c, 0x7, 0x79, 0x2, 0x2, 0x29c, 0x29d, 
    0x5, 0x9c, 0x4f, 0x2, 0x29d, 0x29f, 0x3, 0x2, 0x2, 0x2, 0x29e, 0x299, 
    0x3, 0x2, 0x2, 0x2, 0x29f, 0x2a2, 0x3, 0x2, 0x2, 0x2, 0x2a0, 0x29e, 
    0x3, 0x2, 0x2, 0x2, 0x2a0, 0x2a1, 0x3, 0x2, 0x2, 0x2, 0x2a1, 0x49, 0x3, 
    0x2, 0x2, 0x2, 0x2a2, 0x2a0, 0x3, 0x2, 0x2, 0x2, 0x2a3, 0x2a9, 0x5, 
    0x4c, 0x27, 0x2, 0x2a4, 0x2a5, 0x7, 0x7f, 0x2, 0x2, 0x2a5, 0x2a6, 0x7, 
    0x7, 0x2, 0x2, 0x2a6, 0x2a8, 0x5, 0x4c, 0x27, 0x2, 0x2a7, 0x2a4, 0x3, 
    0x2, 0x2, 0x2, 0x2a8, 0x2ab, 0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2a7, 0x3, 
    0x2, 0x2, 0x2, 0x2a9, 0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2aa, 0x4b, 0x3, 0x2, 
    0x2, 0x2, 0x2ab, 0x2a9, 0x3, 0x2, 0x2, 0x2, 0x2ac, 0x2b2, 0x5, 0x4e, 
    0x28, 0x2, 0x2ad, 0x2ae, 0x7, 0xa2, 0x2, 0x2, 0x2ae, 0x2af, 0x5, 0x4a, 
    0x26, 0x2, 0x2af, 0x2b0, 0x7, 0xac, 0x2, 0x2, 0x2b0, 0x2b2, 0x3, 0x2, 
    0x2, 0x2, 0x2b1, 0x2ac, 0x3, 0x2, 0x2, 0x2, 0x2b1, 0x2ad, 0x3, 0x2, 
    0x2, 0x2, 0x2b2, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2b5, 0x5, 0x50, 
    0x29, 0x2, 0x2b4, 0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2b4, 0x2b5, 0x3, 0x2, 
    0x2, 0x2, 0x2b5, 0x2b6, 0x3, 0x2, 0x2, 0x2, 0x2b6, 0x2b8, 0x7, 0x6a, 
    0x2, 0x2, 0x2b7, 0x2b9, 0x7, 0x2a, 0x2, 0x2, 0x2b8, 0x2b7, 0x3, 0x2, 
    0x2, 0x2, 0x2b8, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2b9, 0x2ba, 0x3, 0x2, 
    0x2, 0x2, 0x2ba, 0x2bc, 0x5, 0x88, 0x45, 0x2, 0x2bb, 0x2bd, 0x5, 0x52, 
    0x2a, 0x2, 0x2bc, 0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2bc, 0x2bd, 0x3, 0x2, 
    0x2, 0x2, 0x2bd, 0x2bf, 0x3, 0x2, 0x2, 0x2, 0x2be, 0x2c0, 0x5, 0x54, 
    0x2b, 0x2, 0x2bf, 0x2be, 0x3, 0x2, 0x2, 0x2, 0x2bf, 0x2c0, 0x3, 0x2, 
    0x2, 0x2, 0x2c0, 0x2c2, 0x3, 0x2, 0x2, 0x2, 0x2c1, 0x2c3, 0x5, 0x56, 
    0x2c, 0x2, 0x2c2, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2c2, 0x2c3, 0x3, 0x2, 
    0x2, 0x2, 0x2c3, 0x2c5, 0x3, 0x2, 0x2, 0x2, 0x2c4, 0x2c6, 0x5, 0x58, 
    0x2d, 0x2, 0x2c5, 0x2c4, 0x3, 0x2, 0x2, 0x2, 0x2c5, 0x2c6, 0x3, 0x2, 
    0x2, 0x2, 0x2c6, 0x2c8, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c9, 0x5, 0x5a, 
    0x2e, 0x2, 0x2c8, 0x2c7, 0x3, 0x2, 0x2, 0x2, 0x2c8, 0x2c9, 0x3, 0x2, 
    0x2, 0x2, 0x2c9, 0x2cb, 0x3, 0x2, 0x2, 0x2, 0x2ca, 0x2cc, 0x5, 0x5c, 
    0x2f, 0x2, 0x2cb, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2cb, 0x2cc, 0x3, 0x2, 
    0x2, 0x2, 0x2cc, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x2cd, 0x2cf, 0x5, 0x5e, 
    0x30, 0x2, 0x2ce, 0x2cd, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2cf, 0x3, 0x2, 
    0x2, 0x2, 0x2cf, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2d2, 0x5, 0x60, 
    0x31, 0x2, 0x2d1, 0x2d0, 0x3, 0x2, 0x2, 0x2, 0x2d1, 0x2d2, 0x3, 0x2, 
    0x2, 0x2, 0x2d2, 0x2d4, 0x3, 0x2, 0x2, 0x2, 0x2d3, 0x2d5, 0x5, 0x62, 
    0x32, 0x2, 0x2d4, 0x2d3, 0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d5, 0x3, 0x2, 
    0x2, 0x2, 0x2d5, 0x2d7, 0x3, 0x2, 0x2, 0x2, 0x2d6, 0x2d8, 0x5, 0x64, 
    0x33, 0x2, 0x2d7, 0x2d6, 0x3, 0x2, 0x2, 0x2, 0x2d7, 0x2d8, 0x3, 0x2, 
    0x2, 0x2, 0x2d8, 0x2da, 0x3, 0x2, 0x2, 0x2, 0x2d9, 0x2db, 0x5, 0x66, 
    0x34, 0x2, 0x2da, 0x2d9, 0x3, 0x2, 0x2, 0x2, 0x2da, 0x2db, 0x3, 0x2, 
    0x2, 0x2, 0x2db, 0x4f, 0x3, 0x2, 0x2, 0x2, 0x2dc, 0x2dd, 0x7, 0x88, 
    0x2, 0x2, 0x2dd, 0x2de, 0x5, 0x88, 0x45, 0x2, 0x2de, 0x51, 0x3, 0x2, 
    0x2, 0x2, 0x2df, 0x2e0, 0x7, 0x36, 0x2, 0x2, 0x2e0, 0x2e2, 0x5, 0x68, 
    0x35, 0x2, 0x2e1, 0x2e3, 0x7, 0x32, 0x2, 0x2, 0x2e2, 0x2e1, 0x3, 0x2, 
    0x2, 0x2, 0x2e2, 0x2e3, 0x3, 0x2, 0x2, 0x2, 0x2e3, 0x53, 0x3, 0x2, 0x2, 
    0x2, 0x2e4, 0x2e5, 0x7, 0x68, 0x2, 0x2, 0x2e5, 0x2e8, 0x5, 0x76, 0x3c, 
    0x2, 0x2e6, 0x2e7, 0x7, 0x58, 0x2, 0x2, 0x2e7, 0x2e9, 0x5, 0x76, 0x3c, 
    0x2, 0x2e8, 0x2e6, 0x3, 0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x3, 0x2, 0x2, 
    0x2, 0x2e9, 0x55, 0x3, 0x2, 0x2, 0x2, 0x2ea, 0x2ec, 0x7, 0x4a, 0x2, 
    0x2, 0x2eb, 0x2ea, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2ec, 0x3, 0x2, 0x2, 
    0x2, 0x2ec, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2ee, 0x7, 0xd, 0x2, 
    0x2, 0x2ee, 0x2ef, 0x7, 0x46, 0x2, 0x2, 0x2ef, 0x2f0, 0x5, 0x88, 0x45, 
    0x2, 0x2f0, 0x57, 0x3, 0x2, 0x2, 0x2, 0x2f1, 0x2f2, 0x7, 0x61, 0x2, 
    0x2, 0x2f2, 0x2f3, 0x5, 0x8c, 0x47, 0x2, 0x2f3, 0x59, 0x3, 0x2, 0x2, 
    0x2, 0x2f4, 0x2f5, 0x7, 0x87, 0x2, 0x2, 0x2f5, 0x2f6, 0x5, 0x8c, 0x47, 
    0x2, 0x2f6, 0x5b, 0x3, 0x2, 0x2, 0x2, 0x2f7, 0x2f8, 0x7, 0x3a, 0x2, 
    0x2, 0x2f8, 0x2f9, 0x7, 0x14, 0x2, 0x2, 0x2f9, 0x2fc, 0x5, 0x88, 0x45, 
    0x2, 0x2fa, 0x2fb, 0x7, 0x88, 0x2, 0x2, 0x2fb, 0x2fd, 0x7, 0x7a, 0x2, 
    0x2, 0x2fc, 0x2fa, 0x3, 0x2, 0x2, 0x2, 0x2fc, 0x2fd, 0x3, 0x2, 0x2, 
    0x2, 0x2fd, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x2fe, 0x2ff, 0x7, 0x3b, 0x2, 
    0x2, 0x2ff, 0x300, 0x5, 0x8c, 0x47, 0x2, 0x300, 0x5f, 0x3, 0x2, 0x2, 
    0x2, 0x301, 0x302, 0x7, 0x5c, 0x2, 0x2, 0x302, 0x303, 0x7, 0x14, 0x2, 
    0x2, 0x303, 0x304, 0x5, 0x72, 0x3a, 0x2, 0x304, 0x61, 0x3, 0x2, 0x2, 
    0x2, 0x305, 0x306, 0x7, 0x4c, 0x2, 0x2, 0x306, 0x307, 0x5, 0x70, 0x39, 
    0x2, 0x307, 0x308, 0x7, 0x14, 0x2, 0x2, 0x308, 0x309, 0x5, 0x88, 0x45, 
    0x2, 0x309, 0x63, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x30b, 0x7, 0x4c, 0x2, 
    0x2, 0x30b, 0x30e, 0x5, 0x70, 0x39, 0x2, 0x30c, 0x30d, 0x7, 0x88, 0x2, 
    0x2, 0x30d, 0x30f, 0x7, 0x78, 0x2, 0x2, 0x30e, 0x30c, 0x3, 0x2, 0x2, 
    0x2, 0x30e, 0x30f, 0x3, 0x2, 0x2, 0x2, 0x30f, 0x65, 0x3, 0x2, 0x2, 0x2, 
    0x310, 0x311, 0x7, 0x6d, 0x2, 0x2, 0x311, 0x312, 0x5, 0x78, 0x3d, 0x2, 
    0x312, 0x67, 0x3, 0x2, 0x2, 0x2, 0x313, 0x314, 0x8, 0x35, 0x1, 0x2, 
    0x314, 0x315, 0x7, 0xa2, 0x2, 0x2, 0x315, 0x316, 0x5, 0x68, 0x35, 0x2, 
    0x316, 0x317, 0x7, 0xac, 0x2, 0x2, 0x317, 0x31a, 0x3, 0x2, 0x2, 0x2, 
    0x318, 0x31a, 0x5, 0x98, 0x4d, 0x2, 0x319, 0x313, 0x3, 0x2, 0x2, 0x2, 
    0x319, 0x318, 0x3, 0x2, 0x2, 0x2, 0x31a, 0x32c, 0x3, 0x2, 0x2, 0x2, 
    0x31b, 0x31c, 0xc, 0x4, 0x2, 0x2, 0x31c, 0x31d, 0x5, 0x6c, 0x37, 0x2, 
    0x31d, 0x31e, 0x5, 0x68, 0x35, 0x5, 0x31e, 0x32b, 0x3, 0x2, 0x2, 0x2, 
    0x31f, 0x321, 0xc, 0x5, 0x2, 0x2, 0x320, 0x322, 0x9, 0x7, 0x2, 0x2, 
    0x321, 0x320, 0x3, 0x2, 0x2, 0x2, 0x321, 0x322, 0x3, 0x2, 0x2, 0x2, 
    0x322, 0x324, 0x3, 0x2, 0x2, 0x2, 0x323, 0x325, 0x5, 0x6a, 0x36, 0x2, 
    0x324, 0x323, 0x3, 0x2, 0x2, 0x2, 0x324, 0x325, 0x3, 0x2, 0x2, 0x2, 
    0x325, 0x326, 0x3, 0x2, 0x2, 0x2, 0x326, 0x327, 0x7, 0x46, 0x2, 0x2, 
    0x327, 0x328, 0x5, 0x68, 0x35, 0x2, 0x328, 0x329, 0x5, 0x6e, 0x38, 0x2, 
    0x329, 0x32b, 0x3, 0x2, 0x2, 0x2, 0x32a, 0x31b, 0x3, 0x2, 0x2, 0x2, 
    0x32a, 0x31f, 0x3, 0x2, 0x2, 0x2, 0x32b, 0x32e, 0x3, 0x2, 0x2, 0x2, 
    0x32c, 0x32a, 0x3, 0x2, 0x2, 0x2, 0x32c, 0x32d, 0x3, 0x2, 0x2, 0x2, 
    0x32d, 0x69, 0x3, 0x2, 0x2, 0x2, 0x32e, 0x32c, 0x3, 0x2, 0x2, 0x2, 0x32f, 
    0x331, 0x7, 0xc, 0x2, 0x2, 0x330, 0x32f, 0x3, 0x2, 0x2, 0x2, 0x330, 
    0x331, 0x3, 0x2, 0x2, 0x2, 0x331, 0x332, 0x3, 0x2, 0x2, 0x2, 0x332, 
    0x338, 0x7, 0x41, 0x2, 0x2, 0x333, 0x335, 0x7, 0x41, 0x2, 0x2, 0x334, 
    0x336, 0x7, 0xc, 0x2, 0x2, 0x335, 0x334, 0x3, 0x2, 0x2, 0x2, 0x335, 
    0x336, 0x3, 0x2, 0x2, 0x2, 0x336, 0x338, 0x3, 0x2, 0x2, 0x2, 0x337, 
    0x330, 0x3, 0x2, 0x2, 0x2, 0x337, 0x333, 0x3, 0x2, 0x2, 0x2, 0x338, 
    0x34e, 0x3, 0x2, 0x2, 0x2, 0x339, 0x33b, 0x9, 0x8, 0x2, 0x2, 0x33a, 
    0x339, 0x3, 0x2, 0x2, 0x2, 0x33a, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x33b, 
    0x33c, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x342, 0x9, 0x9, 0x2, 0x2, 0x33d, 
    0x33f, 0x9, 0x9, 0x2, 0x2, 0x33e, 0x340, 0x9, 0x8, 0x2, 0x2, 0x33f, 
    0x33e, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x340, 0x3, 0x2, 0x2, 0x2, 0x340, 
    0x342, 0x3, 0x2, 0x2, 0x2, 0x341, 0x33a, 0x3, 0x2, 0x2, 0x2, 0x341, 
    0x33d, 0x3, 0x2, 0x2, 0x2, 0x342, 0x34e, 0x3, 0x2, 0x2, 0x2, 0x343, 
    0x345, 0x9, 0xa, 0x2, 0x2, 0x344, 0x343, 0x3, 0x2, 0x2, 0x2, 0x344, 
    0x345, 0x3, 0x2, 0x2, 0x2, 0x345, 0x346, 0x3, 0x2, 0x2, 0x2, 0x346, 
    0x34c, 0x7, 0x37, 0x2, 0x2, 0x347, 0x349, 0x7, 0x37, 0x2, 0x2, 0x348, 
    0x34a, 0x9, 0xa, 0x2, 0x2, 0x349, 0x348, 0x3, 0x2, 0x2, 0x2, 0x349, 
    0x34a, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x34c, 0x3, 0x2, 0x2, 0x2, 0x34b, 
    0x344, 0x3, 0x2, 0x2, 0x2, 0x34b, 0x347, 0x3, 0x2, 0x2, 0x2, 0x34c, 
    0x34e, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x337, 0x3, 0x2, 0x2, 0x2, 0x34d, 
    0x341, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x34b, 0x3, 0x2, 0x2, 0x2, 0x34e, 
    0x6b, 0x3, 0x2, 0x2, 0x2, 0x34f, 0x351, 0x9, 0x7, 0x2, 0x2, 0x350, 0x34f, 
    0x3, 0x2, 0x2, 0x2, 0x350, 0x351, 0x3, 0x2, 0x2, 0x2, 0x351, 0x352, 
    0x3, 0x2, 0x2, 0x2, 0x352, 0x353, 0x7, 0x1e, 0x2, 0x2, 0x353, 0x356, 
    0x7, 0x46, 0x2, 0x2, 0x354, 0x356, 0x7, 0x97, 0x2, 0x2, 0x355, 0x350, 
    0x3, 0x2, 0x2, 0x2, 0x355, 0x354, 0x3, 0x2, 0x2, 0x2, 0x356, 0x6d, 0x3, 
    0x2, 0x2, 0x2, 0x357, 0x358, 0x7, 0x59, 0x2, 0x2, 0x358, 0x361, 0x5, 
    0x88, 0x45, 0x2, 0x359, 0x35a, 0x7, 0x81, 0x2, 0x2, 0x35a, 0x35b, 0x7, 
    0xa2, 0x2, 0x2, 0x35b, 0x35c, 0x5, 0x88, 0x45, 0x2, 0x35c, 0x35d, 0x7, 
    0xac, 0x2, 0x2, 0x35d, 0x361, 0x3, 0x2, 0x2, 0x2, 0x35e, 0x35f, 0x7, 
    0x81, 0x2, 0x2, 0x35f, 0x361, 0x5, 0x88, 0x45, 0x2, 0x360, 0x357, 0x3, 
    0x2, 0x2, 0x2, 0x360, 0x359, 0x3, 0x2, 0x2, 0x2, 0x360, 0x35e, 0x3, 
    0x2, 0x2, 0x2, 0x361, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x362, 0x365, 0x7, 0x8f, 
    0x2, 0x2, 0x363, 0x364, 0x9, 0xb, 0x2, 0x2, 0x364, 0x366, 0x7, 0x8f, 
    0x2, 0x2, 0x365, 0x363, 0x3, 0x2, 0x2, 0x2, 0x365, 0x366, 0x3, 0x2, 
    0x2, 0x2, 0x366, 0x71, 0x3, 0x2, 0x2, 0x2, 0x367, 0x36c, 0x5, 0x74, 
    0x3b, 0x2, 0x368, 0x369, 0x7, 0x97, 0x2, 0x2, 0x369, 0x36b, 0x5, 0x74, 
    0x3b, 0x2, 0x36a, 0x368, 0x3, 0x2, 0x2, 0x2, 0x36b, 0x36e, 0x3, 0x2, 
    0x2, 0x2, 0x36c, 0x36a, 0x3, 0x2, 0x2, 0x2, 0x36c, 0x36d, 0x3, 0x2, 
    0x2, 0x2, 0x36d, 0x73, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x36c, 0x3, 0x2, 0x2, 
    0x2, 0x36f, 0x371, 0x5, 0x8c, 0x47, 0x2, 0x370, 0x372, 0x9, 0xc, 0x2, 
    0x2, 0x371, 0x370, 0x3, 0x2, 0x2, 0x2, 0x371, 0x372, 0x3, 0x2, 0x2, 
    0x2, 0x372, 0x375, 0x3, 0x2, 0x2, 0x2, 0x373, 0x374, 0x7, 0x57, 0x2, 
    0x2, 0x374, 0x376, 0x9, 0xd, 0x2, 0x2, 0x375, 0x373, 0x3, 0x2, 0x2, 
    0x2, 0x375, 0x376, 0x3, 0x2, 0x2, 0x2, 0x376, 0x379, 0x3, 0x2, 0x2, 
    0x2, 0x377, 0x378, 0x7, 0x1a, 0x2, 0x2, 0x378, 0x37a, 0x7, 0x90, 0x2, 
    0x2, 0x379, 0x377, 0x3, 0x2, 0x2, 0x2, 0x379, 0x37a, 0x3, 0x2, 0x2, 
    0x2, 0x37a, 0x75, 0x3, 0x2, 0x2, 0x2, 0x37b, 0x37e, 0x5, 0xa6, 0x54, 
    0x2, 0x37c, 0x37d, 0x7, 0xae, 0x2, 0x2, 0x37d, 0x37f, 0x5, 0xa6, 0x54, 
    0x2, 0x37e, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x37e, 0x37f, 0x3, 0x2, 0x2, 
    0x2, 0x37f, 0x77, 0x3, 0x2, 0x2, 0x2, 0x380, 0x385, 0x5, 0x7a, 0x3e, 
    0x2, 0x381, 0x382, 0x7, 0x97, 0x2, 0x2, 0x382, 0x384, 0x5, 0x7a, 0x3e, 
    0x2, 0x383, 0x381, 0x3, 0x2, 0x2, 0x2, 0x384, 0x387, 0x3, 0x2, 0x2, 
    0x2, 0x385, 0x383, 0x3, 0x2, 0x2, 0x2, 0x385, 0x386, 0x3, 0x2, 0x2, 
    0x2, 0x386, 0x79, 0x3, 0x2, 0x2, 0x2, 0x387, 0x385, 0x3, 0x2, 0x2, 0x2, 
    0x388, 0x389, 0x5, 0xae, 0x58, 0x2, 0x389, 0x38a, 0x7, 0x9c, 0x2, 0x2, 
    0x38a, 0x38b, 0x5, 0xa8, 0x55, 0x2, 0x38b, 0x7b, 0x3, 0x2, 0x2, 0x2, 
    0x38c, 0x38d, 0x7, 0x6c, 0x2, 0x2, 0x38d, 0x38e, 0x5, 0x78, 0x3d, 0x2, 
    0x38e, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x38f, 0x390, 0x7, 0x6e, 0x2, 0x2, 
    0x390, 0x391, 0x7, 0x1d, 0x2, 0x2, 0x391, 0x392, 0x7, 0x1f, 0x2, 0x2, 
    0x392, 0x3ae, 0x5, 0xa2, 0x52, 0x2, 0x393, 0x394, 0x7, 0x6e, 0x2, 0x2, 
    0x394, 0x396, 0x7, 0x1d, 0x2, 0x2, 0x395, 0x397, 0x7, 0x76, 0x2, 0x2, 
    0x396, 0x395, 0x3, 0x2, 0x2, 0x2, 0x396, 0x397, 0x3, 0x2, 0x2, 0x2, 
    0x397, 0x399, 0x3, 0x2, 0x2, 0x2, 0x398, 0x39a, 0x7, 0x74, 0x2, 0x2, 
    0x399, 0x398, 0x3, 0x2, 0x2, 0x2, 0x399, 0x39a, 0x3, 0x2, 0x2, 0x2, 
    0x39a, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x39b, 0x3ae, 0x5, 0x9c, 0x4f, 0x2, 
    0x39c, 0x39e, 0x7, 0x6e, 0x2, 0x2, 0x39d, 0x39f, 0x7, 0x76, 0x2, 0x2, 
    0x39e, 0x39d, 0x3, 0x2, 0x2, 0x2, 0x39e, 0x39f, 0x3, 0x2, 0x2, 0x2, 
    0x39f, 0x3a0, 0x3, 0x2, 0x2, 0x2, 0x3a0, 0x3a3, 0x7, 0x75, 0x2, 0x2, 
    0x3a1, 0x3a2, 0x9, 0xe, 0x2, 0x2, 0x3a2, 0x3a4, 0x5, 0xa2, 0x52, 0x2, 
    0x3a3, 0x3a1, 0x3, 0x2, 0x2, 0x2, 0x3a3, 0x3a4, 0x3, 0x2, 0x2, 0x2, 
    0x3a4, 0x3a8, 0x3, 0x2, 0x2, 0x2, 0x3a5, 0x3a6, 0x7, 0x4b, 0x2, 0x2, 
    0x3a6, 0x3a9, 0x7, 0x90, 0x2, 0x2, 0x3a7, 0x3a9, 0x5, 0x5a, 0x2e, 0x2, 
    0x3a8, 0x3a5, 0x3, 0x2, 0x2, 0x2, 0x3a8, 0x3a7, 0x3, 0x2, 0x2, 0x2, 
    0x3a8, 0x3a9, 0x3, 0x2, 0x2, 0x2, 0x3a9, 0x3ab, 0x3, 0x2, 0x2, 0x2, 
    0x3aa, 0x3ac, 0x5, 0x64, 0x33, 0x2, 0x3ab, 0x3aa, 0x3, 0x2, 0x2, 0x2, 
    0x3ab, 0x3ac, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3ae, 0x3, 0x2, 0x2, 0x2, 
    0x3ad, 0x38f, 0x3, 0x2, 0x2, 0x2, 0x3ad, 0x393, 0x3, 0x2, 0x2, 0x2, 
    0x3ad, 0x39c, 0x3, 0x2, 0x2, 0x2, 0x3ae, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x3af, 
    0x3b0, 0x7, 0x73, 0x2, 0x2, 0x3b0, 0x3b1, 0x9, 0xf, 0x2, 0x2, 0x3b1, 
    0x3b2, 0x9, 0x10, 0x2, 0x2, 0x3b2, 0x3b8, 0x5, 0x9c, 0x4f, 0x2, 0x3b3, 
    0x3b4, 0x7, 0x73, 0x2, 0x2, 0x3b4, 0x3b5, 0x7, 0x72, 0x2, 0x2, 0x3b5, 
    0x3b6, 0x7, 0x66, 0x2, 0x2, 0x3b6, 0x3b8, 0x5, 0x9c, 0x4f, 0x2, 0x3b7, 
    0x3af, 0x3, 0x2, 0x2, 0x2, 0x3b7, 0x3b3, 0x3, 0x2, 0x2, 0x2, 0x3b8, 
    0x81, 0x3, 0x2, 0x2, 0x2, 0x3b9, 0x3bb, 0x7, 0x7d, 0x2, 0x2, 0x3ba, 
    0x3bc, 0x7, 0x76, 0x2, 0x2, 0x3bb, 0x3ba, 0x3, 0x2, 0x2, 0x2, 0x3bb, 
    0x3bc, 0x3, 0x2, 0x2, 0x2, 0x3bc, 0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3bd, 
    0x3c0, 0x7, 0x74, 0x2, 0x2, 0x3be, 0x3bf, 0x7, 0x3e, 0x2, 0x2, 0x3bf, 
    0x3c1, 0x7, 0x2f, 0x2, 0x2, 0x3c0, 0x3be, 0x3, 0x2, 0x2, 0x2, 0x3c0, 
    0x3c1, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3c2, 
    0x3c3, 0x5, 0x9c, 0x4f, 0x2, 0x3c3, 0x83, 0x3, 0x2, 0x2, 0x2, 0x3c4, 
    0x3c5, 0x7, 0x80, 0x2, 0x2, 0x3c5, 0x3c6, 0x5, 0xa2, 0x52, 0x2, 0x3c6, 
    0x85, 0x3, 0x2, 0x2, 0x2, 0x3c7, 0x3f5, 0x5, 0xae, 0x58, 0x2, 0x3c8, 
    0x3c9, 0x5, 0xae, 0x58, 0x2, 0x3c9, 0x3ca, 0x7, 0xa2, 0x2, 0x2, 0x3ca, 
    0x3cb, 0x5, 0x88, 0x45, 0x2, 0x3cb, 0x3cc, 0x7, 0xac, 0x2, 0x2, 0x3cc, 
    0x3f5, 0x3, 0x2, 0x2, 0x2, 0x3cd, 0x3ce, 0x5, 0xae, 0x58, 0x2, 0x3ce, 
    0x3cf, 0x7, 0xa2, 0x2, 0x2, 0x3cf, 0x3d4, 0x5, 0xb6, 0x5c, 0x2, 0x3d0, 
    0x3d1, 0x7, 0x97, 0x2, 0x2, 0x3d1, 0x3d3, 0x5, 0xb6, 0x5c, 0x2, 0x3d2, 
    0x3d0, 0x3, 0x2, 0x2, 0x2, 0x3d3, 0x3d6, 0x3, 0x2, 0x2, 0x2, 0x3d4, 
    0x3d2, 0x3, 0x2, 0x2, 0x2, 0x3d4, 0x3d5, 0x3, 0x2, 0x2, 0x2, 0x3d5, 
    0x3d7, 0x3, 0x2, 0x2, 0x2, 0x3d6, 0x3d4, 0x3, 0x2, 0x2, 0x2, 0x3d7, 
    0x3d8, 0x7, 0xac, 0x2, 0x2, 0x3d8, 0x3f5, 0x3, 0x2, 0x2, 0x2, 0x3d9, 
    0x3da, 0x5, 0xae, 0x58, 0x2, 0x3da, 0x3db, 0x7, 0xa2, 0x2, 0x2, 0x3db, 
    0x3e0, 0x5, 0x86, 0x44, 0x2, 0x3dc, 0x3dd, 0x7, 0x97, 0x2, 0x2, 0x3dd, 
    0x3df, 0x5, 0x86, 0x44, 0x2, 0x3de, 0x3dc, 0x3, 0x2, 0x2, 0x2, 0x3df, 
    0x3e2, 0x3, 0x2, 0x2, 0x2, 0x3e0, 0x3de, 0x3, 0x2, 0x2, 0x2, 0x3e0, 
    0x3e1, 0x3, 0x2, 0x2, 0x2, 0x3e1, 0x3e3, 0x3, 0x2, 0x2, 0x2, 0x3e2, 
    0x3e0, 0x3, 0x2, 0x2, 0x2, 0x3e3, 0x3e4, 0x7, 0xac, 0x2, 0x2, 0x3e4, 
    0x3f5, 0x3, 0x2, 0x2, 0x2, 0x3e5, 0x3e6, 0x5, 0xae, 0x58, 0x2, 0x3e6, 
    0x3e7, 0x7, 0xa2, 0x2, 0x2, 0x3e7, 0x3e8, 0x5, 0xae, 0x58, 0x2, 0x3e8, 
    0x3ef, 0x5, 0x86, 0x44, 0x2, 0x3e9, 0x3ea, 0x7, 0x97, 0x2, 0x2, 0x3ea, 
    0x3eb, 0x5, 0xae, 0x58, 0x2, 0x3eb, 0x3ec, 0x5, 0x86, 0x44, 0x2, 0x3ec, 
    0x3ee, 0x3, 0x2, 0x2, 0x2, 0x3ed, 0x3e9, 0x3, 0x2, 0x2, 0x2, 0x3ee, 
    0x3f1, 0x3, 0x2, 0x2, 0x2, 0x3ef, 0x3ed, 0x3, 0x2, 0x2, 0x2, 0x3ef, 
    0x3f0, 0x3, 0x2, 0x2, 0x2, 0x3f0, 0x3f2, 0x3, 0x2, 0x2, 0x2, 0x3f1, 
    0x3ef, 0x3, 0x2, 0x2, 0x2, 0x3f2, 0x3f3, 0x7, 0xac, 0x2, 0x2, 0x3f3, 
    0x3f5, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3c7, 0x3, 0x2, 0x2, 0x2, 0x3f4, 
    0x3c8, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3cd, 0x3, 0x2, 0x2, 0x2, 0x3f4, 
    0x3d9, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3e5, 0x3, 0x2, 0x2, 0x2, 0x3f5, 
    0x87, 0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3fb, 0x5, 0x8a, 0x46, 0x2, 0x3f7, 
    0x3f8, 0x7, 0x97, 0x2, 0x2, 0x3f8, 0x3fa, 0x5, 0x8a, 0x46, 0x2, 0x3f9, 
    0x3f7, 0x3, 0x2, 0x2, 0x2, 0x3fa, 0x3fd, 0x3, 0x2, 0x2, 0x2, 0x3fb, 
    0x3f9, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3fc, 0x3, 0x2, 0x2, 0x2, 0x3fc, 
    0x89, 0x3, 0x2, 0x2, 0x2, 0x3fd, 0x3fb, 0x3, 0x2, 0x2, 0x2, 0x3fe, 0x3ff, 
    0x5, 0x9c, 0x4f, 0x2, 0x3ff, 0x400, 0x7, 0x9a, 0x2, 0x2, 0x400, 0x402, 
    0x3, 0x2, 0x2, 0x2, 0x401, 0x3fe, 0x3, 0x2, 0x2, 0x2, 0x401, 0x402, 
    0x3, 0x2, 0x2, 0x2, 0x402, 0x403, 0x3, 0x2, 0x2, 0x2, 0x403, 0x40a, 
    0x7, 0x93, 0x2, 0x2, 0x404, 0x405, 0x7, 0xa2, 0x2, 0x2, 0x405, 0x406, 
    0x5, 0x4a, 0x26, 0x2, 0x406, 0x407, 0x7, 0xac, 0x2, 0x2, 0x407, 0x40a, 
    0x3, 0x2, 0x2, 0x2, 0x408, 0x40a, 0x5, 0x8c, 0x47, 0x2, 0x409, 0x401, 
    0x3, 0x2, 0x2, 0x2, 0x409, 0x404, 0x3, 0x2, 0x2, 0x2, 0x409, 0x408, 
    0x3, 0x2, 0x2, 0x2, 0x40a, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x40b, 0x40c, 0x8, 
    0x47, 0x1, 0x2, 0x40c, 0x40e, 0x7, 0x15, 0x2, 0x2, 0x40d, 0x40f, 0x5, 
    0x8c, 0x47, 0x2, 0x40e, 0x40d, 0x3, 0x2, 0x2, 0x2, 0x40e, 0x40f, 0x3, 
    0x2, 0x2, 0x2, 0x40f, 0x415, 0x3, 0x2, 0x2, 0x2, 0x410, 0x411, 0x7, 
    0x86, 0x2, 0x2, 0x411, 0x412, 0x5, 0x8c, 0x47, 0x2, 0x412, 0x413, 0x7, 
    0x77, 0x2, 0x2, 0x413, 0x414, 0x5, 0x8c, 0x47, 0x2, 0x414, 0x416, 0x3, 
    0x2, 0x2, 0x2, 0x415, 0x410, 0x3, 0x2, 0x2, 0x2, 0x416, 0x417, 0x3, 
    0x2, 0x2, 0x2, 0x417, 0x415, 0x3, 0x2, 0x2, 0x2, 0x417, 0x418, 0x3, 
    0x2, 0x2, 0x2, 0x418, 0x41b, 0x3, 0x2, 0x2, 0x2, 0x419, 0x41a, 0x7, 
    0x2c, 0x2, 0x2, 0x41a, 0x41c, 0x5, 0x8c, 0x47, 0x2, 0x41b, 0x419, 0x3, 
    0x2, 0x2, 0x2, 0x41b, 0x41c, 0x3, 0x2, 0x2, 0x2, 0x41c, 0x41d, 0x3, 
    0x2, 0x2, 0x2, 0x41d, 0x41e, 0x7, 0x2d, 0x2, 0x2, 0x41e, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x41f, 0x420, 0x7, 0x16, 0x2, 0x2, 0x420, 0x421, 0x7, 
    0xa2, 0x2, 0x2, 0x421, 0x422, 0x5, 0x8c, 0x47, 0x2, 0x422, 0x423, 0x7, 
    0xe, 0x2, 0x2, 0x423, 0x424, 0x5, 0x86, 0x44, 0x2, 0x424, 0x425, 0x7, 
    0xac, 0x2, 0x2, 0x425, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x426, 0x427, 0x7, 
    0x30, 0x2, 0x2, 0x427, 0x428, 0x7, 0xa2, 0x2, 0x2, 0x428, 0x429, 0x7, 
    0x3, 0x2, 0x2, 0x429, 0x42a, 0x7, 0x36, 0x2, 0x2, 0x42a, 0x42b, 0x5, 
    0x8c, 0x47, 0x2, 0x42b, 0x42c, 0x7, 0xac, 0x2, 0x2, 0x42c, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x42d, 0x42e, 0x7, 0x43, 0x2, 0x2, 0x42e, 0x42f, 0x5, 
    0x8c, 0x47, 0x2, 0x42f, 0x430, 0x7, 0x3, 0x2, 0x2, 0x430, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x431, 0x432, 0x7, 0x71, 0x2, 0x2, 0x432, 0x433, 0x7, 
    0xa2, 0x2, 0x2, 0x433, 0x434, 0x5, 0x8c, 0x47, 0x2, 0x434, 0x435, 0x7, 
    0x36, 0x2, 0x2, 0x435, 0x438, 0x5, 0x8c, 0x47, 0x2, 0x436, 0x437, 0x7, 
    0x34, 0x2, 0x2, 0x437, 0x439, 0x5, 0x8c, 0x47, 0x2, 0x438, 0x436, 0x3, 
    0x2, 0x2, 0x2, 0x438, 0x439, 0x3, 0x2, 0x2, 0x2, 0x439, 0x43a, 0x3, 
    0x2, 0x2, 0x2, 0x43a, 0x43b, 0x7, 0xac, 0x2, 0x2, 0x43b, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x43c, 0x43d, 0x7, 0x7c, 0x2, 0x2, 0x43d, 0x43e, 0x7, 
    0xa2, 0x2, 0x2, 0x43e, 0x43f, 0x9, 0x11, 0x2, 0x2, 0x43f, 0x440, 0x7, 
    0x90, 0x2, 0x2, 0x440, 0x441, 0x7, 0x36, 0x2, 0x2, 0x441, 0x442, 0x5, 
    0x8c, 0x47, 0x2, 0x442, 0x443, 0x7, 0xac, 0x2, 0x2, 0x443, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x444, 0x44a, 0x5, 0xae, 0x58, 0x2, 0x445, 0x447, 0x7, 
    0xa2, 0x2, 0x2, 0x446, 0x448, 0x5, 0x88, 0x45, 0x2, 0x447, 0x446, 0x3, 
    0x2, 0x2, 0x2, 0x447, 0x448, 0x3, 0x2, 0x2, 0x2, 0x448, 0x449, 0x3, 
    0x2, 0x2, 0x2, 0x449, 0x44b, 0x7, 0xac, 0x2, 0x2, 0x44a, 0x445, 0x3, 
    0x2, 0x2, 0x2, 0x44a, 0x44b, 0x3, 0x2, 0x2, 0x2, 0x44b, 0x44c, 0x3, 
    0x2, 0x2, 0x2, 0x44c, 0x44e, 0x7, 0xa2, 0x2, 0x2, 0x44d, 0x44f, 0x5, 
    0x8e, 0x48, 0x2, 0x44e, 0x44d, 0x3, 0x2, 0x2, 0x2, 0x44e, 0x44f, 0x3, 
    0x2, 0x2, 0x2, 0x44f, 0x450, 0x3, 0x2, 0x2, 0x2, 0x450, 0x451, 0x7, 
    0xac, 0x2, 0x2, 0x451, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x452, 0x453, 0x5, 
    0xb2, 0x5a, 0x2, 0x453, 0x454, 0x5, 0x8c, 0x47, 0xf, 0x454, 0x46f, 0x3, 
    0x2, 0x2, 0x2, 0x455, 0x456, 0x5, 0x9c, 0x4f, 0x2, 0x456, 0x457, 0x7, 
    0x9a, 0x2, 0x2, 0x457, 0x459, 0x3, 0x2, 0x2, 0x2, 0x458, 0x455, 0x3, 
    0x2, 0x2, 0x2, 0x458, 0x459, 0x3, 0x2, 0x2, 0x2, 0x459, 0x45a, 0x3, 
    0x2, 0x2, 0x2, 0x45a, 0x46f, 0x7, 0x93, 0x2, 0x2, 0x45b, 0x45c, 0x7, 
    0xa2, 0x2, 0x2, 0x45c, 0x45d, 0x5, 0x4a, 0x26, 0x2, 0x45d, 0x45e, 0x7, 
    0xac, 0x2, 0x2, 0x45e, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x45f, 0x460, 0x7, 
    0xa2, 0x2, 0x2, 0x460, 0x461, 0x5, 0x8c, 0x47, 0x2, 0x461, 0x462, 0x7, 
    0xac, 0x2, 0x2, 0x462, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x463, 0x464, 0x7, 
    0xa2, 0x2, 0x2, 0x464, 0x465, 0x5, 0x88, 0x45, 0x2, 0x465, 0x466, 0x7, 
    0xac, 0x2, 0x2, 0x466, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x467, 0x469, 0x7, 
    0xa0, 0x2, 0x2, 0x468, 0x46a, 0x5, 0x88, 0x45, 0x2, 0x469, 0x468, 0x3, 
    0x2, 0x2, 0x2, 0x469, 0x46a, 0x3, 0x2, 0x2, 0x2, 0x46a, 0x46b, 0x3, 
    0x2, 0x2, 0x2, 0x46b, 0x46f, 0x7, 0xab, 0x2, 0x2, 0x46c, 0x46f, 0x5, 
    0x94, 0x4b, 0x2, 0x46d, 0x46f, 0x5, 0xa8, 0x55, 0x2, 0x46e, 0x40b, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x41f, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x426, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x42d, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x431, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x43c, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x444, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x452, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x458, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x45b, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x45f, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x463, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x467, 0x3, 
    0x2, 0x2, 0x2, 0x46e, 0x46c, 0x3, 0x2, 0x2, 0x2, 0x46e, 0x46d, 0x3, 
    0x2, 0x2, 0x2, 0x46f, 0x498, 0x3, 0x2, 0x2, 0x2, 0x470, 0x471, 0xc, 
    0xd, 0x2, 0x2, 0x471, 0x472, 0x5, 0xb4, 0x5b, 0x2, 0x472, 0x473, 0x5, 
    0x8c, 0x47, 0xe, 0x473, 0x497, 0x3, 0x2, 0x2, 0x2, 0x474, 0x475, 0xc, 
    0xc, 0x2, 0x2, 0x475, 0x476, 0x7, 0xa7, 0x2, 0x2, 0x476, 0x477, 0x5, 
    0x8c, 0x47, 0x2, 0x477, 0x478, 0x7, 0x96, 0x2, 0x2, 0x478, 0x479, 0x5, 
    0x8c, 0x47, 0xd, 0x479, 0x497, 0x3, 0x2, 0x2, 0x2, 0x47a, 0x47c, 0xc, 
    0xb, 0x2, 0x2, 0x47b, 0x47d, 0x7, 0x55, 0x2, 0x2, 0x47c, 0x47b, 0x3, 
    0x2, 0x2, 0x2, 0x47c, 0x47d, 0x3, 0x2, 0x2, 0x2, 0x47d, 0x47e, 0x3, 
    0x2, 0x2, 0x2, 0x47e, 0x47f, 0x7, 0x12, 0x2, 0x2, 0x47f, 0x480, 0x5, 
    0x8c, 0x47, 0x2, 0x480, 0x481, 0x7, 0xa, 0x2, 0x2, 0x481, 0x482, 0x5, 
    0x8c, 0x47, 0xc, 0x482, 0x497, 0x3, 0x2, 0x2, 0x2, 0x483, 0x484, 0xc, 
    0x11, 0x2, 0x2, 0x484, 0x485, 0x7, 0xa0, 0x2, 0x2, 0x485, 0x486, 0x5, 
    0x8c, 0x47, 0x2, 0x486, 0x487, 0x7, 0xab, 0x2, 0x2, 0x487, 0x497, 0x3, 
    0x2, 0x2, 0x2, 0x488, 0x489, 0xc, 0x10, 0x2, 0x2, 0x489, 0x48a, 0x7, 
    0x9a, 0x2, 0x2, 0x48a, 0x497, 0x7, 0x8f, 0x2, 0x2, 0x48b, 0x48c, 0xc, 
    0xe, 0x2, 0x2, 0x48c, 0x48e, 0x7, 0x45, 0x2, 0x2, 0x48d, 0x48f, 0x7, 
    0x55, 0x2, 0x2, 0x48e, 0x48d, 0x3, 0x2, 0x2, 0x2, 0x48e, 0x48f, 0x3, 
    0x2, 0x2, 0x2, 0x48f, 0x490, 0x3, 0x2, 0x2, 0x2, 0x490, 0x497, 0x7, 
    0x56, 0x2, 0x2, 0x491, 0x493, 0xc, 0xa, 0x2, 0x2, 0x492, 0x494, 0x7, 
    0xe, 0x2, 0x2, 0x493, 0x492, 0x3, 0x2, 0x2, 0x2, 0x493, 0x494, 0x3, 
    0x2, 0x2, 0x2, 0x494, 0x495, 0x3, 0x2, 0x2, 0x2, 0x495, 0x497, 0x5, 
    0xae, 0x58, 0x2, 0x496, 0x470, 0x3, 0x2, 0x2, 0x2, 0x496, 0x474, 0x3, 
    0x2, 0x2, 0x2, 0x496, 0x47a, 0x3, 0x2, 0x2, 0x2, 0x496, 0x483, 0x3, 
    0x2, 0x2, 0x2, 0x496, 0x488, 0x3, 0x2, 0x2, 0x2, 0x496, 0x48b, 0x3, 
    0x2, 0x2, 0x2, 0x496, 0x491, 0x3, 0x2, 0x2, 0x2, 0x497, 0x49a, 0x3, 
    0x2, 0x2, 0x2, 0x498, 0x496, 0x3, 0x2, 0x2, 0x2, 0x498, 0x499, 0x3, 
    0x2, 0x2, 0x2, 0x499, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x49a, 0x498, 0x3, 0x2, 
    0x2, 0x2, 0x49b, 0x4a0, 0x5, 0x90, 0x49, 0x2, 0x49c, 0x49d, 0x7, 0x97, 
    0x2, 0x2, 0x49d, 0x49f, 0x5, 0x90, 0x49, 0x2, 0x49e, 0x49c, 0x3, 0x2, 
    0x2, 0x2, 0x49f, 0x4a2, 0x3, 0x2, 0x2, 0x2, 0x4a0, 0x49e, 0x3, 0x2, 
    0x2, 0x2, 0x4a0, 0x4a1, 0x3, 0x2, 0x2, 0x2, 0x4a1, 0x8f, 0x3, 0x2, 0x2, 
    0x2, 0x4a2, 0x4a0, 0x3, 0x2, 0x2, 0x2, 0x4a3, 0x4a6, 0x5, 0x92, 0x4a, 
    0x2, 0x4a4, 0x4a6, 0x5, 0x8c, 0x47, 0x2, 0x4a5, 0x4a3, 0x3, 0x2, 0x2, 
    0x2, 0x4a5, 0x4a4, 0x3, 0x2, 0x2, 0x2, 0x4a6, 0x91, 0x3, 0x2, 0x2, 0x2, 
    0x4a7, 0x4a8, 0x7, 0xa2, 0x2, 0x2, 0x4a8, 0x4ad, 0x5, 0xae, 0x58, 0x2, 
    0x4a9, 0x4aa, 0x7, 0x97, 0x2, 0x2, 0x4aa, 0x4ac, 0x5, 0xae, 0x58, 0x2, 
    0x4ab, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x4ac, 0x4af, 0x3, 0x2, 0x2, 0x2, 
    0x4ad, 0x4ab, 0x3, 0x2, 0x2, 0x2, 0x4ad, 0x4ae, 0x3, 0x2, 0x2, 0x2, 
    0x4ae, 0x4b0, 0x3, 0x2, 0x2, 0x2, 0x4af, 0x4ad, 0x3, 0x2, 0x2, 0x2, 
    0x4b0, 0x4b1, 0x7, 0xac, 0x2, 0x2, 0x4b1, 0x4bb, 0x3, 0x2, 0x2, 0x2, 
    0x4b2, 0x4b7, 0x5, 0xae, 0x58, 0x2, 0x4b3, 0x4b4, 0x7, 0x97, 0x2, 0x2, 
    0x4b4, 0x4b6, 0x5, 0xae, 0x58, 0x2, 0x4b5, 0x4b3, 0x3, 0x2, 0x2, 0x2, 
    0x4b6, 0x4b9, 0x3, 0x2, 0x2, 0x2, 0x4b7, 0x4b5, 0x3, 0x2, 0x2, 0x2, 
    0x4b7, 0x4b8, 0x3, 0x2, 0x2, 0x2, 0x4b8, 0x4bb, 0x3, 0x2, 0x2, 0x2, 
    0x4b9, 0x4b7, 0x3, 0x2, 0x2, 0x2, 0x4ba, 0x4a7, 0x3, 0x2, 0x2, 0x2, 
    0x4ba, 0x4b2, 0x3, 0x2, 0x2, 0x2, 0x4bb, 0x4bc, 0x3, 0x2, 0x2, 0x2, 
    0x4bc, 0x4bd, 0x7, 0x92, 0x2, 0x2, 0x4bd, 0x4be, 0x5, 0x8c, 0x47, 0x2, 
    0x4be, 0x93, 0x3, 0x2, 0x2, 0x2, 0x4bf, 0x4c0, 0x5, 0x9c, 0x4f, 0x2, 
    0x4c0, 0x4c1, 0x7, 0x9a, 0x2, 0x2, 0x4c1, 0x4c3, 0x3, 0x2, 0x2, 0x2, 
    0x4c2, 0x4bf, 0x3, 0x2, 0x2, 0x2, 0x4c2, 0x4c3, 0x3, 0x2, 0x2, 0x2, 
    0x4c3, 0x4c4, 0x3, 0x2, 0x2, 0x2, 0x4c4, 0x4c5, 0x5, 0x96, 0x4c, 0x2, 
    0x4c5, 0x95, 0x3, 0x2, 0x2, 0x2, 0x4c6, 0x4c9, 0x5, 0xae, 0x58, 0x2, 
    0x4c7, 0x4c8, 0x7, 0x9a, 0x2, 0x2, 0x4c8, 0x4ca, 0x5, 0xae, 0x58, 0x2, 
    0x4c9, 0x4c7, 0x3, 0x2, 0x2, 0x2, 0x4c9, 0x4ca, 0x3, 0x2, 0x2, 0x2, 
    0x4ca, 0x97, 0x3, 0x2, 0x2, 0x2, 0x4cb, 0x4cc, 0x8, 0x4d, 0x1, 0x2, 
    0x4cc, 0x4d3, 0x5, 0x9c, 0x4f, 0x2, 0x4cd, 0x4d3, 0x5, 0x9a, 0x4e, 0x2, 
    0x4ce, 0x4cf, 0x7, 0xa2, 0x2, 0x2, 0x4cf, 0x4d0, 0x5, 0x4a, 0x26, 0x2, 
    0x4d0, 0x4d1, 0x7, 0xac, 0x2, 0x2, 0x4d1, 0x4d3, 0x3, 0x2, 0x2, 0x2, 
    0x4d2, 0x4cb, 0x3, 0x2, 0x2, 0x2, 0x4d2, 0x4cd, 0x3, 0x2, 0x2, 0x2, 
    0x4d2, 0x4ce, 0x3, 0x2, 0x2, 0x2, 0x4d3, 0x4db, 0x3, 0x2, 0x2, 0x2, 
    0x4d4, 0x4d6, 0xc, 0x3, 0x2, 0x2, 0x4d5, 0x4d7, 0x7, 0xe, 0x2, 0x2, 
    0x4d6, 0x4d5, 0x3, 0x2, 0x2, 0x2, 0x4d6, 0x4d7, 0x3, 0x2, 0x2, 0x2, 
    0x4d7, 0x4d8, 0x3, 0x2, 0x2, 0x2, 0x4d8, 0x4da, 0x5, 0xae, 0x58, 0x2, 
    0x4d9, 0x4d4, 0x3, 0x2, 0x2, 0x2, 0x4da, 0x4dd, 0x3, 0x2, 0x2, 0x2, 
    0x4db, 0x4d9, 0x3, 0x2, 0x2, 0x2, 0x4db, 0x4dc, 0x3, 0x2, 0x2, 0x2, 
    0x4dc, 0x99, 0x3, 0x2, 0x2, 0x2, 0x4dd, 0x4db, 0x3, 0x2, 0x2, 0x2, 0x4de, 
    0x4df, 0x5, 0xae, 0x58, 0x2, 0x4df, 0x4e1, 0x7, 0xa2, 0x2, 0x2, 0x4e0, 
    0x4e2, 0x5, 0x9e, 0x50, 0x2, 0x4e1, 0x4e0, 0x3, 0x2, 0x2, 0x2, 0x4e1, 
    0x4e2, 0x3, 0x2, 0x2, 0x2, 0x4e2, 0x4e3, 0x3, 0x2, 0x2, 0x2, 0x4e3, 
    0x4e4, 0x7, 0xac, 0x2, 0x2, 0x4e4, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x4e5, 
    0x4e6, 0x5, 0xa2, 0x52, 0x2, 0x4e6, 0x4e7, 0x7, 0x9a, 0x2, 0x2, 0x4e7, 
    0x4e9, 0x3, 0x2, 0x2, 0x2, 0x4e8, 0x4e5, 0x3, 0x2, 0x2, 0x2, 0x4e8, 
    0x4e9, 0x3, 0x2, 0x2, 0x2, 0x4e9, 0x4ea, 0x3, 0x2, 0x2, 0x2, 0x4ea, 
    0x4eb, 0x5, 0xae, 0x58, 0x2, 0x4eb, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x4ec, 
    0x4f1, 0x5, 0xa0, 0x51, 0x2, 0x4ed, 0x4ee, 0x7, 0x97, 0x2, 0x2, 0x4ee, 
    0x4f0, 0x5, 0xa0, 0x51, 0x2, 0x4ef, 0x4ed, 0x3, 0x2, 0x2, 0x2, 0x4f0, 
    0x4f3, 0x3, 0x2, 0x2, 0x2, 0x4f1, 0x4ef, 0x3, 0x2, 0x2, 0x2, 0x4f1, 
    0x4f2, 0x3, 0x2, 0x2, 0x2, 0x4f2, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x4f3, 0x4f1, 
    0x3, 0x2, 0x2, 0x2, 0x4f4, 0x4f7, 0x5, 0x98, 0x4d, 0x2, 0x4f5, 0x4f7, 
    0x5, 0xa8, 0x55, 0x2, 0x4f6, 0x4f4, 0x3, 0x2, 0x2, 0x2, 0x4f6, 0x4f5, 
    0x3, 0x2, 0x2, 0x2, 0x4f7, 0xa1, 0x3, 0x2, 0x2, 0x2, 0x4f8, 0x4f9, 0x5, 
    0xae, 0x58, 0x2, 0x4f9, 0xa3, 0x3, 0x2, 0x2, 0x2, 0x4fa, 0x503, 0x7, 
    0x8d, 0x2, 0x2, 0x4fb, 0x4fc, 0x7, 0x8f, 0x2, 0x2, 0x4fc, 0x4fe, 0x7, 
    0x9a, 0x2, 0x2, 0x4fd, 0x4ff, 0x7, 0x8f, 0x2, 0x2, 0x4fe, 0x4fd, 0x3, 
    0x2, 0x2, 0x2, 0x4fe, 0x4ff, 0x3, 0x2, 0x2, 0x2, 0x4ff, 0x503, 0x3, 
    0x2, 0x2, 0x2, 0x500, 0x501, 0x7, 0x9a, 0x2, 0x2, 0x501, 0x503, 0x7, 
    0x8f, 0x2, 0x2, 0x502, 0x4fa, 0x3, 0x2, 0x2, 0x2, 0x502, 0x4fb, 0x3, 
    0x2, 0x2, 0x2, 0x502, 0x500, 0x3, 0x2, 0x2, 0x2, 0x503, 0xa5, 0x3, 0x2, 
    0x2, 0x2, 0x504, 0x506, 0x9, 0x12, 0x2, 0x2, 0x505, 0x504, 0x3, 0x2, 
    0x2, 0x2, 0x505, 0x506, 0x3, 0x2, 0x2, 0x2, 0x506, 0x50c, 0x3, 0x2, 
    0x2, 0x2, 0x507, 0x50d, 0x5, 0xa4, 0x53, 0x2, 0x508, 0x50d, 0x7, 0x8e, 
    0x2, 0x2, 0x509, 0x50d, 0x7, 0x8f, 0x2, 0x2, 0x50a, 0x50d, 0x7, 0x40, 
    0x2, 0x2, 0x50b, 0x50d, 0x7, 0x53, 0x2, 0x2, 0x50c, 0x507, 0x3, 0x2, 
    0x2, 0x2, 0x50c, 0x508, 0x3, 0x2, 0x2, 0x2, 0x50c, 0x509, 0x3, 0x2, 
    0x2, 0x2, 0x50c, 0x50a, 0x3, 0x2, 0x2, 0x2, 0x50c, 0x50b, 0x3, 0x2, 
    0x2, 0x2, 0x50d, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x50e, 0x512, 0x5, 0xa6, 
    0x54, 0x2, 0x50f, 0x512, 0x7, 0x90, 0x2, 0x2, 0x510, 0x512, 0x7, 0x56, 
    0x2, 0x2, 0x511, 0x50e, 0x3, 0x2, 0x2, 0x2, 0x511, 0x50f, 0x3, 0x2, 
    0x2, 0x2, 0x511, 0x510, 0x3, 0x2, 0x2, 0x2, 0x512, 0xa9, 0x3, 0x2, 0x2, 
    0x2, 0x513, 0x516, 0x5, 0xa6, 0x54, 0x2, 0x514, 0x516, 0x7, 0x91, 0x2, 
    0x2, 0x515, 0x513, 0x3, 0x2, 0x2, 0x2, 0x515, 0x514, 0x3, 0x2, 0x2, 
    0x2, 0x516, 0xab, 0x3, 0x2, 0x2, 0x2, 0x517, 0x518, 0x9, 0x13, 0x2, 
    0x2, 0x518, 0xad, 0x3, 0x2, 0x2, 0x2, 0x519, 0x51e, 0x7, 0x8c, 0x2, 
    0x2, 0x51a, 0x51e, 0x7, 0x91, 0x2, 0x2, 0x51b, 0x51e, 0x7, 0x3, 0x2, 
    0x2, 0x51c, 0x51e, 0x5, 0xac, 0x57, 0x2, 0x51d, 0x519, 0x3, 0x2, 0x2, 
    0x2, 0x51d, 0x51a, 0x3, 0x2, 0x2, 0x2, 0x51d, 0x51b, 0x3, 0x2, 0x2, 
    0x2, 0x51d, 0x51c, 0x3, 0x2, 0x2, 0x2, 0x51e, 0xaf, 0x3, 0x2, 0x2, 0x2, 
    0x51f, 0x522, 0x5, 0xae, 0x58, 0x2, 0x520, 0x522, 0x7, 0x56, 0x2, 0x2, 
    0x521, 0x51f, 0x3, 0x2, 0x2, 0x2, 0x521, 0x520, 0x3, 0x2, 0x2, 0x2, 
    0x522, 0xb1, 0x3, 0x2, 0x2, 0x2, 0x523, 0x524, 0x9, 0x14, 0x2, 0x2, 
    0x524, 0xb3, 0x3, 0x2, 0x2, 0x2, 0x525, 0x540, 0x7, 0x98, 0x2, 0x2, 
    0x526, 0x540, 0x7, 0x93, 0x2, 0x2, 0x527, 0x540, 0x7, 0xae, 0x2, 0x2, 
    0x528, 0x540, 0x7, 0xa6, 0x2, 0x2, 0x529, 0x540, 0x7, 0x99, 0x2, 0x2, 
    0x52a, 0x540, 0x7, 0xa5, 0x2, 0x2, 0x52b, 0x540, 0x7, 0x9b, 0x2, 0x2, 
    0x52c, 0x540, 0x7, 0x9c, 0x2, 0x2, 0x52d, 0x540, 0x7, 0xa4, 0x2, 0x2, 
    0x52e, 0x540, 0x7, 0xa1, 0x2, 0x2, 0x52f, 0x540, 0x7, 0x9d, 0x2, 0x2, 
    0x530, 0x540, 0x7, 0xa3, 0x2, 0x2, 0x531, 0x540, 0x7, 0x9e, 0x2, 0x2, 
    0x532, 0x540, 0x7, 0xa, 0x2, 0x2, 0x533, 0x540, 0x7, 0x5b, 0x2, 0x2, 
    0x534, 0x536, 0x7, 0x55, 0x2, 0x2, 0x535, 0x534, 0x3, 0x2, 0x2, 0x2, 
    0x535, 0x536, 0x3, 0x2, 0x2, 0x2, 0x536, 0x537, 0x3, 0x2, 0x2, 0x2, 
    0x537, 0x540, 0x7, 0x4b, 0x2, 0x2, 0x538, 0x53a, 0x7, 0x39, 0x2, 0x2, 
    0x539, 0x538, 0x3, 0x2, 0x2, 0x2, 0x539, 0x53a, 0x3, 0x2, 0x2, 0x2, 
    0x53a, 0x53c, 0x3, 0x2, 0x2, 0x2, 0x53b, 0x53d, 0x7, 0x55, 0x2, 0x2, 
    0x53c, 0x53b, 0x3, 0x2, 0x2, 0x2, 0x53c, 0x53d, 0x3, 0x2, 0x2, 0x2, 
    0x53d, 0x53e, 0x3, 0x2, 0x2, 0x2, 0x53e, 0x540, 0x7, 0x3f, 0x2, 0x2, 
    0x53f, 0x525, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x526, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x527, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x528, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x529, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x52a, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x52b, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x52c, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x52d, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x52e, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x52f, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x530, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x531, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x532, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x533, 0x3, 0x2, 0x2, 0x2, 0x53f, 0x535, 0x3, 0x2, 0x2, 0x2, 
    0x53f, 0x539, 0x3, 0x2, 0x2, 0x2, 0x540, 0xb5, 0x3, 0x2, 0x2, 0x2, 0x541, 
    0x542, 0x7, 0x90, 0x2, 0x2, 0x542, 0x543, 0x7, 0x9c, 0x2, 0x2, 0x543, 
    0x544, 0x5, 0xa6, 0x54, 0x2, 0x544, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xb0, 
    0xba, 0xc1, 0xc5, 0xcd, 0xd1, 0xe3, 0xed, 0xf5, 0xfa, 0x100, 0x106, 
    0x110, 0x11e, 0x127, 0x133, 0x141, 0x145, 0x14d, 0x151, 0x154, 0x157, 
    0x15a, 0x160, 0x166, 0x16a, 0x16d, 0x170, 0x177, 0x17c, 0x18a, 0x193, 
    0x1ae, 0x1b0, 0x1c5, 0x1ca, 0x1cf, 0x1d2, 0x1d9, 0x1dd, 0x1e1, 0x1e5, 
    0x1ea, 0x1ee, 0x1f0, 0x1fd, 0x201, 0x209, 0x20e, 0x213, 0x218, 0x21a, 
    0x21e, 0x226, 0x22b, 0x22e, 0x238, 0x244, 0x24b, 0x250, 0x255, 0x25b, 
    0x25e, 0x262, 0x267, 0x276, 0x27b, 0x285, 0x288, 0x28b, 0x292, 0x2a0, 
    0x2a9, 0x2b1, 0x2b4, 0x2b8, 0x2bc, 0x2bf, 0x2c2, 0x2c5, 0x2c8, 0x2cb, 
    0x2ce, 0x2d1, 0x2d4, 0x2d7, 0x2da, 0x2e2, 0x2e8, 0x2eb, 0x2fc, 0x30e, 
    0x319, 0x321, 0x324, 0x32a, 0x32c, 0x330, 0x335, 0x337, 0x33a, 0x33f, 
    0x341, 0x344, 0x349, 0x34b, 0x34d, 0x350, 0x355, 0x360, 0x365, 0x36c, 
    0x371, 0x375, 0x379, 0x37e, 0x385, 0x396, 0x399, 0x39e, 0x3a3, 0x3a8, 
    0x3ab, 0x3ad, 0x3b7, 0x3bb, 0x3c0, 0x3d4, 0x3e0, 0x3ef, 0x3f4, 0x3fb, 
    0x401, 0x409, 0x40e, 0x417, 0x41b, 0x438, 0x447, 0x44a, 0x44e, 0x458, 
    0x469, 0x46e, 0x47c, 0x48e, 0x493, 0x496, 0x498, 0x4a0, 0x4a5, 0x4ad, 
    0x4b7, 0x4ba, 0x4c2, 0x4c9, 0x4d2, 0x4d6, 0x4db, 0x4e1, 0x4e8, 0x4f1, 
    0x4f6, 0x4fe, 0x502, 0x505, 0x50c, 0x511, 0x515, 0x51d, 0x521, 0x535, 
    0x539, 0x53c, 0x53f, 
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
