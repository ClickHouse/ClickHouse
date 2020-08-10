
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
    setState(94);
    queryStmt();
    setState(99);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(95);
        match(ClickHouseParser::SEMICOLON);
        setState(96);
        queryStmt(); 
      }
      setState(101);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    }
    setState(103);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SEMICOLON) {
      setState(102);
      match(ClickHouseParser::SEMICOLON);
    }
    setState(105);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::QueryStmtContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
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
    setState(107);
    query();
    setState(111);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INTO) {
      setState(108);
      match(ClickHouseParser::INTO);
      setState(109);
      match(ClickHouseParser::OUTFILE);
      setState(110);
      match(ClickHouseParser::STRING_LITERAL);
    }
    setState(115);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FORMAT) {
      setState(113);
      match(ClickHouseParser::FORMAT);
      setState(114);
      identifier();
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

ClickHouseParser::DistributedStmtContext* ClickHouseParser::QueryContext::distributedStmt() {
  return getRuleContext<ClickHouseParser::DistributedStmtContext>(0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::QueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

ClickHouseParser::SetStmtContext* ClickHouseParser::QueryContext::setStmt() {
  return getRuleContext<ClickHouseParser::SetStmtContext>(0);
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
    setState(120);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::DROP: {
        enterOuterAlt(_localctx, 1);
        setState(117);
        distributedStmt();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 2);
        setState(118);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::SET: {
        enterOuterAlt(_localctx, 3);
        setState(119);
        setStmt();
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

//----------------- DistributedStmtContext ------------------------------------------------------------------

ClickHouseParser::DistributedStmtContext::DistributedStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::DropStmtContext* ClickHouseParser::DistributedStmtContext::dropStmt() {
  return getRuleContext<ClickHouseParser::DropStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::DistributedStmtContext::ON() {
  return getToken(ClickHouseParser::ON, 0);
}

tree::TerminalNode* ClickHouseParser::DistributedStmtContext::CLUSTER() {
  return getToken(ClickHouseParser::CLUSTER, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::DistributedStmtContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}


size_t ClickHouseParser::DistributedStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleDistributedStmt;
}


antlrcpp::Any ClickHouseParser::DistributedStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDistributedStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::DistributedStmtContext* ClickHouseParser::distributedStmt() {
  DistributedStmtContext *_localctx = _tracker.createInstance<DistributedStmtContext>(_ctx, getState());
  enterRule(_localctx, 6, ClickHouseParser::RuleDistributedStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(122);
    dropStmt();
    setState(126);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(123);
      match(ClickHouseParser::ON);
      setState(124);
      match(ClickHouseParser::CLUSTER);
      setState(125);
      identifier();
    }
   
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

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::DropDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::DropDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
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

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::DROP() {
  return getToken(ClickHouseParser::DROP, 0);
}

tree::TerminalNode* ClickHouseParser::DropTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::DropTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
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

ClickHouseParser::DropTableStmtContext::DropTableStmtContext(DropStmtContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::DropTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitDropTableStmt(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::DropStmtContext* ClickHouseParser::dropStmt() {
  DropStmtContext *_localctx = _tracker.createInstance<DropStmtContext>(_ctx, getState());
  enterRule(_localctx, 8, ClickHouseParser::RuleDropStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(145);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(128);
      match(ClickHouseParser::DROP);
      setState(129);
      match(ClickHouseParser::DATABASE);
      setState(132);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx)) {
      case 1: {
        setState(130);
        match(ClickHouseParser::IF);
        setState(131);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(134);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(135);
      match(ClickHouseParser::DROP);
      setState(137);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(136);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(139);
      match(ClickHouseParser::TABLE);
      setState(142);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
      case 1: {
        setState(140);
        match(ClickHouseParser::IF);
        setState(141);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(144);
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
  enterRule(_localctx, 10, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(147);
    selectStmt();
    setState(153);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(148);
      match(ClickHouseParser::UNION);
      setState(149);
      match(ClickHouseParser::ALL);
      setState(150);
      selectStmt();
      setState(155);
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
  enterRule(_localctx, 12, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(157);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(156);
      withClause();
    }
    setState(159);
    match(ClickHouseParser::SELECT);
    setState(161);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
    case 1: {
      setState(160);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(163);
    columnExprList();
    setState(165);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(164);
      fromClause();
    }
    setState(168);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(167);
      sampleClause();
    }
    setState(171);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY

    || _la == ClickHouseParser::LEFT) {
      setState(170);
      arrayJoinClause();
    }
    setState(174);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(173);
      prewhereClause();
    }
    setState(177);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(176);
      whereClause();
    }
    setState(180);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(179);
      groupByClause();
    }
    setState(183);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(182);
      havingClause();
    }
    setState(186);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(185);
      orderByClause();
    }
    setState(189);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx)) {
    case 1: {
      setState(188);
      limitByClause();
      break;
    }

    }
    setState(192);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(191);
      limitClause();
    }
    setState(195);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(194);
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
  enterRule(_localctx, 14, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(197);
    match(ClickHouseParser::WITH);
    setState(198);
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
  enterRule(_localctx, 16, ClickHouseParser::RuleFromClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(200);
    match(ClickHouseParser::FROM);
    setState(201);
    joinExpr(0);
    setState(203);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(202);
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
  enterRule(_localctx, 18, ClickHouseParser::RuleSampleClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(205);
    match(ClickHouseParser::SAMPLE);
    setState(206);
    ratioExpr();
    setState(209);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET) {
      setState(207);
      match(ClickHouseParser::OFFSET);
      setState(208);
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
  enterRule(_localctx, 20, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(212);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LEFT) {
      setState(211);
      match(ClickHouseParser::LEFT);
    }
    setState(214);
    match(ClickHouseParser::ARRAY);
    setState(215);
    match(ClickHouseParser::JOIN);
    setState(216);
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
  enterRule(_localctx, 22, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(218);
    match(ClickHouseParser::PREWHERE);
    setState(219);
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
  enterRule(_localctx, 24, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(221);
    match(ClickHouseParser::WHERE);
    setState(222);
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
  enterRule(_localctx, 26, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(224);
    match(ClickHouseParser::GROUP);
    setState(225);
    match(ClickHouseParser::BY);
    setState(226);
    columnExprList();
    setState(229);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(227);
      match(ClickHouseParser::WITH);
      setState(228);
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
  enterRule(_localctx, 28, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(231);
    match(ClickHouseParser::HAVING);
    setState(232);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(234);
    match(ClickHouseParser::ORDER);
    setState(235);
    match(ClickHouseParser::BY);
    setState(236);
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
  enterRule(_localctx, 32, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(238);
    match(ClickHouseParser::LIMIT);
    setState(239);
    limitExpr();
    setState(240);
    match(ClickHouseParser::BY);
    setState(241);
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
  enterRule(_localctx, 34, ClickHouseParser::RuleLimitClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(243);
    match(ClickHouseParser::LIMIT);
    setState(244);
    limitExpr();
   
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
  enterRule(_localctx, 36, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(246);
    match(ClickHouseParser::SETTINGS);
    setState(247);
    settingExprList();
   
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

ClickHouseParser::SettingExprContext* ClickHouseParser::SetStmtContext::settingExpr() {
  return getRuleContext<ClickHouseParser::SettingExprContext>(0);
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
  enterRule(_localctx, 38, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(249);
    match(ClickHouseParser::SET);
    setState(250);
    settingExpr();
   
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
  size_t startState = 40;
  enterRecursionRule(_localctx, 40, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(258);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 28, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(253);
      tableExpr(0);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<JoinExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(254);
      match(ClickHouseParser::LPAREN);
      setState(255);
      joinExpr(0);
      setState(256);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(275);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(273);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(260);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(261);
          joinOpCross();
          setState(262);
          joinExpr(2);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(264);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(266);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(265);
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
          setState(268);
          joinOp();
          setState(269);
          match(ClickHouseParser::JOIN);
          setState(270);
          joinExpr(0);
          setState(271);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(277);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx);
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
  enterRule(_localctx, 42, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(308);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(286);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
      case 1: {
        setState(279);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(278);
          match(ClickHouseParser::ANY);
        }
        setState(281);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(282);
        match(ClickHouseParser::INNER);
        setState(284);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(283);
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
      setState(296);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx)) {
      case 1: {
        setState(289);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 4) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 4)) & ((1ULL << (ClickHouseParser::ANTI - 4))
          | (1ULL << (ClickHouseParser::ANY - 4))
          | (1ULL << (ClickHouseParser::ASOF - 4))
          | (1ULL << (ClickHouseParser::OUTER - 4))
          | (1ULL << (ClickHouseParser::SEMI - 4)))) != 0)) {
          setState(288);
          _la = _input->LA(1);
          if (!(((((_la - 4) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 4)) & ((1ULL << (ClickHouseParser::ANTI - 4))
            | (1ULL << (ClickHouseParser::ANY - 4))
            | (1ULL << (ClickHouseParser::ASOF - 4))
            | (1ULL << (ClickHouseParser::OUTER - 4))
            | (1ULL << (ClickHouseParser::SEMI - 4)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(291);
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
        setState(292);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(294);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 4) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 4)) & ((1ULL << (ClickHouseParser::ANTI - 4))
          | (1ULL << (ClickHouseParser::ANY - 4))
          | (1ULL << (ClickHouseParser::ASOF - 4))
          | (1ULL << (ClickHouseParser::OUTER - 4))
          | (1ULL << (ClickHouseParser::SEMI - 4)))) != 0)) {
          setState(293);
          _la = _input->LA(1);
          if (!(((((_la - 4) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 4)) & ((1ULL << (ClickHouseParser::ANTI - 4))
            | (1ULL << (ClickHouseParser::ANY - 4))
            | (1ULL << (ClickHouseParser::ASOF - 4))
            | (1ULL << (ClickHouseParser::OUTER - 4))
            | (1ULL << (ClickHouseParser::SEMI - 4)))) != 0))) {
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
      setState(306);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
      case 1: {
        setState(299);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY

        || _la == ClickHouseParser::OUTER) {
          setState(298);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY

          || _la == ClickHouseParser::OUTER)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(301);
        match(ClickHouseParser::FULL);
        break;
      }

      case 2: {
        setState(302);
        match(ClickHouseParser::FULL);
        setState(304);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY

        || _la == ClickHouseParser::OUTER) {
          setState(303);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY

          || _la == ClickHouseParser::OUTER)) {
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
  enterRule(_localctx, 44, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(316);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(311);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(310);
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
        setState(313);
        match(ClickHouseParser::CROSS);
        setState(314);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(315);
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
  enterRule(_localctx, 46, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(327);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(318);
      match(ClickHouseParser::ON);
      setState(319);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(320);
      match(ClickHouseParser::USING);
      setState(321);
      match(ClickHouseParser::LPAREN);
      setState(322);
      columnExprList();
      setState(323);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(325);
      match(ClickHouseParser::USING);
      setState(326);
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

std::vector<tree::TerminalNode *> ClickHouseParser::LimitExprContext::NUMBER_LITERAL() {
  return getTokens(ClickHouseParser::NUMBER_LITERAL);
}

tree::TerminalNode* ClickHouseParser::LimitExprContext::NUMBER_LITERAL(size_t i) {
  return getToken(ClickHouseParser::NUMBER_LITERAL, i);
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
  enterRule(_localctx, 48, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(329);
    match(ClickHouseParser::NUMBER_LITERAL);
    setState(332);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET

    || _la == ClickHouseParser::COMMA) {
      setState(330);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET

      || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(331);
      match(ClickHouseParser::NUMBER_LITERAL);
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
  enterRule(_localctx, 50, ClickHouseParser::RuleOrderExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(334);
    orderExpr();
    setState(339);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(335);
      match(ClickHouseParser::COMMA);
      setState(336);
      orderExpr();
      setState(341);
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
  enterRule(_localctx, 52, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(342);
    columnExpr(0);
    setState(344);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ASCENDING

    || _la == ClickHouseParser::DESCENDING) {
      setState(343);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::ASCENDING

      || _la == ClickHouseParser::DESCENDING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(348);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::NULLS) {
      setState(346);
      match(ClickHouseParser::NULLS);
      setState(347);
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
    setState(352);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::COLLATE) {
      setState(350);
      match(ClickHouseParser::COLLATE);
      setState(351);
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

std::vector<tree::TerminalNode *> ClickHouseParser::RatioExprContext::NUMBER_LITERAL() {
  return getTokens(ClickHouseParser::NUMBER_LITERAL);
}

tree::TerminalNode* ClickHouseParser::RatioExprContext::NUMBER_LITERAL(size_t i) {
  return getToken(ClickHouseParser::NUMBER_LITERAL, i);
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
  enterRule(_localctx, 54, ClickHouseParser::RuleRatioExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(354);
    match(ClickHouseParser::NUMBER_LITERAL);

    setState(355);
    match(ClickHouseParser::SLASH);
    setState(356);
    match(ClickHouseParser::NUMBER_LITERAL);
   
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
  enterRule(_localctx, 56, ClickHouseParser::RuleSettingExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(358);
    settingExpr();
    setState(363);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(359);
      match(ClickHouseParser::COMMA);
      setState(360);
      settingExpr();
      setState(365);
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
  enterRule(_localctx, 58, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(366);
    identifier();
    setState(367);
    match(ClickHouseParser::EQ_SINGLE);
    setState(368);
    literal();
   
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

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ColumnExprListContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnExprListContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(370);
    columnExpr(0);
    setState(375);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(371);
        match(ClickHouseParser::COMMA);
        setState(372);
        columnExpr(0); 
      }
      setState(377);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx);
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

tree::TerminalNode* ClickHouseParser::ColumnExprTupleAccessContext::NUMBER_LITERAL() {
  return getToken(ClickHouseParser::NUMBER_LITERAL, 0);
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
//----------------- ColumnExprAsteriskContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ColumnExprAsteriskContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

ClickHouseParser::ColumnExprAsteriskContext::ColumnExprAsteriskContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ColumnExprAsteriskContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprAsterisk(this);
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

ClickHouseParser::ColumnParamListContext* ClickHouseParser::ColumnExprFunctionContext::columnParamList() {
  return getRuleContext<ClickHouseParser::ColumnParamListContext>(0);
}

ClickHouseParser::ColumnExprFunctionContext::ColumnExprFunctionContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ColumnExprFunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnExprFunction(this);
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
  size_t startState = 62;
  enterRecursionRule(_localctx, 62, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(446);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 59, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(379);
      literal();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(380);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(381);
      match(ClickHouseParser::LPAREN);
      setState(382);
      columnExprList();
      setState(383);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(385);
      match(ClickHouseParser::LBRACKET);
      setState(387);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NOT)
        | (1ULL << ClickHouseParser::NULL_SQL)
        | (1ULL << ClickHouseParser::NULLS)
        | (1ULL << ClickHouseParser::OFFSET)
        | (1ULL << ClickHouseParser::ON)
        | (1ULL << ClickHouseParser::OR)
        | (1ULL << ClickHouseParser::ORDER)
        | (1ULL << ClickHouseParser::OUTER)
        | (1ULL << ClickHouseParser::OUTFILE)
        | (1ULL << ClickHouseParser::PREWHERE)
        | (1ULL << ClickHouseParser::QUARTER)
        | (1ULL << ClickHouseParser::RIGHT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USING - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WHERE - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::NUMBER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::ASTERISK - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::LBRACKET - 64))
        | (1ULL << (ClickHouseParser::LPAREN - 64)))) != 0)) {
        setState(386);
        columnExprList();
      }
      setState(389);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(390);
      match(ClickHouseParser::CASE);
      setState(392);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
      case 1: {
        setState(391);
        columnExpr(0);
        break;
      }

      }
      setState(399); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(394);
        match(ClickHouseParser::WHEN);
        setState(395);
        columnExpr(0);
        setState(396);
        match(ClickHouseParser::THEN);
        setState(397);
        columnExpr(0);
        setState(401); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(405);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(403);
        match(ClickHouseParser::ELSE);
        setState(404);
        columnExpr(0);
      }
      setState(407);
      match(ClickHouseParser::END);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(409);
      match(ClickHouseParser::EXTRACT);
      setState(410);
      match(ClickHouseParser::LPAREN);
      setState(411);
      match(ClickHouseParser::INTERVAL_TYPE);
      setState(412);
      match(ClickHouseParser::FROM);
      setState(413);
      columnExpr(0);
      setState(414);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(416);
      match(ClickHouseParser::TRIM);
      setState(417);
      match(ClickHouseParser::LPAREN);
      setState(418);
      _la = _input->LA(1);
      if (!(((((_la - 11) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 11)) & ((1ULL << (ClickHouseParser::BOTH - 11))
        | (1ULL << (ClickHouseParser::LEADING - 11))
        | (1ULL << (ClickHouseParser::TRAILING - 11)))) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(419);
      match(ClickHouseParser::STRING_LITERAL);
      setState(420);
      match(ClickHouseParser::FROM);
      setState(421);
      columnExpr(0);
      setState(422);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(424);
      match(ClickHouseParser::INTERVAL);
      setState(425);
      columnExpr(0);
      setState(426);
      match(ClickHouseParser::INTERVAL_TYPE);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(428);
      columnIdentifier();
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(429);
      identifier();
      setState(435);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 57, _ctx)) {
      case 1: {
        setState(430);
        match(ClickHouseParser::LPAREN);
        setState(432);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 53) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 53)) & ((1ULL << (ClickHouseParser::NULL_SQL - 53))
          | (1ULL << (ClickHouseParser::NUMBER_LITERAL - 53))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 53)))) != 0)) {
          setState(431);
          columnParamList();
        }
        setState(434);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(437);
      match(ClickHouseParser::LPAREN);
      setState(439);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NOT)
        | (1ULL << ClickHouseParser::NULL_SQL)
        | (1ULL << ClickHouseParser::NULLS)
        | (1ULL << ClickHouseParser::OFFSET)
        | (1ULL << ClickHouseParser::ON)
        | (1ULL << ClickHouseParser::OR)
        | (1ULL << ClickHouseParser::ORDER)
        | (1ULL << ClickHouseParser::OUTER)
        | (1ULL << ClickHouseParser::OUTFILE)
        | (1ULL << ClickHouseParser::PREWHERE)
        | (1ULL << ClickHouseParser::QUARTER)
        | (1ULL << ClickHouseParser::RIGHT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USING - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WHERE - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::NUMBER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::ASTERISK - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::LBRACKET - 64))
        | (1ULL << (ClickHouseParser::LPAREN - 64)))) != 0)) {
        setState(438);
        columnArgList();
      }
      setState(441);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprUnaryOpContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(443);
      unaryOp();
      setState(444);
      columnExpr(6);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(486);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 63, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(484);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprBinaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(448);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(449);
          binaryOp();
          setState(450);
          columnExpr(5);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(452);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(453);
          match(ClickHouseParser::QUERY);
          setState(454);
          columnExpr(0);
          setState(455);
          match(ClickHouseParser::COLON);
          setState(456);
          columnExpr(4);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(458);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(460);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(459);
            match(ClickHouseParser::NOT);
          }
          setState(462);
          match(ClickHouseParser::BETWEEN);
          setState(463);
          columnExpr(0);
          setState(464);
          match(ClickHouseParser::AND);
          setState(465);
          columnExpr(3);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(467);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(468);
          match(ClickHouseParser::LBRACKET);
          setState(469);
          columnExpr(0);
          setState(470);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(472);

          if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
          setState(473);
          match(ClickHouseParser::DOT);
          setState(474);
          match(ClickHouseParser::NUMBER_LITERAL);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(475);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(476);
          match(ClickHouseParser::IS);
          setState(478);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(477);
            match(ClickHouseParser::NOT);
          }
          setState(480);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(481);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(482);
          match(ClickHouseParser::AS);
          setState(483);
          identifier();
          break;
        }

        } 
      }
      setState(488);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 63, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- ColumnParamListContext ------------------------------------------------------------------

ClickHouseParser::ColumnParamListContext::ColumnParamListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::LiteralContext *> ClickHouseParser::ColumnParamListContext::literal() {
  return getRuleContexts<ClickHouseParser::LiteralContext>();
}

ClickHouseParser::LiteralContext* ClickHouseParser::ColumnParamListContext::literal(size_t i) {
  return getRuleContext<ClickHouseParser::LiteralContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnParamListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnParamListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ColumnParamListContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnParamList;
}


antlrcpp::Any ClickHouseParser::ColumnParamListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnParamList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnParamListContext* ClickHouseParser::columnParamList() {
  ColumnParamListContext *_localctx = _tracker.createInstance<ColumnParamListContext>(_ctx, getState());
  enterRule(_localctx, 64, ClickHouseParser::RuleColumnParamList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(489);
    literal();
    setState(494);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(490);
      match(ClickHouseParser::COMMA);
      setState(491);
      literal();
      setState(496);
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
  enterRule(_localctx, 66, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(497);
    columnArgExpr();
    setState(502);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(498);
      match(ClickHouseParser::COMMA);
      setState(499);
      columnArgExpr();
      setState(504);
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
  enterRule(_localctx, 68, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(507);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 66, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(505);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(506);
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
  enterRule(_localctx, 70, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(528);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(509);
        match(ClickHouseParser::LPAREN);
        setState(510);
        identifier();
        setState(515);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(511);
          match(ClickHouseParser::COMMA);
          setState(512);
          identifier();
          setState(517);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(518);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::ALL:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USING:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        setState(520);
        identifier();
        setState(525);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(521);
          match(ClickHouseParser::COMMA);
          setState(522);
          identifier();
          setState(527);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(530);
    match(ClickHouseParser::ARROW);
    setState(531);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnIdentifierContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
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
  enterRule(_localctx, 72, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(536);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 70, _ctx)) {
    case 1: {
      setState(533);
      tableIdentifier();
      setState(534);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(538);
    identifier();
   
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
  size_t startState = 74;
  enterRecursionRule(_localctx, 74, ClickHouseParser::RuleTableExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(553);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(541);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(542);
      identifier();
      setState(543);
      match(ClickHouseParser::LPAREN);
      setState(545);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALL)
        | (1ULL << ClickHouseParser::AND)
        | (1ULL << ClickHouseParser::ANTI)
        | (1ULL << ClickHouseParser::ANY)
        | (1ULL << ClickHouseParser::ARRAY)
        | (1ULL << ClickHouseParser::AS)
        | (1ULL << ClickHouseParser::ASCENDING)
        | (1ULL << ClickHouseParser::ASOF)
        | (1ULL << ClickHouseParser::BETWEEN)
        | (1ULL << ClickHouseParser::BOTH)
        | (1ULL << ClickHouseParser::BY)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::CLUSTER)
        | (1ULL << ClickHouseParser::COLLATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DESCENDING)
        | (1ULL << ClickHouseParser::DISTINCT)
        | (1ULL << ClickHouseParser::DROP)
        | (1ULL << ClickHouseParser::ELSE)
        | (1ULL << ClickHouseParser::END)
        | (1ULL << ClickHouseParser::EXISTS)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::FINAL)
        | (1ULL << ClickHouseParser::FIRST)
        | (1ULL << ClickHouseParser::FORMAT)
        | (1ULL << ClickHouseParser::FROM)
        | (1ULL << ClickHouseParser::FULL)
        | (1ULL << ClickHouseParser::GLOBAL)
        | (1ULL << ClickHouseParser::GROUP)
        | (1ULL << ClickHouseParser::HAVING)
        | (1ULL << ClickHouseParser::HOUR)
        | (1ULL << ClickHouseParser::IF)
        | (1ULL << ClickHouseParser::IN)
        | (1ULL << ClickHouseParser::INNER)
        | (1ULL << ClickHouseParser::INSERT)
        | (1ULL << ClickHouseParser::INTERVAL)
        | (1ULL << ClickHouseParser::INTO)
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NOT)
        | (1ULL << ClickHouseParser::NULL_SQL)
        | (1ULL << ClickHouseParser::NULLS)
        | (1ULL << ClickHouseParser::OFFSET)
        | (1ULL << ClickHouseParser::ON)
        | (1ULL << ClickHouseParser::OR)
        | (1ULL << ClickHouseParser::ORDER)
        | (1ULL << ClickHouseParser::OUTER)
        | (1ULL << ClickHouseParser::OUTFILE)
        | (1ULL << ClickHouseParser::PREWHERE)
        | (1ULL << ClickHouseParser::QUARTER)
        | (1ULL << ClickHouseParser::RIGHT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SELECT - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USING - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WHERE - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::NUMBER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64)))) != 0)) {
        setState(544);
        tableArgList();
      }
      setState(547);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(549);
      match(ClickHouseParser::LPAREN);
      setState(550);
      selectUnionStmt();
      setState(551);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(560);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 73, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(555);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(556);
        match(ClickHouseParser::AS);
        setState(557);
        identifier(); 
      }
      setState(562);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 73, _ctx);
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
  enterRule(_localctx, 76, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(566);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
    case 1: {
      setState(563);
      databaseIdentifier();
      setState(564);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(568);
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
  enterRule(_localctx, 78, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(570);
    tableArgExpr();
    setState(575);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(571);
      match(ClickHouseParser::COMMA);
      setState(572);
      tableArgExpr();
      setState(577);
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

ClickHouseParser::LiteralContext* ClickHouseParser::TableArgExprContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::TableArgExprContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
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
  enterRule(_localctx, 80, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(580);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::NULL_SQL:
      case ClickHouseParser::NUMBER_LITERAL:
      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(578);
        literal();
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::ALL:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USING:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 2);
        setState(579);
        tableIdentifier();
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
  enterRule(_localctx, 82, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(582);
    identifier();
   
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

tree::TerminalNode* ClickHouseParser::LiteralContext::NUMBER_LITERAL() {
  return getToken(ClickHouseParser::NUMBER_LITERAL, 0);
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
  enterRule(_localctx, 84, ClickHouseParser::RuleLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(584);
    _la = _input->LA(1);
    if (!(((((_la - 53) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 53)) & ((1ULL << (ClickHouseParser::NULL_SQL - 53))
      | (1ULL << (ClickHouseParser::NUMBER_LITERAL - 53))
      | (1ULL << (ClickHouseParser::STRING_LITERAL - 53)))) != 0))) {
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

tree::TerminalNode* ClickHouseParser::KeywordContext::ALL() {
  return getToken(ClickHouseParser::ALL, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::CLUSTER() {
  return getToken(ClickHouseParser::CLUSTER, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::COLLATE() {
  return getToken(ClickHouseParser::COLLATE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::CROSS() {
  return getToken(ClickHouseParser::CROSS, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DAY() {
  return getToken(ClickHouseParser::DAY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCENDING() {
  return getToken(ClickHouseParser::DESCENDING, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::MINUTE() {
  return getToken(ClickHouseParser::MINUTE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::MONTH() {
  return getToken(ClickHouseParser::MONTH, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::PREWHERE() {
  return getToken(ClickHouseParser::PREWHERE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::QUARTER() {
  return getToken(ClickHouseParser::QUARTER, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::SELECT() {
  return getToken(ClickHouseParser::SELECT, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::TEMPORARY() {
  return getToken(ClickHouseParser::TEMPORARY, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::THEN() {
  return getToken(ClickHouseParser::THEN, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::UNION() {
  return getToken(ClickHouseParser::UNION, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::USING() {
  return getToken(ClickHouseParser::USING, 0);
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
  enterRule(_localctx, 86, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(586);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ALL)
      | (1ULL << ClickHouseParser::AND)
      | (1ULL << ClickHouseParser::ANTI)
      | (1ULL << ClickHouseParser::ANY)
      | (1ULL << ClickHouseParser::ARRAY)
      | (1ULL << ClickHouseParser::AS)
      | (1ULL << ClickHouseParser::ASCENDING)
      | (1ULL << ClickHouseParser::ASOF)
      | (1ULL << ClickHouseParser::BETWEEN)
      | (1ULL << ClickHouseParser::BOTH)
      | (1ULL << ClickHouseParser::BY)
      | (1ULL << ClickHouseParser::CASE)
      | (1ULL << ClickHouseParser::CAST)
      | (1ULL << ClickHouseParser::CLUSTER)
      | (1ULL << ClickHouseParser::COLLATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DAY)
      | (1ULL << ClickHouseParser::DESCENDING)
      | (1ULL << ClickHouseParser::DISTINCT)
      | (1ULL << ClickHouseParser::DROP)
      | (1ULL << ClickHouseParser::ELSE)
      | (1ULL << ClickHouseParser::END)
      | (1ULL << ClickHouseParser::EXISTS)
      | (1ULL << ClickHouseParser::EXTRACT)
      | (1ULL << ClickHouseParser::FINAL)
      | (1ULL << ClickHouseParser::FIRST)
      | (1ULL << ClickHouseParser::FORMAT)
      | (1ULL << ClickHouseParser::FROM)
      | (1ULL << ClickHouseParser::FULL)
      | (1ULL << ClickHouseParser::GLOBAL)
      | (1ULL << ClickHouseParser::GROUP)
      | (1ULL << ClickHouseParser::HAVING)
      | (1ULL << ClickHouseParser::HOUR)
      | (1ULL << ClickHouseParser::IF)
      | (1ULL << ClickHouseParser::IN)
      | (1ULL << ClickHouseParser::INNER)
      | (1ULL << ClickHouseParser::INSERT)
      | (1ULL << ClickHouseParser::INTERVAL)
      | (1ULL << ClickHouseParser::INTO)
      | (1ULL << ClickHouseParser::IS)
      | (1ULL << ClickHouseParser::JOIN)
      | (1ULL << ClickHouseParser::LAST)
      | (1ULL << ClickHouseParser::LEADING)
      | (1ULL << ClickHouseParser::LEFT)
      | (1ULL << ClickHouseParser::LIKE)
      | (1ULL << ClickHouseParser::LIMIT)
      | (1ULL << ClickHouseParser::LOCAL)
      | (1ULL << ClickHouseParser::MINUTE)
      | (1ULL << ClickHouseParser::MONTH)
      | (1ULL << ClickHouseParser::NOT)
      | (1ULL << ClickHouseParser::NULLS)
      | (1ULL << ClickHouseParser::OFFSET)
      | (1ULL << ClickHouseParser::ON)
      | (1ULL << ClickHouseParser::OR)
      | (1ULL << ClickHouseParser::ORDER)
      | (1ULL << ClickHouseParser::OUTER)
      | (1ULL << ClickHouseParser::OUTFILE)
      | (1ULL << ClickHouseParser::PREWHERE)
      | (1ULL << ClickHouseParser::QUARTER)
      | (1ULL << ClickHouseParser::RIGHT))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SELECT - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::TABLE - 64))
      | (1ULL << (ClickHouseParser::TEMPORARY - 64))
      | (1ULL << (ClickHouseParser::THEN - 64))
      | (1ULL << (ClickHouseParser::TOTALS - 64))
      | (1ULL << (ClickHouseParser::TRAILING - 64))
      | (1ULL << (ClickHouseParser::TRIM - 64))
      | (1ULL << (ClickHouseParser::UNION - 64))
      | (1ULL << (ClickHouseParser::USING - 64))
      | (1ULL << (ClickHouseParser::WEEK - 64))
      | (1ULL << (ClickHouseParser::WHEN - 64))
      | (1ULL << (ClickHouseParser::WHERE - 64))
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
  enterRule(_localctx, 88, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(591);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(588);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE: {
        enterOuterAlt(_localctx, 2);
        setState(589);
        match(ClickHouseParser::INTERVAL_TYPE);
        break;
      }

      case ClickHouseParser::ALL:
      case ClickHouseParser::AND:
      case ClickHouseParser::ANTI:
      case ClickHouseParser::ANY:
      case ClickHouseParser::ARRAY:
      case ClickHouseParser::AS:
      case ClickHouseParser::ASCENDING:
      case ClickHouseParser::ASOF:
      case ClickHouseParser::BETWEEN:
      case ClickHouseParser::BOTH:
      case ClickHouseParser::BY:
      case ClickHouseParser::CASE:
      case ClickHouseParser::CAST:
      case ClickHouseParser::CLUSTER:
      case ClickHouseParser::COLLATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DESCENDING:
      case ClickHouseParser::DISTINCT:
      case ClickHouseParser::DROP:
      case ClickHouseParser::ELSE:
      case ClickHouseParser::END:
      case ClickHouseParser::EXISTS:
      case ClickHouseParser::EXTRACT:
      case ClickHouseParser::FINAL:
      case ClickHouseParser::FIRST:
      case ClickHouseParser::FORMAT:
      case ClickHouseParser::FROM:
      case ClickHouseParser::FULL:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::GROUP:
      case ClickHouseParser::HAVING:
      case ClickHouseParser::HOUR:
      case ClickHouseParser::IF:
      case ClickHouseParser::IN:
      case ClickHouseParser::INNER:
      case ClickHouseParser::INSERT:
      case ClickHouseParser::INTERVAL:
      case ClickHouseParser::INTO:
      case ClickHouseParser::IS:
      case ClickHouseParser::JOIN:
      case ClickHouseParser::LAST:
      case ClickHouseParser::LEADING:
      case ClickHouseParser::LEFT:
      case ClickHouseParser::LIKE:
      case ClickHouseParser::LIMIT:
      case ClickHouseParser::LOCAL:
      case ClickHouseParser::MINUTE:
      case ClickHouseParser::MONTH:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULLS:
      case ClickHouseParser::OFFSET:
      case ClickHouseParser::ON:
      case ClickHouseParser::OR:
      case ClickHouseParser::ORDER:
      case ClickHouseParser::OUTER:
      case ClickHouseParser::OUTFILE:
      case ClickHouseParser::PREWHERE:
      case ClickHouseParser::QUARTER:
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SELECT:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USING:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WHERE:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR: {
        enterOuterAlt(_localctx, 3);
        setState(590);
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
  enterRule(_localctx, 90, ClickHouseParser::RuleUnaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(593);
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
  enterRule(_localctx, 92, ClickHouseParser::RuleBinaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(621);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 81, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(595);
      match(ClickHouseParser::CONCAT);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(596);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(597);
      match(ClickHouseParser::SLASH);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(598);
      match(ClickHouseParser::PLUS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(599);
      match(ClickHouseParser::DASH);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(600);
      match(ClickHouseParser::PERCENT);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(601);
      match(ClickHouseParser::EQ_DOUBLE);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(602);
      match(ClickHouseParser::EQ_SINGLE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(603);
      match(ClickHouseParser::NOT_EQ);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(604);
      match(ClickHouseParser::LE);
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(605);
      match(ClickHouseParser::GE);
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(606);
      match(ClickHouseParser::LT);
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(607);
      match(ClickHouseParser::GT);
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(608);
      match(ClickHouseParser::AND);
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(609);
      match(ClickHouseParser::OR);
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(611);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(610);
        match(ClickHouseParser::NOT);
      }
      setState(613);
      match(ClickHouseParser::LIKE);
      break;
    }

    case 17: {
      enterOuterAlt(_localctx, 17);
      setState(615);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::GLOBAL) {
        setState(614);
        match(ClickHouseParser::GLOBAL);
      }
      setState(618);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(617);
        match(ClickHouseParser::NOT);
      }
      setState(620);
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

bool ClickHouseParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 20: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 31: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 37: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

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
    case 2: return precpred(_ctx, 4);
    case 3: return precpred(_ctx, 3);
    case 4: return precpred(_ctx, 2);
    case 5: return precpred(_ctx, 8);
    case 6: return precpred(_ctx, 7);
    case 7: return precpred(_ctx, 5);
    case 8: return precpred(_ctx, 1);

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
  "queryList", "queryStmt", "query", "distributedStmt", "dropStmt", "selectUnionStmt", 
  "selectStmt", "withClause", "fromClause", "sampleClause", "arrayJoinClause", 
  "prewhereClause", "whereClause", "groupByClause", "havingClause", "orderByClause", 
  "limitByClause", "limitClause", "settingsClause", "setStmt", "joinExpr", 
  "joinOp", "joinOpCross", "joinConstraintClause", "limitExpr", "orderExprList", 
  "orderExpr", "ratioExpr", "settingExprList", "settingExpr", "columnExprList", 
  "columnExpr", "columnParamList", "columnArgList", "columnArgExpr", "columnLambdaExpr", 
  "columnIdentifier", "tableExpr", "tableIdentifier", "tableArgList", "tableArgExpr", 
  "databaseIdentifier", "literal", "keyword", "identifier", "unaryOp", "binaryOp"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "'->'", "'*'", 
  "'`'", "'\\'", "':'", "','", "'||'", "'-'", "'.'", "'=='", "'='", "'>='", 
  "'>'", "'['", "'<='", "'('", "'<'", "", "'%'", "'+'", "'?'", "'''", "']'", 
  "')'", "';'", "'/'", "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "INTERVAL_TYPE", "ALL", "AND", "ANTI", "ANY", "ARRAY", "AS", "ASCENDING", 
  "ASOF", "BETWEEN", "BOTH", "BY", "CASE", "CAST", "CLUSTER", "COLLATE", 
  "CROSS", "DATABASE", "DAY", "DESCENDING", "DISTINCT", "DROP", "ELSE", 
  "END", "EXISTS", "EXTRACT", "FINAL", "FIRST", "FORMAT", "FROM", "FULL", 
  "GLOBAL", "GROUP", "HAVING", "HOUR", "IF", "IN", "INNER", "INSERT", "INTERVAL", 
  "INTO", "IS", "JOIN", "LAST", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCAL", 
  "MINUTE", "MONTH", "NOT", "NULL_SQL", "NULLS", "OFFSET", "ON", "OR", "ORDER", 
  "OUTER", "OUTFILE", "PREWHERE", "QUARTER", "RIGHT", "SAMPLE", "SECOND", 
  "SELECT", "SEMI", "SET", "SETTINGS", "TABLE", "TEMPORARY", "THEN", "TOTALS", 
  "TRAILING", "TRIM", "UNION", "USING", "WEEK", "WHEN", "WHERE", "WITH", 
  "YEAR", "IDENTIFIER", "NUMBER_LITERAL", "STRING_LITERAL", "ARROW", "ASTERISK", 
  "BACKQUOTE", "BACKSLASH", "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ_DOUBLE", 
  "EQ_SINGLE", "GE", "GT", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", 
  "PLUS", "QUERY", "QUOTE_SINGLE", "RBRACKET", "RPAREN", "SEMICOLON", "SLASH", 
  "UNDERSCORE", "LINE_COMMENT", "WHITESPACE"
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
    0x3, 0x74, 0x272, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x7, 
    0x2, 0x64, 0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 0x67, 0xb, 0x2, 0x3, 0x2, 0x5, 
    0x2, 0x6a, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x5, 0x3, 0x72, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x76, 
    0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0x7b, 0xa, 0x4, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x81, 0xa, 0x5, 0x3, 0x6, 
    0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x87, 0xa, 0x6, 0x3, 0x6, 0x3, 
    0x6, 0x3, 0x6, 0x5, 0x6, 0x8c, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 
    0x5, 0x6, 0x91, 0xa, 0x6, 0x3, 0x6, 0x5, 0x6, 0x94, 0xa, 0x6, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x7, 0x7, 0x9a, 0xa, 0x7, 0xc, 0x7, 0xe, 
    0x7, 0x9d, 0xb, 0x7, 0x3, 0x8, 0x5, 0x8, 0xa0, 0xa, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x5, 0x8, 0xa4, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0xa8, 0xa, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xab, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xae, 0xa, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xb1, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xb4, 0xa, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xb7, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xba, 0xa, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xbd, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xc0, 0xa, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xc3, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xc6, 0xa, 
    0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 
    0xa, 0xce, 0xa, 0xa, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 
    0xd4, 0xa, 0xb, 0x3, 0xc, 0x5, 0xc, 0xd7, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 
    0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x5, 0xf, 
    0xe8, 0xa, 0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 
    0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 
    0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 
    0x16, 0x3, 0x16, 0x3, 0x16, 0x5, 0x16, 0x105, 0xa, 0x16, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x5, 0x16, 0x10d, 
    0xa, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x7, 
    0x16, 0x114, 0xa, 0x16, 0xc, 0x16, 0xe, 0x16, 0x117, 0xb, 0x16, 0x3, 
    0x17, 0x5, 0x17, 0x11a, 0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 
    0x5, 0x17, 0x11f, 0xa, 0x17, 0x5, 0x17, 0x121, 0xa, 0x17, 0x3, 0x17, 
    0x5, 0x17, 0x124, 0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 
    0x129, 0xa, 0x17, 0x5, 0x17, 0x12b, 0xa, 0x17, 0x3, 0x17, 0x5, 0x17, 
    0x12e, 0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x133, 
    0xa, 0x17, 0x5, 0x17, 0x135, 0xa, 0x17, 0x5, 0x17, 0x137, 0xa, 0x17, 
    0x3, 0x18, 0x5, 0x18, 0x13a, 0xa, 0x18, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 
    0x5, 0x18, 0x13f, 0xa, 0x18, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x14a, 
    0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x5, 0x1a, 0x14f, 0xa, 0x1a, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x7, 0x1b, 0x154, 0xa, 0x1b, 0xc, 0x1b, 
    0xe, 0x1b, 0x157, 0xb, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x15b, 
    0xa, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x15f, 0xa, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x5, 0x1c, 0x163, 0xa, 0x1c, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 
    0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x7, 0x1e, 0x16c, 0xa, 0x1e, 
    0xc, 0x1e, 0xe, 0x1e, 0x16f, 0xb, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x7, 0x20, 0x178, 0xa, 0x20, 
    0xc, 0x20, 0xe, 0x20, 0x17b, 0xb, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 
    0x21, 0x186, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 
    0x18b, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x6, 0x21, 0x192, 0xa, 0x21, 0xd, 0x21, 0xe, 0x21, 0x193, 0x3, 0x21, 
    0x3, 0x21, 0x5, 0x21, 0x198, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 
    0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 
    0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x1b3, 0xa, 0x21, 0x3, 0x21, 
    0x5, 0x21, 0x1b6, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x1ba, 
    0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 
    0x21, 0x1c1, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 
    0x21, 0x3, 0x21, 0x5, 0x21, 0x1cf, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 
    0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x5, 0x21, 0x1e1, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
    0x3, 0x21, 0x7, 0x21, 0x1e7, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x1ea, 
    0xb, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x7, 0x22, 0x1ef, 0xa, 0x22, 
    0xc, 0x22, 0xe, 0x22, 0x1f2, 0xb, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 
    0x7, 0x23, 0x1f7, 0xa, 0x23, 0xc, 0x23, 0xe, 0x23, 0x1fa, 0xb, 0x23, 
    0x3, 0x24, 0x3, 0x24, 0x5, 0x24, 0x1fe, 0xa, 0x24, 0x3, 0x25, 0x3, 0x25, 
    0x3, 0x25, 0x3, 0x25, 0x7, 0x25, 0x204, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 
    0x207, 0xb, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 
    0x7, 0x25, 0x20e, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 0x211, 0xb, 0x25, 
    0x5, 0x25, 0x213, 0xa, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 
    0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x21b, 0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 
    0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x5, 0x27, 0x224, 
    0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 
    0x27, 0x5, 0x27, 0x22c, 0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 
    0x7, 0x27, 0x231, 0xa, 0x27, 0xc, 0x27, 0xe, 0x27, 0x234, 0xb, 0x27, 
    0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 0x239, 0xa, 0x28, 0x3, 0x28, 
    0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x7, 0x29, 0x240, 0xa, 0x29, 
    0xc, 0x29, 0xe, 0x29, 0x243, 0xb, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 
    0x247, 0xa, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 
    0x3, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x252, 0xa, 0x2e, 
    0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 
    0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x266, 
    0xa, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x26a, 0xa, 0x30, 0x3, 0x30, 
    0x5, 0x30, 0x26d, 0xa, 0x30, 0x3, 0x30, 0x5, 0x30, 0x270, 0xa, 0x30, 
    0x3, 0x30, 0x2, 0x5, 0x2a, 0x40, 0x4c, 0x31, 0x2, 0x4, 0x6, 0x8, 0xa, 
    0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 
    0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 
    0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 
    0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x2, 0xd, 0x4, 0x2, 0x22, 0x22, 
    0x33, 0x33, 0x6, 0x2, 0x6, 0x7, 0xb, 0xb, 0x3d, 0x3d, 0x45, 0x45, 0x4, 
    0x2, 0x30, 0x30, 0x41, 0x41, 0x4, 0x2, 0x7, 0x7, 0x3d, 0x3d, 0x4, 0x2, 
    0x39, 0x39, 0x5d, 0x5d, 0x4, 0x2, 0xa, 0xa, 0x16, 0x16, 0x4, 0x2, 0x1e, 
    0x1e, 0x2e, 0x2e, 0x5, 0x2, 0xd, 0xd, 0x2f, 0x2f, 0x4c, 0x4c, 0x4, 0x2, 
    0x37, 0x37, 0x56, 0x57, 0x4, 0x2, 0x4, 0x36, 0x38, 0x54, 0x4, 0x2, 0x36, 
    0x36, 0x5f, 0x5f, 0x2, 0x2b6, 0x2, 0x60, 0x3, 0x2, 0x2, 0x2, 0x4, 0x6d, 
    0x3, 0x2, 0x2, 0x2, 0x6, 0x7a, 0x3, 0x2, 0x2, 0x2, 0x8, 0x7c, 0x3, 0x2, 
    0x2, 0x2, 0xa, 0x93, 0x3, 0x2, 0x2, 0x2, 0xc, 0x95, 0x3, 0x2, 0x2, 0x2, 
    0xe, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x10, 0xc7, 0x3, 0x2, 0x2, 0x2, 0x12, 
    0xca, 0x3, 0x2, 0x2, 0x2, 0x14, 0xcf, 0x3, 0x2, 0x2, 0x2, 0x16, 0xd6, 
    0x3, 0x2, 0x2, 0x2, 0x18, 0xdc, 0x3, 0x2, 0x2, 0x2, 0x1a, 0xdf, 0x3, 
    0x2, 0x2, 0x2, 0x1c, 0xe2, 0x3, 0x2, 0x2, 0x2, 0x1e, 0xe9, 0x3, 0x2, 
    0x2, 0x2, 0x20, 0xec, 0x3, 0x2, 0x2, 0x2, 0x22, 0xf0, 0x3, 0x2, 0x2, 
    0x2, 0x24, 0xf5, 0x3, 0x2, 0x2, 0x2, 0x26, 0xf8, 0x3, 0x2, 0x2, 0x2, 
    0x28, 0xfb, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x104, 0x3, 0x2, 0x2, 0x2, 0x2c, 
    0x136, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x13e, 0x3, 0x2, 0x2, 0x2, 0x30, 0x149, 
    0x3, 0x2, 0x2, 0x2, 0x32, 0x14b, 0x3, 0x2, 0x2, 0x2, 0x34, 0x150, 0x3, 
    0x2, 0x2, 0x2, 0x36, 0x158, 0x3, 0x2, 0x2, 0x2, 0x38, 0x164, 0x3, 0x2, 
    0x2, 0x2, 0x3a, 0x168, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x170, 0x3, 0x2, 0x2, 
    0x2, 0x3e, 0x174, 0x3, 0x2, 0x2, 0x2, 0x40, 0x1c0, 0x3, 0x2, 0x2, 0x2, 
    0x42, 0x1eb, 0x3, 0x2, 0x2, 0x2, 0x44, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x46, 
    0x1fd, 0x3, 0x2, 0x2, 0x2, 0x48, 0x212, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x21a, 
    0x3, 0x2, 0x2, 0x2, 0x4c, 0x22b, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x238, 0x3, 
    0x2, 0x2, 0x2, 0x50, 0x23c, 0x3, 0x2, 0x2, 0x2, 0x52, 0x246, 0x3, 0x2, 
    0x2, 0x2, 0x54, 0x248, 0x3, 0x2, 0x2, 0x2, 0x56, 0x24a, 0x3, 0x2, 0x2, 
    0x2, 0x58, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x251, 0x3, 0x2, 0x2, 0x2, 
    0x5c, 0x253, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x60, 
    0x65, 0x5, 0x4, 0x3, 0x2, 0x61, 0x62, 0x7, 0x70, 0x2, 0x2, 0x62, 0x64, 
    0x5, 0x4, 0x3, 0x2, 0x63, 0x61, 0x3, 0x2, 0x2, 0x2, 0x64, 0x67, 0x3, 
    0x2, 0x2, 0x2, 0x65, 0x63, 0x3, 0x2, 0x2, 0x2, 0x65, 0x66, 0x3, 0x2, 
    0x2, 0x2, 0x66, 0x69, 0x3, 0x2, 0x2, 0x2, 0x67, 0x65, 0x3, 0x2, 0x2, 
    0x2, 0x68, 0x6a, 0x7, 0x70, 0x2, 0x2, 0x69, 0x68, 0x3, 0x2, 0x2, 0x2, 
    0x69, 0x6a, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x6b, 0x3, 0x2, 0x2, 0x2, 0x6b, 
    0x6c, 0x7, 0x2, 0x2, 0x3, 0x6c, 0x3, 0x3, 0x2, 0x2, 0x2, 0x6d, 0x71, 
    0x5, 0x6, 0x4, 0x2, 0x6e, 0x6f, 0x7, 0x2b, 0x2, 0x2, 0x6f, 0x70, 0x7, 
    0x3e, 0x2, 0x2, 0x70, 0x72, 0x7, 0x57, 0x2, 0x2, 0x71, 0x6e, 0x3, 0x2, 
    0x2, 0x2, 0x71, 0x72, 0x3, 0x2, 0x2, 0x2, 0x72, 0x75, 0x3, 0x2, 0x2, 
    0x2, 0x73, 0x74, 0x7, 0x1f, 0x2, 0x2, 0x74, 0x76, 0x5, 0x5a, 0x2e, 0x2, 
    0x75, 0x73, 0x3, 0x2, 0x2, 0x2, 0x75, 0x76, 0x3, 0x2, 0x2, 0x2, 0x76, 
    0x5, 0x3, 0x2, 0x2, 0x2, 0x77, 0x7b, 0x5, 0x8, 0x5, 0x2, 0x78, 0x7b, 
    0x5, 0xc, 0x7, 0x2, 0x79, 0x7b, 0x5, 0x28, 0x15, 0x2, 0x7a, 0x77, 0x3, 
    0x2, 0x2, 0x2, 0x7a, 0x78, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x79, 0x3, 0x2, 
    0x2, 0x2, 0x7b, 0x7, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x80, 0x5, 0xa, 0x6, 
    0x2, 0x7d, 0x7e, 0x7, 0x3a, 0x2, 0x2, 0x7e, 0x7f, 0x7, 0x11, 0x2, 0x2, 
    0x7f, 0x81, 0x5, 0x5a, 0x2e, 0x2, 0x80, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x80, 
    0x81, 0x3, 0x2, 0x2, 0x2, 0x81, 0x9, 0x3, 0x2, 0x2, 0x2, 0x82, 0x83, 
    0x7, 0x18, 0x2, 0x2, 0x83, 0x86, 0x7, 0x14, 0x2, 0x2, 0x84, 0x85, 0x7, 
    0x26, 0x2, 0x2, 0x85, 0x87, 0x7, 0x1b, 0x2, 0x2, 0x86, 0x84, 0x3, 0x2, 
    0x2, 0x2, 0x86, 0x87, 0x3, 0x2, 0x2, 0x2, 0x87, 0x88, 0x3, 0x2, 0x2, 
    0x2, 0x88, 0x94, 0x5, 0x54, 0x2b, 0x2, 0x89, 0x8b, 0x7, 0x18, 0x2, 0x2, 
    0x8a, 0x8c, 0x7, 0x49, 0x2, 0x2, 0x8b, 0x8a, 0x3, 0x2, 0x2, 0x2, 0x8b, 
    0x8c, 0x3, 0x2, 0x2, 0x2, 0x8c, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x8d, 0x90, 
    0x7, 0x48, 0x2, 0x2, 0x8e, 0x8f, 0x7, 0x26, 0x2, 0x2, 0x8f, 0x91, 0x7, 
    0x1b, 0x2, 0x2, 0x90, 0x8e, 0x3, 0x2, 0x2, 0x2, 0x90, 0x91, 0x3, 0x2, 
    0x2, 0x2, 0x91, 0x92, 0x3, 0x2, 0x2, 0x2, 0x92, 0x94, 0x5, 0x4e, 0x28, 
    0x2, 0x93, 0x82, 0x3, 0x2, 0x2, 0x2, 0x93, 0x89, 0x3, 0x2, 0x2, 0x2, 
    0x94, 0xb, 0x3, 0x2, 0x2, 0x2, 0x95, 0x9b, 0x5, 0xe, 0x8, 0x2, 0x96, 
    0x97, 0x7, 0x4e, 0x2, 0x2, 0x97, 0x98, 0x7, 0x4, 0x2, 0x2, 0x98, 0x9a, 
    0x5, 0xe, 0x8, 0x2, 0x99, 0x96, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x9d, 0x3, 
    0x2, 0x2, 0x2, 0x9b, 0x99, 0x3, 0x2, 0x2, 0x2, 0x9b, 0x9c, 0x3, 0x2, 
    0x2, 0x2, 0x9c, 0xd, 0x3, 0x2, 0x2, 0x2, 0x9d, 0x9b, 0x3, 0x2, 0x2, 
    0x2, 0x9e, 0xa0, 0x5, 0x10, 0x9, 0x2, 0x9f, 0x9e, 0x3, 0x2, 0x2, 0x2, 
    0x9f, 0xa0, 0x3, 0x2, 0x2, 0x2, 0xa0, 0xa1, 0x3, 0x2, 0x2, 0x2, 0xa1, 
    0xa3, 0x7, 0x44, 0x2, 0x2, 0xa2, 0xa4, 0x7, 0x17, 0x2, 0x2, 0xa3, 0xa2, 
    0x3, 0x2, 0x2, 0x2, 0xa3, 0xa4, 0x3, 0x2, 0x2, 0x2, 0xa4, 0xa5, 0x3, 
    0x2, 0x2, 0x2, 0xa5, 0xa7, 0x5, 0x3e, 0x20, 0x2, 0xa6, 0xa8, 0x5, 0x12, 
    0xa, 0x2, 0xa7, 0xa6, 0x3, 0x2, 0x2, 0x2, 0xa7, 0xa8, 0x3, 0x2, 0x2, 
    0x2, 0xa8, 0xaa, 0x3, 0x2, 0x2, 0x2, 0xa9, 0xab, 0x5, 0x14, 0xb, 0x2, 
    0xaa, 0xa9, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xab, 0x3, 0x2, 0x2, 0x2, 0xab, 
    0xad, 0x3, 0x2, 0x2, 0x2, 0xac, 0xae, 0x5, 0x16, 0xc, 0x2, 0xad, 0xac, 
    0x3, 0x2, 0x2, 0x2, 0xad, 0xae, 0x3, 0x2, 0x2, 0x2, 0xae, 0xb0, 0x3, 
    0x2, 0x2, 0x2, 0xaf, 0xb1, 0x5, 0x18, 0xd, 0x2, 0xb0, 0xaf, 0x3, 0x2, 
    0x2, 0x2, 0xb0, 0xb1, 0x3, 0x2, 0x2, 0x2, 0xb1, 0xb3, 0x3, 0x2, 0x2, 
    0x2, 0xb2, 0xb4, 0x5, 0x1a, 0xe, 0x2, 0xb3, 0xb2, 0x3, 0x2, 0x2, 0x2, 
    0xb3, 0xb4, 0x3, 0x2, 0x2, 0x2, 0xb4, 0xb6, 0x3, 0x2, 0x2, 0x2, 0xb5, 
    0xb7, 0x5, 0x1c, 0xf, 0x2, 0xb6, 0xb5, 0x3, 0x2, 0x2, 0x2, 0xb6, 0xb7, 
    0x3, 0x2, 0x2, 0x2, 0xb7, 0xb9, 0x3, 0x2, 0x2, 0x2, 0xb8, 0xba, 0x5, 
    0x1e, 0x10, 0x2, 0xb9, 0xb8, 0x3, 0x2, 0x2, 0x2, 0xb9, 0xba, 0x3, 0x2, 
    0x2, 0x2, 0xba, 0xbc, 0x3, 0x2, 0x2, 0x2, 0xbb, 0xbd, 0x5, 0x20, 0x11, 
    0x2, 0xbc, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xbc, 0xbd, 0x3, 0x2, 0x2, 0x2, 
    0xbd, 0xbf, 0x3, 0x2, 0x2, 0x2, 0xbe, 0xc0, 0x5, 0x22, 0x12, 0x2, 0xbf, 
    0xbe, 0x3, 0x2, 0x2, 0x2, 0xbf, 0xc0, 0x3, 0x2, 0x2, 0x2, 0xc0, 0xc2, 
    0x3, 0x2, 0x2, 0x2, 0xc1, 0xc3, 0x5, 0x24, 0x13, 0x2, 0xc2, 0xc1, 0x3, 
    0x2, 0x2, 0x2, 0xc2, 0xc3, 0x3, 0x2, 0x2, 0x2, 0xc3, 0xc5, 0x3, 0x2, 
    0x2, 0x2, 0xc4, 0xc6, 0x5, 0x26, 0x14, 0x2, 0xc5, 0xc4, 0x3, 0x2, 0x2, 
    0x2, 0xc5, 0xc6, 0x3, 0x2, 0x2, 0x2, 0xc6, 0xf, 0x3, 0x2, 0x2, 0x2, 
    0xc7, 0xc8, 0x7, 0x53, 0x2, 0x2, 0xc8, 0xc9, 0x5, 0x3e, 0x20, 0x2, 0xc9, 
    0x11, 0x3, 0x2, 0x2, 0x2, 0xca, 0xcb, 0x7, 0x20, 0x2, 0x2, 0xcb, 0xcd, 
    0x5, 0x2a, 0x16, 0x2, 0xcc, 0xce, 0x7, 0x1d, 0x2, 0x2, 0xcd, 0xcc, 0x3, 
    0x2, 0x2, 0x2, 0xcd, 0xce, 0x3, 0x2, 0x2, 0x2, 0xce, 0x13, 0x3, 0x2, 
    0x2, 0x2, 0xcf, 0xd0, 0x7, 0x42, 0x2, 0x2, 0xd0, 0xd3, 0x5, 0x38, 0x1d, 
    0x2, 0xd1, 0xd2, 0x7, 0x39, 0x2, 0x2, 0xd2, 0xd4, 0x5, 0x38, 0x1d, 0x2, 
    0xd3, 0xd1, 0x3, 0x2, 0x2, 0x2, 0xd3, 0xd4, 0x3, 0x2, 0x2, 0x2, 0xd4, 
    0x15, 0x3, 0x2, 0x2, 0x2, 0xd5, 0xd7, 0x7, 0x30, 0x2, 0x2, 0xd6, 0xd5, 
    0x3, 0x2, 0x2, 0x2, 0xd6, 0xd7, 0x3, 0x2, 0x2, 0x2, 0xd7, 0xd8, 0x3, 
    0x2, 0x2, 0x2, 0xd8, 0xd9, 0x7, 0x8, 0x2, 0x2, 0xd9, 0xda, 0x7, 0x2d, 
    0x2, 0x2, 0xda, 0xdb, 0x5, 0x3e, 0x20, 0x2, 0xdb, 0x17, 0x3, 0x2, 0x2, 
    0x2, 0xdc, 0xdd, 0x7, 0x3f, 0x2, 0x2, 0xdd, 0xde, 0x5, 0x40, 0x21, 0x2, 
    0xde, 0x19, 0x3, 0x2, 0x2, 0x2, 0xdf, 0xe0, 0x7, 0x52, 0x2, 0x2, 0xe0, 
    0xe1, 0x5, 0x40, 0x21, 0x2, 0xe1, 0x1b, 0x3, 0x2, 0x2, 0x2, 0xe2, 0xe3, 
    0x7, 0x23, 0x2, 0x2, 0xe3, 0xe4, 0x7, 0xe, 0x2, 0x2, 0xe4, 0xe7, 0x5, 
    0x3e, 0x20, 0x2, 0xe5, 0xe6, 0x7, 0x53, 0x2, 0x2, 0xe6, 0xe8, 0x7, 0x4b, 
    0x2, 0x2, 0xe7, 0xe5, 0x3, 0x2, 0x2, 0x2, 0xe7, 0xe8, 0x3, 0x2, 0x2, 
    0x2, 0xe8, 0x1d, 0x3, 0x2, 0x2, 0x2, 0xe9, 0xea, 0x7, 0x24, 0x2, 0x2, 
    0xea, 0xeb, 0x5, 0x40, 0x21, 0x2, 0xeb, 0x1f, 0x3, 0x2, 0x2, 0x2, 0xec, 
    0xed, 0x7, 0x3c, 0x2, 0x2, 0xed, 0xee, 0x7, 0xe, 0x2, 0x2, 0xee, 0xef, 
    0x5, 0x34, 0x1b, 0x2, 0xef, 0x21, 0x3, 0x2, 0x2, 0x2, 0xf0, 0xf1, 0x7, 
    0x32, 0x2, 0x2, 0xf1, 0xf2, 0x5, 0x32, 0x1a, 0x2, 0xf2, 0xf3, 0x7, 0xe, 
    0x2, 0x2, 0xf3, 0xf4, 0x5, 0x3e, 0x20, 0x2, 0xf4, 0x23, 0x3, 0x2, 0x2, 
    0x2, 0xf5, 0xf6, 0x7, 0x32, 0x2, 0x2, 0xf6, 0xf7, 0x5, 0x32, 0x1a, 0x2, 
    0xf7, 0x25, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xf9, 0x7, 0x47, 0x2, 0x2, 0xf9, 
    0xfa, 0x5, 0x3a, 0x1e, 0x2, 0xfa, 0x27, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 
    0x7, 0x46, 0x2, 0x2, 0xfc, 0xfd, 0x5, 0x3c, 0x1f, 0x2, 0xfd, 0x29, 0x3, 
    0x2, 0x2, 0x2, 0xfe, 0xff, 0x8, 0x16, 0x1, 0x2, 0xff, 0x105, 0x5, 0x4c, 
    0x27, 0x2, 0x100, 0x101, 0x7, 0x67, 0x2, 0x2, 0x101, 0x102, 0x5, 0x2a, 
    0x16, 0x2, 0x102, 0x103, 0x7, 0x6f, 0x2, 0x2, 0x103, 0x105, 0x3, 0x2, 
    0x2, 0x2, 0x104, 0xfe, 0x3, 0x2, 0x2, 0x2, 0x104, 0x100, 0x3, 0x2, 0x2, 
    0x2, 0x105, 0x115, 0x3, 0x2, 0x2, 0x2, 0x106, 0x107, 0xc, 0x3, 0x2, 
    0x2, 0x107, 0x108, 0x5, 0x2e, 0x18, 0x2, 0x108, 0x109, 0x5, 0x2a, 0x16, 
    0x4, 0x109, 0x114, 0x3, 0x2, 0x2, 0x2, 0x10a, 0x10c, 0xc, 0x4, 0x2, 
    0x2, 0x10b, 0x10d, 0x9, 0x2, 0x2, 0x2, 0x10c, 0x10b, 0x3, 0x2, 0x2, 
    0x2, 0x10c, 0x10d, 0x3, 0x2, 0x2, 0x2, 0x10d, 0x10e, 0x3, 0x2, 0x2, 
    0x2, 0x10e, 0x10f, 0x5, 0x2c, 0x17, 0x2, 0x10f, 0x110, 0x7, 0x2d, 0x2, 
    0x2, 0x110, 0x111, 0x5, 0x2a, 0x16, 0x2, 0x111, 0x112, 0x5, 0x30, 0x19, 
    0x2, 0x112, 0x114, 0x3, 0x2, 0x2, 0x2, 0x113, 0x106, 0x3, 0x2, 0x2, 
    0x2, 0x113, 0x10a, 0x3, 0x2, 0x2, 0x2, 0x114, 0x117, 0x3, 0x2, 0x2, 
    0x2, 0x115, 0x113, 0x3, 0x2, 0x2, 0x2, 0x115, 0x116, 0x3, 0x2, 0x2, 
    0x2, 0x116, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x117, 0x115, 0x3, 0x2, 0x2, 0x2, 
    0x118, 0x11a, 0x7, 0x7, 0x2, 0x2, 0x119, 0x118, 0x3, 0x2, 0x2, 0x2, 
    0x119, 0x11a, 0x3, 0x2, 0x2, 0x2, 0x11a, 0x11b, 0x3, 0x2, 0x2, 0x2, 
    0x11b, 0x121, 0x7, 0x28, 0x2, 0x2, 0x11c, 0x11e, 0x7, 0x28, 0x2, 0x2, 
    0x11d, 0x11f, 0x7, 0x7, 0x2, 0x2, 0x11e, 0x11d, 0x3, 0x2, 0x2, 0x2, 
    0x11e, 0x11f, 0x3, 0x2, 0x2, 0x2, 0x11f, 0x121, 0x3, 0x2, 0x2, 0x2, 
    0x120, 0x119, 0x3, 0x2, 0x2, 0x2, 0x120, 0x11c, 0x3, 0x2, 0x2, 0x2, 
    0x121, 0x137, 0x3, 0x2, 0x2, 0x2, 0x122, 0x124, 0x9, 0x3, 0x2, 0x2, 
    0x123, 0x122, 0x3, 0x2, 0x2, 0x2, 0x123, 0x124, 0x3, 0x2, 0x2, 0x2, 
    0x124, 0x125, 0x3, 0x2, 0x2, 0x2, 0x125, 0x12b, 0x9, 0x4, 0x2, 0x2, 
    0x126, 0x128, 0x9, 0x4, 0x2, 0x2, 0x127, 0x129, 0x9, 0x3, 0x2, 0x2, 
    0x128, 0x127, 0x3, 0x2, 0x2, 0x2, 0x128, 0x129, 0x3, 0x2, 0x2, 0x2, 
    0x129, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x123, 0x3, 0x2, 0x2, 0x2, 
    0x12a, 0x126, 0x3, 0x2, 0x2, 0x2, 0x12b, 0x137, 0x3, 0x2, 0x2, 0x2, 
    0x12c, 0x12e, 0x9, 0x5, 0x2, 0x2, 0x12d, 0x12c, 0x3, 0x2, 0x2, 0x2, 
    0x12d, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x12e, 0x12f, 0x3, 0x2, 0x2, 0x2, 
    0x12f, 0x135, 0x7, 0x21, 0x2, 0x2, 0x130, 0x132, 0x7, 0x21, 0x2, 0x2, 
    0x131, 0x133, 0x9, 0x5, 0x2, 0x2, 0x132, 0x131, 0x3, 0x2, 0x2, 0x2, 
    0x132, 0x133, 0x3, 0x2, 0x2, 0x2, 0x133, 0x135, 0x3, 0x2, 0x2, 0x2, 
    0x134, 0x12d, 0x3, 0x2, 0x2, 0x2, 0x134, 0x130, 0x3, 0x2, 0x2, 0x2, 
    0x135, 0x137, 0x3, 0x2, 0x2, 0x2, 0x136, 0x120, 0x3, 0x2, 0x2, 0x2, 
    0x136, 0x12a, 0x3, 0x2, 0x2, 0x2, 0x136, 0x134, 0x3, 0x2, 0x2, 0x2, 
    0x137, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x138, 0x13a, 0x9, 0x2, 0x2, 0x2, 0x139, 
    0x138, 0x3, 0x2, 0x2, 0x2, 0x139, 0x13a, 0x3, 0x2, 0x2, 0x2, 0x13a, 
    0x13b, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x13c, 0x7, 0x13, 0x2, 0x2, 0x13c, 
    0x13f, 0x7, 0x2d, 0x2, 0x2, 0x13d, 0x13f, 0x7, 0x5d, 0x2, 0x2, 0x13e, 
    0x139, 0x3, 0x2, 0x2, 0x2, 0x13e, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13f, 
    0x2f, 0x3, 0x2, 0x2, 0x2, 0x140, 0x141, 0x7, 0x3a, 0x2, 0x2, 0x141, 
    0x14a, 0x5, 0x3e, 0x20, 0x2, 0x142, 0x143, 0x7, 0x4f, 0x2, 0x2, 0x143, 
    0x144, 0x7, 0x67, 0x2, 0x2, 0x144, 0x145, 0x5, 0x3e, 0x20, 0x2, 0x145, 
    0x146, 0x7, 0x6f, 0x2, 0x2, 0x146, 0x14a, 0x3, 0x2, 0x2, 0x2, 0x147, 
    0x148, 0x7, 0x4f, 0x2, 0x2, 0x148, 0x14a, 0x5, 0x3e, 0x20, 0x2, 0x149, 
    0x140, 0x3, 0x2, 0x2, 0x2, 0x149, 0x142, 0x3, 0x2, 0x2, 0x2, 0x149, 
    0x147, 0x3, 0x2, 0x2, 0x2, 0x14a, 0x31, 0x3, 0x2, 0x2, 0x2, 0x14b, 0x14e, 
    0x7, 0x56, 0x2, 0x2, 0x14c, 0x14d, 0x9, 0x6, 0x2, 0x2, 0x14d, 0x14f, 
    0x7, 0x56, 0x2, 0x2, 0x14e, 0x14c, 0x3, 0x2, 0x2, 0x2, 0x14e, 0x14f, 
    0x3, 0x2, 0x2, 0x2, 0x14f, 0x33, 0x3, 0x2, 0x2, 0x2, 0x150, 0x155, 0x5, 
    0x36, 0x1c, 0x2, 0x151, 0x152, 0x7, 0x5d, 0x2, 0x2, 0x152, 0x154, 0x5, 
    0x36, 0x1c, 0x2, 0x153, 0x151, 0x3, 0x2, 0x2, 0x2, 0x154, 0x157, 0x3, 
    0x2, 0x2, 0x2, 0x155, 0x153, 0x3, 0x2, 0x2, 0x2, 0x155, 0x156, 0x3, 
    0x2, 0x2, 0x2, 0x156, 0x35, 0x3, 0x2, 0x2, 0x2, 0x157, 0x155, 0x3, 0x2, 
    0x2, 0x2, 0x158, 0x15a, 0x5, 0x40, 0x21, 0x2, 0x159, 0x15b, 0x9, 0x7, 
    0x2, 0x2, 0x15a, 0x159, 0x3, 0x2, 0x2, 0x2, 0x15a, 0x15b, 0x3, 0x2, 
    0x2, 0x2, 0x15b, 0x15e, 0x3, 0x2, 0x2, 0x2, 0x15c, 0x15d, 0x7, 0x38, 
    0x2, 0x2, 0x15d, 0x15f, 0x9, 0x8, 0x2, 0x2, 0x15e, 0x15c, 0x3, 0x2, 
    0x2, 0x2, 0x15e, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15f, 0x162, 0x3, 0x2, 
    0x2, 0x2, 0x160, 0x161, 0x7, 0x12, 0x2, 0x2, 0x161, 0x163, 0x7, 0x57, 
    0x2, 0x2, 0x162, 0x160, 0x3, 0x2, 0x2, 0x2, 0x162, 0x163, 0x3, 0x2, 
    0x2, 0x2, 0x163, 0x37, 0x3, 0x2, 0x2, 0x2, 0x164, 0x165, 0x7, 0x56, 
    0x2, 0x2, 0x165, 0x166, 0x7, 0x71, 0x2, 0x2, 0x166, 0x167, 0x7, 0x56, 
    0x2, 0x2, 0x167, 0x39, 0x3, 0x2, 0x2, 0x2, 0x168, 0x16d, 0x5, 0x3c, 
    0x1f, 0x2, 0x169, 0x16a, 0x7, 0x5d, 0x2, 0x2, 0x16a, 0x16c, 0x5, 0x3c, 
    0x1f, 0x2, 0x16b, 0x169, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16f, 0x3, 0x2, 
    0x2, 0x2, 0x16d, 0x16b, 0x3, 0x2, 0x2, 0x2, 0x16d, 0x16e, 0x3, 0x2, 
    0x2, 0x2, 0x16e, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x16d, 0x3, 0x2, 0x2, 
    0x2, 0x170, 0x171, 0x5, 0x5a, 0x2e, 0x2, 0x171, 0x172, 0x7, 0x62, 0x2, 
    0x2, 0x172, 0x173, 0x5, 0x56, 0x2c, 0x2, 0x173, 0x3d, 0x3, 0x2, 0x2, 
    0x2, 0x174, 0x179, 0x5, 0x40, 0x21, 0x2, 0x175, 0x176, 0x7, 0x5d, 0x2, 
    0x2, 0x176, 0x178, 0x5, 0x40, 0x21, 0x2, 0x177, 0x175, 0x3, 0x2, 0x2, 
    0x2, 0x178, 0x17b, 0x3, 0x2, 0x2, 0x2, 0x179, 0x177, 0x3, 0x2, 0x2, 
    0x2, 0x179, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17a, 0x3f, 0x3, 0x2, 0x2, 0x2, 
    0x17b, 0x179, 0x3, 0x2, 0x2, 0x2, 0x17c, 0x17d, 0x8, 0x21, 0x1, 0x2, 
    0x17d, 0x1c1, 0x5, 0x56, 0x2c, 0x2, 0x17e, 0x1c1, 0x7, 0x59, 0x2, 0x2, 
    0x17f, 0x180, 0x7, 0x67, 0x2, 0x2, 0x180, 0x181, 0x5, 0x3e, 0x20, 0x2, 
    0x181, 0x182, 0x7, 0x6f, 0x2, 0x2, 0x182, 0x1c1, 0x3, 0x2, 0x2, 0x2, 
    0x183, 0x185, 0x7, 0x65, 0x2, 0x2, 0x184, 0x186, 0x5, 0x3e, 0x20, 0x2, 
    0x185, 0x184, 0x3, 0x2, 0x2, 0x2, 0x185, 0x186, 0x3, 0x2, 0x2, 0x2, 
    0x186, 0x187, 0x3, 0x2, 0x2, 0x2, 0x187, 0x1c1, 0x7, 0x6e, 0x2, 0x2, 
    0x188, 0x18a, 0x7, 0xf, 0x2, 0x2, 0x189, 0x18b, 0x5, 0x40, 0x21, 0x2, 
    0x18a, 0x189, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x18b, 0x3, 0x2, 0x2, 0x2, 
    0x18b, 0x191, 0x3, 0x2, 0x2, 0x2, 0x18c, 0x18d, 0x7, 0x51, 0x2, 0x2, 
    0x18d, 0x18e, 0x5, 0x40, 0x21, 0x2, 0x18e, 0x18f, 0x7, 0x4a, 0x2, 0x2, 
    0x18f, 0x190, 0x5, 0x40, 0x21, 0x2, 0x190, 0x192, 0x3, 0x2, 0x2, 0x2, 
    0x191, 0x18c, 0x3, 0x2, 0x2, 0x2, 0x192, 0x193, 0x3, 0x2, 0x2, 0x2, 
    0x193, 0x191, 0x3, 0x2, 0x2, 0x2, 0x193, 0x194, 0x3, 0x2, 0x2, 0x2, 
    0x194, 0x197, 0x3, 0x2, 0x2, 0x2, 0x195, 0x196, 0x7, 0x19, 0x2, 0x2, 
    0x196, 0x198, 0x5, 0x40, 0x21, 0x2, 0x197, 0x195, 0x3, 0x2, 0x2, 0x2, 
    0x197, 0x198, 0x3, 0x2, 0x2, 0x2, 0x198, 0x199, 0x3, 0x2, 0x2, 0x2, 
    0x199, 0x19a, 0x7, 0x1a, 0x2, 0x2, 0x19a, 0x1c1, 0x3, 0x2, 0x2, 0x2, 
    0x19b, 0x19c, 0x7, 0x1c, 0x2, 0x2, 0x19c, 0x19d, 0x7, 0x67, 0x2, 0x2, 
    0x19d, 0x19e, 0x7, 0x3, 0x2, 0x2, 0x19e, 0x19f, 0x7, 0x20, 0x2, 0x2, 
    0x19f, 0x1a0, 0x5, 0x40, 0x21, 0x2, 0x1a0, 0x1a1, 0x7, 0x6f, 0x2, 0x2, 
    0x1a1, 0x1c1, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a3, 0x7, 0x4d, 0x2, 0x2, 
    0x1a3, 0x1a4, 0x7, 0x67, 0x2, 0x2, 0x1a4, 0x1a5, 0x9, 0x9, 0x2, 0x2, 
    0x1a5, 0x1a6, 0x7, 0x57, 0x2, 0x2, 0x1a6, 0x1a7, 0x7, 0x20, 0x2, 0x2, 
    0x1a7, 0x1a8, 0x5, 0x40, 0x21, 0x2, 0x1a8, 0x1a9, 0x7, 0x6f, 0x2, 0x2, 
    0x1a9, 0x1c1, 0x3, 0x2, 0x2, 0x2, 0x1aa, 0x1ab, 0x7, 0x2a, 0x2, 0x2, 
    0x1ab, 0x1ac, 0x5, 0x40, 0x21, 0x2, 0x1ac, 0x1ad, 0x7, 0x3, 0x2, 0x2, 
    0x1ad, 0x1c1, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1c1, 0x5, 0x4a, 0x26, 0x2, 
    0x1af, 0x1b5, 0x5, 0x5a, 0x2e, 0x2, 0x1b0, 0x1b2, 0x7, 0x67, 0x2, 0x2, 
    0x1b1, 0x1b3, 0x5, 0x42, 0x22, 0x2, 0x1b2, 0x1b1, 0x3, 0x2, 0x2, 0x2, 
    0x1b2, 0x1b3, 0x3, 0x2, 0x2, 0x2, 0x1b3, 0x1b4, 0x3, 0x2, 0x2, 0x2, 
    0x1b4, 0x1b6, 0x7, 0x6f, 0x2, 0x2, 0x1b5, 0x1b0, 0x3, 0x2, 0x2, 0x2, 
    0x1b5, 0x1b6, 0x3, 0x2, 0x2, 0x2, 0x1b6, 0x1b7, 0x3, 0x2, 0x2, 0x2, 
    0x1b7, 0x1b9, 0x7, 0x67, 0x2, 0x2, 0x1b8, 0x1ba, 0x5, 0x44, 0x23, 0x2, 
    0x1b9, 0x1b8, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1ba, 0x3, 0x2, 0x2, 0x2, 
    0x1ba, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x7, 0x6f, 0x2, 0x2, 
    0x1bc, 0x1c1, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1be, 0x5, 0x5c, 0x2f, 0x2, 
    0x1be, 0x1bf, 0x5, 0x40, 0x21, 0x8, 0x1bf, 0x1c1, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x17c, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x17e, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x17f, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x183, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x188, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x19b, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x1aa, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x1af, 0x3, 0x2, 0x2, 0x2, 
    0x1c0, 0x1bd, 0x3, 0x2, 0x2, 0x2, 0x1c1, 0x1e8, 0x3, 0x2, 0x2, 0x2, 
    0x1c2, 0x1c3, 0xc, 0x6, 0x2, 0x2, 0x1c3, 0x1c4, 0x5, 0x5e, 0x30, 0x2, 
    0x1c4, 0x1c5, 0x5, 0x40, 0x21, 0x7, 0x1c5, 0x1e7, 0x3, 0x2, 0x2, 0x2, 
    0x1c6, 0x1c7, 0xc, 0x5, 0x2, 0x2, 0x1c7, 0x1c8, 0x7, 0x6c, 0x2, 0x2, 
    0x1c8, 0x1c9, 0x5, 0x40, 0x21, 0x2, 0x1c9, 0x1ca, 0x7, 0x5c, 0x2, 0x2, 
    0x1ca, 0x1cb, 0x5, 0x40, 0x21, 0x6, 0x1cb, 0x1e7, 0x3, 0x2, 0x2, 0x2, 
    0x1cc, 0x1ce, 0xc, 0x4, 0x2, 0x2, 0x1cd, 0x1cf, 0x7, 0x36, 0x2, 0x2, 
    0x1ce, 0x1cd, 0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1cf, 0x3, 0x2, 0x2, 0x2, 
    0x1cf, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x7, 0xc, 0x2, 0x2, 
    0x1d1, 0x1d2, 0x5, 0x40, 0x21, 0x2, 0x1d2, 0x1d3, 0x7, 0x5, 0x2, 0x2, 
    0x1d3, 0x1d4, 0x5, 0x40, 0x21, 0x5, 0x1d4, 0x1e7, 0x3, 0x2, 0x2, 0x2, 
    0x1d5, 0x1d6, 0xc, 0xa, 0x2, 0x2, 0x1d6, 0x1d7, 0x7, 0x65, 0x2, 0x2, 
    0x1d7, 0x1d8, 0x5, 0x40, 0x21, 0x2, 0x1d8, 0x1d9, 0x7, 0x6e, 0x2, 0x2, 
    0x1d9, 0x1e7, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1db, 0xc, 0x9, 0x2, 0x2, 
    0x1db, 0x1dc, 0x7, 0x60, 0x2, 0x2, 0x1dc, 0x1e7, 0x7, 0x56, 0x2, 0x2, 
    0x1dd, 0x1de, 0xc, 0x7, 0x2, 0x2, 0x1de, 0x1e0, 0x7, 0x2c, 0x2, 0x2, 
    0x1df, 0x1e1, 0x7, 0x36, 0x2, 0x2, 0x1e0, 0x1df, 0x3, 0x2, 0x2, 0x2, 
    0x1e0, 0x1e1, 0x3, 0x2, 0x2, 0x2, 0x1e1, 0x1e2, 0x3, 0x2, 0x2, 0x2, 
    0x1e2, 0x1e7, 0x7, 0x37, 0x2, 0x2, 0x1e3, 0x1e4, 0xc, 0x3, 0x2, 0x2, 
    0x1e4, 0x1e5, 0x7, 0x9, 0x2, 0x2, 0x1e5, 0x1e7, 0x5, 0x5a, 0x2e, 0x2, 
    0x1e6, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1c6, 0x3, 0x2, 0x2, 0x2, 
    0x1e6, 0x1cc, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1d5, 0x3, 0x2, 0x2, 0x2, 
    0x1e6, 0x1da, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1dd, 0x3, 0x2, 0x2, 0x2, 
    0x1e6, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1e7, 0x1ea, 0x3, 0x2, 0x2, 0x2, 
    0x1e8, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0x1e8, 0x1e9, 0x3, 0x2, 0x2, 0x2, 
    0x1e9, 0x41, 0x3, 0x2, 0x2, 0x2, 0x1ea, 0x1e8, 0x3, 0x2, 0x2, 0x2, 0x1eb, 
    0x1f0, 0x5, 0x56, 0x2c, 0x2, 0x1ec, 0x1ed, 0x7, 0x5d, 0x2, 0x2, 0x1ed, 
    0x1ef, 0x5, 0x56, 0x2c, 0x2, 0x1ee, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1ef, 
    0x1f2, 0x3, 0x2, 0x2, 0x2, 0x1f0, 0x1ee, 0x3, 0x2, 0x2, 0x2, 0x1f0, 
    0x1f1, 0x3, 0x2, 0x2, 0x2, 0x1f1, 0x43, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1f0, 
    0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1f8, 0x5, 0x46, 0x24, 0x2, 0x1f4, 0x1f5, 
    0x7, 0x5d, 0x2, 0x2, 0x1f5, 0x1f7, 0x5, 0x46, 0x24, 0x2, 0x1f6, 0x1f4, 
    0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1fa, 0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1f6, 
    0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x1f9, 0x45, 0x3, 
    0x2, 0x2, 0x2, 0x1fa, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1fe, 0x5, 
    0x48, 0x25, 0x2, 0x1fc, 0x1fe, 0x5, 0x40, 0x21, 0x2, 0x1fd, 0x1fb, 0x3, 
    0x2, 0x2, 0x2, 0x1fd, 0x1fc, 0x3, 0x2, 0x2, 0x2, 0x1fe, 0x47, 0x3, 0x2, 
    0x2, 0x2, 0x1ff, 0x200, 0x7, 0x67, 0x2, 0x2, 0x200, 0x205, 0x5, 0x5a, 
    0x2e, 0x2, 0x201, 0x202, 0x7, 0x5d, 0x2, 0x2, 0x202, 0x204, 0x5, 0x5a, 
    0x2e, 0x2, 0x203, 0x201, 0x3, 0x2, 0x2, 0x2, 0x204, 0x207, 0x3, 0x2, 
    0x2, 0x2, 0x205, 0x203, 0x3, 0x2, 0x2, 0x2, 0x205, 0x206, 0x3, 0x2, 
    0x2, 0x2, 0x206, 0x208, 0x3, 0x2, 0x2, 0x2, 0x207, 0x205, 0x3, 0x2, 
    0x2, 0x2, 0x208, 0x209, 0x7, 0x6f, 0x2, 0x2, 0x209, 0x213, 0x3, 0x2, 
    0x2, 0x2, 0x20a, 0x20f, 0x5, 0x5a, 0x2e, 0x2, 0x20b, 0x20c, 0x7, 0x5d, 
    0x2, 0x2, 0x20c, 0x20e, 0x5, 0x5a, 0x2e, 0x2, 0x20d, 0x20b, 0x3, 0x2, 
    0x2, 0x2, 0x20e, 0x211, 0x3, 0x2, 0x2, 0x2, 0x20f, 0x20d, 0x3, 0x2, 
    0x2, 0x2, 0x20f, 0x210, 0x3, 0x2, 0x2, 0x2, 0x210, 0x213, 0x3, 0x2, 
    0x2, 0x2, 0x211, 0x20f, 0x3, 0x2, 0x2, 0x2, 0x212, 0x1ff, 0x3, 0x2, 
    0x2, 0x2, 0x212, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x213, 0x214, 0x3, 0x2, 
    0x2, 0x2, 0x214, 0x215, 0x7, 0x58, 0x2, 0x2, 0x215, 0x216, 0x5, 0x40, 
    0x21, 0x2, 0x216, 0x49, 0x3, 0x2, 0x2, 0x2, 0x217, 0x218, 0x5, 0x4e, 
    0x28, 0x2, 0x218, 0x219, 0x7, 0x60, 0x2, 0x2, 0x219, 0x21b, 0x3, 0x2, 
    0x2, 0x2, 0x21a, 0x217, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x21b, 0x3, 0x2, 
    0x2, 0x2, 0x21b, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x21c, 0x21d, 0x5, 0x5a, 
    0x2e, 0x2, 0x21d, 0x4b, 0x3, 0x2, 0x2, 0x2, 0x21e, 0x21f, 0x8, 0x27, 
    0x1, 0x2, 0x21f, 0x22c, 0x5, 0x4e, 0x28, 0x2, 0x220, 0x221, 0x5, 0x5a, 
    0x2e, 0x2, 0x221, 0x223, 0x7, 0x67, 0x2, 0x2, 0x222, 0x224, 0x5, 0x50, 
    0x29, 0x2, 0x223, 0x222, 0x3, 0x2, 0x2, 0x2, 0x223, 0x224, 0x3, 0x2, 
    0x2, 0x2, 0x224, 0x225, 0x3, 0x2, 0x2, 0x2, 0x225, 0x226, 0x7, 0x6f, 
    0x2, 0x2, 0x226, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x227, 0x228, 0x7, 0x67, 
    0x2, 0x2, 0x228, 0x229, 0x5, 0xc, 0x7, 0x2, 0x229, 0x22a, 0x7, 0x6f, 
    0x2, 0x2, 0x22a, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22b, 0x21e, 0x3, 0x2, 
    0x2, 0x2, 0x22b, 0x220, 0x3, 0x2, 0x2, 0x2, 0x22b, 0x227, 0x3, 0x2, 
    0x2, 0x2, 0x22c, 0x232, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22e, 0xc, 0x3, 
    0x2, 0x2, 0x22e, 0x22f, 0x7, 0x9, 0x2, 0x2, 0x22f, 0x231, 0x5, 0x5a, 
    0x2e, 0x2, 0x230, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x231, 0x234, 0x3, 0x2, 
    0x2, 0x2, 0x232, 0x230, 0x3, 0x2, 0x2, 0x2, 0x232, 0x233, 0x3, 0x2, 
    0x2, 0x2, 0x233, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x234, 0x232, 0x3, 0x2, 0x2, 
    0x2, 0x235, 0x236, 0x5, 0x54, 0x2b, 0x2, 0x236, 0x237, 0x7, 0x60, 0x2, 
    0x2, 0x237, 0x239, 0x3, 0x2, 0x2, 0x2, 0x238, 0x235, 0x3, 0x2, 0x2, 
    0x2, 0x238, 0x239, 0x3, 0x2, 0x2, 0x2, 0x239, 0x23a, 0x3, 0x2, 0x2, 
    0x2, 0x23a, 0x23b, 0x5, 0x5a, 0x2e, 0x2, 0x23b, 0x4f, 0x3, 0x2, 0x2, 
    0x2, 0x23c, 0x241, 0x5, 0x52, 0x2a, 0x2, 0x23d, 0x23e, 0x7, 0x5d, 0x2, 
    0x2, 0x23e, 0x240, 0x5, 0x52, 0x2a, 0x2, 0x23f, 0x23d, 0x3, 0x2, 0x2, 
    0x2, 0x240, 0x243, 0x3, 0x2, 0x2, 0x2, 0x241, 0x23f, 0x3, 0x2, 0x2, 
    0x2, 0x241, 0x242, 0x3, 0x2, 0x2, 0x2, 0x242, 0x51, 0x3, 0x2, 0x2, 0x2, 
    0x243, 0x241, 0x3, 0x2, 0x2, 0x2, 0x244, 0x247, 0x5, 0x56, 0x2c, 0x2, 
    0x245, 0x247, 0x5, 0x4e, 0x28, 0x2, 0x246, 0x244, 0x3, 0x2, 0x2, 0x2, 
    0x246, 0x245, 0x3, 0x2, 0x2, 0x2, 0x247, 0x53, 0x3, 0x2, 0x2, 0x2, 0x248, 
    0x249, 0x5, 0x5a, 0x2e, 0x2, 0x249, 0x55, 0x3, 0x2, 0x2, 0x2, 0x24a, 
    0x24b, 0x9, 0xa, 0x2, 0x2, 0x24b, 0x57, 0x3, 0x2, 0x2, 0x2, 0x24c, 0x24d, 
    0x9, 0xb, 0x2, 0x2, 0x24d, 0x59, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x252, 0x7, 
    0x55, 0x2, 0x2, 0x24f, 0x252, 0x7, 0x3, 0x2, 0x2, 0x250, 0x252, 0x5, 
    0x58, 0x2d, 0x2, 0x251, 0x24e, 0x3, 0x2, 0x2, 0x2, 0x251, 0x24f, 0x3, 
    0x2, 0x2, 0x2, 0x251, 0x250, 0x3, 0x2, 0x2, 0x2, 0x252, 0x5b, 0x3, 0x2, 
    0x2, 0x2, 0x253, 0x254, 0x9, 0xc, 0x2, 0x2, 0x254, 0x5d, 0x3, 0x2, 0x2, 
    0x2, 0x255, 0x270, 0x7, 0x5e, 0x2, 0x2, 0x256, 0x270, 0x7, 0x59, 0x2, 
    0x2, 0x257, 0x270, 0x7, 0x71, 0x2, 0x2, 0x258, 0x270, 0x7, 0x6b, 0x2, 
    0x2, 0x259, 0x270, 0x7, 0x5f, 0x2, 0x2, 0x25a, 0x270, 0x7, 0x6a, 0x2, 
    0x2, 0x25b, 0x270, 0x7, 0x61, 0x2, 0x2, 0x25c, 0x270, 0x7, 0x62, 0x2, 
    0x2, 0x25d, 0x270, 0x7, 0x69, 0x2, 0x2, 0x25e, 0x270, 0x7, 0x66, 0x2, 
    0x2, 0x25f, 0x270, 0x7, 0x63, 0x2, 0x2, 0x260, 0x270, 0x7, 0x68, 0x2, 
    0x2, 0x261, 0x270, 0x7, 0x64, 0x2, 0x2, 0x262, 0x270, 0x7, 0x5, 0x2, 
    0x2, 0x263, 0x270, 0x7, 0x3b, 0x2, 0x2, 0x264, 0x266, 0x7, 0x36, 0x2, 
    0x2, 0x265, 0x264, 0x3, 0x2, 0x2, 0x2, 0x265, 0x266, 0x3, 0x2, 0x2, 
    0x2, 0x266, 0x267, 0x3, 0x2, 0x2, 0x2, 0x267, 0x270, 0x7, 0x31, 0x2, 
    0x2, 0x268, 0x26a, 0x7, 0x22, 0x2, 0x2, 0x269, 0x268, 0x3, 0x2, 0x2, 
    0x2, 0x269, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x26a, 0x26c, 0x3, 0x2, 0x2, 
    0x2, 0x26b, 0x26d, 0x7, 0x36, 0x2, 0x2, 0x26c, 0x26b, 0x3, 0x2, 0x2, 
    0x2, 0x26c, 0x26d, 0x3, 0x2, 0x2, 0x2, 0x26d, 0x26e, 0x3, 0x2, 0x2, 
    0x2, 0x26e, 0x270, 0x7, 0x27, 0x2, 0x2, 0x26f, 0x255, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x256, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x257, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x258, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x259, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x25b, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x25c, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x25d, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x25f, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x260, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x261, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x262, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x263, 0x3, 0x2, 0x2, 
    0x2, 0x26f, 0x265, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x269, 0x3, 0x2, 0x2, 
    0x2, 0x270, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x54, 0x65, 0x69, 0x71, 0x75, 
    0x7a, 0x80, 0x86, 0x8b, 0x90, 0x93, 0x9b, 0x9f, 0xa3, 0xa7, 0xaa, 0xad, 
    0xb0, 0xb3, 0xb6, 0xb9, 0xbc, 0xbf, 0xc2, 0xc5, 0xcd, 0xd3, 0xd6, 0xe7, 
    0x104, 0x10c, 0x113, 0x115, 0x119, 0x11e, 0x120, 0x123, 0x128, 0x12a, 
    0x12d, 0x132, 0x134, 0x136, 0x139, 0x13e, 0x149, 0x14e, 0x155, 0x15a, 
    0x15e, 0x162, 0x16d, 0x179, 0x185, 0x18a, 0x193, 0x197, 0x1b2, 0x1b5, 
    0x1b9, 0x1c0, 0x1ce, 0x1e0, 0x1e6, 0x1e8, 0x1f0, 0x1f8, 0x1fd, 0x205, 
    0x20f, 0x212, 0x21a, 0x223, 0x22b, 0x232, 0x238, 0x241, 0x246, 0x251, 
    0x265, 0x269, 0x26c, 0x26f, 
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
