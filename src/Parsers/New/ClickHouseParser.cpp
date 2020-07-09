
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
    setState(72);
    queryStmt();
    setState(77);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(73);
        match(ClickHouseParser::SEMICOLON);
        setState(74);
        queryStmt(); 
      }
      setState(79);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    }
    setState(81);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SEMICOLON) {
      setState(80);
      match(ClickHouseParser::SEMICOLON);
    }
   
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

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::QueryStmtContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

ClickHouseParser::InsertStmtContext* ClickHouseParser::QueryStmtContext::insertStmt() {
  return getRuleContext<ClickHouseParser::InsertStmtContext>(0);
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
    setState(85);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        setState(83);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::INSERT: {
        setState(84);
        insertStmt();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(90);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INTO) {
      setState(87);
      match(ClickHouseParser::INTO);
      setState(88);
      match(ClickHouseParser::OUTFILE);
      setState(89);
      match(ClickHouseParser::STRING_LITERAL);
    }
    setState(94);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FORMAT) {
      setState(92);
      match(ClickHouseParser::FORMAT);
      setState(93);
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
  enterRule(_localctx, 4, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(96);
    selectStmt();
    setState(102);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(97);
      match(ClickHouseParser::UNION);
      setState(98);
      match(ClickHouseParser::ALL);
      setState(99);
      selectStmt();
      setState(104);
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
  enterRule(_localctx, 6, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(106);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(105);
      withClause();
    }
    setState(108);
    match(ClickHouseParser::SELECT);
    setState(110);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DISTINCT) {
      setState(109);
      match(ClickHouseParser::DISTINCT);
    }
    setState(112);
    columnExprList();
    setState(114);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(113);
      fromClause();
    }
    setState(117);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(116);
      sampleClause();
    }
    setState(120);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY

    || _la == ClickHouseParser::LEFT) {
      setState(119);
      arrayJoinClause();
    }
    setState(123);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(122);
      prewhereClause();
    }
    setState(126);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(125);
      whereClause();
    }
    setState(129);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(128);
      groupByClause();
    }
    setState(132);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(131);
      havingClause();
    }
    setState(135);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(134);
      orderByClause();
    }
    setState(138);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
    case 1: {
      setState(137);
      limitByClause();
      break;
    }

    }
    setState(141);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(140);
      limitClause();
    }
    setState(144);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(143);
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
  enterRule(_localctx, 8, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(146);
    match(ClickHouseParser::WITH);
    setState(147);
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
  enterRule(_localctx, 10, ClickHouseParser::RuleFromClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(149);
    match(ClickHouseParser::FROM);
    setState(150);
    joinExpr();
    setState(152);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(151);
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
  enterRule(_localctx, 12, ClickHouseParser::RuleSampleClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(154);
    match(ClickHouseParser::SAMPLE);
    setState(155);
    ratioExpr();
    setState(158);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET) {
      setState(156);
      match(ClickHouseParser::OFFSET);
      setState(157);
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
  enterRule(_localctx, 14, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(161);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LEFT) {
      setState(160);
      match(ClickHouseParser::LEFT);
    }
    setState(163);
    match(ClickHouseParser::ARRAY);
    setState(164);
    match(ClickHouseParser::JOIN);
    setState(165);
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
  enterRule(_localctx, 16, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(167);
    match(ClickHouseParser::PREWHERE);
    setState(168);
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
  enterRule(_localctx, 18, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(170);
    match(ClickHouseParser::WHERE);
    setState(171);
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
  enterRule(_localctx, 20, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(173);
    match(ClickHouseParser::GROUP);
    setState(174);
    match(ClickHouseParser::BY);
    setState(175);
    columnExprList();
    setState(178);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(176);
      match(ClickHouseParser::WITH);
      setState(177);
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
  enterRule(_localctx, 22, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(180);
    match(ClickHouseParser::HAVING);
    setState(181);
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
  enterRule(_localctx, 24, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(183);
    match(ClickHouseParser::ORDER);
    setState(184);
    match(ClickHouseParser::BY);
    setState(185);
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
  enterRule(_localctx, 26, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(187);
    match(ClickHouseParser::LIMIT);
    setState(188);
    limitExpr();
    setState(189);
    match(ClickHouseParser::BY);
    setState(190);
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
  enterRule(_localctx, 28, ClickHouseParser::RuleLimitClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(192);
    match(ClickHouseParser::LIMIT);
    setState(193);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(195);
    match(ClickHouseParser::SETTINGS);
    setState(196);
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

ClickHouseParser::TableIdentifierContext* ClickHouseParser::JoinExprContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}


size_t ClickHouseParser::JoinExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleJoinExpr;
}


antlrcpp::Any ClickHouseParser::JoinExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitJoinExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::JoinExprContext* ClickHouseParser::joinExpr() {
  JoinExprContext *_localctx = _tracker.createInstance<JoinExprContext>(_ctx, getState());
  enterRule(_localctx, 32, ClickHouseParser::RuleJoinExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(198);
    tableIdentifier();
   
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
  enterRule(_localctx, 34, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(200);
    match(ClickHouseParser::NUMBER_LITERAL);
    setState(203);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::COMMA || _la == ClickHouseParser::OFFSET) {
      setState(201);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::COMMA || _la == ClickHouseParser::OFFSET)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(202);
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
  enterRule(_localctx, 36, ClickHouseParser::RuleOrderExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(205);
    orderExpr();
    setState(210);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(206);
      match(ClickHouseParser::COMMA);
      setState(207);
      orderExpr();
      setState(212);
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
  enterRule(_localctx, 38, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(213);
    columnExpr(0);
    setState(215);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ASCENDING

    || _la == ClickHouseParser::DESCENDING) {
      setState(214);
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
    setState(219);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::NULLS) {
      setState(217);
      match(ClickHouseParser::NULLS);
      setState(218);
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
    setState(223);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::COLLATE) {
      setState(221);
      match(ClickHouseParser::COLLATE);
      setState(222);
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
  enterRule(_localctx, 40, ClickHouseParser::RuleRatioExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(225);
    match(ClickHouseParser::NUMBER_LITERAL);

    setState(226);
    match(ClickHouseParser::SLASH);
    setState(227);
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
  enterRule(_localctx, 42, ClickHouseParser::RuleSettingExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(229);
    settingExpr();
    setState(234);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(230);
      match(ClickHouseParser::COMMA);
      setState(231);
      settingExpr();
      setState(236);
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

tree::TerminalNode* ClickHouseParser::SettingExprContext::LITERAL() {
  return getToken(ClickHouseParser::LITERAL, 0);
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
  enterRule(_localctx, 44, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(237);
    identifier();
    setState(238);
    match(ClickHouseParser::EQ_SINGLE);
    setState(239);
    match(ClickHouseParser::LITERAL);
   
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

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(241);
    match(ClickHouseParser::INSERT);
    setState(242);
    match(ClickHouseParser::INTO);
   
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
  enterRule(_localctx, 48, ClickHouseParser::RuleColumnExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(244);
    columnExpr(0);
    setState(249);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(245);
      match(ClickHouseParser::COMMA);
      setState(246);
      columnExpr(0);
      setState(251);
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

//----------------- TupleAccessContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::TupleAccessContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TupleAccessContext::DOT() {
  return getToken(ClickHouseParser::DOT, 0);
}

tree::TerminalNode* ClickHouseParser::TupleAccessContext::NUMBER_LITERAL() {
  return getToken(ClickHouseParser::NUMBER_LITERAL, 0);
}

ClickHouseParser::TupleAccessContext::TupleAccessContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TupleAccessContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTupleAccess(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TernaryContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::TernaryContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TernaryContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::TernaryContext::QUERY() {
  return getToken(ClickHouseParser::QUERY, 0);
}

tree::TerminalNode* ClickHouseParser::TernaryContext::COLON() {
  return getToken(ClickHouseParser::COLON, 0);
}

ClickHouseParser::TernaryContext::TernaryContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TernaryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTernary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BetweenContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::BetweenContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::BetweenContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::BetweenContext::BETWEEN() {
  return getToken(ClickHouseParser::BETWEEN, 0);
}

tree::TerminalNode* ClickHouseParser::BetweenContext::AND() {
  return getToken(ClickHouseParser::AND, 0);
}

tree::TerminalNode* ClickHouseParser::BetweenContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::BetweenContext::BetweenContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::BetweenContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitBetween(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SubqueryContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SubqueryContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::SelectStmtContext* ClickHouseParser::SubqueryContext::selectStmt() {
  return getRuleContext<ClickHouseParser::SelectStmtContext>(0);
}

tree::TerminalNode* ClickHouseParser::SubqueryContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::SubqueryContext::SubqueryContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::SubqueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSubquery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UnaryContext ------------------------------------------------------------------

ClickHouseParser::UnaryOpContext* ClickHouseParser::UnaryContext::unaryOp() {
  return getRuleContext<ClickHouseParser::UnaryOpContext>(0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::UnaryContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::UnaryContext::UnaryContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::UnaryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitUnary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CaseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::CaseContext::CASE() {
  return getToken(ClickHouseParser::CASE, 0);
}

tree::TerminalNode* ClickHouseParser::CaseContext::END() {
  return getToken(ClickHouseParser::END, 0);
}

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::CaseContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::CaseContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::CaseContext::WHEN() {
  return getTokens(ClickHouseParser::WHEN);
}

tree::TerminalNode* ClickHouseParser::CaseContext::WHEN(size_t i) {
  return getToken(ClickHouseParser::WHEN, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::CaseContext::THEN() {
  return getTokens(ClickHouseParser::THEN);
}

tree::TerminalNode* ClickHouseParser::CaseContext::THEN(size_t i) {
  return getToken(ClickHouseParser::THEN, i);
}

tree::TerminalNode* ClickHouseParser::CaseContext::ELSE() {
  return getToken(ClickHouseParser::ELSE, 0);
}

ClickHouseParser::CaseContext::CaseContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::CaseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCase(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ArrayContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ArrayContext::LBRACKET() {
  return getToken(ClickHouseParser::LBRACKET, 0);
}

tree::TerminalNode* ClickHouseParser::ArrayContext::RBRACKET() {
  return getToken(ClickHouseParser::RBRACKET, 0);
}

ClickHouseParser::ColumnExprListContext* ClickHouseParser::ArrayContext::columnExprList() {
  return getRuleContext<ClickHouseParser::ColumnExprListContext>(0);
}

ClickHouseParser::ArrayContext::ArrayContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ArrayContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitArray(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ArrayAccessContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::ArrayAccessContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ArrayAccessContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

tree::TerminalNode* ClickHouseParser::ArrayAccessContext::LBRACKET() {
  return getToken(ClickHouseParser::LBRACKET, 0);
}

tree::TerminalNode* ClickHouseParser::ArrayAccessContext::RBRACKET() {
  return getToken(ClickHouseParser::RBRACKET, 0);
}

ClickHouseParser::ArrayAccessContext::ArrayAccessContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ArrayAccessContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitArrayAccess(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AliasContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::AliasContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::AliasContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::IdentifierContext* ClickHouseParser::AliasContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::AliasContext::AliasContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAlias(this);
  else
    return visitor->visitChildren(this);
}
//----------------- LiteralContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::LiteralContext::LITERAL() {
  return getToken(ClickHouseParser::LITERAL, 0);
}

ClickHouseParser::LiteralContext::LiteralContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::LiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IsNullContext ------------------------------------------------------------------

ClickHouseParser::ColumnExprContext* ClickHouseParser::IsNullContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::IsNullContext::IS() {
  return getToken(ClickHouseParser::IS, 0);
}

tree::TerminalNode* ClickHouseParser::IsNullContext::NULL_SQL() {
  return getToken(ClickHouseParser::NULL_SQL, 0);
}

tree::TerminalNode* ClickHouseParser::IsNullContext::NOT() {
  return getToken(ClickHouseParser::NOT, 0);
}

ClickHouseParser::IsNullContext::IsNullContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::IsNullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitIsNull(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IdContext ------------------------------------------------------------------

ClickHouseParser::ColumnIdentifierContext* ClickHouseParser::IdContext::columnIdentifier() {
  return getRuleContext<ClickHouseParser::ColumnIdentifierContext>(0);
}

ClickHouseParser::IdContext::IdContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::IdContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitId(this);
  else
    return visitor->visitChildren(this);
}
//----------------- FunctionCallContext ------------------------------------------------------------------

ClickHouseParser::ColumnFunctionExprContext* ClickHouseParser::FunctionCallContext::columnFunctionExpr() {
  return getRuleContext<ClickHouseParser::ColumnFunctionExprContext>(0);
}

ClickHouseParser::FunctionCallContext::FunctionCallContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::FunctionCallContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitFunctionCall(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BinaryContext ------------------------------------------------------------------

std::vector<ClickHouseParser::ColumnExprContext *> ClickHouseParser::BinaryContext::columnExpr() {
  return getRuleContexts<ClickHouseParser::ColumnExprContext>();
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::BinaryContext::columnExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(i);
}

ClickHouseParser::BinaryOpContext* ClickHouseParser::BinaryContext::binaryOp() {
  return getRuleContext<ClickHouseParser::BinaryOpContext>(0);
}

ClickHouseParser::BinaryContext::BinaryContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::BinaryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitBinary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AsteriskContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::AsteriskContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

ClickHouseParser::AsteriskContext::AsteriskContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::AsteriskContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitAsterisk(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TupleContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::TupleContext::LPAREN() {
  return getToken(ClickHouseParser::LPAREN, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TupleContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TupleContext::RPAREN() {
  return getToken(ClickHouseParser::RPAREN, 0);
}

ClickHouseParser::TupleContext::TupleContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TupleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTuple(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IntervalContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::IntervalContext::INTERVAL() {
  return getToken(ClickHouseParser::INTERVAL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::IntervalContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::IntervalContext::INTERVAL_TYPE() {
  return getToken(ClickHouseParser::INTERVAL_TYPE, 0);
}

ClickHouseParser::IntervalContext::IntervalContext(ColumnExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::IntervalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitInterval(this);
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
  size_t startState = 50;
  enterRecursionRule(_localctx, 50, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(296);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<LiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(253);
      match(ClickHouseParser::LITERAL);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<AsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(254);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<IdContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(255);
      columnIdentifier();
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<TupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(256);
      match(ClickHouseParser::LPAREN);
      setState(257);
      columnExpr(0);
      setState(258);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<SubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(260);
      match(ClickHouseParser::LPAREN);
      setState(261);
      selectStmt();
      setState(262);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(264);
      match(ClickHouseParser::LBRACKET);
      setState(266);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::ASTERISK)
        | (1ULL << ClickHouseParser::DASH)
        | (1ULL << ClickHouseParser::LBRACKET)
        | (1ULL << ClickHouseParser::LPAREN)
        | (1ULL << ClickHouseParser::IDENTIFIER)
        | (1ULL << ClickHouseParser::LITERAL)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::INTERVAL))) != 0) || _la == ClickHouseParser::NOT

      || _la == ClickHouseParser::TRIM) {
        setState(265);
        columnExprList();
      }
      setState(268);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<UnaryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(269);
      unaryOp();
      setState(270);
      columnExpr(9);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<CaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(272);
      match(ClickHouseParser::CASE);
      setState(274);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::ASTERISK)
        | (1ULL << ClickHouseParser::DASH)
        | (1ULL << ClickHouseParser::LBRACKET)
        | (1ULL << ClickHouseParser::LPAREN)
        | (1ULL << ClickHouseParser::IDENTIFIER)
        | (1ULL << ClickHouseParser::LITERAL)
        | (1ULL << ClickHouseParser::CASE)
        | (1ULL << ClickHouseParser::CAST)
        | (1ULL << ClickHouseParser::EXTRACT)
        | (1ULL << ClickHouseParser::INTERVAL))) != 0) || _la == ClickHouseParser::NOT

      || _la == ClickHouseParser::TRIM) {
        setState(273);
        columnExpr(0);
      }
      setState(281); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(276);
        match(ClickHouseParser::WHEN);
        setState(277);
        columnExpr(0);
        setState(278);
        match(ClickHouseParser::THEN);
        setState(279);
        columnExpr(0);
        setState(283); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(287);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(285);
        match(ClickHouseParser::ELSE);
        setState(286);
        columnExpr(0);
      }
      setState(289);
      match(ClickHouseParser::END);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<IntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(291);
      match(ClickHouseParser::INTERVAL);
      setState(292);
      columnExpr(0);
      setState(293);
      match(ClickHouseParser::INTERVAL_TYPE);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(295);
      columnFunctionExpr();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(336);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(334);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<BinaryContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(298);

          if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
          setState(299);
          binaryOp();
          setState(300);
          columnExpr(8);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<TernaryContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(302);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(303);
          match(ClickHouseParser::QUERY);
          setState(304);
          columnExpr(0);
          setState(305);
          match(ClickHouseParser::COLON);
          setState(306);
          columnExpr(7);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<BetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(308);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(310);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(309);
            match(ClickHouseParser::NOT);
          }
          setState(312);
          match(ClickHouseParser::BETWEEN);
          setState(313);
          columnExpr(0);
          setState(314);
          match(ClickHouseParser::AND);
          setState(315);
          columnExpr(6);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(317);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(318);
          match(ClickHouseParser::LBRACKET);
          setState(319);
          columnExpr(0);
          setState(320);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<TupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(322);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(323);
          match(ClickHouseParser::DOT);
          setState(324);
          match(ClickHouseParser::NUMBER_LITERAL);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<IsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(325);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(326);
          match(ClickHouseParser::IS);
          setState(328);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(327);
            match(ClickHouseParser::NOT);
          }
          setState(330);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<AliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(331);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(332);
          match(ClickHouseParser::AS);
          setState(333);
          identifier();
          break;
        }

        } 
      }
      setState(338);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- ColumnFunctionExprContext ------------------------------------------------------------------

ClickHouseParser::ColumnFunctionExprContext::ColumnFunctionExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnFunctionExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnFunctionExprContext::LPAREN() {
  return getTokens(ClickHouseParser::LPAREN);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::LPAREN(size_t i) {
  return getToken(ClickHouseParser::LPAREN, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnFunctionExprContext::RPAREN() {
  return getTokens(ClickHouseParser::RPAREN);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::RPAREN(size_t i) {
  return getToken(ClickHouseParser::RPAREN, i);
}

ClickHouseParser::ColumnArgListContext* ClickHouseParser::ColumnFunctionExprContext::columnArgList() {
  return getRuleContext<ClickHouseParser::ColumnArgListContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnFunctionExprContext::LITERAL() {
  return getTokens(ClickHouseParser::LITERAL);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::LITERAL(size_t i) {
  return getToken(ClickHouseParser::LITERAL, i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnFunctionExprContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::EXTRACT() {
  return getToken(ClickHouseParser::EXTRACT, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::INTERVAL_TYPE() {
  return getToken(ClickHouseParser::INTERVAL_TYPE, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::FROM() {
  return getToken(ClickHouseParser::FROM, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnFunctionExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::CAST() {
  return getToken(ClickHouseParser::CAST, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::TRIM() {
  return getToken(ClickHouseParser::TRIM, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::STRING_LITERAL() {
  return getToken(ClickHouseParser::STRING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::BOTH() {
  return getToken(ClickHouseParser::BOTH, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::LEADING() {
  return getToken(ClickHouseParser::LEADING, 0);
}

tree::TerminalNode* ClickHouseParser::ColumnFunctionExprContext::TRAILING() {
  return getToken(ClickHouseParser::TRAILING, 0);
}


size_t ClickHouseParser::ColumnFunctionExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleColumnFunctionExpr;
}


antlrcpp::Any ClickHouseParser::ColumnFunctionExprContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitColumnFunctionExpr(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ColumnFunctionExprContext* ClickHouseParser::columnFunctionExpr() {
  ColumnFunctionExprContext *_localctx = _tracker.createInstance<ColumnFunctionExprContext>(_ctx, getState());
  enterRule(_localctx, 52, ClickHouseParser::RuleColumnFunctionExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(382);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(339);
        identifier();
        setState(352);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
        case 1: {
          setState(340);
          match(ClickHouseParser::LPAREN);
          setState(349);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::LITERAL) {
            setState(341);
            match(ClickHouseParser::LITERAL);
            setState(346);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == ClickHouseParser::COMMA) {
              setState(342);
              match(ClickHouseParser::COMMA);
              setState(343);
              match(ClickHouseParser::LITERAL);
              setState(348);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
          }
          setState(351);
          match(ClickHouseParser::RPAREN);
          break;
        }

        }
        setState(354);
        match(ClickHouseParser::LPAREN);
        setState(356);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ASTERISK)
          | (1ULL << ClickHouseParser::DASH)
          | (1ULL << ClickHouseParser::LBRACKET)
          | (1ULL << ClickHouseParser::LPAREN)
          | (1ULL << ClickHouseParser::IDENTIFIER)
          | (1ULL << ClickHouseParser::LITERAL)
          | (1ULL << ClickHouseParser::CASE)
          | (1ULL << ClickHouseParser::CAST)
          | (1ULL << ClickHouseParser::EXTRACT)
          | (1ULL << ClickHouseParser::INTERVAL))) != 0) || _la == ClickHouseParser::NOT

        || _la == ClickHouseParser::TRIM) {
          setState(355);
          columnArgList();
        }
        setState(358);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::EXTRACT: {
        enterOuterAlt(_localctx, 2);
        setState(360);
        match(ClickHouseParser::EXTRACT);
        setState(361);
        match(ClickHouseParser::LPAREN);
        setState(362);
        match(ClickHouseParser::INTERVAL_TYPE);
        setState(363);
        match(ClickHouseParser::FROM);
        setState(364);
        columnExpr(0);
        setState(365);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::CAST: {
        enterOuterAlt(_localctx, 3);
        setState(367);
        match(ClickHouseParser::CAST);
        setState(368);
        match(ClickHouseParser::LPAREN);
        setState(369);
        columnExpr(0);
        setState(370);
        match(ClickHouseParser::AS);
        setState(371);
        identifier();
        setState(372);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::TRIM: {
        enterOuterAlt(_localctx, 4);
        setState(374);
        match(ClickHouseParser::TRIM);
        setState(375);
        match(ClickHouseParser::LPAREN);
        setState(376);
        _la = _input->LA(1);
        if (!(((((_la - 41) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 41)) & ((1ULL << (ClickHouseParser::BOTH - 41))
          | (1ULL << (ClickHouseParser::LEADING - 41))
          | (1ULL << (ClickHouseParser::TRAILING - 41)))) != 0))) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(377);
        match(ClickHouseParser::STRING_LITERAL);
        setState(378);
        match(ClickHouseParser::FROM);
        setState(379);
        columnExpr(0);
        setState(380);
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
  enterRule(_localctx, 54, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(384);
    columnArgExpr();
    setState(389);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(385);
      match(ClickHouseParser::COMMA);
      setState(386);
      columnArgExpr();
      setState(391);
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

ClickHouseParser::ColumnExprContext* ClickHouseParser::ColumnArgExprContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::ColumnLambdaExprContext* ClickHouseParser::ColumnArgExprContext::columnLambdaExpr() {
  return getRuleContext<ClickHouseParser::ColumnLambdaExprContext>(0);
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
  enterRule(_localctx, 56, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(394);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(392);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(393);
      columnLambdaExpr();
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
  enterRule(_localctx, 58, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(415);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(396);
        match(ClickHouseParser::LPAREN);
        setState(397);
        identifier();
        setState(402);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(398);
          match(ClickHouseParser::COMMA);
          setState(399);
          identifier();
          setState(404);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(405);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::IDENTIFIER: {
        setState(407);
        identifier();
        setState(412);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(408);
          match(ClickHouseParser::COMMA);
          setState(409);
          identifier();
          setState(414);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(417);
    match(ClickHouseParser::ARROW);
    setState(418);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(423);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
    case 1: {
      setState(420);
      tableIdentifier();
      setState(421);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(425);
    identifier();
   
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
  enterRule(_localctx, 62, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(430);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 50, _ctx)) {
    case 1: {
      setState(427);
      databaseIdentifier();
      setState(428);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(432);
    identifier();
   
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
  enterRule(_localctx, 64, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(434);
    identifier();
   
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
  enterRule(_localctx, 66, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(436);
    match(ClickHouseParser::IDENTIFIER);
   
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
  enterRule(_localctx, 68, ClickHouseParser::RuleUnaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(438);
    _la = _input->LA(1);
    if (!(_la == ClickHouseParser::DASH

    || _la == ClickHouseParser::NOT)) {
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

tree::TerminalNode* ClickHouseParser::BinaryOpContext::ASTERISK() {
  return getToken(ClickHouseParser::ASTERISK, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::SLASH() {
  return getToken(ClickHouseParser::SLASH, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::PERCENT() {
  return getToken(ClickHouseParser::PERCENT, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::PLUS() {
  return getToken(ClickHouseParser::PLUS, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
}

tree::TerminalNode* ClickHouseParser::BinaryOpContext::EQ() {
  return getToken(ClickHouseParser::EQ, 0);
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

tree::TerminalNode* ClickHouseParser::BinaryOpContext::CONCAT() {
  return getToken(ClickHouseParser::CONCAT, 0);
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
  enterRule(_localctx, 70, ClickHouseParser::RuleBinaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(465);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(440);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(441);
      match(ClickHouseParser::SLASH);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(442);
      match(ClickHouseParser::PERCENT);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(443);
      match(ClickHouseParser::PLUS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(444);
      match(ClickHouseParser::DASH);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(445);
      match(ClickHouseParser::EQ);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(446);
      match(ClickHouseParser::NOT_EQ);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(447);
      match(ClickHouseParser::LE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(448);
      match(ClickHouseParser::GE);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(449);
      match(ClickHouseParser::LT);
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(450);
      match(ClickHouseParser::GT);
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(451);
      match(ClickHouseParser::CONCAT);
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(452);
      match(ClickHouseParser::AND);
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(453);
      match(ClickHouseParser::OR);
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(455);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(454);
        match(ClickHouseParser::NOT);
      }
      setState(457);
      match(ClickHouseParser::LIKE);
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(459);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::GLOBAL) {
        setState(458);
        match(ClickHouseParser::GLOBAL);
      }
      setState(462);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(461);
        match(ClickHouseParser::NOT);
      }
      setState(464);
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
    case 25: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool ClickHouseParser::columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 7);
    case 1: return precpred(_ctx, 6);
    case 2: return precpred(_ctx, 5);
    case 3: return precpred(_ctx, 11);
    case 4: return precpred(_ctx, 10);
    case 5: return precpred(_ctx, 8);
    case 6: return precpred(_ctx, 1);

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
  "queryList", "queryStmt", "selectUnionStmt", "selectStmt", "withClause", 
  "fromClause", "sampleClause", "arrayJoinClause", "prewhereClause", "whereClause", 
  "groupByClause", "havingClause", "orderByClause", "limitByClause", "limitClause", 
  "settingsClause", "joinExpr", "limitExpr", "orderExprList", "orderExpr", 
  "ratioExpr", "settingExprList", "settingExpr", "insertStmt", "columnExprList", 
  "columnExpr", "columnFunctionExpr", "columnArgList", "columnArgExpr", 
  "columnLambdaExpr", "columnIdentifier", "tableIdentifier", "databaseIdentifier", 
  "identifier", "unaryOp", "binaryOp"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "'->'", "'*'", "'`'", "'\\'", "':'", "','", "'||'", "'-'", 
  "'.'", "", "'=='", "'='", "'>='", "'>'", "'['", "'<='", "'('", "'<'", 
  "", "'%'", "'+'", "'?'", "'''", "']'", "')'", "';'", "'/'", "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "LINE_COMMENT", "WHITESPACE", "ARROW", "ASTERISK", "BACKQUOTE", "BACKSLASH", 
  "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ", "EQ_DOUBLE", "EQ_SINGLE", 
  "GE", "GT", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", "PLUS", 
  "QUERY", "QUOTE_SINGLE", "RBRACKET", "RPAREN", "SEMICOLON", "SLASH", "UNDERSCORE", 
  "IDENTIFIER", "LITERAL", "NUMBER_LITERAL", "STRING_LITERAL", "ALL", "AND", 
  "ARRAY", "AS", "ASCENDING", "BETWEEN", "BOTH", "BY", "CASE", "CAST", "COLLATE", 
  "DAY", "DESCENDING", "DISTINCT", "ELSE", "END", "EXTRACT", "FINAL", "FIRST", 
  "FORMAT", "FROM", "GLOBAL", "GROUP", "HAVING", "HOUR", "IN", "INSERT", 
  "INTERVAL", "INTO", "IS", "JOIN", "LAST", "LEADING", "LEFT", "LIKE", "LIMIT", 
  "MINUTE", "MONTH", "NOT", "NULL_SQL", "NULLS", "OFFSET", "OR", "ORDER", 
  "OUTFILE", "PREWHERE", "QUARTER", "SAMPLE", "SECOND", "SELECT", "SETTINGS", 
  "THEN", "TOTALS", "TRAILING", "TRIM", "UNION", "WEEK", "WHEN", "WHERE", 
  "WITH", "YEAR", "INTERVAL_TYPE"
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
    0x3, 0x62, 0x1d6, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x25, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x7, 0x2, 0x4e, 0xa, 0x2, 0xc, 0x2, 
    0xe, 0x2, 0x51, 0xb, 0x2, 0x3, 0x2, 0x5, 0x2, 0x54, 0xa, 0x2, 0x3, 0x3, 
    0x3, 0x3, 0x5, 0x3, 0x58, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 
    0x3, 0x5d, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x61, 0xa, 0x3, 0x3, 
    0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x7, 0x4, 0x67, 0xa, 0x4, 0xc, 0x4, 
    0xe, 0x4, 0x6a, 0xb, 0x4, 0x3, 0x5, 0x5, 0x5, 0x6d, 0xa, 0x5, 0x3, 0x5, 
    0x3, 0x5, 0x5, 0x5, 0x71, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x75, 
    0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x78, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x7b, 
    0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x7e, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x81, 
    0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x84, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x87, 
    0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x8a, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x8d, 
    0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x90, 0xa, 0x5, 0x3, 0x5, 0x5, 0x5, 0x93, 
    0xa, 0x5, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x5, 0x7, 0x9b, 0xa, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 
    0x8, 0xa1, 0xa, 0x8, 0x3, 0x9, 0x5, 0x9, 0xa4, 0xa, 0x9, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xb, 0x3, 
    0xb, 0x3, 0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 
    0xc, 0xb5, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 
    0x3, 0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 
    0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 
    0x12, 0x3, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0xce, 0xa, 
    0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x7, 0x14, 0xd3, 0xa, 0x14, 0xc, 
    0x14, 0xe, 0x14, 0xd6, 0xb, 0x14, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0xda, 
    0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0xde, 0xa, 0x15, 0x3, 0x15, 
    0x3, 0x15, 0x5, 0x15, 0xe2, 0xa, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x7, 0x17, 0xeb, 0xa, 0x17, 
    0xc, 0x17, 0xe, 0x17, 0xee, 0xb, 0x17, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 
    0x3, 0x18, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 
    0x1a, 0x7, 0x1a, 0xfa, 0xa, 0x1a, 0xc, 0x1a, 0xe, 0x1a, 0xfd, 0xb, 0x1a, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 
    0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x5, 0x1b, 0x10d, 0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x115, 0xa, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x6, 0x1b, 0x11c, 0xa, 0x1b, 
    0xd, 0x1b, 0xe, 0x1b, 0x11d, 0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x122, 
    0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 
    0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x12b, 0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 
    0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x139, 0xa, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 
    0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x5, 0x1b, 0x14b, 0xa, 0x1b, 0x3, 0x1b, 
    0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x7, 0x1b, 0x151, 0xa, 0x1b, 0xc, 0x1b, 
    0xe, 0x1b, 0x154, 0xb, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x7, 0x1c, 0x15b, 0xa, 0x1c, 0xc, 0x1c, 0xe, 0x1c, 0x15e, 
    0xb, 0x1c, 0x5, 0x1c, 0x160, 0xa, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x163, 
    0xa, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x167, 0xa, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x181, 0xa, 0x1c, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x7, 0x1d, 0x186, 0xa, 0x1d, 0xc, 0x1d, 
    0xe, 0x1d, 0x189, 0xb, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x18d, 
    0xa, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x7, 0x1f, 0x193, 
    0xa, 0x1f, 0xc, 0x1f, 0xe, 0x1f, 0x196, 0xb, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x7, 0x1f, 0x19d, 0xa, 0x1f, 0xc, 0x1f, 
    0xe, 0x1f, 0x1a0, 0xb, 0x1f, 0x5, 0x1f, 0x1a2, 0xa, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x1aa, 
    0xa, 0x20, 0x3, 0x20, 0x3, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 
    0x21, 0x1b1, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x22, 0x3, 0x22, 
    0x3, 0x23, 0x3, 0x23, 0x3, 0x24, 0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 
    0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 
    0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x5, 
    0x25, 0x1ca, 0xa, 0x25, 0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 0x1ce, 0xa, 
    0x25, 0x3, 0x25, 0x5, 0x25, 0x1d1, 0xa, 0x25, 0x3, 0x25, 0x5, 0x25, 
    0x1d4, 0xa, 0x25, 0x3, 0x25, 0x2, 0x3, 0x34, 0x26, 0x2, 0x4, 0x6, 0x8, 
    0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 
    0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 
    0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x2, 0x7, 0x4, 0x2, 
    0xa, 0xa, 0x4e, 0x4e, 0x4, 0x2, 0x29, 0x29, 0x31, 0x31, 0x4, 0x2, 0x37, 
    0x37, 0x44, 0x44, 0x5, 0x2, 0x2b, 0x2b, 0x45, 0x45, 0x5a, 0x5a, 0x4, 
    0x2, 0xc, 0xc, 0x4b, 0x4b, 0x2, 0x205, 0x2, 0x4a, 0x3, 0x2, 0x2, 0x2, 
    0x4, 0x57, 0x3, 0x2, 0x2, 0x2, 0x6, 0x62, 0x3, 0x2, 0x2, 0x2, 0x8, 0x6c, 
    0x3, 0x2, 0x2, 0x2, 0xa, 0x94, 0x3, 0x2, 0x2, 0x2, 0xc, 0x97, 0x3, 0x2, 
    0x2, 0x2, 0xe, 0x9c, 0x3, 0x2, 0x2, 0x2, 0x10, 0xa3, 0x3, 0x2, 0x2, 
    0x2, 0x12, 0xa9, 0x3, 0x2, 0x2, 0x2, 0x14, 0xac, 0x3, 0x2, 0x2, 0x2, 
    0x16, 0xaf, 0x3, 0x2, 0x2, 0x2, 0x18, 0xb6, 0x3, 0x2, 0x2, 0x2, 0x1a, 
    0xb9, 0x3, 0x2, 0x2, 0x2, 0x1c, 0xbd, 0x3, 0x2, 0x2, 0x2, 0x1e, 0xc2, 
    0x3, 0x2, 0x2, 0x2, 0x20, 0xc5, 0x3, 0x2, 0x2, 0x2, 0x22, 0xc8, 0x3, 
    0x2, 0x2, 0x2, 0x24, 0xca, 0x3, 0x2, 0x2, 0x2, 0x26, 0xcf, 0x3, 0x2, 
    0x2, 0x2, 0x28, 0xd7, 0x3, 0x2, 0x2, 0x2, 0x2a, 0xe3, 0x3, 0x2, 0x2, 
    0x2, 0x2c, 0xe7, 0x3, 0x2, 0x2, 0x2, 0x2e, 0xef, 0x3, 0x2, 0x2, 0x2, 
    0x30, 0xf3, 0x3, 0x2, 0x2, 0x2, 0x32, 0xf6, 0x3, 0x2, 0x2, 0x2, 0x34, 
    0x12a, 0x3, 0x2, 0x2, 0x2, 0x36, 0x180, 0x3, 0x2, 0x2, 0x2, 0x38, 0x182, 
    0x3, 0x2, 0x2, 0x2, 0x3a, 0x18c, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x1a1, 0x3, 
    0x2, 0x2, 0x2, 0x3e, 0x1a9, 0x3, 0x2, 0x2, 0x2, 0x40, 0x1b0, 0x3, 0x2, 
    0x2, 0x2, 0x42, 0x1b4, 0x3, 0x2, 0x2, 0x2, 0x44, 0x1b6, 0x3, 0x2, 0x2, 
    0x2, 0x46, 0x1b8, 0x3, 0x2, 0x2, 0x2, 0x48, 0x1d3, 0x3, 0x2, 0x2, 0x2, 
    0x4a, 0x4f, 0x5, 0x4, 0x3, 0x2, 0x4b, 0x4c, 0x7, 0x1e, 0x2, 0x2, 0x4c, 
    0x4e, 0x5, 0x4, 0x3, 0x2, 0x4d, 0x4b, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x51, 
    0x3, 0x2, 0x2, 0x2, 0x4f, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x4f, 0x50, 0x3, 
    0x2, 0x2, 0x2, 0x50, 0x53, 0x3, 0x2, 0x2, 0x2, 0x51, 0x4f, 0x3, 0x2, 
    0x2, 0x2, 0x52, 0x54, 0x7, 0x1e, 0x2, 0x2, 0x53, 0x52, 0x3, 0x2, 0x2, 
    0x2, 0x53, 0x54, 0x3, 0x2, 0x2, 0x2, 0x54, 0x3, 0x3, 0x2, 0x2, 0x2, 
    0x55, 0x58, 0x5, 0x6, 0x4, 0x2, 0x56, 0x58, 0x5, 0x30, 0x19, 0x2, 0x57, 
    0x55, 0x3, 0x2, 0x2, 0x2, 0x57, 0x56, 0x3, 0x2, 0x2, 0x2, 0x58, 0x5c, 
    0x3, 0x2, 0x2, 0x2, 0x59, 0x5a, 0x7, 0x41, 0x2, 0x2, 0x5a, 0x5b, 0x7, 
    0x51, 0x2, 0x2, 0x5b, 0x5d, 0x7, 0x24, 0x2, 0x2, 0x5c, 0x59, 0x3, 0x2, 
    0x2, 0x2, 0x5c, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x5d, 0x60, 0x3, 0x2, 0x2, 
    0x2, 0x5e, 0x5f, 0x7, 0x38, 0x2, 0x2, 0x5f, 0x61, 0x5, 0x44, 0x23, 0x2, 
    0x60, 0x5e, 0x3, 0x2, 0x2, 0x2, 0x60, 0x61, 0x3, 0x2, 0x2, 0x2, 0x61, 
    0x5, 0x3, 0x2, 0x2, 0x2, 0x62, 0x68, 0x5, 0x8, 0x5, 0x2, 0x63, 0x64, 
    0x7, 0x5c, 0x2, 0x2, 0x64, 0x65, 0x7, 0x25, 0x2, 0x2, 0x65, 0x67, 0x5, 
    0x8, 0x5, 0x2, 0x66, 0x63, 0x3, 0x2, 0x2, 0x2, 0x67, 0x6a, 0x3, 0x2, 
    0x2, 0x2, 0x68, 0x66, 0x3, 0x2, 0x2, 0x2, 0x68, 0x69, 0x3, 0x2, 0x2, 
    0x2, 0x69, 0x7, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x68, 0x3, 0x2, 0x2, 0x2, 
    0x6b, 0x6d, 0x5, 0xa, 0x6, 0x2, 0x6c, 0x6b, 0x3, 0x2, 0x2, 0x2, 0x6c, 
    0x6d, 0x3, 0x2, 0x2, 0x2, 0x6d, 0x6e, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x70, 
    0x7, 0x56, 0x2, 0x2, 0x6f, 0x71, 0x7, 0x32, 0x2, 0x2, 0x70, 0x6f, 0x3, 
    0x2, 0x2, 0x2, 0x70, 0x71, 0x3, 0x2, 0x2, 0x2, 0x71, 0x72, 0x3, 0x2, 
    0x2, 0x2, 0x72, 0x74, 0x5, 0x32, 0x1a, 0x2, 0x73, 0x75, 0x5, 0xc, 0x7, 
    0x2, 0x74, 0x73, 0x3, 0x2, 0x2, 0x2, 0x74, 0x75, 0x3, 0x2, 0x2, 0x2, 
    0x75, 0x77, 0x3, 0x2, 0x2, 0x2, 0x76, 0x78, 0x5, 0xe, 0x8, 0x2, 0x77, 
    0x76, 0x3, 0x2, 0x2, 0x2, 0x77, 0x78, 0x3, 0x2, 0x2, 0x2, 0x78, 0x7a, 
    0x3, 0x2, 0x2, 0x2, 0x79, 0x7b, 0x5, 0x10, 0x9, 0x2, 0x7a, 0x79, 0x3, 
    0x2, 0x2, 0x2, 0x7a, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x7b, 0x7d, 0x3, 0x2, 
    0x2, 0x2, 0x7c, 0x7e, 0x5, 0x12, 0xa, 0x2, 0x7d, 0x7c, 0x3, 0x2, 0x2, 
    0x2, 0x7d, 0x7e, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x80, 0x3, 0x2, 0x2, 0x2, 
    0x7f, 0x81, 0x5, 0x14, 0xb, 0x2, 0x80, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x80, 
    0x81, 0x3, 0x2, 0x2, 0x2, 0x81, 0x83, 0x3, 0x2, 0x2, 0x2, 0x82, 0x84, 
    0x5, 0x16, 0xc, 0x2, 0x83, 0x82, 0x3, 0x2, 0x2, 0x2, 0x83, 0x84, 0x3, 
    0x2, 0x2, 0x2, 0x84, 0x86, 0x3, 0x2, 0x2, 0x2, 0x85, 0x87, 0x5, 0x18, 
    0xd, 0x2, 0x86, 0x85, 0x3, 0x2, 0x2, 0x2, 0x86, 0x87, 0x3, 0x2, 0x2, 
    0x2, 0x87, 0x89, 0x3, 0x2, 0x2, 0x2, 0x88, 0x8a, 0x5, 0x1a, 0xe, 0x2, 
    0x89, 0x88, 0x3, 0x2, 0x2, 0x2, 0x89, 0x8a, 0x3, 0x2, 0x2, 0x2, 0x8a, 
    0x8c, 0x3, 0x2, 0x2, 0x2, 0x8b, 0x8d, 0x5, 0x1c, 0xf, 0x2, 0x8c, 0x8b, 
    0x3, 0x2, 0x2, 0x2, 0x8c, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x8d, 0x8f, 0x3, 
    0x2, 0x2, 0x2, 0x8e, 0x90, 0x5, 0x1e, 0x10, 0x2, 0x8f, 0x8e, 0x3, 0x2, 
    0x2, 0x2, 0x8f, 0x90, 0x3, 0x2, 0x2, 0x2, 0x90, 0x92, 0x3, 0x2, 0x2, 
    0x2, 0x91, 0x93, 0x5, 0x20, 0x11, 0x2, 0x92, 0x91, 0x3, 0x2, 0x2, 0x2, 
    0x92, 0x93, 0x3, 0x2, 0x2, 0x2, 0x93, 0x9, 0x3, 0x2, 0x2, 0x2, 0x94, 
    0x95, 0x7, 0x60, 0x2, 0x2, 0x95, 0x96, 0x5, 0x32, 0x1a, 0x2, 0x96, 0xb, 
    0x3, 0x2, 0x2, 0x2, 0x97, 0x98, 0x7, 0x39, 0x2, 0x2, 0x98, 0x9a, 0x5, 
    0x22, 0x12, 0x2, 0x99, 0x9b, 0x7, 0x36, 0x2, 0x2, 0x9a, 0x99, 0x3, 0x2, 
    0x2, 0x2, 0x9a, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x9b, 0xd, 0x3, 0x2, 0x2, 
    0x2, 0x9c, 0x9d, 0x7, 0x54, 0x2, 0x2, 0x9d, 0xa0, 0x5, 0x2a, 0x16, 0x2, 
    0x9e, 0x9f, 0x7, 0x4e, 0x2, 0x2, 0x9f, 0xa1, 0x5, 0x2a, 0x16, 0x2, 0xa0, 
    0x9e, 0x3, 0x2, 0x2, 0x2, 0xa0, 0xa1, 0x3, 0x2, 0x2, 0x2, 0xa1, 0xf, 
    0x3, 0x2, 0x2, 0x2, 0xa2, 0xa4, 0x7, 0x46, 0x2, 0x2, 0xa3, 0xa2, 0x3, 
    0x2, 0x2, 0x2, 0xa3, 0xa4, 0x3, 0x2, 0x2, 0x2, 0xa4, 0xa5, 0x3, 0x2, 
    0x2, 0x2, 0xa5, 0xa6, 0x7, 0x27, 0x2, 0x2, 0xa6, 0xa7, 0x7, 0x43, 0x2, 
    0x2, 0xa7, 0xa8, 0x5, 0x32, 0x1a, 0x2, 0xa8, 0x11, 0x3, 0x2, 0x2, 0x2, 
    0xa9, 0xaa, 0x7, 0x52, 0x2, 0x2, 0xaa, 0xab, 0x5, 0x34, 0x1b, 0x2, 0xab, 
    0x13, 0x3, 0x2, 0x2, 0x2, 0xac, 0xad, 0x7, 0x5f, 0x2, 0x2, 0xad, 0xae, 
    0x5, 0x34, 0x1b, 0x2, 0xae, 0x15, 0x3, 0x2, 0x2, 0x2, 0xaf, 0xb0, 0x7, 
    0x3b, 0x2, 0x2, 0xb0, 0xb1, 0x7, 0x2c, 0x2, 0x2, 0xb1, 0xb4, 0x5, 0x32, 
    0x1a, 0x2, 0xb2, 0xb3, 0x7, 0x60, 0x2, 0x2, 0xb3, 0xb5, 0x7, 0x59, 0x2, 
    0x2, 0xb4, 0xb2, 0x3, 0x2, 0x2, 0x2, 0xb4, 0xb5, 0x3, 0x2, 0x2, 0x2, 
    0xb5, 0x17, 0x3, 0x2, 0x2, 0x2, 0xb6, 0xb7, 0x7, 0x3c, 0x2, 0x2, 0xb7, 
    0xb8, 0x5, 0x34, 0x1b, 0x2, 0xb8, 0x19, 0x3, 0x2, 0x2, 0x2, 0xb9, 0xba, 
    0x7, 0x50, 0x2, 0x2, 0xba, 0xbb, 0x7, 0x2c, 0x2, 0x2, 0xbb, 0xbc, 0x5, 
    0x26, 0x14, 0x2, 0xbc, 0x1b, 0x3, 0x2, 0x2, 0x2, 0xbd, 0xbe, 0x7, 0x48, 
    0x2, 0x2, 0xbe, 0xbf, 0x5, 0x24, 0x13, 0x2, 0xbf, 0xc0, 0x7, 0x2c, 0x2, 
    0x2, 0xc0, 0xc1, 0x5, 0x32, 0x1a, 0x2, 0xc1, 0x1d, 0x3, 0x2, 0x2, 0x2, 
    0xc2, 0xc3, 0x7, 0x48, 0x2, 0x2, 0xc3, 0xc4, 0x5, 0x24, 0x13, 0x2, 0xc4, 
    0x1f, 0x3, 0x2, 0x2, 0x2, 0xc5, 0xc6, 0x7, 0x57, 0x2, 0x2, 0xc6, 0xc7, 
    0x5, 0x2c, 0x17, 0x2, 0xc7, 0x21, 0x3, 0x2, 0x2, 0x2, 0xc8, 0xc9, 0x5, 
    0x40, 0x21, 0x2, 0xc9, 0x23, 0x3, 0x2, 0x2, 0x2, 0xca, 0xcd, 0x7, 0x23, 
    0x2, 0x2, 0xcb, 0xcc, 0x9, 0x2, 0x2, 0x2, 0xcc, 0xce, 0x7, 0x23, 0x2, 
    0x2, 0xcd, 0xcb, 0x3, 0x2, 0x2, 0x2, 0xcd, 0xce, 0x3, 0x2, 0x2, 0x2, 
    0xce, 0x25, 0x3, 0x2, 0x2, 0x2, 0xcf, 0xd4, 0x5, 0x28, 0x15, 0x2, 0xd0, 
    0xd1, 0x7, 0xa, 0x2, 0x2, 0xd1, 0xd3, 0x5, 0x28, 0x15, 0x2, 0xd2, 0xd0, 
    0x3, 0x2, 0x2, 0x2, 0xd3, 0xd6, 0x3, 0x2, 0x2, 0x2, 0xd4, 0xd2, 0x3, 
    0x2, 0x2, 0x2, 0xd4, 0xd5, 0x3, 0x2, 0x2, 0x2, 0xd5, 0x27, 0x3, 0x2, 
    0x2, 0x2, 0xd6, 0xd4, 0x3, 0x2, 0x2, 0x2, 0xd7, 0xd9, 0x5, 0x34, 0x1b, 
    0x2, 0xd8, 0xda, 0x9, 0x3, 0x2, 0x2, 0xd9, 0xd8, 0x3, 0x2, 0x2, 0x2, 
    0xd9, 0xda, 0x3, 0x2, 0x2, 0x2, 0xda, 0xdd, 0x3, 0x2, 0x2, 0x2, 0xdb, 
    0xdc, 0x7, 0x4d, 0x2, 0x2, 0xdc, 0xde, 0x9, 0x4, 0x2, 0x2, 0xdd, 0xdb, 
    0x3, 0x2, 0x2, 0x2, 0xdd, 0xde, 0x3, 0x2, 0x2, 0x2, 0xde, 0xe1, 0x3, 
    0x2, 0x2, 0x2, 0xdf, 0xe0, 0x7, 0x2f, 0x2, 0x2, 0xe0, 0xe2, 0x7, 0x24, 
    0x2, 0x2, 0xe1, 0xdf, 0x3, 0x2, 0x2, 0x2, 0xe1, 0xe2, 0x3, 0x2, 0x2, 
    0x2, 0xe2, 0x29, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xe4, 0x7, 0x23, 0x2, 0x2, 
    0xe4, 0xe5, 0x7, 0x1f, 0x2, 0x2, 0xe5, 0xe6, 0x7, 0x23, 0x2, 0x2, 0xe6, 
    0x2b, 0x3, 0x2, 0x2, 0x2, 0xe7, 0xec, 0x5, 0x2e, 0x18, 0x2, 0xe8, 0xe9, 
    0x7, 0xa, 0x2, 0x2, 0xe9, 0xeb, 0x5, 0x2e, 0x18, 0x2, 0xea, 0xe8, 0x3, 
    0x2, 0x2, 0x2, 0xeb, 0xee, 0x3, 0x2, 0x2, 0x2, 0xec, 0xea, 0x3, 0x2, 
    0x2, 0x2, 0xec, 0xed, 0x3, 0x2, 0x2, 0x2, 0xed, 0x2d, 0x3, 0x2, 0x2, 
    0x2, 0xee, 0xec, 0x3, 0x2, 0x2, 0x2, 0xef, 0xf0, 0x5, 0x44, 0x23, 0x2, 
    0xf0, 0xf1, 0x7, 0x10, 0x2, 0x2, 0xf1, 0xf2, 0x7, 0x22, 0x2, 0x2, 0xf2, 
    0x2f, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf4, 0x7, 0x3f, 0x2, 0x2, 0xf4, 0xf5, 
    0x7, 0x41, 0x2, 0x2, 0xf5, 0x31, 0x3, 0x2, 0x2, 0x2, 0xf6, 0xfb, 0x5, 
    0x34, 0x1b, 0x2, 0xf7, 0xf8, 0x7, 0xa, 0x2, 0x2, 0xf8, 0xfa, 0x5, 0x34, 
    0x1b, 0x2, 0xf9, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xfa, 0xfd, 0x3, 0x2, 0x2, 
    0x2, 0xfb, 0xf9, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 
    0xfc, 0x33, 0x3, 0x2, 0x2, 0x2, 0xfd, 0xfb, 0x3, 0x2, 0x2, 0x2, 0xfe, 
    0xff, 0x8, 0x1b, 0x1, 0x2, 0xff, 0x12b, 0x7, 0x22, 0x2, 0x2, 0x100, 
    0x12b, 0x7, 0x6, 0x2, 0x2, 0x101, 0x12b, 0x5, 0x3e, 0x20, 0x2, 0x102, 
    0x103, 0x7, 0x15, 0x2, 0x2, 0x103, 0x104, 0x5, 0x34, 0x1b, 0x2, 0x104, 
    0x105, 0x7, 0x1d, 0x2, 0x2, 0x105, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x106, 
    0x107, 0x7, 0x15, 0x2, 0x2, 0x107, 0x108, 0x5, 0x8, 0x5, 0x2, 0x108, 
    0x109, 0x7, 0x1d, 0x2, 0x2, 0x109, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x10a, 
    0x10c, 0x7, 0x13, 0x2, 0x2, 0x10b, 0x10d, 0x5, 0x32, 0x1a, 0x2, 0x10c, 
    0x10b, 0x3, 0x2, 0x2, 0x2, 0x10c, 0x10d, 0x3, 0x2, 0x2, 0x2, 0x10d, 
    0x10e, 0x3, 0x2, 0x2, 0x2, 0x10e, 0x12b, 0x7, 0x1c, 0x2, 0x2, 0x10f, 
    0x110, 0x5, 0x46, 0x24, 0x2, 0x110, 0x111, 0x5, 0x34, 0x1b, 0xb, 0x111, 
    0x12b, 0x3, 0x2, 0x2, 0x2, 0x112, 0x114, 0x7, 0x2d, 0x2, 0x2, 0x113, 
    0x115, 0x5, 0x34, 0x1b, 0x2, 0x114, 0x113, 0x3, 0x2, 0x2, 0x2, 0x114, 
    0x115, 0x3, 0x2, 0x2, 0x2, 0x115, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x116, 
    0x117, 0x7, 0x5e, 0x2, 0x2, 0x117, 0x118, 0x5, 0x34, 0x1b, 0x2, 0x118, 
    0x119, 0x7, 0x58, 0x2, 0x2, 0x119, 0x11a, 0x5, 0x34, 0x1b, 0x2, 0x11a, 
    0x11c, 0x3, 0x2, 0x2, 0x2, 0x11b, 0x116, 0x3, 0x2, 0x2, 0x2, 0x11c, 
    0x11d, 0x3, 0x2, 0x2, 0x2, 0x11d, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x11d, 
    0x11e, 0x3, 0x2, 0x2, 0x2, 0x11e, 0x121, 0x3, 0x2, 0x2, 0x2, 0x11f, 
    0x120, 0x7, 0x33, 0x2, 0x2, 0x120, 0x122, 0x5, 0x34, 0x1b, 0x2, 0x121, 
    0x11f, 0x3, 0x2, 0x2, 0x2, 0x121, 0x122, 0x3, 0x2, 0x2, 0x2, 0x122, 
    0x123, 0x3, 0x2, 0x2, 0x2, 0x123, 0x124, 0x7, 0x34, 0x2, 0x2, 0x124, 
    0x12b, 0x3, 0x2, 0x2, 0x2, 0x125, 0x126, 0x7, 0x40, 0x2, 0x2, 0x126, 
    0x127, 0x5, 0x34, 0x1b, 0x2, 0x127, 0x128, 0x7, 0x62, 0x2, 0x2, 0x128, 
    0x12b, 0x3, 0x2, 0x2, 0x2, 0x129, 0x12b, 0x5, 0x36, 0x1c, 0x2, 0x12a, 
    0xfe, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x100, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x101, 
    0x3, 0x2, 0x2, 0x2, 0x12a, 0x102, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x106, 
    0x3, 0x2, 0x2, 0x2, 0x12a, 0x10a, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x10f, 
    0x3, 0x2, 0x2, 0x2, 0x12a, 0x112, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x125, 
    0x3, 0x2, 0x2, 0x2, 0x12a, 0x129, 0x3, 0x2, 0x2, 0x2, 0x12b, 0x152, 
    0x3, 0x2, 0x2, 0x2, 0x12c, 0x12d, 0xc, 0x9, 0x2, 0x2, 0x12d, 0x12e, 
    0x5, 0x48, 0x25, 0x2, 0x12e, 0x12f, 0x5, 0x34, 0x1b, 0xa, 0x12f, 0x151, 
    0x3, 0x2, 0x2, 0x2, 0x130, 0x131, 0xc, 0x8, 0x2, 0x2, 0x131, 0x132, 
    0x7, 0x1a, 0x2, 0x2, 0x132, 0x133, 0x5, 0x34, 0x1b, 0x2, 0x133, 0x134, 
    0x7, 0x9, 0x2, 0x2, 0x134, 0x135, 0x5, 0x34, 0x1b, 0x9, 0x135, 0x151, 
    0x3, 0x2, 0x2, 0x2, 0x136, 0x138, 0xc, 0x7, 0x2, 0x2, 0x137, 0x139, 
    0x7, 0x4b, 0x2, 0x2, 0x138, 0x137, 0x3, 0x2, 0x2, 0x2, 0x138, 0x139, 
    0x3, 0x2, 0x2, 0x2, 0x139, 0x13a, 0x3, 0x2, 0x2, 0x2, 0x13a, 0x13b, 
    0x7, 0x2a, 0x2, 0x2, 0x13b, 0x13c, 0x5, 0x34, 0x1b, 0x2, 0x13c, 0x13d, 
    0x7, 0x26, 0x2, 0x2, 0x13d, 0x13e, 0x5, 0x34, 0x1b, 0x8, 0x13e, 0x151, 
    0x3, 0x2, 0x2, 0x2, 0x13f, 0x140, 0xc, 0xd, 0x2, 0x2, 0x140, 0x141, 
    0x7, 0x13, 0x2, 0x2, 0x141, 0x142, 0x5, 0x34, 0x1b, 0x2, 0x142, 0x143, 
    0x7, 0x1c, 0x2, 0x2, 0x143, 0x151, 0x3, 0x2, 0x2, 0x2, 0x144, 0x145, 
    0xc, 0xc, 0x2, 0x2, 0x145, 0x146, 0x7, 0xd, 0x2, 0x2, 0x146, 0x151, 
    0x7, 0x23, 0x2, 0x2, 0x147, 0x148, 0xc, 0xa, 0x2, 0x2, 0x148, 0x14a, 
    0x7, 0x42, 0x2, 0x2, 0x149, 0x14b, 0x7, 0x4b, 0x2, 0x2, 0x14a, 0x149, 
    0x3, 0x2, 0x2, 0x2, 0x14a, 0x14b, 0x3, 0x2, 0x2, 0x2, 0x14b, 0x14c, 
    0x3, 0x2, 0x2, 0x2, 0x14c, 0x151, 0x7, 0x4c, 0x2, 0x2, 0x14d, 0x14e, 
    0xc, 0x3, 0x2, 0x2, 0x14e, 0x14f, 0x7, 0x28, 0x2, 0x2, 0x14f, 0x151, 
    0x5, 0x44, 0x23, 0x2, 0x150, 0x12c, 0x3, 0x2, 0x2, 0x2, 0x150, 0x130, 
    0x3, 0x2, 0x2, 0x2, 0x150, 0x136, 0x3, 0x2, 0x2, 0x2, 0x150, 0x13f, 
    0x3, 0x2, 0x2, 0x2, 0x150, 0x144, 0x3, 0x2, 0x2, 0x2, 0x150, 0x147, 
    0x3, 0x2, 0x2, 0x2, 0x150, 0x14d, 0x3, 0x2, 0x2, 0x2, 0x151, 0x154, 
    0x3, 0x2, 0x2, 0x2, 0x152, 0x150, 0x3, 0x2, 0x2, 0x2, 0x152, 0x153, 
    0x3, 0x2, 0x2, 0x2, 0x153, 0x35, 0x3, 0x2, 0x2, 0x2, 0x154, 0x152, 0x3, 
    0x2, 0x2, 0x2, 0x155, 0x162, 0x5, 0x44, 0x23, 0x2, 0x156, 0x15f, 0x7, 
    0x15, 0x2, 0x2, 0x157, 0x15c, 0x7, 0x22, 0x2, 0x2, 0x158, 0x159, 0x7, 
    0xa, 0x2, 0x2, 0x159, 0x15b, 0x7, 0x22, 0x2, 0x2, 0x15a, 0x158, 0x3, 
    0x2, 0x2, 0x2, 0x15b, 0x15e, 0x3, 0x2, 0x2, 0x2, 0x15c, 0x15a, 0x3, 
    0x2, 0x2, 0x2, 0x15c, 0x15d, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x160, 0x3, 
    0x2, 0x2, 0x2, 0x15e, 0x15c, 0x3, 0x2, 0x2, 0x2, 0x15f, 0x157, 0x3, 
    0x2, 0x2, 0x2, 0x15f, 0x160, 0x3, 0x2, 0x2, 0x2, 0x160, 0x161, 0x3, 
    0x2, 0x2, 0x2, 0x161, 0x163, 0x7, 0x1d, 0x2, 0x2, 0x162, 0x156, 0x3, 
    0x2, 0x2, 0x2, 0x162, 0x163, 0x3, 0x2, 0x2, 0x2, 0x163, 0x164, 0x3, 
    0x2, 0x2, 0x2, 0x164, 0x166, 0x7, 0x15, 0x2, 0x2, 0x165, 0x167, 0x5, 
    0x38, 0x1d, 0x2, 0x166, 0x165, 0x3, 0x2, 0x2, 0x2, 0x166, 0x167, 0x3, 
    0x2, 0x2, 0x2, 0x167, 0x168, 0x3, 0x2, 0x2, 0x2, 0x168, 0x169, 0x7, 
    0x1d, 0x2, 0x2, 0x169, 0x181, 0x3, 0x2, 0x2, 0x2, 0x16a, 0x16b, 0x7, 
    0x35, 0x2, 0x2, 0x16b, 0x16c, 0x7, 0x15, 0x2, 0x2, 0x16c, 0x16d, 0x7, 
    0x62, 0x2, 0x2, 0x16d, 0x16e, 0x7, 0x39, 0x2, 0x2, 0x16e, 0x16f, 0x5, 
    0x34, 0x1b, 0x2, 0x16f, 0x170, 0x7, 0x1d, 0x2, 0x2, 0x170, 0x181, 0x3, 
    0x2, 0x2, 0x2, 0x171, 0x172, 0x7, 0x2e, 0x2, 0x2, 0x172, 0x173, 0x7, 
    0x15, 0x2, 0x2, 0x173, 0x174, 0x5, 0x34, 0x1b, 0x2, 0x174, 0x175, 0x7, 
    0x28, 0x2, 0x2, 0x175, 0x176, 0x5, 0x44, 0x23, 0x2, 0x176, 0x177, 0x7, 
    0x1d, 0x2, 0x2, 0x177, 0x181, 0x3, 0x2, 0x2, 0x2, 0x178, 0x179, 0x7, 
    0x5b, 0x2, 0x2, 0x179, 0x17a, 0x7, 0x15, 0x2, 0x2, 0x17a, 0x17b, 0x9, 
    0x5, 0x2, 0x2, 0x17b, 0x17c, 0x7, 0x24, 0x2, 0x2, 0x17c, 0x17d, 0x7, 
    0x39, 0x2, 0x2, 0x17d, 0x17e, 0x5, 0x34, 0x1b, 0x2, 0x17e, 0x17f, 0x7, 
    0x1d, 0x2, 0x2, 0x17f, 0x181, 0x3, 0x2, 0x2, 0x2, 0x180, 0x155, 0x3, 
    0x2, 0x2, 0x2, 0x180, 0x16a, 0x3, 0x2, 0x2, 0x2, 0x180, 0x171, 0x3, 
    0x2, 0x2, 0x2, 0x180, 0x178, 0x3, 0x2, 0x2, 0x2, 0x181, 0x37, 0x3, 0x2, 
    0x2, 0x2, 0x182, 0x187, 0x5, 0x3a, 0x1e, 0x2, 0x183, 0x184, 0x7, 0xa, 
    0x2, 0x2, 0x184, 0x186, 0x5, 0x3a, 0x1e, 0x2, 0x185, 0x183, 0x3, 0x2, 
    0x2, 0x2, 0x186, 0x189, 0x3, 0x2, 0x2, 0x2, 0x187, 0x185, 0x3, 0x2, 
    0x2, 0x2, 0x187, 0x188, 0x3, 0x2, 0x2, 0x2, 0x188, 0x39, 0x3, 0x2, 0x2, 
    0x2, 0x189, 0x187, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x18d, 0x5, 0x34, 0x1b, 
    0x2, 0x18b, 0x18d, 0x5, 0x3c, 0x1f, 0x2, 0x18c, 0x18a, 0x3, 0x2, 0x2, 
    0x2, 0x18c, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x18d, 0x3b, 0x3, 0x2, 0x2, 0x2, 
    0x18e, 0x18f, 0x7, 0x15, 0x2, 0x2, 0x18f, 0x194, 0x5, 0x44, 0x23, 0x2, 
    0x190, 0x191, 0x7, 0xa, 0x2, 0x2, 0x191, 0x193, 0x5, 0x44, 0x23, 0x2, 
    0x192, 0x190, 0x3, 0x2, 0x2, 0x2, 0x193, 0x196, 0x3, 0x2, 0x2, 0x2, 
    0x194, 0x192, 0x3, 0x2, 0x2, 0x2, 0x194, 0x195, 0x3, 0x2, 0x2, 0x2, 
    0x195, 0x197, 0x3, 0x2, 0x2, 0x2, 0x196, 0x194, 0x3, 0x2, 0x2, 0x2, 
    0x197, 0x198, 0x7, 0x1d, 0x2, 0x2, 0x198, 0x1a2, 0x3, 0x2, 0x2, 0x2, 
    0x199, 0x19e, 0x5, 0x44, 0x23, 0x2, 0x19a, 0x19b, 0x7, 0xa, 0x2, 0x2, 
    0x19b, 0x19d, 0x5, 0x44, 0x23, 0x2, 0x19c, 0x19a, 0x3, 0x2, 0x2, 0x2, 
    0x19d, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x19e, 0x19c, 0x3, 0x2, 0x2, 0x2, 
    0x19e, 0x19f, 0x3, 0x2, 0x2, 0x2, 0x19f, 0x1a2, 0x3, 0x2, 0x2, 0x2, 
    0x1a0, 0x19e, 0x3, 0x2, 0x2, 0x2, 0x1a1, 0x18e, 0x3, 0x2, 0x2, 0x2, 
    0x1a1, 0x199, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a3, 0x3, 0x2, 0x2, 0x2, 
    0x1a3, 0x1a4, 0x7, 0x5, 0x2, 0x2, 0x1a4, 0x1a5, 0x5, 0x34, 0x1b, 0x2, 
    0x1a5, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x1a6, 0x1a7, 0x5, 0x40, 0x21, 0x2, 
    0x1a7, 0x1a8, 0x7, 0xd, 0x2, 0x2, 0x1a8, 0x1aa, 0x3, 0x2, 0x2, 0x2, 
    0x1a9, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1a9, 0x1aa, 0x3, 0x2, 0x2, 0x2, 
    0x1aa, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1ab, 0x1ac, 0x5, 0x44, 0x23, 0x2, 
    0x1ac, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x1ad, 0x1ae, 0x5, 0x42, 0x22, 0x2, 
    0x1ae, 0x1af, 0x7, 0xd, 0x2, 0x2, 0x1af, 0x1b1, 0x3, 0x2, 0x2, 0x2, 
    0x1b0, 0x1ad, 0x3, 0x2, 0x2, 0x2, 0x1b0, 0x1b1, 0x3, 0x2, 0x2, 0x2, 
    0x1b1, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b2, 0x1b3, 0x5, 0x44, 0x23, 0x2, 
    0x1b3, 0x41, 0x3, 0x2, 0x2, 0x2, 0x1b4, 0x1b5, 0x5, 0x44, 0x23, 0x2, 
    0x1b5, 0x43, 0x3, 0x2, 0x2, 0x2, 0x1b6, 0x1b7, 0x7, 0x21, 0x2, 0x2, 
    0x1b7, 0x45, 0x3, 0x2, 0x2, 0x2, 0x1b8, 0x1b9, 0x9, 0x6, 0x2, 0x2, 0x1b9, 
    0x47, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1d4, 0x7, 0x6, 0x2, 0x2, 0x1bb, 0x1d4, 
    0x7, 0x1f, 0x2, 0x2, 0x1bc, 0x1d4, 0x7, 0x18, 0x2, 0x2, 0x1bd, 0x1d4, 
    0x7, 0x19, 0x2, 0x2, 0x1be, 0x1d4, 0x7, 0xc, 0x2, 0x2, 0x1bf, 0x1d4, 
    0x7, 0xe, 0x2, 0x2, 0x1c0, 0x1d4, 0x7, 0x17, 0x2, 0x2, 0x1c1, 0x1d4, 
    0x7, 0x14, 0x2, 0x2, 0x1c2, 0x1d4, 0x7, 0x11, 0x2, 0x2, 0x1c3, 0x1d4, 
    0x7, 0x16, 0x2, 0x2, 0x1c4, 0x1d4, 0x7, 0x12, 0x2, 0x2, 0x1c5, 0x1d4, 
    0x7, 0xb, 0x2, 0x2, 0x1c6, 0x1d4, 0x7, 0x26, 0x2, 0x2, 0x1c7, 0x1d4, 
    0x7, 0x4f, 0x2, 0x2, 0x1c8, 0x1ca, 0x7, 0x4b, 0x2, 0x2, 0x1c9, 0x1c8, 
    0x3, 0x2, 0x2, 0x2, 0x1c9, 0x1ca, 0x3, 0x2, 0x2, 0x2, 0x1ca, 0x1cb, 
    0x3, 0x2, 0x2, 0x2, 0x1cb, 0x1d4, 0x7, 0x47, 0x2, 0x2, 0x1cc, 0x1ce, 
    0x7, 0x3a, 0x2, 0x2, 0x1cd, 0x1cc, 0x3, 0x2, 0x2, 0x2, 0x1cd, 0x1ce, 
    0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d1, 
    0x7, 0x4b, 0x2, 0x2, 0x1d0, 0x1cf, 0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 
    0x3, 0x2, 0x2, 0x2, 0x1d1, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x1d2, 0x1d4, 
    0x7, 0x3e, 0x2, 0x2, 0x1d3, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1bb, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1bc, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1bd, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1be, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1bf, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c0, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c1, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c3, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c5, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c7, 
    0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1c9, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1cd, 
    0x3, 0x2, 0x2, 0x2, 0x1d4, 0x49, 0x3, 0x2, 0x2, 0x2, 0x39, 0x4f, 0x53, 
    0x57, 0x5c, 0x60, 0x68, 0x6c, 0x70, 0x74, 0x77, 0x7a, 0x7d, 0x80, 0x83, 
    0x86, 0x89, 0x8c, 0x8f, 0x92, 0x9a, 0xa0, 0xa3, 0xb4, 0xcd, 0xd4, 0xd9, 
    0xdd, 0xe1, 0xec, 0xfb, 0x10c, 0x114, 0x11d, 0x121, 0x12a, 0x138, 0x14a, 
    0x150, 0x152, 0x15c, 0x15f, 0x162, 0x166, 0x180, 0x187, 0x18c, 0x194, 
    0x19e, 0x1a1, 0x1a9, 0x1b0, 0x1c9, 0x1cd, 0x1d0, 0x1d3, 
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
