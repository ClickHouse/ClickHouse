
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
    setState(140);
    queryStmt();
    setState(145);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(141);
        match(ClickHouseParser::SEMICOLON);
        setState(142);
        queryStmt(); 
      }
      setState(147);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    }
    setState(149);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SEMICOLON) {
      setState(148);
      match(ClickHouseParser::SEMICOLON);
    }
    setState(151);
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
    setState(153);
    query();
    setState(157);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::INTO) {
      setState(154);
      match(ClickHouseParser::INTO);
      setState(155);
      match(ClickHouseParser::OUTFILE);
      setState(156);
      match(ClickHouseParser::STRING_LITERAL);
    }
    setState(161);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FORMAT) {
      setState(159);
      match(ClickHouseParser::FORMAT);
      setState(160);
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

ClickHouseParser::InsertStmtContext* ClickHouseParser::QueryContext::insertStmt() {
  return getRuleContext<ClickHouseParser::InsertStmtContext>(0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::QueryContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

ClickHouseParser::SetStmtContext* ClickHouseParser::QueryContext::setStmt() {
  return getRuleContext<ClickHouseParser::SetStmtContext>(0);
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
    setState(168);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CREATE:
      case ClickHouseParser::DROP:
      case ClickHouseParser::OPTIMIZE: {
        enterOuterAlt(_localctx, 1);
        setState(163);
        distributedStmt();
        break;
      }

      case ClickHouseParser::INSERT: {
        enterOuterAlt(_localctx, 2);
        setState(164);
        insertStmt();
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 3);
        setState(165);
        selectUnionStmt();
        break;
      }

      case ClickHouseParser::SET: {
        enterOuterAlt(_localctx, 4);
        setState(166);
        setStmt();
        break;
      }

      case ClickHouseParser::USE: {
        enterOuterAlt(_localctx, 5);
        setState(167);
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

//----------------- DistributedStmtContext ------------------------------------------------------------------

ClickHouseParser::DistributedStmtContext::DistributedStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ClickHouseParser::CreateDatabaseStmtContext* ClickHouseParser::DistributedStmtContext::createDatabaseStmt() {
  return getRuleContext<ClickHouseParser::CreateDatabaseStmtContext>(0);
}

ClickHouseParser::CreateTableStmtContext* ClickHouseParser::DistributedStmtContext::createTableStmt() {
  return getRuleContext<ClickHouseParser::CreateTableStmtContext>(0);
}

ClickHouseParser::DropStmtContext* ClickHouseParser::DistributedStmtContext::dropStmt() {
  return getRuleContext<ClickHouseParser::DropStmtContext>(0);
}

ClickHouseParser::OptimizeStmtContext* ClickHouseParser::DistributedStmtContext::optimizeStmt() {
  return getRuleContext<ClickHouseParser::OptimizeStmtContext>(0);
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
    setState(174);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(170);
      createDatabaseStmt();
      break;
    }

    case 2: {
      setState(171);
      createTableStmt();
      break;
    }

    case 3: {
      setState(172);
      dropStmt();
      break;
    }

    case 4: {
      setState(173);
      optimizeStmt();
      break;
    }

    }
    setState(179);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ON) {
      setState(176);
      match(ClickHouseParser::ON);
      setState(177);
      match(ClickHouseParser::CLUSTER);
      setState(178);
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

//----------------- CreateDatabaseStmtContext ------------------------------------------------------------------

ClickHouseParser::CreateDatabaseStmtContext::CreateDatabaseStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateDatabaseStmtContext::DATABASE() {
  return getToken(ClickHouseParser::DATABASE, 0);
}

ClickHouseParser::DatabaseIdentifierContext* ClickHouseParser::CreateDatabaseStmtContext::databaseIdentifier() {
  return getRuleContext<ClickHouseParser::DatabaseIdentifierContext>(0);
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


size_t ClickHouseParser::CreateDatabaseStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleCreateDatabaseStmt;
}


antlrcpp::Any ClickHouseParser::CreateDatabaseStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateDatabaseStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::CreateDatabaseStmtContext* ClickHouseParser::createDatabaseStmt() {
  CreateDatabaseStmtContext *_localctx = _tracker.createInstance<CreateDatabaseStmtContext>(_ctx, getState());
  enterRule(_localctx, 8, ClickHouseParser::RuleCreateDatabaseStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(181);
    match(ClickHouseParser::CREATE);
    setState(182);
    match(ClickHouseParser::DATABASE);
    setState(186);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
    case 1: {
      setState(183);
      match(ClickHouseParser::IF);
      setState(184);
      match(ClickHouseParser::NOT);
      setState(185);
      match(ClickHouseParser::EXISTS);
      break;
    }

    }
    setState(188);
    databaseIdentifier();
    setState(190);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ENGINE) {
      setState(189);
      engineExpr();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CreateTableStmtContext ------------------------------------------------------------------

ClickHouseParser::CreateTableStmtContext::CreateTableStmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::CREATE() {
  return getToken(ClickHouseParser::CREATE, 0);
}

tree::TerminalNode* ClickHouseParser::CreateTableStmtContext::TABLE() {
  return getToken(ClickHouseParser::TABLE, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::CreateTableStmtContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::SchemaClauseContext* ClickHouseParser::CreateTableStmtContext::schemaClause() {
  return getRuleContext<ClickHouseParser::SchemaClauseContext>(0);
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


size_t ClickHouseParser::CreateTableStmtContext::getRuleIndex() const {
  return ClickHouseParser::RuleCreateTableStmt;
}


antlrcpp::Any ClickHouseParser::CreateTableStmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitCreateTableStmt(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::CreateTableStmtContext* ClickHouseParser::createTableStmt() {
  CreateTableStmtContext *_localctx = _tracker.createInstance<CreateTableStmtContext>(_ctx, getState());
  enterRule(_localctx, 10, ClickHouseParser::RuleCreateTableStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(192);
    match(ClickHouseParser::CREATE);
    setState(193);
    match(ClickHouseParser::TABLE);
    setState(197);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(194);
      match(ClickHouseParser::IF);
      setState(195);
      match(ClickHouseParser::NOT);
      setState(196);
      match(ClickHouseParser::EXISTS);
      break;
    }

    }
    setState(199);
    tableIdentifier();
    setState(200);
    schemaClause();
   
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

//----------------- SchemaAsSubqueryClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaAsSubqueryClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::SelectUnionStmtContext* ClickHouseParser::SchemaAsSubqueryClauseContext::selectUnionStmt() {
  return getRuleContext<ClickHouseParser::SelectUnionStmtContext>(0);
}

ClickHouseParser::EngineClauseContext* ClickHouseParser::SchemaAsSubqueryClauseContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
}

ClickHouseParser::SchemaAsSubqueryClauseContext::SchemaAsSubqueryClauseContext(SchemaClauseContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::SchemaAsSubqueryClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitSchemaAsSubqueryClause(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SchemaAsTableClauseContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::SchemaAsTableClauseContext::AS() {
  return getToken(ClickHouseParser::AS, 0);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::SchemaAsTableClauseContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

ClickHouseParser::EngineClauseContext* ClickHouseParser::SchemaAsTableClauseContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
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

ClickHouseParser::EngineClauseContext* ClickHouseParser::SchemaDescriptionClauseContext::engineClause() {
  return getRuleContext<ClickHouseParser::EngineClauseContext>(0);
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
  enterRule(_localctx, 12, ClickHouseParser::RuleSchemaClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(232);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaDescriptionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(202);
      match(ClickHouseParser::LPAREN);
      setState(203);
      tableElementExpr();
      setState(208);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(204);
        match(ClickHouseParser::COMMA);
        setState(205);
        tableElementExpr();
        setState(210);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(211);
      match(ClickHouseParser::RPAREN);
      setState(212);
      engineClause();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsSubqueryClauseContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(215);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(214);
        engineClause();
      }
      setState(217);
      match(ClickHouseParser::AS);
      setState(218);
      selectUnionStmt();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsTableClauseContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(219);
      match(ClickHouseParser::AS);
      setState(220);
      tableIdentifier();
      setState(222);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ENGINE) {
        setState(221);
        engineClause();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<SchemaClauseContext *>(_tracker.createInstance<ClickHouseParser::SchemaAsFunctionClauseContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(224);
      match(ClickHouseParser::AS);
      setState(225);
      identifier();
      setState(226);
      match(ClickHouseParser::LPAREN);
      setState(228);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
        setState(227);
        tableArgList();
      }
      setState(230);
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
  enterRule(_localctx, 14, ClickHouseParser::RuleEngineClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(234);
    engineExpr();
    setState(236);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(235);
      orderByClause();
    }
    setState(239);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(238);
      partitionByClause();
    }
    setState(242);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PRIMARY) {
      setState(241);
      primaryKeyClause();
    }
    setState(245);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(244);
      sampleByClause();
    }
    setState(248);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::TTL) {
      setState(247);
      ttlClause();
    }
    setState(251);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(250);
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
  enterRule(_localctx, 16, ClickHouseParser::RulePartitionByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(253);
    match(ClickHouseParser::PARTITION);
    setState(254);
    match(ClickHouseParser::BY);
    setState(255);
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
  enterRule(_localctx, 18, ClickHouseParser::RulePrimaryKeyClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(257);
    match(ClickHouseParser::PRIMARY);
    setState(258);
    match(ClickHouseParser::KEY);
    setState(259);
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
  enterRule(_localctx, 20, ClickHouseParser::RuleSampleByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(261);
    match(ClickHouseParser::SAMPLE);
    setState(262);
    match(ClickHouseParser::BY);
    setState(263);
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
  enterRule(_localctx, 22, ClickHouseParser::RuleTtlClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(265);
    match(ClickHouseParser::TTL);
    setState(266);
    ttlExpr();
    setState(271);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(267);
      match(ClickHouseParser::COMMA);
      setState(268);
      ttlExpr();
      setState(273);
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

ClickHouseParser::IdentifierContext* ClickHouseParser::EngineExprContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
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
  enterRule(_localctx, 24, ClickHouseParser::RuleEngineExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(274);
    match(ClickHouseParser::ENGINE);
    setState(276);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::EQ_SINGLE) {
      setState(275);
      match(ClickHouseParser::EQ_SINGLE);
    }
    setState(278);
    identifier();
    setState(284);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(279);
      match(ClickHouseParser::LPAREN);
      setState(281);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::ASTERISK - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::LBRACKET - 64))
        | (1ULL << (ClickHouseParser::LPAREN - 64))
        | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
        setState(280);
        columnExprList();
      }
      setState(283);
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

//----------------- TableElementExprColumnWithTypeContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::TableElementExprColumnWithTypeContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::TableElementExprColumnWithTypeContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

ClickHouseParser::TableColumnPropertyExprContext* ClickHouseParser::TableElementExprColumnWithTypeContext::tableColumnPropertyExpr() {
  return getRuleContext<ClickHouseParser::TableColumnPropertyExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprColumnWithTypeContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableElementExprColumnWithTypeContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::TableElementExprColumnWithTypeContext::TableElementExprColumnWithTypeContext(TableElementExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TableElementExprColumnWithTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprColumnWithType(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableElementExprColumnWithDefaultContext ------------------------------------------------------------------

ClickHouseParser::IdentifierContext* ClickHouseParser::TableElementExprColumnWithDefaultContext::identifier() {
  return getRuleContext<ClickHouseParser::IdentifierContext>(0);
}

ClickHouseParser::TableColumnPropertyExprContext* ClickHouseParser::TableElementExprColumnWithDefaultContext::tableColumnPropertyExpr() {
  return getRuleContext<ClickHouseParser::TableColumnPropertyExprContext>(0);
}

ClickHouseParser::ColumnTypeExprContext* ClickHouseParser::TableElementExprColumnWithDefaultContext::columnTypeExpr() {
  return getRuleContext<ClickHouseParser::ColumnTypeExprContext>(0);
}

tree::TerminalNode* ClickHouseParser::TableElementExprColumnWithDefaultContext::TTL() {
  return getToken(ClickHouseParser::TTL, 0);
}

ClickHouseParser::ColumnExprContext* ClickHouseParser::TableElementExprColumnWithDefaultContext::columnExpr() {
  return getRuleContext<ClickHouseParser::ColumnExprContext>(0);
}

ClickHouseParser::TableElementExprColumnWithDefaultContext::TableElementExprColumnWithDefaultContext(TableElementExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::TableElementExprColumnWithDefaultContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitTableElementExprColumnWithDefault(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::TableElementExprContext* ClickHouseParser::tableElementExpr() {
  TableElementExprContext *_localctx = _tracker.createInstance<TableElementExprContext>(_ctx, getState());
  enterRule(_localctx, 26, ClickHouseParser::RuleTableElementExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(304);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnWithTypeContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(286);
      identifier();
      setState(287);
      columnTypeExpr();
      setState(289);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::ALIAS)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::MATERIALIZED))) != 0)) {
        setState(288);
        tableColumnPropertyExpr();
      }
      setState(293);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(291);
        match(ClickHouseParser::TTL);
        setState(292);
        columnExpr(0);
      }
      break;
    }

    case 2: {
      _localctx = dynamic_cast<TableElementExprContext *>(_tracker.createInstance<ClickHouseParser::TableElementExprColumnWithDefaultContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(295);
      identifier();
      setState(297);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
      case 1: {
        setState(296);
        columnTypeExpr();
        break;
      }

      }
      setState(299);
      tableColumnPropertyExpr();
      setState(302);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TTL) {
        setState(300);
        match(ClickHouseParser::TTL);
        setState(301);
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
  enterRule(_localctx, 28, ClickHouseParser::RuleTableColumnPropertyExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(306);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ALIAS)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::MATERIALIZED))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(307);
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
  enterRule(_localctx, 30, ClickHouseParser::RuleTtlExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(309);
    columnExpr(0);
    setState(317);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
    case 1: {
      setState(310);
      match(ClickHouseParser::DELETE);
      break;
    }

    case 2: {
      setState(311);
      match(ClickHouseParser::TO);
      setState(312);
      match(ClickHouseParser::DISK);
      setState(313);
      match(ClickHouseParser::STRING_LITERAL);
      break;
    }

    case 3: {
      setState(314);
      match(ClickHouseParser::TO);
      setState(315);
      match(ClickHouseParser::VOLUME);
      setState(316);
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
  enterRule(_localctx, 32, ClickHouseParser::RuleDropStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(336);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropDatabaseStmtContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(319);
      match(ClickHouseParser::DROP);
      setState(320);
      match(ClickHouseParser::DATABASE);
      setState(323);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
      case 1: {
        setState(321);
        match(ClickHouseParser::IF);
        setState(322);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(325);
      databaseIdentifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<DropStmtContext *>(_tracker.createInstance<ClickHouseParser::DropTableStmtContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(326);
      match(ClickHouseParser::DROP);
      setState(328);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::TEMPORARY) {
        setState(327);
        match(ClickHouseParser::TEMPORARY);
      }
      setState(330);
      match(ClickHouseParser::TABLE);
      setState(333);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
      case 1: {
        setState(331);
        match(ClickHouseParser::IF);
        setState(332);
        match(ClickHouseParser::EXISTS);
        break;
      }

      }
      setState(335);
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

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::InsertStmtContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::InsertStmtContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
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
  enterRule(_localctx, 34, ClickHouseParser::RuleInsertStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(338);
    match(ClickHouseParser::INSERT);
    setState(339);
    match(ClickHouseParser::INTO);
    setState(340);
    tableIdentifier();
    setState(352);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LPAREN) {
      setState(341);
      match(ClickHouseParser::LPAREN);
      setState(342);
      identifier();
      setState(347);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(343);
        match(ClickHouseParser::COMMA);
        setState(344);
        identifier();
        setState(349);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(350);
      match(ClickHouseParser::RPAREN);
    }
    setState(354);
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
  enterRule(_localctx, 36, ClickHouseParser::RuleValuesClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(368);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::VALUES: {
        enterOuterAlt(_localctx, 1);
        setState(356);
        match(ClickHouseParser::VALUES);
        setState(357);
        valueTupleExpr();
        setState(364);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA

        || _la == ClickHouseParser::LPAREN) {
          setState(359);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::COMMA) {
            setState(358);
            match(ClickHouseParser::COMMA);
          }
          setState(361);
          valueTupleExpr();
          setState(366);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      case ClickHouseParser::SELECT:
      case ClickHouseParser::WITH: {
        enterOuterAlt(_localctx, 2);
        setState(367);
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

ClickHouseParser::ValueExprListContext* ClickHouseParser::ValueTupleExprContext::valueExprList() {
  return getRuleContext<ClickHouseParser::ValueExprListContext>(0);
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
  enterRule(_localctx, 38, ClickHouseParser::RuleValueTupleExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(370);
    match(ClickHouseParser::LPAREN);
    setState(371);
    valueExprList();
    setState(372);
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
  enterRule(_localctx, 40, ClickHouseParser::RuleOptimizeStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(374);
    match(ClickHouseParser::OPTIMIZE);
    setState(375);
    match(ClickHouseParser::TABLE);
    setState(376);
    tableIdentifier();
    setState(378);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PARTITION) {
      setState(377);
      partitionClause();
    }
    setState(381);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(380);
      match(ClickHouseParser::FINAL);
    }
    setState(384);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::DEDUPLICATE) {
      setState(383);
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
  enterRule(_localctx, 42, ClickHouseParser::RulePartitionClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(391);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(386);
      match(ClickHouseParser::PARTITION);
      setState(387);
      columnExpr(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(388);
      match(ClickHouseParser::PARTITION);
      setState(389);
      match(ClickHouseParser::ID);
      setState(390);
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
  enterRule(_localctx, 44, ClickHouseParser::RuleSelectUnionStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(393);
    selectStmt();
    setState(399);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::UNION) {
      setState(394);
      match(ClickHouseParser::UNION);
      setState(395);
      match(ClickHouseParser::ALL);
      setState(396);
      selectStmt();
      setState(401);
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
  enterRule(_localctx, 46, ClickHouseParser::RuleSelectStmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(403);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(402);
      withClause();
    }
    setState(405);
    match(ClickHouseParser::SELECT);
    setState(407);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
    case 1: {
      setState(406);
      match(ClickHouseParser::DISTINCT);
      break;
    }

    }
    setState(409);
    columnExprList();
    setState(411);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FROM) {
      setState(410);
      fromClause();
    }
    setState(414);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SAMPLE) {
      setState(413);
      sampleClause();
    }
    setState(417);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ARRAY

    || _la == ClickHouseParser::LEFT) {
      setState(416);
      arrayJoinClause();
    }
    setState(420);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::PREWHERE) {
      setState(419);
      prewhereClause();
    }
    setState(423);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WHERE) {
      setState(422);
      whereClause();
    }
    setState(426);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::GROUP) {
      setState(425);
      groupByClause();
    }
    setState(429);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::HAVING) {
      setState(428);
      havingClause();
    }
    setState(432);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ORDER) {
      setState(431);
      orderByClause();
    }
    setState(435);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 55, _ctx)) {
    case 1: {
      setState(434);
      limitByClause();
      break;
    }

    }
    setState(438);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LIMIT) {
      setState(437);
      limitClause();
    }
    setState(441);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::SETTINGS) {
      setState(440);
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
  enterRule(_localctx, 48, ClickHouseParser::RuleWithClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(443);
    match(ClickHouseParser::WITH);
    setState(444);
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
  enterRule(_localctx, 50, ClickHouseParser::RuleFromClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(446);
    match(ClickHouseParser::FROM);
    setState(447);
    joinExpr(0);
    setState(449);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::FINAL) {
      setState(448);
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
  enterRule(_localctx, 52, ClickHouseParser::RuleSampleClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(451);
    match(ClickHouseParser::SAMPLE);
    setState(452);
    ratioExpr();
    setState(455);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET) {
      setState(453);
      match(ClickHouseParser::OFFSET);
      setState(454);
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
  enterRule(_localctx, 54, ClickHouseParser::RuleArrayJoinClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(458);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::LEFT) {
      setState(457);
      match(ClickHouseParser::LEFT);
    }
    setState(460);
    match(ClickHouseParser::ARRAY);
    setState(461);
    match(ClickHouseParser::JOIN);
    setState(462);
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
  enterRule(_localctx, 56, ClickHouseParser::RulePrewhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(464);
    match(ClickHouseParser::PREWHERE);
    setState(465);
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
  enterRule(_localctx, 58, ClickHouseParser::RuleWhereClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(467);
    match(ClickHouseParser::WHERE);
    setState(468);
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
  enterRule(_localctx, 60, ClickHouseParser::RuleGroupByClause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(470);
    match(ClickHouseParser::GROUP);
    setState(471);
    match(ClickHouseParser::BY);
    setState(472);
    columnExprList();
    setState(475);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::WITH) {
      setState(473);
      match(ClickHouseParser::WITH);
      setState(474);
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
  enterRule(_localctx, 62, ClickHouseParser::RuleHavingClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(477);
    match(ClickHouseParser::HAVING);
    setState(478);
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
  enterRule(_localctx, 64, ClickHouseParser::RuleOrderByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(480);
    match(ClickHouseParser::ORDER);
    setState(481);
    match(ClickHouseParser::BY);
    setState(482);
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
  enterRule(_localctx, 66, ClickHouseParser::RuleLimitByClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(484);
    match(ClickHouseParser::LIMIT);
    setState(485);
    limitExpr();
    setState(486);
    match(ClickHouseParser::BY);
    setState(487);
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
  enterRule(_localctx, 68, ClickHouseParser::RuleLimitClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(489);
    match(ClickHouseParser::LIMIT);
    setState(490);
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
  enterRule(_localctx, 70, ClickHouseParser::RuleSettingsClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(492);
    match(ClickHouseParser::SETTINGS);
    setState(493);
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
  size_t startState = 72;
  enterRecursionRule(_localctx, 72, ClickHouseParser::RuleJoinExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(501);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<JoinExprTableContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(496);
      tableExpr(0);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<JoinExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(497);
      match(ClickHouseParser::LPAREN);
      setState(498);
      joinExpr(0);
      setState(499);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(518);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 65, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(516);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 64, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<JoinExprCrossOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(503);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(504);
          joinOpCross();
          setState(505);
          joinExpr(2);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<JoinExprOpContext>(_tracker.createInstance<JoinExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleJoinExpr);
          setState(507);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(509);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::GLOBAL

          || _la == ClickHouseParser::LOCAL) {
            setState(508);
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
          setState(511);
          joinOp();
          setState(512);
          match(ClickHouseParser::JOIN);
          setState(513);
          joinExpr(0);
          setState(514);
          joinConstraintClause();
          break;
        }

        } 
      }
      setState(520);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 65, _ctx);
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
  enterRule(_localctx, 74, ClickHouseParser::RuleJoinOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(551);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<JoinOpContext *>(_tracker.createInstance<ClickHouseParser::JoinOpInnerContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(529);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 68, _ctx)) {
      case 1: {
        setState(522);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(521);
          match(ClickHouseParser::ANY);
        }
        setState(524);
        match(ClickHouseParser::INNER);
        break;
      }

      case 2: {
        setState(525);
        match(ClickHouseParser::INNER);
        setState(527);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY) {
          setState(526);
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
      setState(539);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 71, _ctx)) {
      case 1: {
        setState(532);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(531);
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
        setState(534);
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
        setState(535);
        _la = _input->LA(1);
        if (!(_la == ClickHouseParser::LEFT

        || _la == ClickHouseParser::RIGHT)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(537);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::ANTI)
          | (1ULL << ClickHouseParser::ANY)
          | (1ULL << ClickHouseParser::ASOF))) != 0) || _la == ClickHouseParser::OUTER

        || _la == ClickHouseParser::SEMI) {
          setState(536);
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
      setState(549);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
      case 1: {
        setState(542);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(541);
          _la = _input->LA(1);
          if (!(_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(544);
        match(ClickHouseParser::FULL);
        break;
      }

      case 2: {
        setState(545);
        match(ClickHouseParser::FULL);
        setState(547);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::ANY || _la == ClickHouseParser::OUTER) {
          setState(546);
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
  enterRule(_localctx, 76, ClickHouseParser::RuleJoinOpCross);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(559);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::CROSS:
      case ClickHouseParser::GLOBAL:
      case ClickHouseParser::LOCAL: {
        enterOuterAlt(_localctx, 1);
        setState(554);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::GLOBAL

        || _la == ClickHouseParser::LOCAL) {
          setState(553);
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
        setState(556);
        match(ClickHouseParser::CROSS);
        setState(557);
        match(ClickHouseParser::JOIN);
        break;
      }

      case ClickHouseParser::COMMA: {
        enterOuterAlt(_localctx, 2);
        setState(558);
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
  enterRule(_localctx, 78, ClickHouseParser::RuleJoinConstraintClause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(570);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(561);
      match(ClickHouseParser::ON);
      setState(562);
      columnExprList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(563);
      match(ClickHouseParser::USING);
      setState(564);
      match(ClickHouseParser::LPAREN);
      setState(565);
      columnExprList();
      setState(566);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(568);
      match(ClickHouseParser::USING);
      setState(569);
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
  enterRule(_localctx, 80, ClickHouseParser::RuleLimitExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(572);
    match(ClickHouseParser::INTEGER_LITERAL);
    setState(575);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::OFFSET

    || _la == ClickHouseParser::COMMA) {
      setState(573);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::OFFSET

      || _la == ClickHouseParser::COMMA)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(574);
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
  enterRule(_localctx, 82, ClickHouseParser::RuleOrderExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(577);
    orderExpr();
    setState(582);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(578);
      match(ClickHouseParser::COMMA);
      setState(579);
      orderExpr();
      setState(584);
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
  enterRule(_localctx, 84, ClickHouseParser::RuleOrderExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(585);
    columnExpr(0);
    setState(587);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::ASCENDING

    || _la == ClickHouseParser::DESCENDING) {
      setState(586);
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
    setState(591);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::NULLS) {
      setState(589);
      match(ClickHouseParser::NULLS);
      setState(590);
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
    setState(595);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == ClickHouseParser::COLLATE) {
      setState(593);
      match(ClickHouseParser::COLLATE);
      setState(594);
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

std::vector<tree::TerminalNode *> ClickHouseParser::RatioExprContext::INTEGER_LITERAL() {
  return getTokens(ClickHouseParser::INTEGER_LITERAL);
}

tree::TerminalNode* ClickHouseParser::RatioExprContext::INTEGER_LITERAL(size_t i) {
  return getToken(ClickHouseParser::INTEGER_LITERAL, i);
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
  enterRule(_localctx, 86, ClickHouseParser::RuleRatioExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(597);
    match(ClickHouseParser::INTEGER_LITERAL);

    setState(598);
    match(ClickHouseParser::SLASH);
    setState(599);
    match(ClickHouseParser::INTEGER_LITERAL);
   
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
  enterRule(_localctx, 88, ClickHouseParser::RuleSettingExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(601);
    settingExpr();
    setState(606);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(602);
      match(ClickHouseParser::COMMA);
      setState(603);
      settingExpr();
      setState(608);
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
  enterRule(_localctx, 90, ClickHouseParser::RuleSettingExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(609);
    identifier();
    setState(610);
    match(ClickHouseParser::EQ_SINGLE);
    setState(611);
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
  enterRule(_localctx, 92, ClickHouseParser::RuleSetStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(613);
    match(ClickHouseParser::SET);
    setState(614);
    settingExprList();
   
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
  enterRule(_localctx, 94, ClickHouseParser::RuleUseStmt);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(616);
    match(ClickHouseParser::USE);
    setState(617);
    databaseIdentifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueExprListContext ------------------------------------------------------------------

ClickHouseParser::ValueExprListContext::ValueExprListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ClickHouseParser::ValueExprContext *> ClickHouseParser::ValueExprListContext::valueExpr() {
  return getRuleContexts<ClickHouseParser::ValueExprContext>();
}

ClickHouseParser::ValueExprContext* ClickHouseParser::ValueExprListContext::valueExpr(size_t i) {
  return getRuleContext<ClickHouseParser::ValueExprContext>(i);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ValueExprListContext::COMMA() {
  return getTokens(ClickHouseParser::COMMA);
}

tree::TerminalNode* ClickHouseParser::ValueExprListContext::COMMA(size_t i) {
  return getToken(ClickHouseParser::COMMA, i);
}


size_t ClickHouseParser::ValueExprListContext::getRuleIndex() const {
  return ClickHouseParser::RuleValueExprList;
}


antlrcpp::Any ClickHouseParser::ValueExprListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValueExprList(this);
  else
    return visitor->visitChildren(this);
}

ClickHouseParser::ValueExprListContext* ClickHouseParser::valueExprList() {
  ValueExprListContext *_localctx = _tracker.createInstance<ValueExprListContext>(_ctx, getState());
  enterRule(_localctx, 96, ClickHouseParser::RuleValueExprList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(619);
    valueExpr();
    setState(624);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(620);
      match(ClickHouseParser::COMMA);
      setState(621);
      valueExpr();
      setState(626);
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

//----------------- ValueExprContext ------------------------------------------------------------------

ClickHouseParser::ValueExprContext::ValueExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t ClickHouseParser::ValueExprContext::getRuleIndex() const {
  return ClickHouseParser::RuleValueExpr;
}

void ClickHouseParser::ValueExprContext::copyFrom(ValueExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ValueExprArrayContext ------------------------------------------------------------------

tree::TerminalNode* ClickHouseParser::ValueExprArrayContext::LBRACKET() {
  return getToken(ClickHouseParser::LBRACKET, 0);
}

tree::TerminalNode* ClickHouseParser::ValueExprArrayContext::RBRACKET() {
  return getToken(ClickHouseParser::RBRACKET, 0);
}

ClickHouseParser::ValueExprListContext* ClickHouseParser::ValueExprArrayContext::valueExprList() {
  return getRuleContext<ClickHouseParser::ValueExprListContext>(0);
}

ClickHouseParser::ValueExprArrayContext::ValueExprArrayContext(ValueExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ValueExprArrayContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValueExprArray(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ValueExprTupleContext ------------------------------------------------------------------

ClickHouseParser::ValueTupleExprContext* ClickHouseParser::ValueExprTupleContext::valueTupleExpr() {
  return getRuleContext<ClickHouseParser::ValueTupleExprContext>(0);
}

ClickHouseParser::ValueExprTupleContext::ValueExprTupleContext(ValueExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ValueExprTupleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValueExprTuple(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ValueExprLiteralContext ------------------------------------------------------------------

ClickHouseParser::LiteralContext* ClickHouseParser::ValueExprLiteralContext::literal() {
  return getRuleContext<ClickHouseParser::LiteralContext>(0);
}

ClickHouseParser::ValueExprLiteralContext::ValueExprLiteralContext(ValueExprContext *ctx) { copyFrom(ctx); }


antlrcpp::Any ClickHouseParser::ValueExprLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<ClickHouseParserVisitor*>(visitor))
    return parserVisitor->visitValueExprLiteral(this);
  else
    return visitor->visitChildren(this);
}
ClickHouseParser::ValueExprContext* ClickHouseParser::valueExpr() {
  ValueExprContext *_localctx = _tracker.createInstance<ValueExprContext>(_ctx, getState());
  enterRule(_localctx, 98, ClickHouseParser::RuleValueExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(634);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::ALIAS:
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
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESCENDING:
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
      case ClickHouseParser::INF:
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
      case ClickHouseParser::MONTH:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::NOT:
      case ClickHouseParser::NULL_SQL:
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
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::INTEGER_LITERAL:
      case ClickHouseParser::STRING_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::PLUS: {
        _localctx = dynamic_cast<ValueExprContext *>(_tracker.createInstance<ClickHouseParser::ValueExprLiteralContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(627);
        literal();
        break;
      }

      case ClickHouseParser::LPAREN: {
        _localctx = dynamic_cast<ValueExprContext *>(_tracker.createInstance<ClickHouseParser::ValueExprTupleContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(628);
        valueTupleExpr();
        break;
      }

      case ClickHouseParser::LBRACKET: {
        _localctx = dynamic_cast<ValueExprContext *>(_tracker.createInstance<ClickHouseParser::ValueExprArrayContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(629);
        match(ClickHouseParser::LBRACKET);
        setState(631);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
          | (1ULL << ClickHouseParser::ALIAS)
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
          | (1ULL << ClickHouseParser::CREATE)
          | (1ULL << ClickHouseParser::CROSS)
          | (1ULL << ClickHouseParser::DATABASE)
          | (1ULL << ClickHouseParser::DAY)
          | (1ULL << ClickHouseParser::DEDUPLICATE)
          | (1ULL << ClickHouseParser::DEFAULT)
          | (1ULL << ClickHouseParser::DELETE)
          | (1ULL << ClickHouseParser::DESCENDING)
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
          | (1ULL << ClickHouseParser::IS)
          | (1ULL << ClickHouseParser::JOIN)
          | (1ULL << ClickHouseParser::KEY)
          | (1ULL << ClickHouseParser::LAST)
          | (1ULL << ClickHouseParser::LEADING)
          | (1ULL << ClickHouseParser::LEFT)
          | (1ULL << ClickHouseParser::LIKE)
          | (1ULL << ClickHouseParser::LIMIT)
          | (1ULL << ClickHouseParser::LOCAL)
          | (1ULL << ClickHouseParser::MATERIALIZED)
          | (1ULL << ClickHouseParser::MINUTE)
          | (1ULL << ClickHouseParser::MONTH)
          | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
          | (1ULL << (ClickHouseParser::RIGHT - 64))
          | (1ULL << (ClickHouseParser::SAMPLE - 64))
          | (1ULL << (ClickHouseParser::SECOND - 64))
          | (1ULL << (ClickHouseParser::SEMI - 64))
          | (1ULL << (ClickHouseParser::SET - 64))
          | (1ULL << (ClickHouseParser::SETTINGS - 64))
          | (1ULL << (ClickHouseParser::TABLE - 64))
          | (1ULL << (ClickHouseParser::TEMPORARY - 64))
          | (1ULL << (ClickHouseParser::THEN - 64))
          | (1ULL << (ClickHouseParser::TO - 64))
          | (1ULL << (ClickHouseParser::TOTALS - 64))
          | (1ULL << (ClickHouseParser::TRAILING - 64))
          | (1ULL << (ClickHouseParser::TRIM - 64))
          | (1ULL << (ClickHouseParser::TTL - 64))
          | (1ULL << (ClickHouseParser::UNION - 64))
          | (1ULL << (ClickHouseParser::USE - 64))
          | (1ULL << (ClickHouseParser::VALUES - 64))
          | (1ULL << (ClickHouseParser::VOLUME - 64))
          | (1ULL << (ClickHouseParser::WEEK - 64))
          | (1ULL << (ClickHouseParser::WHEN - 64))
          | (1ULL << (ClickHouseParser::WITH - 64))
          | (1ULL << (ClickHouseParser::YEAR - 64))
          | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
          | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
          | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
          | (1ULL << (ClickHouseParser::DASH - 64))
          | (1ULL << (ClickHouseParser::LBRACKET - 64))
          | (1ULL << (ClickHouseParser::LPAREN - 64))
          | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
          setState(630);
          valueExprList();
        }
        setState(633);
        match(ClickHouseParser::RBRACKET);
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

ClickHouseParser::ColumnParamListContext* ClickHouseParser::ColumnTypeExprParamContext::columnParamList() {
  return getRuleContext<ClickHouseParser::ColumnParamListContext>(0);
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
  enterRule(_localctx, 100, ClickHouseParser::RuleColumnTypeExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(681);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprSimpleContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(636);
      identifier();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprParamContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(637);
      identifier();
      setState(638);
      match(ClickHouseParser::LPAREN);
      setState(639);
      columnParamList();
      setState(640);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprEnumContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(642);
      identifier();
      setState(643);
      match(ClickHouseParser::LPAREN);
      setState(644);
      enumValue();
      setState(649);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(645);
        match(ClickHouseParser::COMMA);
        setState(646);
        enumValue();
        setState(651);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(652);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprComplexContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(654);
      identifier();
      setState(655);
      match(ClickHouseParser::LPAREN);
      setState(656);
      columnTypeExpr();
      setState(661);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(657);
        match(ClickHouseParser::COMMA);
        setState(658);
        columnTypeExpr();
        setState(663);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(664);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ColumnTypeExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnTypeExprNestedContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(666);
      identifier();
      setState(667);
      match(ClickHouseParser::LPAREN);
      setState(668);
      identifier();
      setState(669);
      columnTypeExpr();
      setState(676);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == ClickHouseParser::COMMA) {
        setState(670);
        match(ClickHouseParser::COMMA);
        setState(671);
        identifier();
        setState(672);
        columnTypeExpr();
        setState(678);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(679);
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
  enterRule(_localctx, 102, ClickHouseParser::RuleColumnExprList);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(683);
    columnsExpr();
    setState(688);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 92, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(684);
        match(ClickHouseParser::COMMA);
        setState(685);
        columnsExpr(); 
      }
      setState(690);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 92, _ctx);
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
  enterRule(_localctx, 104, ClickHouseParser::RuleColumnsExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(702);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 94, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprAsteriskContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(694);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64)))) != 0)) {
        setState(691);
        tableIdentifier();
        setState(692);
        match(ClickHouseParser::DOT);
      }
      setState(696);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprSubqueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(697);
      match(ClickHouseParser::LPAREN);
      setState(698);
      selectUnionStmt();
      setState(699);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ColumnsExprContext *>(_tracker.createInstance<ClickHouseParser::ColumnsExprColumnContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(701);
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
  size_t startState = 106;
  enterRecursionRule(_localctx, 106, ClickHouseParser::RuleColumnExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(785);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 103, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ColumnExprLiteralContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(705);
      literal();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ColumnExprAsteriskContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(709);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64)))) != 0)) {
        setState(706);
        tableIdentifier();
        setState(707);
        match(ClickHouseParser::DOT);
      }
      setState(711);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<ColumnExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(712);
      match(ClickHouseParser::LPAREN);
      setState(713);
      selectUnionStmt();
      setState(714);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<ColumnExprParensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(716);
      match(ClickHouseParser::LPAREN);
      setState(717);
      columnExpr(0);
      setState(718);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<ColumnExprTupleContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(720);
      match(ClickHouseParser::LPAREN);
      setState(721);
      columnExprList();
      setState(722);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ColumnExprArrayContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(724);
      match(ClickHouseParser::LBRACKET);
      setState(726);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::ASTERISK - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::LBRACKET - 64))
        | (1ULL << (ClickHouseParser::LPAREN - 64))
        | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
        setState(725);
        columnExprList();
      }
      setState(728);
      match(ClickHouseParser::RBRACKET);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ColumnExprCaseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(729);
      match(ClickHouseParser::CASE);
      setState(731);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 97, _ctx)) {
      case 1: {
        setState(730);
        columnExpr(0);
        break;
      }

      }
      setState(738); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(733);
        match(ClickHouseParser::WHEN);
        setState(734);
        columnExpr(0);
        setState(735);
        match(ClickHouseParser::THEN);
        setState(736);
        columnExpr(0);
        setState(740); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == ClickHouseParser::WHEN);
      setState(744);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::ELSE) {
        setState(742);
        match(ClickHouseParser::ELSE);
        setState(743);
        columnExpr(0);
      }
      setState(746);
      match(ClickHouseParser::END);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnExprExtractContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(748);
      match(ClickHouseParser::EXTRACT);
      setState(749);
      match(ClickHouseParser::LPAREN);
      setState(750);
      match(ClickHouseParser::INTERVAL_TYPE);
      setState(751);
      match(ClickHouseParser::FROM);
      setState(752);
      columnExpr(0);
      setState(753);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<ColumnExprTrimContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(755);
      match(ClickHouseParser::TRIM);
      setState(756);
      match(ClickHouseParser::LPAREN);
      setState(757);
      _la = _input->LA(1);
      if (!(_la == ClickHouseParser::BOTH

      || _la == ClickHouseParser::LEADING || _la == ClickHouseParser::TRAILING)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(758);
      match(ClickHouseParser::STRING_LITERAL);
      setState(759);
      match(ClickHouseParser::FROM);
      setState(760);
      columnExpr(0);
      setState(761);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<ColumnExprIntervalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(763);
      match(ClickHouseParser::INTERVAL);
      setState(764);
      columnExpr(0);
      setState(765);
      match(ClickHouseParser::INTERVAL_TYPE);
      break;
    }

    case 11: {
      _localctx = _tracker.createInstance<ColumnExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(767);
      columnIdentifier();
      break;
    }

    case 12: {
      _localctx = _tracker.createInstance<ColumnExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(768);
      identifier();
      setState(774);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx)) {
      case 1: {
        setState(769);
        match(ClickHouseParser::LPAREN);
        setState(771);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
          | (1ULL << ClickHouseParser::ALIAS)
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
          | (1ULL << ClickHouseParser::CREATE)
          | (1ULL << ClickHouseParser::CROSS)
          | (1ULL << ClickHouseParser::DATABASE)
          | (1ULL << ClickHouseParser::DAY)
          | (1ULL << ClickHouseParser::DEDUPLICATE)
          | (1ULL << ClickHouseParser::DEFAULT)
          | (1ULL << ClickHouseParser::DELETE)
          | (1ULL << ClickHouseParser::DESCENDING)
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
          | (1ULL << ClickHouseParser::IS)
          | (1ULL << ClickHouseParser::JOIN)
          | (1ULL << ClickHouseParser::KEY)
          | (1ULL << ClickHouseParser::LAST)
          | (1ULL << ClickHouseParser::LEADING)
          | (1ULL << ClickHouseParser::LEFT)
          | (1ULL << ClickHouseParser::LIKE)
          | (1ULL << ClickHouseParser::LIMIT)
          | (1ULL << ClickHouseParser::LOCAL)
          | (1ULL << ClickHouseParser::MATERIALIZED)
          | (1ULL << ClickHouseParser::MINUTE)
          | (1ULL << ClickHouseParser::MONTH)
          | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
          | (1ULL << (ClickHouseParser::RIGHT - 64))
          | (1ULL << (ClickHouseParser::SAMPLE - 64))
          | (1ULL << (ClickHouseParser::SECOND - 64))
          | (1ULL << (ClickHouseParser::SEMI - 64))
          | (1ULL << (ClickHouseParser::SET - 64))
          | (1ULL << (ClickHouseParser::SETTINGS - 64))
          | (1ULL << (ClickHouseParser::TABLE - 64))
          | (1ULL << (ClickHouseParser::TEMPORARY - 64))
          | (1ULL << (ClickHouseParser::THEN - 64))
          | (1ULL << (ClickHouseParser::TO - 64))
          | (1ULL << (ClickHouseParser::TOTALS - 64))
          | (1ULL << (ClickHouseParser::TRAILING - 64))
          | (1ULL << (ClickHouseParser::TRIM - 64))
          | (1ULL << (ClickHouseParser::TTL - 64))
          | (1ULL << (ClickHouseParser::UNION - 64))
          | (1ULL << (ClickHouseParser::USE - 64))
          | (1ULL << (ClickHouseParser::VALUES - 64))
          | (1ULL << (ClickHouseParser::VOLUME - 64))
          | (1ULL << (ClickHouseParser::WEEK - 64))
          | (1ULL << (ClickHouseParser::WHEN - 64))
          | (1ULL << (ClickHouseParser::WITH - 64))
          | (1ULL << (ClickHouseParser::YEAR - 64))
          | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
          | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
          | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
          | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
          | (1ULL << (ClickHouseParser::DASH - 64))
          | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
          setState(770);
          columnParamList();
        }
        setState(773);
        match(ClickHouseParser::RPAREN);
        break;
      }

      }
      setState(776);
      match(ClickHouseParser::LPAREN);
      setState(778);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::ASTERISK - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::LBRACKET - 64))
        | (1ULL << (ClickHouseParser::LPAREN - 64))
        | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
        setState(777);
        columnArgList();
      }
      setState(780);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 13: {
      _localctx = _tracker.createInstance<ColumnExprUnaryOpContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(782);
      unaryOp();
      setState(783);
      columnExpr(6);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(825);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 107, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(823);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 106, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ColumnExprBinaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(787);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(788);
          binaryOp();
          setState(789);
          columnExpr(5);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ColumnExprTernaryOpContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(791);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(792);
          match(ClickHouseParser::QUERY);
          setState(793);
          columnExpr(0);
          setState(794);
          match(ClickHouseParser::COLON);
          setState(795);
          columnExpr(4);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ColumnExprBetweenContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(797);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(799);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(798);
            match(ClickHouseParser::NOT);
          }
          setState(801);
          match(ClickHouseParser::BETWEEN);
          setState(802);
          columnExpr(0);
          setState(803);
          match(ClickHouseParser::AND);
          setState(804);
          columnExpr(3);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ColumnExprArrayAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(806);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(807);
          match(ClickHouseParser::LBRACKET);
          setState(808);
          columnExpr(0);
          setState(809);
          match(ClickHouseParser::RBRACKET);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ColumnExprTupleAccessContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(811);

          if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
          setState(812);
          match(ClickHouseParser::DOT);
          setState(813);
          match(ClickHouseParser::INTEGER_LITERAL);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ColumnExprIsNullContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(814);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(815);
          match(ClickHouseParser::IS);
          setState(817);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == ClickHouseParser::NOT) {
            setState(816);
            match(ClickHouseParser::NOT);
          }
          setState(819);
          match(ClickHouseParser::NULL_SQL);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<ColumnExprAliasContext>(_tracker.createInstance<ColumnExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleColumnExpr);
          setState(820);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(821);
          match(ClickHouseParser::AS);
          setState(822);
          identifier();
          break;
        }

        } 
      }
      setState(827);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 107, _ctx);
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
  enterRule(_localctx, 108, ClickHouseParser::RuleColumnParamList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(828);
    literal();
    setState(833);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(829);
      match(ClickHouseParser::COMMA);
      setState(830);
      literal();
      setState(835);
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
  enterRule(_localctx, 110, ClickHouseParser::RuleColumnArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(836);
    columnArgExpr();
    setState(841);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(837);
      match(ClickHouseParser::COMMA);
      setState(838);
      columnArgExpr();
      setState(843);
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
  enterRule(_localctx, 112, ClickHouseParser::RuleColumnArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(846);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 110, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(844);
      columnLambdaExpr();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(845);
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
  enterRule(_localctx, 114, ClickHouseParser::RuleColumnLambdaExpr);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(867);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::LPAREN: {
        setState(848);
        match(ClickHouseParser::LPAREN);
        setState(849);
        identifier();
        setState(854);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(850);
          match(ClickHouseParser::COMMA);
          setState(851);
          identifier();
          setState(856);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(857);
        match(ClickHouseParser::RPAREN);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::ALIAS:
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
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESCENDING:
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
      case ClickHouseParser::MONTH:
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
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        setState(859);
        identifier();
        setState(864);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == ClickHouseParser::COMMA) {
          setState(860);
          match(ClickHouseParser::COMMA);
          setState(861);
          identifier();
          setState(866);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(869);
    match(ClickHouseParser::ARROW);
    setState(870);
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

std::vector<ClickHouseParser::IdentifierContext *> ClickHouseParser::ColumnIdentifierContext::identifier() {
  return getRuleContexts<ClickHouseParser::IdentifierContext>();
}

ClickHouseParser::IdentifierContext* ClickHouseParser::ColumnIdentifierContext::identifier(size_t i) {
  return getRuleContext<ClickHouseParser::IdentifierContext>(i);
}

ClickHouseParser::TableIdentifierContext* ClickHouseParser::ColumnIdentifierContext::tableIdentifier() {
  return getRuleContext<ClickHouseParser::TableIdentifierContext>(0);
}

std::vector<tree::TerminalNode *> ClickHouseParser::ColumnIdentifierContext::DOT() {
  return getTokens(ClickHouseParser::DOT);
}

tree::TerminalNode* ClickHouseParser::ColumnIdentifierContext::DOT(size_t i) {
  return getToken(ClickHouseParser::DOT, i);
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
  enterRule(_localctx, 116, ClickHouseParser::RuleColumnIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(875);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 114, _ctx)) {
    case 1: {
      setState(872);
      tableIdentifier();
      setState(873);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(877);
    identifier();
    setState(880);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx)) {
    case 1: {
      setState(878);
      match(ClickHouseParser::DOT);
      setState(879);
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
  size_t startState = 118;
  enterRecursionRule(_localctx, 118, ClickHouseParser::RuleTableExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(895);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 117, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<TableExprIdentifierContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(883);
      tableIdentifier();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<TableExprFunctionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(884);
      identifier();
      setState(885);
      match(ClickHouseParser::LPAREN);
      setState(887);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << ClickHouseParser::INTERVAL_TYPE)
        | (1ULL << ClickHouseParser::ALIAS)
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
        | (1ULL << ClickHouseParser::CREATE)
        | (1ULL << ClickHouseParser::CROSS)
        | (1ULL << ClickHouseParser::DATABASE)
        | (1ULL << ClickHouseParser::DAY)
        | (1ULL << ClickHouseParser::DEDUPLICATE)
        | (1ULL << ClickHouseParser::DEFAULT)
        | (1ULL << ClickHouseParser::DELETE)
        | (1ULL << ClickHouseParser::DESCENDING)
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
        | (1ULL << ClickHouseParser::IS)
        | (1ULL << ClickHouseParser::JOIN)
        | (1ULL << ClickHouseParser::KEY)
        | (1ULL << ClickHouseParser::LAST)
        | (1ULL << ClickHouseParser::LEADING)
        | (1ULL << ClickHouseParser::LEFT)
        | (1ULL << ClickHouseParser::LIKE)
        | (1ULL << ClickHouseParser::LIMIT)
        | (1ULL << ClickHouseParser::LOCAL)
        | (1ULL << ClickHouseParser::MATERIALIZED)
        | (1ULL << ClickHouseParser::MINUTE)
        | (1ULL << ClickHouseParser::MONTH)
        | (1ULL << ClickHouseParser::NAN_SQL))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
        | (1ULL << (ClickHouseParser::RIGHT - 64))
        | (1ULL << (ClickHouseParser::SAMPLE - 64))
        | (1ULL << (ClickHouseParser::SECOND - 64))
        | (1ULL << (ClickHouseParser::SEMI - 64))
        | (1ULL << (ClickHouseParser::SET - 64))
        | (1ULL << (ClickHouseParser::SETTINGS - 64))
        | (1ULL << (ClickHouseParser::TABLE - 64))
        | (1ULL << (ClickHouseParser::TEMPORARY - 64))
        | (1ULL << (ClickHouseParser::THEN - 64))
        | (1ULL << (ClickHouseParser::TO - 64))
        | (1ULL << (ClickHouseParser::TOTALS - 64))
        | (1ULL << (ClickHouseParser::TRAILING - 64))
        | (1ULL << (ClickHouseParser::TRIM - 64))
        | (1ULL << (ClickHouseParser::TTL - 64))
        | (1ULL << (ClickHouseParser::UNION - 64))
        | (1ULL << (ClickHouseParser::USE - 64))
        | (1ULL << (ClickHouseParser::VALUES - 64))
        | (1ULL << (ClickHouseParser::VOLUME - 64))
        | (1ULL << (ClickHouseParser::WEEK - 64))
        | (1ULL << (ClickHouseParser::WHEN - 64))
        | (1ULL << (ClickHouseParser::WITH - 64))
        | (1ULL << (ClickHouseParser::YEAR - 64))
        | (1ULL << (ClickHouseParser::IDENTIFIER - 64))
        | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 64))
        | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 64))
        | (1ULL << (ClickHouseParser::STRING_LITERAL - 64))
        | (1ULL << (ClickHouseParser::DASH - 64))
        | (1ULL << (ClickHouseParser::PLUS - 64)))) != 0)) {
        setState(886);
        tableArgList();
      }
      setState(889);
      match(ClickHouseParser::RPAREN);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<TableExprSubqueryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(891);
      match(ClickHouseParser::LPAREN);
      setState(892);
      selectUnionStmt();
      setState(893);
      match(ClickHouseParser::RPAREN);
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(904);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 119, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<TableExprAliasContext>(_tracker.createInstance<TableExprContext>(parentContext, parentState));
        _localctx = newContext;
        pushNewRecursionContext(newContext, startState, RuleTableExpr);
        setState(897);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(899);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 118, _ctx)) {
        case 1: {
          setState(898);
          match(ClickHouseParser::AS);
          break;
        }

        }
        setState(901);
        identifier(); 
      }
      setState(906);
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
  enterRule(_localctx, 120, ClickHouseParser::RuleTableIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(910);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 120, _ctx)) {
    case 1: {
      setState(907);
      databaseIdentifier();
      setState(908);
      match(ClickHouseParser::DOT);
      break;
    }

    }
    setState(912);
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
  enterRule(_localctx, 122, ClickHouseParser::RuleTableArgList);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(914);
    tableArgExpr();
    setState(919);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == ClickHouseParser::COMMA) {
      setState(915);
      match(ClickHouseParser::COMMA);
      setState(916);
      tableArgExpr();
      setState(921);
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
  enterRule(_localctx, 124, ClickHouseParser::RuleTableArgExpr);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(924);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 122, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(922);
      literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(923);
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
  enterRule(_localctx, 126, ClickHouseParser::RuleDatabaseIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(926);
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

tree::TerminalNode* ClickHouseParser::LiteralContext::FLOATING_LITERAL() {
  return getToken(ClickHouseParser::FLOATING_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::HEXADECIMAL_LITERAL() {
  return getToken(ClickHouseParser::HEXADECIMAL_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::INTEGER_LITERAL() {
  return getToken(ClickHouseParser::INTEGER_LITERAL, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::INF() {
  return getToken(ClickHouseParser::INF, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::NAN_SQL() {
  return getToken(ClickHouseParser::NAN_SQL, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::PLUS() {
  return getToken(ClickHouseParser::PLUS, 0);
}

tree::TerminalNode* ClickHouseParser::LiteralContext::DASH() {
  return getToken(ClickHouseParser::DASH, 0);
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
  enterRule(_localctx, 128, ClickHouseParser::RuleLiteral);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(938);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::INF:
      case ClickHouseParser::NAN_SQL:
      case ClickHouseParser::FLOATING_LITERAL:
      case ClickHouseParser::HEXADECIMAL_LITERAL:
      case ClickHouseParser::INTEGER_LITERAL:
      case ClickHouseParser::DASH:
      case ClickHouseParser::PLUS: {
        enterOuterAlt(_localctx, 1);
        setState(929);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == ClickHouseParser::DASH

        || _la == ClickHouseParser::PLUS) {
          setState(928);
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
        setState(931);
        _la = _input->LA(1);
        if (!(((((_la - 46) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 46)) & ((1ULL << (ClickHouseParser::INF - 46))
          | (1ULL << (ClickHouseParser::NAN_SQL - 46))
          | (1ULL << (ClickHouseParser::FLOATING_LITERAL - 46))
          | (1ULL << (ClickHouseParser::HEXADECIMAL_LITERAL - 46))
          | (1ULL << (ClickHouseParser::INTEGER_LITERAL - 46)))) != 0))) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case ClickHouseParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 2);
        setState(932);
        match(ClickHouseParser::STRING_LITERAL);
        break;
      }

      case ClickHouseParser::NULL_SQL: {
        enterOuterAlt(_localctx, 3);
        setState(933);
        match(ClickHouseParser::NULL_SQL);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE:
      case ClickHouseParser::ALIAS:
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
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESCENDING:
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
      case ClickHouseParser::MONTH:
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
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR:
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(934);
        identifier();
        setState(935);
        match(ClickHouseParser::LPAREN);
        setState(936);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::ALIAS() {
  return getToken(ClickHouseParser::ALIAS, 0);
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

tree::TerminalNode* ClickHouseParser::KeywordContext::DELETE() {
  return getToken(ClickHouseParser::DELETE, 0);
}

tree::TerminalNode* ClickHouseParser::KeywordContext::DESCENDING() {
  return getToken(ClickHouseParser::DESCENDING, 0);
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
  enterRule(_localctx, 130, ClickHouseParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(940);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << ClickHouseParser::ALIAS)
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
      | (1ULL << ClickHouseParser::CREATE)
      | (1ULL << ClickHouseParser::CROSS)
      | (1ULL << ClickHouseParser::DATABASE)
      | (1ULL << ClickHouseParser::DAY)
      | (1ULL << ClickHouseParser::DEDUPLICATE)
      | (1ULL << ClickHouseParser::DEFAULT)
      | (1ULL << ClickHouseParser::DELETE)
      | (1ULL << ClickHouseParser::DESCENDING)
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
      | (1ULL << ClickHouseParser::IS)
      | (1ULL << ClickHouseParser::JOIN)
      | (1ULL << ClickHouseParser::KEY)
      | (1ULL << ClickHouseParser::LAST)
      | (1ULL << ClickHouseParser::LEADING)
      | (1ULL << ClickHouseParser::LEFT)
      | (1ULL << ClickHouseParser::LIKE)
      | (1ULL << ClickHouseParser::LIMIT)
      | (1ULL << ClickHouseParser::LOCAL)
      | (1ULL << ClickHouseParser::MATERIALIZED)
      | (1ULL << ClickHouseParser::MINUTE)
      | (1ULL << ClickHouseParser::MONTH))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (ClickHouseParser::NOT - 64))
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
      | (1ULL << (ClickHouseParser::RIGHT - 64))
      | (1ULL << (ClickHouseParser::SAMPLE - 64))
      | (1ULL << (ClickHouseParser::SECOND - 64))
      | (1ULL << (ClickHouseParser::SEMI - 64))
      | (1ULL << (ClickHouseParser::SET - 64))
      | (1ULL << (ClickHouseParser::SETTINGS - 64))
      | (1ULL << (ClickHouseParser::TABLE - 64))
      | (1ULL << (ClickHouseParser::TEMPORARY - 64))
      | (1ULL << (ClickHouseParser::THEN - 64))
      | (1ULL << (ClickHouseParser::TO - 64))
      | (1ULL << (ClickHouseParser::TOTALS - 64))
      | (1ULL << (ClickHouseParser::TRAILING - 64))
      | (1ULL << (ClickHouseParser::TRIM - 64))
      | (1ULL << (ClickHouseParser::TTL - 64))
      | (1ULL << (ClickHouseParser::UNION - 64))
      | (1ULL << (ClickHouseParser::USE - 64))
      | (1ULL << (ClickHouseParser::VALUES - 64))
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
  enterRule(_localctx, 132, ClickHouseParser::RuleIdentifier);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(945);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ClickHouseParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(942);
        match(ClickHouseParser::IDENTIFIER);
        break;
      }

      case ClickHouseParser::INTERVAL_TYPE: {
        enterOuterAlt(_localctx, 2);
        setState(943);
        match(ClickHouseParser::INTERVAL_TYPE);
        break;
      }

      case ClickHouseParser::ALIAS:
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
      case ClickHouseParser::CREATE:
      case ClickHouseParser::CROSS:
      case ClickHouseParser::DATABASE:
      case ClickHouseParser::DAY:
      case ClickHouseParser::DEDUPLICATE:
      case ClickHouseParser::DEFAULT:
      case ClickHouseParser::DELETE:
      case ClickHouseParser::DESCENDING:
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
      case ClickHouseParser::MONTH:
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
      case ClickHouseParser::RIGHT:
      case ClickHouseParser::SAMPLE:
      case ClickHouseParser::SECOND:
      case ClickHouseParser::SEMI:
      case ClickHouseParser::SET:
      case ClickHouseParser::SETTINGS:
      case ClickHouseParser::TABLE:
      case ClickHouseParser::TEMPORARY:
      case ClickHouseParser::THEN:
      case ClickHouseParser::TO:
      case ClickHouseParser::TOTALS:
      case ClickHouseParser::TRAILING:
      case ClickHouseParser::TRIM:
      case ClickHouseParser::TTL:
      case ClickHouseParser::UNION:
      case ClickHouseParser::USE:
      case ClickHouseParser::VALUES:
      case ClickHouseParser::VOLUME:
      case ClickHouseParser::WEEK:
      case ClickHouseParser::WHEN:
      case ClickHouseParser::WITH:
      case ClickHouseParser::YEAR: {
        enterOuterAlt(_localctx, 3);
        setState(944);
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
  enterRule(_localctx, 134, ClickHouseParser::RuleUnaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(947);
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
  enterRule(_localctx, 136, ClickHouseParser::RuleBinaryOp);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(975);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 129, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(949);
      match(ClickHouseParser::CONCAT);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(950);
      match(ClickHouseParser::ASTERISK);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(951);
      match(ClickHouseParser::SLASH);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(952);
      match(ClickHouseParser::PLUS);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(953);
      match(ClickHouseParser::DASH);
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(954);
      match(ClickHouseParser::PERCENT);
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(955);
      match(ClickHouseParser::EQ_DOUBLE);
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(956);
      match(ClickHouseParser::EQ_SINGLE);
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(957);
      match(ClickHouseParser::NOT_EQ);
      break;
    }

    case 10: {
      enterOuterAlt(_localctx, 10);
      setState(958);
      match(ClickHouseParser::LE);
      break;
    }

    case 11: {
      enterOuterAlt(_localctx, 11);
      setState(959);
      match(ClickHouseParser::GE);
      break;
    }

    case 12: {
      enterOuterAlt(_localctx, 12);
      setState(960);
      match(ClickHouseParser::LT);
      break;
    }

    case 13: {
      enterOuterAlt(_localctx, 13);
      setState(961);
      match(ClickHouseParser::GT);
      break;
    }

    case 14: {
      enterOuterAlt(_localctx, 14);
      setState(962);
      match(ClickHouseParser::AND);
      break;
    }

    case 15: {
      enterOuterAlt(_localctx, 15);
      setState(963);
      match(ClickHouseParser::OR);
      break;
    }

    case 16: {
      enterOuterAlt(_localctx, 16);
      setState(965);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(964);
        match(ClickHouseParser::NOT);
      }
      setState(967);
      match(ClickHouseParser::LIKE);
      break;
    }

    case 17: {
      enterOuterAlt(_localctx, 17);
      setState(969);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::GLOBAL) {
        setState(968);
        match(ClickHouseParser::GLOBAL);
      }
      setState(972);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == ClickHouseParser::NOT) {
        setState(971);
        match(ClickHouseParser::NOT);
      }
      setState(974);
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

tree::TerminalNode* ClickHouseParser::EnumValueContext::INTEGER_LITERAL() {
  return getToken(ClickHouseParser::INTEGER_LITERAL, 0);
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
  enterRule(_localctx, 138, ClickHouseParser::RuleEnumValue);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(977);
    match(ClickHouseParser::STRING_LITERAL);
    setState(978);
    match(ClickHouseParser::EQ_SINGLE);
    setState(979);
    match(ClickHouseParser::INTEGER_LITERAL);
   
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
    case 36: return joinExprSempred(dynamic_cast<JoinExprContext *>(context), predicateIndex);
    case 53: return columnExprSempred(dynamic_cast<ColumnExprContext *>(context), predicateIndex);
    case 59: return tableExprSempred(dynamic_cast<TableExprContext *>(context), predicateIndex);

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
  "queryList", "queryStmt", "query", "distributedStmt", "createDatabaseStmt", 
  "createTableStmt", "schemaClause", "engineClause", "partitionByClause", 
  "primaryKeyClause", "sampleByClause", "ttlClause", "engineExpr", "tableElementExpr", 
  "tableColumnPropertyExpr", "ttlExpr", "dropStmt", "insertStmt", "valuesClause", 
  "valueTupleExpr", "optimizeStmt", "partitionClause", "selectUnionStmt", 
  "selectStmt", "withClause", "fromClause", "sampleClause", "arrayJoinClause", 
  "prewhereClause", "whereClause", "groupByClause", "havingClause", "orderByClause", 
  "limitByClause", "limitClause", "settingsClause", "joinExpr", "joinOp", 
  "joinOpCross", "joinConstraintClause", "limitExpr", "orderExprList", "orderExpr", 
  "ratioExpr", "settingExprList", "settingExpr", "setStmt", "useStmt", "valueExprList", 
  "valueExpr", "columnTypeExpr", "columnExprList", "columnsExpr", "columnExpr", 
  "columnParamList", "columnArgList", "columnArgExpr", "columnLambdaExpr", 
  "columnIdentifier", "tableExpr", "tableIdentifier", "tableArgList", "tableArgExpr", 
  "databaseIdentifier", "literal", "keyword", "identifier", "unaryOp", "binaryOp", 
  "enumValue"
};

std::vector<std::string> ClickHouseParser::_literalNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "'->'", "'*'", "'`'", "'\\'", "':'", "','", "'||'", "'-'", "'.'", "'=='", 
  "'='", "'>='", "'>'", "'['", "'<='", "'('", "'<'", "", "'%'", "'+'", "'?'", 
  "'''", "']'", "')'", "';'", "'/'", "'_'"
};

std::vector<std::string> ClickHouseParser::_symbolicNames = {
  "", "INTERVAL_TYPE", "ALIAS", "ALL", "AND", "ANTI", "ANY", "ARRAY", "AS", 
  "ASCENDING", "ASOF", "BETWEEN", "BOTH", "BY", "CASE", "CAST", "CLUSTER", 
  "COLLATE", "CREATE", "CROSS", "DATABASE", "DAY", "DEDUPLICATE", "DEFAULT", 
  "DELETE", "DESCENDING", "DISK", "DISTINCT", "DROP", "ELSE", "END", "ENGINE", 
  "EXISTS", "EXTRACT", "FINAL", "FIRST", "FORMAT", "FROM", "FULL", "GLOBAL", 
  "GROUP", "HAVING", "HOUR", "ID", "IF", "IN", "INF", "INNER", "INSERT", 
  "INTERVAL", "INTO", "IS", "JOIN", "KEY", "LAST", "LEADING", "LEFT", "LIKE", 
  "LIMIT", "LOCAL", "MATERIALIZED", "MINUTE", "MONTH", "NAN_SQL", "NOT", 
  "NULL_SQL", "NULLS", "OFFSET", "ON", "OPTIMIZE", "OR", "ORDER", "OUTER", 
  "OUTFILE", "PARTITION", "PREWHERE", "PRIMARY", "QUARTER", "RIGHT", "SAMPLE", 
  "SECOND", "SELECT", "SEMI", "SET", "SETTINGS", "TABLE", "TEMPORARY", "THEN", 
  "TO", "TOTALS", "TRAILING", "TRIM", "TTL", "UNION", "USE", "USING", "VALUES", 
  "VOLUME", "WEEK", "WHEN", "WHERE", "WITH", "YEAR", "IDENTIFIER", "FLOATING_LITERAL", 
  "HEXADECIMAL_LITERAL", "INTEGER_LITERAL", "STRING_LITERAL", "ARROW", "ASTERISK", 
  "BACKQUOTE", "BACKSLASH", "COLON", "COMMA", "CONCAT", "DASH", "DOT", "EQ_DOUBLE", 
  "EQ_SINGLE", "GE", "GT", "LBRACKET", "LE", "LPAREN", "LT", "NOT_EQ", "PERCENT", 
  "PLUS", "QUERY", "QUOTE_SINGLE", "RBRACKET", "RPAREN", "SEMICOLON", "SLASH", 
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
    0x3, 0x8b, 0x3d8, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
    0x46, 0x9, 0x46, 0x4, 0x47, 0x9, 0x47, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 
    0x7, 0x2, 0x92, 0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 0x95, 0xb, 0x2, 0x3, 0x2, 
    0x5, 0x2, 0x98, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x5, 0x3, 0xa0, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 
    0xa4, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 
    0x4, 0xab, 0xa, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 
    0xb1, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xb6, 0xa, 0x5, 
    0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0xbd, 0xa, 
    0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0xc1, 0xa, 0x6, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0xc8, 0xa, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x7, 0x8, 0xd1, 
    0xa, 0x8, 0xc, 0x8, 0xe, 0x8, 0xd4, 0xb, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xda, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 
    0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0xe1, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x3, 0x8, 0x5, 0x8, 0xe7, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 
    0xeb, 0xa, 0x8, 0x3, 0x9, 0x3, 0x9, 0x5, 0x9, 0xef, 0xa, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0xf2, 0xa, 0x9, 0x3, 0x9, 0x5, 0x9, 0xf5, 0xa, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0xf8, 0xa, 0x9, 0x3, 0x9, 0x5, 0x9, 0xfb, 0xa, 0x9, 0x3, 0x9, 
    0x5, 0x9, 0xfe, 0xa, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 
    0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x7, 0xd, 0x110, 0xa, 0xd, 
    0xc, 0xd, 0xe, 0xd, 0x113, 0xb, 0xd, 0x3, 0xe, 0x3, 0xe, 0x5, 0xe, 0x117, 
    0xa, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x5, 0xe, 0x11c, 0xa, 0xe, 0x3, 
    0xe, 0x5, 0xe, 0x11f, 0xa, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x5, 0xf, 
    0x124, 0xa, 0xf, 0x3, 0xf, 0x3, 0xf, 0x5, 0xf, 0x128, 0xa, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x5, 0xf, 0x12c, 0xa, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 
    0x5, 0xf, 0x131, 0xa, 0xf, 0x5, 0xf, 0x133, 0xa, 0xf, 0x3, 0x10, 0x3, 
    0x10, 0x3, 0x10, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x5, 0x11, 0x140, 0xa, 0x11, 0x3, 0x12, 
    0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x146, 0xa, 0x12, 0x3, 0x12, 
    0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x14b, 0xa, 0x12, 0x3, 0x12, 0x3, 0x12, 
    0x3, 0x12, 0x5, 0x12, 0x150, 0xa, 0x12, 0x3, 0x12, 0x5, 0x12, 0x153, 
    0xa, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 
    0x13, 0x3, 0x13, 0x7, 0x13, 0x15c, 0xa, 0x13, 0xc, 0x13, 0xe, 0x13, 
    0x15f, 0xb, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x163, 0xa, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x16a, 
    0xa, 0x14, 0x3, 0x14, 0x7, 0x14, 0x16d, 0xa, 0x14, 0xc, 0x14, 0xe, 0x14, 
    0x170, 0xb, 0x14, 0x3, 0x14, 0x5, 0x14, 0x173, 0xa, 0x14, 0x3, 0x15, 
    0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 
    0x16, 0x5, 0x16, 0x17d, 0xa, 0x16, 0x3, 0x16, 0x5, 0x16, 0x180, 0xa, 
    0x16, 0x3, 0x16, 0x5, 0x16, 0x183, 0xa, 0x16, 0x3, 0x17, 0x3, 0x17, 
    0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x18a, 0xa, 0x17, 0x3, 0x18, 
    0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 0x7, 0x18, 0x190, 0xa, 0x18, 0xc, 0x18, 
    0xe, 0x18, 0x193, 0xb, 0x18, 0x3, 0x19, 0x5, 0x19, 0x196, 0xa, 0x19, 
    0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x19a, 0xa, 0x19, 0x3, 0x19, 0x3, 0x19, 
    0x5, 0x19, 0x19e, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1a1, 0xa, 0x19, 
    0x3, 0x19, 0x5, 0x19, 0x1a4, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1a7, 
    0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1aa, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 
    0x1ad, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1b0, 0xa, 0x19, 0x3, 0x19, 
    0x5, 0x19, 0x1b3, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1b6, 0xa, 0x19, 
    0x3, 0x19, 0x5, 0x19, 0x1b9, 0xa, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1bc, 
    0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 
    0x1b, 0x5, 0x1b, 0x1c4, 0xa, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x5, 0x1c, 0x1ca, 0xa, 0x1c, 0x3, 0x1d, 0x5, 0x1d, 0x1cd, 
    0xa, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 
    0x1e, 0x3, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 
    0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x1de, 0xa, 0x20, 0x3, 0x21, 
    0x3, 0x21, 0x3, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 0x22, 0x3, 
    0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x24, 0x3, 0x24, 
    0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 0x3, 0x26, 0x3, 
    0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x1f8, 0xa, 0x26, 
    0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x5, 
    0x26, 0x200, 0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 
    0x3, 0x26, 0x7, 0x26, 0x207, 0xa, 0x26, 0xc, 0x26, 0xe, 0x26, 0x20a, 
    0xb, 0x26, 0x3, 0x27, 0x5, 0x27, 0x20d, 0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 
    0x3, 0x27, 0x5, 0x27, 0x212, 0xa, 0x27, 0x5, 0x27, 0x214, 0xa, 0x27, 
    0x3, 0x27, 0x5, 0x27, 0x217, 0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 
    0x5, 0x27, 0x21c, 0xa, 0x27, 0x5, 0x27, 0x21e, 0xa, 0x27, 0x3, 0x27, 
    0x5, 0x27, 0x221, 0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x5, 0x27, 
    0x226, 0xa, 0x27, 0x5, 0x27, 0x228, 0xa, 0x27, 0x5, 0x27, 0x22a, 0xa, 
    0x27, 0x3, 0x28, 0x5, 0x28, 0x22d, 0xa, 0x28, 0x3, 0x28, 0x3, 0x28, 
    0x3, 0x28, 0x5, 0x28, 0x232, 0xa, 0x28, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 
    0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x5, 
    0x29, 0x23d, 0xa, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 
    0x242, 0xa, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x7, 0x2b, 0x247, 
    0xa, 0x2b, 0xc, 0x2b, 0xe, 0x2b, 0x24a, 0xb, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 
    0x5, 0x2c, 0x24e, 0xa, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 0x252, 
    0xa, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 0x256, 0xa, 0x2c, 0x3, 0x2d, 
    0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x7, 
    0x2e, 0x25f, 0xa, 0x2e, 0xc, 0x2e, 0xe, 0x2e, 0x262, 0xb, 0x2e, 0x3, 
    0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x7, 
    0x32, 0x271, 0xa, 0x32, 0xc, 0x32, 0xe, 0x32, 0x274, 0xb, 0x32, 0x3, 
    0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x27a, 0xa, 0x33, 
    0x3, 0x33, 0x5, 0x33, 0x27d, 0xa, 0x33, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 
    0x34, 0x3, 0x34, 0x7, 0x34, 0x28a, 0xa, 0x34, 0xc, 0x34, 0xe, 0x34, 
    0x28d, 0xb, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x7, 0x34, 0x296, 0xa, 0x34, 0xc, 0x34, 0xe, 0x34, 
    0x299, 0xb, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 
    0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x7, 0x34, 0x2a5, 
    0xa, 0x34, 0xc, 0x34, 0xe, 0x34, 0x2a8, 0xb, 0x34, 0x3, 0x34, 0x3, 0x34, 
    0x5, 0x34, 0x2ac, 0xa, 0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x7, 0x35, 
    0x2b1, 0xa, 0x35, 0xc, 0x35, 0xe, 0x35, 0x2b4, 0xb, 0x35, 0x3, 0x36, 
    0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x2b9, 0xa, 0x36, 0x3, 0x36, 0x3, 0x36, 
    0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x2c1, 0xa, 0x36, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x2c8, 
    0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x2d9, 0xa, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x2de, 0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x6, 0x37, 0x2e5, 0xa, 0x37, 0xd, 0x37, 
    0xe, 0x37, 0x2e6, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x2eb, 0xa, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 
    0x306, 0xa, 0x37, 0x3, 0x37, 0x5, 0x37, 0x309, 0xa, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x5, 0x37, 0x30d, 0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x314, 0xa, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x322, 0xa, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
    0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x334, 0xa, 0x37, 0x3, 0x37, 
    0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x7, 0x37, 0x33a, 0xa, 0x37, 0xc, 0x37, 
    0xe, 0x37, 0x33d, 0xb, 0x37, 0x3, 0x38, 0x3, 0x38, 0x3, 0x38, 0x7, 0x38, 
    0x342, 0xa, 0x38, 0xc, 0x38, 0xe, 0x38, 0x345, 0xb, 0x38, 0x3, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x7, 0x39, 0x34a, 0xa, 0x39, 0xc, 0x39, 0xe, 0x39, 
    0x34d, 0xb, 0x39, 0x3, 0x3a, 0x3, 0x3a, 0x5, 0x3a, 0x351, 0xa, 0x3a, 
    0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x7, 0x3b, 0x357, 0xa, 0x3b, 
    0xc, 0x3b, 0xe, 0x3b, 0x35a, 0xb, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3b, 0x7, 0x3b, 0x361, 0xa, 0x3b, 0xc, 0x3b, 0xe, 0x3b, 
    0x364, 0xb, 0x3b, 0x5, 0x3b, 0x366, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 
    0x3, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x36e, 0xa, 0x3c, 
    0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x373, 0xa, 0x3c, 0x3, 0x3d, 
    0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x37a, 0xa, 0x3d, 
    0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 
    0x3d, 0x382, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x386, 0xa, 
    0x3d, 0x3, 0x3d, 0x7, 0x3d, 0x389, 0xa, 0x3d, 0xc, 0x3d, 0xe, 0x3d, 
    0x38c, 0xb, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x391, 
    0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x7, 
    0x3f, 0x398, 0xa, 0x3f, 0xc, 0x3f, 0xe, 0x3f, 0x39b, 0xb, 0x3f, 0x3, 
    0x40, 0x3, 0x40, 0x5, 0x40, 0x39f, 0xa, 0x40, 0x3, 0x41, 0x3, 0x41, 
    0x3, 0x42, 0x5, 0x42, 0x3a4, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x3ad, 0xa, 0x42, 
    0x3, 0x43, 0x3, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x5, 0x44, 0x3b4, 
    0xa, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 
    0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
    0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 
    0x46, 0x3c8, 0xa, 0x46, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x3cc, 0xa, 
    0x46, 0x3, 0x46, 0x5, 0x46, 0x3cf, 0xa, 0x46, 0x3, 0x46, 0x5, 0x46, 
    0x3d2, 0xa, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x2, 0x5, 0x4a, 0x6c, 0x78, 0x48, 0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 
    0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 
    0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 
    0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 
    0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 
    0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 
    0x88, 0x8a, 0x8c, 0x2, 0xf, 0x5, 0x2, 0x4, 0x4, 0x19, 0x19, 0x3e, 0x3e, 
    0x4, 0x2, 0x29, 0x29, 0x3d, 0x3d, 0x6, 0x2, 0x7, 0x8, 0xc, 0xc, 0x4a, 
    0x4a, 0x54, 0x54, 0x4, 0x2, 0x3a, 0x3a, 0x50, 0x50, 0x4, 0x2, 0x8, 0x8, 
    0x4a, 0x4a, 0x4, 0x2, 0x45, 0x45, 0x73, 0x73, 0x4, 0x2, 0xb, 0xb, 0x1b, 
    0x1b, 0x4, 0x2, 0x25, 0x25, 0x38, 0x38, 0x5, 0x2, 0xe, 0xe, 0x39, 0x39, 
    0x5c, 0x5c, 0x4, 0x2, 0x75, 0x75, 0x81, 0x81, 0x5, 0x2, 0x30, 0x30, 
    0x41, 0x41, 0x6a, 0x6c, 0xa, 0x2, 0x4, 0x26, 0x28, 0x2f, 0x31, 0x40, 
    0x42, 0x42, 0x44, 0x52, 0x54, 0x60, 0x62, 0x65, 0x67, 0x68, 0x4, 0x2, 
    0x42, 0x42, 0x75, 0x75, 0x2, 0x446, 0x2, 0x8e, 0x3, 0x2, 0x2, 0x2, 0x4, 
    0x9b, 0x3, 0x2, 0x2, 0x2, 0x6, 0xaa, 0x3, 0x2, 0x2, 0x2, 0x8, 0xb0, 
    0x3, 0x2, 0x2, 0x2, 0xa, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xc, 0xc2, 0x3, 0x2, 
    0x2, 0x2, 0xe, 0xea, 0x3, 0x2, 0x2, 0x2, 0x10, 0xec, 0x3, 0x2, 0x2, 
    0x2, 0x12, 0xff, 0x3, 0x2, 0x2, 0x2, 0x14, 0x103, 0x3, 0x2, 0x2, 0x2, 
    0x16, 0x107, 0x3, 0x2, 0x2, 0x2, 0x18, 0x10b, 0x3, 0x2, 0x2, 0x2, 0x1a, 
    0x114, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x132, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x134, 
    0x3, 0x2, 0x2, 0x2, 0x20, 0x137, 0x3, 0x2, 0x2, 0x2, 0x22, 0x152, 0x3, 
    0x2, 0x2, 0x2, 0x24, 0x154, 0x3, 0x2, 0x2, 0x2, 0x26, 0x172, 0x3, 0x2, 
    0x2, 0x2, 0x28, 0x174, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x178, 0x3, 0x2, 0x2, 
    0x2, 0x2c, 0x189, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x18b, 0x3, 0x2, 0x2, 0x2, 
    0x30, 0x195, 0x3, 0x2, 0x2, 0x2, 0x32, 0x1bd, 0x3, 0x2, 0x2, 0x2, 0x34, 
    0x1c0, 0x3, 0x2, 0x2, 0x2, 0x36, 0x1c5, 0x3, 0x2, 0x2, 0x2, 0x38, 0x1cc, 
    0x3, 0x2, 0x2, 0x2, 0x3a, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x1d5, 0x3, 
    0x2, 0x2, 0x2, 0x3e, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x40, 0x1df, 0x3, 0x2, 
    0x2, 0x2, 0x42, 0x1e2, 0x3, 0x2, 0x2, 0x2, 0x44, 0x1e6, 0x3, 0x2, 0x2, 
    0x2, 0x46, 0x1eb, 0x3, 0x2, 0x2, 0x2, 0x48, 0x1ee, 0x3, 0x2, 0x2, 0x2, 
    0x4a, 0x1f7, 0x3, 0x2, 0x2, 0x2, 0x4c, 0x229, 0x3, 0x2, 0x2, 0x2, 0x4e, 
    0x231, 0x3, 0x2, 0x2, 0x2, 0x50, 0x23c, 0x3, 0x2, 0x2, 0x2, 0x52, 0x23e, 
    0x3, 0x2, 0x2, 0x2, 0x54, 0x243, 0x3, 0x2, 0x2, 0x2, 0x56, 0x24b, 0x3, 
    0x2, 0x2, 0x2, 0x58, 0x257, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x25b, 0x3, 0x2, 
    0x2, 0x2, 0x5c, 0x263, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x267, 0x3, 0x2, 0x2, 
    0x2, 0x60, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x62, 0x26d, 0x3, 0x2, 0x2, 0x2, 
    0x64, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x66, 0x2ab, 0x3, 0x2, 0x2, 0x2, 0x68, 
    0x2ad, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x2c0, 0x3, 0x2, 0x2, 0x2, 0x6c, 0x313, 
    0x3, 0x2, 0x2, 0x2, 0x6e, 0x33e, 0x3, 0x2, 0x2, 0x2, 0x70, 0x346, 0x3, 
    0x2, 0x2, 0x2, 0x72, 0x350, 0x3, 0x2, 0x2, 0x2, 0x74, 0x365, 0x3, 0x2, 
    0x2, 0x2, 0x76, 0x36d, 0x3, 0x2, 0x2, 0x2, 0x78, 0x381, 0x3, 0x2, 0x2, 
    0x2, 0x7a, 0x390, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x394, 0x3, 0x2, 0x2, 0x2, 
    0x7e, 0x39e, 0x3, 0x2, 0x2, 0x2, 0x80, 0x3a0, 0x3, 0x2, 0x2, 0x2, 0x82, 
    0x3ac, 0x3, 0x2, 0x2, 0x2, 0x84, 0x3ae, 0x3, 0x2, 0x2, 0x2, 0x86, 0x3b3, 
    0x3, 0x2, 0x2, 0x2, 0x88, 0x3b5, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x3d1, 0x3, 
    0x2, 0x2, 0x2, 0x8c, 0x3d3, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x93, 0x5, 0x4, 
    0x3, 0x2, 0x8f, 0x90, 0x7, 0x86, 0x2, 0x2, 0x90, 0x92, 0x5, 0x4, 0x3, 
    0x2, 0x91, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x92, 0x95, 0x3, 0x2, 0x2, 0x2, 
    0x93, 0x91, 0x3, 0x2, 0x2, 0x2, 0x93, 0x94, 0x3, 0x2, 0x2, 0x2, 0x94, 
    0x97, 0x3, 0x2, 0x2, 0x2, 0x95, 0x93, 0x3, 0x2, 0x2, 0x2, 0x96, 0x98, 
    0x7, 0x86, 0x2, 0x2, 0x97, 0x96, 0x3, 0x2, 0x2, 0x2, 0x97, 0x98, 0x3, 
    0x2, 0x2, 0x2, 0x98, 0x99, 0x3, 0x2, 0x2, 0x2, 0x99, 0x9a, 0x7, 0x2, 
    0x2, 0x3, 0x9a, 0x3, 0x3, 0x2, 0x2, 0x2, 0x9b, 0x9f, 0x5, 0x6, 0x4, 
    0x2, 0x9c, 0x9d, 0x7, 0x34, 0x2, 0x2, 0x9d, 0x9e, 0x7, 0x4b, 0x2, 0x2, 
    0x9e, 0xa0, 0x7, 0x6d, 0x2, 0x2, 0x9f, 0x9c, 0x3, 0x2, 0x2, 0x2, 0x9f, 
    0xa0, 0x3, 0x2, 0x2, 0x2, 0xa0, 0xa3, 0x3, 0x2, 0x2, 0x2, 0xa1, 0xa2, 
    0x7, 0x26, 0x2, 0x2, 0xa2, 0xa4, 0x5, 0x86, 0x44, 0x2, 0xa3, 0xa1, 0x3, 
    0x2, 0x2, 0x2, 0xa3, 0xa4, 0x3, 0x2, 0x2, 0x2, 0xa4, 0x5, 0x3, 0x2, 
    0x2, 0x2, 0xa5, 0xab, 0x5, 0x8, 0x5, 0x2, 0xa6, 0xab, 0x5, 0x24, 0x13, 
    0x2, 0xa7, 0xab, 0x5, 0x2e, 0x18, 0x2, 0xa8, 0xab, 0x5, 0x5e, 0x30, 
    0x2, 0xa9, 0xab, 0x5, 0x60, 0x31, 0x2, 0xaa, 0xa5, 0x3, 0x2, 0x2, 0x2, 
    0xaa, 0xa6, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xa7, 0x3, 0x2, 0x2, 0x2, 0xaa, 
    0xa8, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xa9, 0x3, 0x2, 0x2, 0x2, 0xab, 0x7, 
    0x3, 0x2, 0x2, 0x2, 0xac, 0xb1, 0x5, 0xa, 0x6, 0x2, 0xad, 0xb1, 0x5, 
    0xc, 0x7, 0x2, 0xae, 0xb1, 0x5, 0x22, 0x12, 0x2, 0xaf, 0xb1, 0x5, 0x2a, 
    0x16, 0x2, 0xb0, 0xac, 0x3, 0x2, 0x2, 0x2, 0xb0, 0xad, 0x3, 0x2, 0x2, 
    0x2, 0xb0, 0xae, 0x3, 0x2, 0x2, 0x2, 0xb0, 0xaf, 0x3, 0x2, 0x2, 0x2, 
    0xb1, 0xb5, 0x3, 0x2, 0x2, 0x2, 0xb2, 0xb3, 0x7, 0x46, 0x2, 0x2, 0xb3, 
    0xb4, 0x7, 0x12, 0x2, 0x2, 0xb4, 0xb6, 0x5, 0x86, 0x44, 0x2, 0xb5, 0xb2, 
    0x3, 0x2, 0x2, 0x2, 0xb5, 0xb6, 0x3, 0x2, 0x2, 0x2, 0xb6, 0x9, 0x3, 
    0x2, 0x2, 0x2, 0xb7, 0xb8, 0x7, 0x14, 0x2, 0x2, 0xb8, 0xbc, 0x7, 0x16, 
    0x2, 0x2, 0xb9, 0xba, 0x7, 0x2e, 0x2, 0x2, 0xba, 0xbb, 0x7, 0x42, 0x2, 
    0x2, 0xbb, 0xbd, 0x7, 0x22, 0x2, 0x2, 0xbc, 0xb9, 0x3, 0x2, 0x2, 0x2, 
    0xbc, 0xbd, 0x3, 0x2, 0x2, 0x2, 0xbd, 0xbe, 0x3, 0x2, 0x2, 0x2, 0xbe, 
    0xc0, 0x5, 0x80, 0x41, 0x2, 0xbf, 0xc1, 0x5, 0x1a, 0xe, 0x2, 0xc0, 0xbf, 
    0x3, 0x2, 0x2, 0x2, 0xc0, 0xc1, 0x3, 0x2, 0x2, 0x2, 0xc1, 0xb, 0x3, 
    0x2, 0x2, 0x2, 0xc2, 0xc3, 0x7, 0x14, 0x2, 0x2, 0xc3, 0xc7, 0x7, 0x57, 
    0x2, 0x2, 0xc4, 0xc5, 0x7, 0x2e, 0x2, 0x2, 0xc5, 0xc6, 0x7, 0x42, 0x2, 
    0x2, 0xc6, 0xc8, 0x7, 0x22, 0x2, 0x2, 0xc7, 0xc4, 0x3, 0x2, 0x2, 0x2, 
    0xc7, 0xc8, 0x3, 0x2, 0x2, 0x2, 0xc8, 0xc9, 0x3, 0x2, 0x2, 0x2, 0xc9, 
    0xca, 0x5, 0x7a, 0x3e, 0x2, 0xca, 0xcb, 0x5, 0xe, 0x8, 0x2, 0xcb, 0xd, 
    0x3, 0x2, 0x2, 0x2, 0xcc, 0xcd, 0x7, 0x7d, 0x2, 0x2, 0xcd, 0xd2, 0x5, 
    0x1c, 0xf, 0x2, 0xce, 0xcf, 0x7, 0x73, 0x2, 0x2, 0xcf, 0xd1, 0x5, 0x1c, 
    0xf, 0x2, 0xd0, 0xce, 0x3, 0x2, 0x2, 0x2, 0xd1, 0xd4, 0x3, 0x2, 0x2, 
    0x2, 0xd2, 0xd0, 0x3, 0x2, 0x2, 0x2, 0xd2, 0xd3, 0x3, 0x2, 0x2, 0x2, 
    0xd3, 0xd5, 0x3, 0x2, 0x2, 0x2, 0xd4, 0xd2, 0x3, 0x2, 0x2, 0x2, 0xd5, 
    0xd6, 0x7, 0x85, 0x2, 0x2, 0xd6, 0xd7, 0x5, 0x10, 0x9, 0x2, 0xd7, 0xeb, 
    0x3, 0x2, 0x2, 0x2, 0xd8, 0xda, 0x5, 0x10, 0x9, 0x2, 0xd9, 0xd8, 0x3, 
    0x2, 0x2, 0x2, 0xd9, 0xda, 0x3, 0x2, 0x2, 0x2, 0xda, 0xdb, 0x3, 0x2, 
    0x2, 0x2, 0xdb, 0xdc, 0x7, 0xa, 0x2, 0x2, 0xdc, 0xeb, 0x5, 0x2e, 0x18, 
    0x2, 0xdd, 0xde, 0x7, 0xa, 0x2, 0x2, 0xde, 0xe0, 0x5, 0x7a, 0x3e, 0x2, 
    0xdf, 0xe1, 0x5, 0x10, 0x9, 0x2, 0xe0, 0xdf, 0x3, 0x2, 0x2, 0x2, 0xe0, 
    0xe1, 0x3, 0x2, 0x2, 0x2, 0xe1, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xe2, 0xe3, 
    0x7, 0xa, 0x2, 0x2, 0xe3, 0xe4, 0x5, 0x86, 0x44, 0x2, 0xe4, 0xe6, 0x7, 
    0x7d, 0x2, 0x2, 0xe5, 0xe7, 0x5, 0x7c, 0x3f, 0x2, 0xe6, 0xe5, 0x3, 0x2, 
    0x2, 0x2, 0xe6, 0xe7, 0x3, 0x2, 0x2, 0x2, 0xe7, 0xe8, 0x3, 0x2, 0x2, 
    0x2, 0xe8, 0xe9, 0x7, 0x85, 0x2, 0x2, 0xe9, 0xeb, 0x3, 0x2, 0x2, 0x2, 
    0xea, 0xcc, 0x3, 0x2, 0x2, 0x2, 0xea, 0xd9, 0x3, 0x2, 0x2, 0x2, 0xea, 
    0xdd, 0x3, 0x2, 0x2, 0x2, 0xea, 0xe2, 0x3, 0x2, 0x2, 0x2, 0xeb, 0xf, 
    0x3, 0x2, 0x2, 0x2, 0xec, 0xee, 0x5, 0x1a, 0xe, 0x2, 0xed, 0xef, 0x5, 
    0x42, 0x22, 0x2, 0xee, 0xed, 0x3, 0x2, 0x2, 0x2, 0xee, 0xef, 0x3, 0x2, 
    0x2, 0x2, 0xef, 0xf1, 0x3, 0x2, 0x2, 0x2, 0xf0, 0xf2, 0x5, 0x12, 0xa, 
    0x2, 0xf1, 0xf0, 0x3, 0x2, 0x2, 0x2, 0xf1, 0xf2, 0x3, 0x2, 0x2, 0x2, 
    0xf2, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf5, 0x5, 0x14, 0xb, 0x2, 0xf4, 
    0xf3, 0x3, 0x2, 0x2, 0x2, 0xf4, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf5, 0xf7, 
    0x3, 0x2, 0x2, 0x2, 0xf6, 0xf8, 0x5, 0x16, 0xc, 0x2, 0xf7, 0xf6, 0x3, 
    0x2, 0x2, 0x2, 0xf7, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xfa, 0x3, 0x2, 
    0x2, 0x2, 0xf9, 0xfb, 0x5, 0x18, 0xd, 0x2, 0xfa, 0xf9, 0x3, 0x2, 0x2, 
    0x2, 0xfa, 0xfb, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfd, 0x3, 0x2, 0x2, 0x2, 
    0xfc, 0xfe, 0x5, 0x48, 0x25, 0x2, 0xfd, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfd, 
    0xfe, 0x3, 0x2, 0x2, 0x2, 0xfe, 0x11, 0x3, 0x2, 0x2, 0x2, 0xff, 0x100, 
    0x7, 0x4c, 0x2, 0x2, 0x100, 0x101, 0x7, 0xf, 0x2, 0x2, 0x101, 0x102, 
    0x5, 0x6c, 0x37, 0x2, 0x102, 0x13, 0x3, 0x2, 0x2, 0x2, 0x103, 0x104, 
    0x7, 0x4e, 0x2, 0x2, 0x104, 0x105, 0x7, 0x37, 0x2, 0x2, 0x105, 0x106, 
    0x5, 0x6c, 0x37, 0x2, 0x106, 0x15, 0x3, 0x2, 0x2, 0x2, 0x107, 0x108, 
    0x7, 0x51, 0x2, 0x2, 0x108, 0x109, 0x7, 0xf, 0x2, 0x2, 0x109, 0x10a, 
    0x5, 0x6c, 0x37, 0x2, 0x10a, 0x17, 0x3, 0x2, 0x2, 0x2, 0x10b, 0x10c, 
    0x7, 0x5e, 0x2, 0x2, 0x10c, 0x111, 0x5, 0x20, 0x11, 0x2, 0x10d, 0x10e, 
    0x7, 0x73, 0x2, 0x2, 0x10e, 0x110, 0x5, 0x20, 0x11, 0x2, 0x10f, 0x10d, 
    0x3, 0x2, 0x2, 0x2, 0x110, 0x113, 0x3, 0x2, 0x2, 0x2, 0x111, 0x10f, 
    0x3, 0x2, 0x2, 0x2, 0x111, 0x112, 0x3, 0x2, 0x2, 0x2, 0x112, 0x19, 0x3, 
    0x2, 0x2, 0x2, 0x113, 0x111, 0x3, 0x2, 0x2, 0x2, 0x114, 0x116, 0x7, 
    0x21, 0x2, 0x2, 0x115, 0x117, 0x7, 0x78, 0x2, 0x2, 0x116, 0x115, 0x3, 
    0x2, 0x2, 0x2, 0x116, 0x117, 0x3, 0x2, 0x2, 0x2, 0x117, 0x118, 0x3, 
    0x2, 0x2, 0x2, 0x118, 0x11e, 0x5, 0x86, 0x44, 0x2, 0x119, 0x11b, 0x7, 
    0x7d, 0x2, 0x2, 0x11a, 0x11c, 0x5, 0x68, 0x35, 0x2, 0x11b, 0x11a, 0x3, 
    0x2, 0x2, 0x2, 0x11b, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x11c, 0x11d, 0x3, 
    0x2, 0x2, 0x2, 0x11d, 0x11f, 0x7, 0x85, 0x2, 0x2, 0x11e, 0x119, 0x3, 
    0x2, 0x2, 0x2, 0x11e, 0x11f, 0x3, 0x2, 0x2, 0x2, 0x11f, 0x1b, 0x3, 0x2, 
    0x2, 0x2, 0x120, 0x121, 0x5, 0x86, 0x44, 0x2, 0x121, 0x123, 0x5, 0x66, 
    0x34, 0x2, 0x122, 0x124, 0x5, 0x1e, 0x10, 0x2, 0x123, 0x122, 0x3, 0x2, 
    0x2, 0x2, 0x123, 0x124, 0x3, 0x2, 0x2, 0x2, 0x124, 0x127, 0x3, 0x2, 
    0x2, 0x2, 0x125, 0x126, 0x7, 0x5e, 0x2, 0x2, 0x126, 0x128, 0x5, 0x6c, 
    0x37, 0x2, 0x127, 0x125, 0x3, 0x2, 0x2, 0x2, 0x127, 0x128, 0x3, 0x2, 
    0x2, 0x2, 0x128, 0x133, 0x3, 0x2, 0x2, 0x2, 0x129, 0x12b, 0x5, 0x86, 
    0x44, 0x2, 0x12a, 0x12c, 0x5, 0x66, 0x34, 0x2, 0x12b, 0x12a, 0x3, 0x2, 
    0x2, 0x2, 0x12b, 0x12c, 0x3, 0x2, 0x2, 0x2, 0x12c, 0x12d, 0x3, 0x2, 
    0x2, 0x2, 0x12d, 0x130, 0x5, 0x1e, 0x10, 0x2, 0x12e, 0x12f, 0x7, 0x5e, 
    0x2, 0x2, 0x12f, 0x131, 0x5, 0x6c, 0x37, 0x2, 0x130, 0x12e, 0x3, 0x2, 
    0x2, 0x2, 0x130, 0x131, 0x3, 0x2, 0x2, 0x2, 0x131, 0x133, 0x3, 0x2, 
    0x2, 0x2, 0x132, 0x120, 0x3, 0x2, 0x2, 0x2, 0x132, 0x129, 0x3, 0x2, 
    0x2, 0x2, 0x133, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x134, 0x135, 0x9, 0x2, 0x2, 
    0x2, 0x135, 0x136, 0x5, 0x6c, 0x37, 0x2, 0x136, 0x1f, 0x3, 0x2, 0x2, 
    0x2, 0x137, 0x13f, 0x5, 0x6c, 0x37, 0x2, 0x138, 0x140, 0x7, 0x1a, 0x2, 
    0x2, 0x139, 0x13a, 0x7, 0x5a, 0x2, 0x2, 0x13a, 0x13b, 0x7, 0x1c, 0x2, 
    0x2, 0x13b, 0x140, 0x7, 0x6d, 0x2, 0x2, 0x13c, 0x13d, 0x7, 0x5a, 0x2, 
    0x2, 0x13d, 0x13e, 0x7, 0x63, 0x2, 0x2, 0x13e, 0x140, 0x7, 0x6d, 0x2, 
    0x2, 0x13f, 0x138, 0x3, 0x2, 0x2, 0x2, 0x13f, 0x139, 0x3, 0x2, 0x2, 
    0x2, 0x13f, 0x13c, 0x3, 0x2, 0x2, 0x2, 0x13f, 0x140, 0x3, 0x2, 0x2, 
    0x2, 0x140, 0x21, 0x3, 0x2, 0x2, 0x2, 0x141, 0x142, 0x7, 0x1e, 0x2, 
    0x2, 0x142, 0x145, 0x7, 0x16, 0x2, 0x2, 0x143, 0x144, 0x7, 0x2e, 0x2, 
    0x2, 0x144, 0x146, 0x7, 0x22, 0x2, 0x2, 0x145, 0x143, 0x3, 0x2, 0x2, 
    0x2, 0x145, 0x146, 0x3, 0x2, 0x2, 0x2, 0x146, 0x147, 0x3, 0x2, 0x2, 
    0x2, 0x147, 0x153, 0x5, 0x80, 0x41, 0x2, 0x148, 0x14a, 0x7, 0x1e, 0x2, 
    0x2, 0x149, 0x14b, 0x7, 0x58, 0x2, 0x2, 0x14a, 0x149, 0x3, 0x2, 0x2, 
    0x2, 0x14a, 0x14b, 0x3, 0x2, 0x2, 0x2, 0x14b, 0x14c, 0x3, 0x2, 0x2, 
    0x2, 0x14c, 0x14f, 0x7, 0x57, 0x2, 0x2, 0x14d, 0x14e, 0x7, 0x2e, 0x2, 
    0x2, 0x14e, 0x150, 0x7, 0x22, 0x2, 0x2, 0x14f, 0x14d, 0x3, 0x2, 0x2, 
    0x2, 0x14f, 0x150, 0x3, 0x2, 0x2, 0x2, 0x150, 0x151, 0x3, 0x2, 0x2, 
    0x2, 0x151, 0x153, 0x5, 0x7a, 0x3e, 0x2, 0x152, 0x141, 0x3, 0x2, 0x2, 
    0x2, 0x152, 0x148, 0x3, 0x2, 0x2, 0x2, 0x153, 0x23, 0x3, 0x2, 0x2, 0x2, 
    0x154, 0x155, 0x7, 0x32, 0x2, 0x2, 0x155, 0x156, 0x7, 0x34, 0x2, 0x2, 
    0x156, 0x162, 0x5, 0x7a, 0x3e, 0x2, 0x157, 0x158, 0x7, 0x7d, 0x2, 0x2, 
    0x158, 0x15d, 0x5, 0x86, 0x44, 0x2, 0x159, 0x15a, 0x7, 0x73, 0x2, 0x2, 
    0x15a, 0x15c, 0x5, 0x86, 0x44, 0x2, 0x15b, 0x159, 0x3, 0x2, 0x2, 0x2, 
    0x15c, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x15b, 0x3, 0x2, 0x2, 0x2, 
    0x15d, 0x15e, 0x3, 0x2, 0x2, 0x2, 0x15e, 0x160, 0x3, 0x2, 0x2, 0x2, 
    0x15f, 0x15d, 0x3, 0x2, 0x2, 0x2, 0x160, 0x161, 0x7, 0x85, 0x2, 0x2, 
    0x161, 0x163, 0x3, 0x2, 0x2, 0x2, 0x162, 0x157, 0x3, 0x2, 0x2, 0x2, 
    0x162, 0x163, 0x3, 0x2, 0x2, 0x2, 0x163, 0x164, 0x3, 0x2, 0x2, 0x2, 
    0x164, 0x165, 0x5, 0x26, 0x14, 0x2, 0x165, 0x25, 0x3, 0x2, 0x2, 0x2, 
    0x166, 0x167, 0x7, 0x62, 0x2, 0x2, 0x167, 0x16e, 0x5, 0x28, 0x15, 0x2, 
    0x168, 0x16a, 0x7, 0x73, 0x2, 0x2, 0x169, 0x168, 0x3, 0x2, 0x2, 0x2, 
    0x169, 0x16a, 0x3, 0x2, 0x2, 0x2, 0x16a, 0x16b, 0x3, 0x2, 0x2, 0x2, 
    0x16b, 0x16d, 0x5, 0x28, 0x15, 0x2, 0x16c, 0x169, 0x3, 0x2, 0x2, 0x2, 
    0x16d, 0x170, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x16c, 0x3, 0x2, 0x2, 0x2, 
    0x16e, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x16f, 0x173, 0x3, 0x2, 0x2, 0x2, 
    0x170, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x171, 0x173, 0x5, 0x2e, 0x18, 0x2, 
    0x172, 0x166, 0x3, 0x2, 0x2, 0x2, 0x172, 0x171, 0x3, 0x2, 0x2, 0x2, 
    0x173, 0x27, 0x3, 0x2, 0x2, 0x2, 0x174, 0x175, 0x7, 0x7d, 0x2, 0x2, 
    0x175, 0x176, 0x5, 0x62, 0x32, 0x2, 0x176, 0x177, 0x7, 0x85, 0x2, 0x2, 
    0x177, 0x29, 0x3, 0x2, 0x2, 0x2, 0x178, 0x179, 0x7, 0x47, 0x2, 0x2, 
    0x179, 0x17a, 0x7, 0x57, 0x2, 0x2, 0x17a, 0x17c, 0x5, 0x7a, 0x3e, 0x2, 
    0x17b, 0x17d, 0x5, 0x2c, 0x17, 0x2, 0x17c, 0x17b, 0x3, 0x2, 0x2, 0x2, 
    0x17c, 0x17d, 0x3, 0x2, 0x2, 0x2, 0x17d, 0x17f, 0x3, 0x2, 0x2, 0x2, 
    0x17e, 0x180, 0x7, 0x24, 0x2, 0x2, 0x17f, 0x17e, 0x3, 0x2, 0x2, 0x2, 
    0x17f, 0x180, 0x3, 0x2, 0x2, 0x2, 0x180, 0x182, 0x3, 0x2, 0x2, 0x2, 
    0x181, 0x183, 0x7, 0x18, 0x2, 0x2, 0x182, 0x181, 0x3, 0x2, 0x2, 0x2, 
    0x182, 0x183, 0x3, 0x2, 0x2, 0x2, 0x183, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x184, 
    0x185, 0x7, 0x4c, 0x2, 0x2, 0x185, 0x18a, 0x5, 0x6c, 0x37, 0x2, 0x186, 
    0x187, 0x7, 0x4c, 0x2, 0x2, 0x187, 0x188, 0x7, 0x2d, 0x2, 0x2, 0x188, 
    0x18a, 0x7, 0x6d, 0x2, 0x2, 0x189, 0x184, 0x3, 0x2, 0x2, 0x2, 0x189, 
    0x186, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x191, 
    0x5, 0x30, 0x19, 0x2, 0x18c, 0x18d, 0x7, 0x5f, 0x2, 0x2, 0x18d, 0x18e, 
    0x7, 0x5, 0x2, 0x2, 0x18e, 0x190, 0x5, 0x30, 0x19, 0x2, 0x18f, 0x18c, 
    0x3, 0x2, 0x2, 0x2, 0x190, 0x193, 0x3, 0x2, 0x2, 0x2, 0x191, 0x18f, 
    0x3, 0x2, 0x2, 0x2, 0x191, 0x192, 0x3, 0x2, 0x2, 0x2, 0x192, 0x2f, 0x3, 
    0x2, 0x2, 0x2, 0x193, 0x191, 0x3, 0x2, 0x2, 0x2, 0x194, 0x196, 0x5, 
    0x32, 0x1a, 0x2, 0x195, 0x194, 0x3, 0x2, 0x2, 0x2, 0x195, 0x196, 0x3, 
    0x2, 0x2, 0x2, 0x196, 0x197, 0x3, 0x2, 0x2, 0x2, 0x197, 0x199, 0x7, 
    0x53, 0x2, 0x2, 0x198, 0x19a, 0x7, 0x1d, 0x2, 0x2, 0x199, 0x198, 0x3, 
    0x2, 0x2, 0x2, 0x199, 0x19a, 0x3, 0x2, 0x2, 0x2, 0x19a, 0x19b, 0x3, 
    0x2, 0x2, 0x2, 0x19b, 0x19d, 0x5, 0x68, 0x35, 0x2, 0x19c, 0x19e, 0x5, 
    0x34, 0x1b, 0x2, 0x19d, 0x19c, 0x3, 0x2, 0x2, 0x2, 0x19d, 0x19e, 0x3, 
    0x2, 0x2, 0x2, 0x19e, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x19f, 0x1a1, 0x5, 
    0x36, 0x1c, 0x2, 0x1a0, 0x19f, 0x3, 0x2, 0x2, 0x2, 0x1a0, 0x1a1, 0x3, 
    0x2, 0x2, 0x2, 0x1a1, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a4, 0x5, 
    0x38, 0x1d, 0x2, 0x1a3, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1a3, 0x1a4, 0x3, 
    0x2, 0x2, 0x2, 0x1a4, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1a5, 0x1a7, 0x5, 
    0x3a, 0x1e, 0x2, 0x1a6, 0x1a5, 0x3, 0x2, 0x2, 0x2, 0x1a6, 0x1a7, 0x3, 
    0x2, 0x2, 0x2, 0x1a7, 0x1a9, 0x3, 0x2, 0x2, 0x2, 0x1a8, 0x1aa, 0x5, 
    0x3c, 0x1f, 0x2, 0x1a9, 0x1a8, 0x3, 0x2, 0x2, 0x2, 0x1a9, 0x1aa, 0x3, 
    0x2, 0x2, 0x2, 0x1aa, 0x1ac, 0x3, 0x2, 0x2, 0x2, 0x1ab, 0x1ad, 0x5, 
    0x3e, 0x20, 0x2, 0x1ac, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1ad, 0x3, 
    0x2, 0x2, 0x2, 0x1ad, 0x1af, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1b0, 0x5, 
    0x40, 0x21, 0x2, 0x1af, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1af, 0x1b0, 0x3, 
    0x2, 0x2, 0x2, 0x1b0, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b3, 0x5, 
    0x42, 0x22, 0x2, 0x1b2, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1b2, 0x1b3, 0x3, 
    0x2, 0x2, 0x2, 0x1b3, 0x1b5, 0x3, 0x2, 0x2, 0x2, 0x1b4, 0x1b6, 0x5, 
    0x44, 0x23, 0x2, 0x1b5, 0x1b4, 0x3, 0x2, 0x2, 0x2, 0x1b5, 0x1b6, 0x3, 
    0x2, 0x2, 0x2, 0x1b6, 0x1b8, 0x3, 0x2, 0x2, 0x2, 0x1b7, 0x1b9, 0x5, 
    0x46, 0x24, 0x2, 0x1b8, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x1b8, 0x1b9, 0x3, 
    0x2, 0x2, 0x2, 0x1b9, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1bc, 0x5, 
    0x48, 0x25, 0x2, 0x1bb, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x3, 
    0x2, 0x2, 0x2, 0x1bc, 0x31, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1be, 0x7, 0x67, 
    0x2, 0x2, 0x1be, 0x1bf, 0x5, 0x68, 0x35, 0x2, 0x1bf, 0x33, 0x3, 0x2, 
    0x2, 0x2, 0x1c0, 0x1c1, 0x7, 0x27, 0x2, 0x2, 0x1c1, 0x1c3, 0x5, 0x4a, 
    0x26, 0x2, 0x1c2, 0x1c4, 0x7, 0x24, 0x2, 0x2, 0x1c3, 0x1c2, 0x3, 0x2, 
    0x2, 0x2, 0x1c3, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x1c4, 0x35, 0x3, 0x2, 0x2, 
    0x2, 0x1c5, 0x1c6, 0x7, 0x51, 0x2, 0x2, 0x1c6, 0x1c9, 0x5, 0x58, 0x2d, 
    0x2, 0x1c7, 0x1c8, 0x7, 0x45, 0x2, 0x2, 0x1c8, 0x1ca, 0x5, 0x58, 0x2d, 
    0x2, 0x1c9, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1c9, 0x1ca, 0x3, 0x2, 0x2, 
    0x2, 0x1ca, 0x37, 0x3, 0x2, 0x2, 0x2, 0x1cb, 0x1cd, 0x7, 0x3a, 0x2, 
    0x2, 0x1cc, 0x1cb, 0x3, 0x2, 0x2, 0x2, 0x1cc, 0x1cd, 0x3, 0x2, 0x2, 
    0x2, 0x1cd, 0x1ce, 0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1cf, 0x7, 0x9, 0x2, 
    0x2, 0x1cf, 0x1d0, 0x7, 0x36, 0x2, 0x2, 0x1d0, 0x1d1, 0x5, 0x68, 0x35, 
    0x2, 0x1d1, 0x39, 0x3, 0x2, 0x2, 0x2, 0x1d2, 0x1d3, 0x7, 0x4d, 0x2, 
    0x2, 0x1d3, 0x1d4, 0x5, 0x6c, 0x37, 0x2, 0x1d4, 0x3b, 0x3, 0x2, 0x2, 
    0x2, 0x1d5, 0x1d6, 0x7, 0x66, 0x2, 0x2, 0x1d6, 0x1d7, 0x5, 0x6c, 0x37, 
    0x2, 0x1d7, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x1d8, 0x1d9, 0x7, 0x2a, 0x2, 
    0x2, 0x1d9, 0x1da, 0x7, 0xf, 0x2, 0x2, 0x1da, 0x1dd, 0x5, 0x68, 0x35, 
    0x2, 0x1db, 0x1dc, 0x7, 0x67, 0x2, 0x2, 0x1dc, 0x1de, 0x7, 0x5b, 0x2, 
    0x2, 0x1dd, 0x1db, 0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1de, 0x3, 0x2, 0x2, 
    0x2, 0x1de, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x1df, 0x1e0, 0x7, 0x2b, 0x2, 
    0x2, 0x1e0, 0x1e1, 0x5, 0x6c, 0x37, 0x2, 0x1e1, 0x41, 0x3, 0x2, 0x2, 
    0x2, 0x1e2, 0x1e3, 0x7, 0x49, 0x2, 0x2, 0x1e3, 0x1e4, 0x7, 0xf, 0x2, 
    0x2, 0x1e4, 0x1e5, 0x5, 0x54, 0x2b, 0x2, 0x1e5, 0x43, 0x3, 0x2, 0x2, 
    0x2, 0x1e6, 0x1e7, 0x7, 0x3c, 0x2, 0x2, 0x1e7, 0x1e8, 0x5, 0x52, 0x2a, 
    0x2, 0x1e8, 0x1e9, 0x7, 0xf, 0x2, 0x2, 0x1e9, 0x1ea, 0x5, 0x68, 0x35, 
    0x2, 0x1ea, 0x45, 0x3, 0x2, 0x2, 0x2, 0x1eb, 0x1ec, 0x7, 0x3c, 0x2, 
    0x2, 0x1ec, 0x1ed, 0x5, 0x52, 0x2a, 0x2, 0x1ed, 0x47, 0x3, 0x2, 0x2, 
    0x2, 0x1ee, 0x1ef, 0x7, 0x56, 0x2, 0x2, 0x1ef, 0x1f0, 0x5, 0x5a, 0x2e, 
    0x2, 0x1f0, 0x49, 0x3, 0x2, 0x2, 0x2, 0x1f1, 0x1f2, 0x8, 0x26, 0x1, 
    0x2, 0x1f2, 0x1f8, 0x5, 0x78, 0x3d, 0x2, 0x1f3, 0x1f4, 0x7, 0x7d, 0x2, 
    0x2, 0x1f4, 0x1f5, 0x5, 0x4a, 0x26, 0x2, 0x1f5, 0x1f6, 0x7, 0x85, 0x2, 
    0x2, 0x1f6, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1f1, 0x3, 0x2, 0x2, 
    0x2, 0x1f7, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f8, 0x208, 0x3, 0x2, 0x2, 
    0x2, 0x1f9, 0x1fa, 0xc, 0x3, 0x2, 0x2, 0x1fa, 0x1fb, 0x5, 0x4e, 0x28, 
    0x2, 0x1fb, 0x1fc, 0x5, 0x4a, 0x26, 0x4, 0x1fc, 0x207, 0x3, 0x2, 0x2, 
    0x2, 0x1fd, 0x1ff, 0xc, 0x4, 0x2, 0x2, 0x1fe, 0x200, 0x9, 0x3, 0x2, 
    0x2, 0x1ff, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1ff, 0x200, 0x3, 0x2, 0x2, 
    0x2, 0x200, 0x201, 0x3, 0x2, 0x2, 0x2, 0x201, 0x202, 0x5, 0x4c, 0x27, 
    0x2, 0x202, 0x203, 0x7, 0x36, 0x2, 0x2, 0x203, 0x204, 0x5, 0x4a, 0x26, 
    0x2, 0x204, 0x205, 0x5, 0x50, 0x29, 0x2, 0x205, 0x207, 0x3, 0x2, 0x2, 
    0x2, 0x206, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x206, 0x1fd, 0x3, 0x2, 0x2, 
    0x2, 0x207, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x208, 0x206, 0x3, 0x2, 0x2, 
    0x2, 0x208, 0x209, 0x3, 0x2, 0x2, 0x2, 0x209, 0x4b, 0x3, 0x2, 0x2, 0x2, 
    0x20a, 0x208, 0x3, 0x2, 0x2, 0x2, 0x20b, 0x20d, 0x7, 0x8, 0x2, 0x2, 
    0x20c, 0x20b, 0x3, 0x2, 0x2, 0x2, 0x20c, 0x20d, 0x3, 0x2, 0x2, 0x2, 
    0x20d, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x20e, 0x214, 0x7, 0x31, 0x2, 0x2, 
    0x20f, 0x211, 0x7, 0x31, 0x2, 0x2, 0x210, 0x212, 0x7, 0x8, 0x2, 0x2, 
    0x211, 0x210, 0x3, 0x2, 0x2, 0x2, 0x211, 0x212, 0x3, 0x2, 0x2, 0x2, 
    0x212, 0x214, 0x3, 0x2, 0x2, 0x2, 0x213, 0x20c, 0x3, 0x2, 0x2, 0x2, 
    0x213, 0x20f, 0x3, 0x2, 0x2, 0x2, 0x214, 0x22a, 0x3, 0x2, 0x2, 0x2, 
    0x215, 0x217, 0x9, 0x4, 0x2, 0x2, 0x216, 0x215, 0x3, 0x2, 0x2, 0x2, 
    0x216, 0x217, 0x3, 0x2, 0x2, 0x2, 0x217, 0x218, 0x3, 0x2, 0x2, 0x2, 
    0x218, 0x21e, 0x9, 0x5, 0x2, 0x2, 0x219, 0x21b, 0x9, 0x5, 0x2, 0x2, 
    0x21a, 0x21c, 0x9, 0x4, 0x2, 0x2, 0x21b, 0x21a, 0x3, 0x2, 0x2, 0x2, 
    0x21b, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x21c, 0x21e, 0x3, 0x2, 0x2, 0x2, 
    0x21d, 0x216, 0x3, 0x2, 0x2, 0x2, 0x21d, 0x219, 0x3, 0x2, 0x2, 0x2, 
    0x21e, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x21f, 0x221, 0x9, 0x6, 0x2, 0x2, 
    0x220, 0x21f, 0x3, 0x2, 0x2, 0x2, 0x220, 0x221, 0x3, 0x2, 0x2, 0x2, 
    0x221, 0x222, 0x3, 0x2, 0x2, 0x2, 0x222, 0x228, 0x7, 0x28, 0x2, 0x2, 
    0x223, 0x225, 0x7, 0x28, 0x2, 0x2, 0x224, 0x226, 0x9, 0x6, 0x2, 0x2, 
    0x225, 0x224, 0x3, 0x2, 0x2, 0x2, 0x225, 0x226, 0x3, 0x2, 0x2, 0x2, 
    0x226, 0x228, 0x3, 0x2, 0x2, 0x2, 0x227, 0x220, 0x3, 0x2, 0x2, 0x2, 
    0x227, 0x223, 0x3, 0x2, 0x2, 0x2, 0x228, 0x22a, 0x3, 0x2, 0x2, 0x2, 
    0x229, 0x213, 0x3, 0x2, 0x2, 0x2, 0x229, 0x21d, 0x3, 0x2, 0x2, 0x2, 
    0x229, 0x227, 0x3, 0x2, 0x2, 0x2, 0x22a, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x22b, 
    0x22d, 0x9, 0x3, 0x2, 0x2, 0x22c, 0x22b, 0x3, 0x2, 0x2, 0x2, 0x22c, 
    0x22d, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22e, 0x3, 0x2, 0x2, 0x2, 0x22e, 
    0x22f, 0x7, 0x15, 0x2, 0x2, 0x22f, 0x232, 0x7, 0x36, 0x2, 0x2, 0x230, 
    0x232, 0x7, 0x73, 0x2, 0x2, 0x231, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x231, 
    0x230, 0x3, 0x2, 0x2, 0x2, 0x232, 0x4f, 0x3, 0x2, 0x2, 0x2, 0x233, 0x234, 
    0x7, 0x46, 0x2, 0x2, 0x234, 0x23d, 0x5, 0x68, 0x35, 0x2, 0x235, 0x236, 
    0x7, 0x61, 0x2, 0x2, 0x236, 0x237, 0x7, 0x7d, 0x2, 0x2, 0x237, 0x238, 
    0x5, 0x68, 0x35, 0x2, 0x238, 0x239, 0x7, 0x85, 0x2, 0x2, 0x239, 0x23d, 
    0x3, 0x2, 0x2, 0x2, 0x23a, 0x23b, 0x7, 0x61, 0x2, 0x2, 0x23b, 0x23d, 
    0x5, 0x68, 0x35, 0x2, 0x23c, 0x233, 0x3, 0x2, 0x2, 0x2, 0x23c, 0x235, 
    0x3, 0x2, 0x2, 0x2, 0x23c, 0x23a, 0x3, 0x2, 0x2, 0x2, 0x23d, 0x51, 0x3, 
    0x2, 0x2, 0x2, 0x23e, 0x241, 0x7, 0x6c, 0x2, 0x2, 0x23f, 0x240, 0x9, 
    0x7, 0x2, 0x2, 0x240, 0x242, 0x7, 0x6c, 0x2, 0x2, 0x241, 0x23f, 0x3, 
    0x2, 0x2, 0x2, 0x241, 0x242, 0x3, 0x2, 0x2, 0x2, 0x242, 0x53, 0x3, 0x2, 
    0x2, 0x2, 0x243, 0x248, 0x5, 0x56, 0x2c, 0x2, 0x244, 0x245, 0x7, 0x73, 
    0x2, 0x2, 0x245, 0x247, 0x5, 0x56, 0x2c, 0x2, 0x246, 0x244, 0x3, 0x2, 
    0x2, 0x2, 0x247, 0x24a, 0x3, 0x2, 0x2, 0x2, 0x248, 0x246, 0x3, 0x2, 
    0x2, 0x2, 0x248, 0x249, 0x3, 0x2, 0x2, 0x2, 0x249, 0x55, 0x3, 0x2, 0x2, 
    0x2, 0x24a, 0x248, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24d, 0x5, 0x6c, 0x37, 
    0x2, 0x24c, 0x24e, 0x9, 0x8, 0x2, 0x2, 0x24d, 0x24c, 0x3, 0x2, 0x2, 
    0x2, 0x24d, 0x24e, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x251, 0x3, 0x2, 0x2, 
    0x2, 0x24f, 0x250, 0x7, 0x44, 0x2, 0x2, 0x250, 0x252, 0x9, 0x9, 0x2, 
    0x2, 0x251, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x251, 0x252, 0x3, 0x2, 0x2, 
    0x2, 0x252, 0x255, 0x3, 0x2, 0x2, 0x2, 0x253, 0x254, 0x7, 0x13, 0x2, 
    0x2, 0x254, 0x256, 0x7, 0x6d, 0x2, 0x2, 0x255, 0x253, 0x3, 0x2, 0x2, 
    0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 0x2, 0x256, 0x57, 0x3, 0x2, 0x2, 0x2, 
    0x257, 0x258, 0x7, 0x6c, 0x2, 0x2, 0x258, 0x259, 0x7, 0x87, 0x2, 0x2, 
    0x259, 0x25a, 0x7, 0x6c, 0x2, 0x2, 0x25a, 0x59, 0x3, 0x2, 0x2, 0x2, 
    0x25b, 0x260, 0x5, 0x5c, 0x2f, 0x2, 0x25c, 0x25d, 0x7, 0x73, 0x2, 0x2, 
    0x25d, 0x25f, 0x5, 0x5c, 0x2f, 0x2, 0x25e, 0x25c, 0x3, 0x2, 0x2, 0x2, 
    0x25f, 0x262, 0x3, 0x2, 0x2, 0x2, 0x260, 0x25e, 0x3, 0x2, 0x2, 0x2, 
    0x260, 0x261, 0x3, 0x2, 0x2, 0x2, 0x261, 0x5b, 0x3, 0x2, 0x2, 0x2, 0x262, 
    0x260, 0x3, 0x2, 0x2, 0x2, 0x263, 0x264, 0x5, 0x86, 0x44, 0x2, 0x264, 
    0x265, 0x7, 0x78, 0x2, 0x2, 0x265, 0x266, 0x5, 0x82, 0x42, 0x2, 0x266, 
    0x5d, 0x3, 0x2, 0x2, 0x2, 0x267, 0x268, 0x7, 0x55, 0x2, 0x2, 0x268, 
    0x269, 0x5, 0x5a, 0x2e, 0x2, 0x269, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x26a, 
    0x26b, 0x7, 0x60, 0x2, 0x2, 0x26b, 0x26c, 0x5, 0x80, 0x41, 0x2, 0x26c, 
    0x61, 0x3, 0x2, 0x2, 0x2, 0x26d, 0x272, 0x5, 0x64, 0x33, 0x2, 0x26e, 
    0x26f, 0x7, 0x73, 0x2, 0x2, 0x26f, 0x271, 0x5, 0x64, 0x33, 0x2, 0x270, 
    0x26e, 0x3, 0x2, 0x2, 0x2, 0x271, 0x274, 0x3, 0x2, 0x2, 0x2, 0x272, 
    0x270, 0x3, 0x2, 0x2, 0x2, 0x272, 0x273, 0x3, 0x2, 0x2, 0x2, 0x273, 
    0x63, 0x3, 0x2, 0x2, 0x2, 0x274, 0x272, 0x3, 0x2, 0x2, 0x2, 0x275, 0x27d, 
    0x5, 0x82, 0x42, 0x2, 0x276, 0x27d, 0x5, 0x28, 0x15, 0x2, 0x277, 0x279, 
    0x7, 0x7b, 0x2, 0x2, 0x278, 0x27a, 0x5, 0x62, 0x32, 0x2, 0x279, 0x278, 
    0x3, 0x2, 0x2, 0x2, 0x279, 0x27a, 0x3, 0x2, 0x2, 0x2, 0x27a, 0x27b, 
    0x3, 0x2, 0x2, 0x2, 0x27b, 0x27d, 0x7, 0x84, 0x2, 0x2, 0x27c, 0x275, 
    0x3, 0x2, 0x2, 0x2, 0x27c, 0x276, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x277, 
    0x3, 0x2, 0x2, 0x2, 0x27d, 0x65, 0x3, 0x2, 0x2, 0x2, 0x27e, 0x2ac, 0x5, 
    0x86, 0x44, 0x2, 0x27f, 0x280, 0x5, 0x86, 0x44, 0x2, 0x280, 0x281, 0x7, 
    0x7d, 0x2, 0x2, 0x281, 0x282, 0x5, 0x6e, 0x38, 0x2, 0x282, 0x283, 0x7, 
    0x85, 0x2, 0x2, 0x283, 0x2ac, 0x3, 0x2, 0x2, 0x2, 0x284, 0x285, 0x5, 
    0x86, 0x44, 0x2, 0x285, 0x286, 0x7, 0x7d, 0x2, 0x2, 0x286, 0x28b, 0x5, 
    0x8c, 0x47, 0x2, 0x287, 0x288, 0x7, 0x73, 0x2, 0x2, 0x288, 0x28a, 0x5, 
    0x8c, 0x47, 0x2, 0x289, 0x287, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x28d, 0x3, 
    0x2, 0x2, 0x2, 0x28b, 0x289, 0x3, 0x2, 0x2, 0x2, 0x28b, 0x28c, 0x3, 
    0x2, 0x2, 0x2, 0x28c, 0x28e, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x28b, 0x3, 
    0x2, 0x2, 0x2, 0x28e, 0x28f, 0x7, 0x85, 0x2, 0x2, 0x28f, 0x2ac, 0x3, 
    0x2, 0x2, 0x2, 0x290, 0x291, 0x5, 0x86, 0x44, 0x2, 0x291, 0x292, 0x7, 
    0x7d, 0x2, 0x2, 0x292, 0x297, 0x5, 0x66, 0x34, 0x2, 0x293, 0x294, 0x7, 
    0x73, 0x2, 0x2, 0x294, 0x296, 0x5, 0x66, 0x34, 0x2, 0x295, 0x293, 0x3, 
    0x2, 0x2, 0x2, 0x296, 0x299, 0x3, 0x2, 0x2, 0x2, 0x297, 0x295, 0x3, 
    0x2, 0x2, 0x2, 0x297, 0x298, 0x3, 0x2, 0x2, 0x2, 0x298, 0x29a, 0x3, 
    0x2, 0x2, 0x2, 0x299, 0x297, 0x3, 0x2, 0x2, 0x2, 0x29a, 0x29b, 0x7, 
    0x85, 0x2, 0x2, 0x29b, 0x2ac, 0x3, 0x2, 0x2, 0x2, 0x29c, 0x29d, 0x5, 
    0x86, 0x44, 0x2, 0x29d, 0x29e, 0x7, 0x7d, 0x2, 0x2, 0x29e, 0x29f, 0x5, 
    0x86, 0x44, 0x2, 0x29f, 0x2a6, 0x5, 0x66, 0x34, 0x2, 0x2a0, 0x2a1, 0x7, 
    0x73, 0x2, 0x2, 0x2a1, 0x2a2, 0x5, 0x86, 0x44, 0x2, 0x2a2, 0x2a3, 0x5, 
    0x66, 0x34, 0x2, 0x2a3, 0x2a5, 0x3, 0x2, 0x2, 0x2, 0x2a4, 0x2a0, 0x3, 
    0x2, 0x2, 0x2, 0x2a5, 0x2a8, 0x3, 0x2, 0x2, 0x2, 0x2a6, 0x2a4, 0x3, 
    0x2, 0x2, 0x2, 0x2a6, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x2a7, 0x2a9, 0x3, 
    0x2, 0x2, 0x2, 0x2a8, 0x2a6, 0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2aa, 0x7, 
    0x85, 0x2, 0x2, 0x2aa, 0x2ac, 0x3, 0x2, 0x2, 0x2, 0x2ab, 0x27e, 0x3, 
    0x2, 0x2, 0x2, 0x2ab, 0x27f, 0x3, 0x2, 0x2, 0x2, 0x2ab, 0x284, 0x3, 
    0x2, 0x2, 0x2, 0x2ab, 0x290, 0x3, 0x2, 0x2, 0x2, 0x2ab, 0x29c, 0x3, 
    0x2, 0x2, 0x2, 0x2ac, 0x67, 0x3, 0x2, 0x2, 0x2, 0x2ad, 0x2b2, 0x5, 0x6a, 
    0x36, 0x2, 0x2ae, 0x2af, 0x7, 0x73, 0x2, 0x2, 0x2af, 0x2b1, 0x5, 0x6a, 
    0x36, 0x2, 0x2b0, 0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2b1, 0x2b4, 0x3, 0x2, 
    0x2, 0x2, 0x2b2, 0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x2b3, 0x3, 0x2, 
    0x2, 0x2, 0x2b3, 0x69, 0x3, 0x2, 0x2, 0x2, 0x2b4, 0x2b2, 0x3, 0x2, 0x2, 
    0x2, 0x2b5, 0x2b6, 0x5, 0x7a, 0x3e, 0x2, 0x2b6, 0x2b7, 0x7, 0x76, 0x2, 
    0x2, 0x2b7, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b5, 0x3, 0x2, 0x2, 
    0x2, 0x2b8, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2b9, 0x2ba, 0x3, 0x2, 0x2, 
    0x2, 0x2ba, 0x2c1, 0x7, 0x6f, 0x2, 0x2, 0x2bb, 0x2bc, 0x7, 0x7d, 0x2, 
    0x2, 0x2bc, 0x2bd, 0x5, 0x2e, 0x18, 0x2, 0x2bd, 0x2be, 0x7, 0x85, 0x2, 
    0x2, 0x2be, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2bf, 0x2c1, 0x5, 0x6c, 0x37, 
    0x2, 0x2c0, 0x2b8, 0x3, 0x2, 0x2, 0x2, 0x2c0, 0x2bb, 0x3, 0x2, 0x2, 
    0x2, 0x2c0, 0x2bf, 0x3, 0x2, 0x2, 0x2, 0x2c1, 0x6b, 0x3, 0x2, 0x2, 0x2, 
    0x2c2, 0x2c3, 0x8, 0x37, 0x1, 0x2, 0x2c3, 0x314, 0x5, 0x82, 0x42, 0x2, 
    0x2c4, 0x2c5, 0x5, 0x7a, 0x3e, 0x2, 0x2c5, 0x2c6, 0x7, 0x76, 0x2, 0x2, 
    0x2c6, 0x2c8, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c4, 0x3, 0x2, 0x2, 0x2, 
    0x2c7, 0x2c8, 0x3, 0x2, 0x2, 0x2, 0x2c8, 0x2c9, 0x3, 0x2, 0x2, 0x2, 
    0x2c9, 0x314, 0x7, 0x6f, 0x2, 0x2, 0x2ca, 0x2cb, 0x7, 0x7d, 0x2, 0x2, 
    0x2cb, 0x2cc, 0x5, 0x2e, 0x18, 0x2, 0x2cc, 0x2cd, 0x7, 0x85, 0x2, 0x2, 
    0x2cd, 0x314, 0x3, 0x2, 0x2, 0x2, 0x2ce, 0x2cf, 0x7, 0x7d, 0x2, 0x2, 
    0x2cf, 0x2d0, 0x5, 0x6c, 0x37, 0x2, 0x2d0, 0x2d1, 0x7, 0x85, 0x2, 0x2, 
    0x2d1, 0x314, 0x3, 0x2, 0x2, 0x2, 0x2d2, 0x2d3, 0x7, 0x7d, 0x2, 0x2, 
    0x2d3, 0x2d4, 0x5, 0x68, 0x35, 0x2, 0x2d4, 0x2d5, 0x7, 0x85, 0x2, 0x2, 
    0x2d5, 0x314, 0x3, 0x2, 0x2, 0x2, 0x2d6, 0x2d8, 0x7, 0x7b, 0x2, 0x2, 
    0x2d7, 0x2d9, 0x5, 0x68, 0x35, 0x2, 0x2d8, 0x2d7, 0x3, 0x2, 0x2, 0x2, 
    0x2d8, 0x2d9, 0x3, 0x2, 0x2, 0x2, 0x2d9, 0x2da, 0x3, 0x2, 0x2, 0x2, 
    0x2da, 0x314, 0x7, 0x84, 0x2, 0x2, 0x2db, 0x2dd, 0x7, 0x10, 0x2, 0x2, 
    0x2dc, 0x2de, 0x5, 0x6c, 0x37, 0x2, 0x2dd, 0x2dc, 0x3, 0x2, 0x2, 0x2, 
    0x2dd, 0x2de, 0x3, 0x2, 0x2, 0x2, 0x2de, 0x2e4, 0x3, 0x2, 0x2, 0x2, 
    0x2df, 0x2e0, 0x7, 0x65, 0x2, 0x2, 0x2e0, 0x2e1, 0x5, 0x6c, 0x37, 0x2, 
    0x2e1, 0x2e2, 0x7, 0x59, 0x2, 0x2, 0x2e2, 0x2e3, 0x5, 0x6c, 0x37, 0x2, 
    0x2e3, 0x2e5, 0x3, 0x2, 0x2, 0x2, 0x2e4, 0x2df, 0x3, 0x2, 0x2, 0x2, 
    0x2e5, 0x2e6, 0x3, 0x2, 0x2, 0x2, 0x2e6, 0x2e4, 0x3, 0x2, 0x2, 0x2, 
    0x2e6, 0x2e7, 0x3, 0x2, 0x2, 0x2, 0x2e7, 0x2ea, 0x3, 0x2, 0x2, 0x2, 
    0x2e8, 0x2e9, 0x7, 0x1f, 0x2, 0x2, 0x2e9, 0x2eb, 0x5, 0x6c, 0x37, 0x2, 
    0x2ea, 0x2e8, 0x3, 0x2, 0x2, 0x2, 0x2ea, 0x2eb, 0x3, 0x2, 0x2, 0x2, 
    0x2eb, 0x2ec, 0x3, 0x2, 0x2, 0x2, 0x2ec, 0x2ed, 0x7, 0x20, 0x2, 0x2, 
    0x2ed, 0x314, 0x3, 0x2, 0x2, 0x2, 0x2ee, 0x2ef, 0x7, 0x23, 0x2, 0x2, 
    0x2ef, 0x2f0, 0x7, 0x7d, 0x2, 0x2, 0x2f0, 0x2f1, 0x7, 0x3, 0x2, 0x2, 
    0x2f1, 0x2f2, 0x7, 0x27, 0x2, 0x2, 0x2f2, 0x2f3, 0x5, 0x6c, 0x37, 0x2, 
    0x2f3, 0x2f4, 0x7, 0x85, 0x2, 0x2, 0x2f4, 0x314, 0x3, 0x2, 0x2, 0x2, 
    0x2f5, 0x2f6, 0x7, 0x5d, 0x2, 0x2, 0x2f6, 0x2f7, 0x7, 0x7d, 0x2, 0x2, 
    0x2f7, 0x2f8, 0x9, 0xa, 0x2, 0x2, 0x2f8, 0x2f9, 0x7, 0x6d, 0x2, 0x2, 
    0x2f9, 0x2fa, 0x7, 0x27, 0x2, 0x2, 0x2fa, 0x2fb, 0x5, 0x6c, 0x37, 0x2, 
    0x2fb, 0x2fc, 0x7, 0x85, 0x2, 0x2, 0x2fc, 0x314, 0x3, 0x2, 0x2, 0x2, 
    0x2fd, 0x2fe, 0x7, 0x33, 0x2, 0x2, 0x2fe, 0x2ff, 0x5, 0x6c, 0x37, 0x2, 
    0x2ff, 0x300, 0x7, 0x3, 0x2, 0x2, 0x300, 0x314, 0x3, 0x2, 0x2, 0x2, 
    0x301, 0x314, 0x5, 0x76, 0x3c, 0x2, 0x302, 0x308, 0x5, 0x86, 0x44, 0x2, 
    0x303, 0x305, 0x7, 0x7d, 0x2, 0x2, 0x304, 0x306, 0x5, 0x6e, 0x38, 0x2, 
    0x305, 0x304, 0x3, 0x2, 0x2, 0x2, 0x305, 0x306, 0x3, 0x2, 0x2, 0x2, 
    0x306, 0x307, 0x3, 0x2, 0x2, 0x2, 0x307, 0x309, 0x7, 0x85, 0x2, 0x2, 
    0x308, 0x303, 0x3, 0x2, 0x2, 0x2, 0x308, 0x309, 0x3, 0x2, 0x2, 0x2, 
    0x309, 0x30a, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x30c, 0x7, 0x7d, 0x2, 0x2, 
    0x30b, 0x30d, 0x5, 0x70, 0x39, 0x2, 0x30c, 0x30b, 0x3, 0x2, 0x2, 0x2, 
    0x30c, 0x30d, 0x3, 0x2, 0x2, 0x2, 0x30d, 0x30e, 0x3, 0x2, 0x2, 0x2, 
    0x30e, 0x30f, 0x7, 0x85, 0x2, 0x2, 0x30f, 0x314, 0x3, 0x2, 0x2, 0x2, 
    0x310, 0x311, 0x5, 0x88, 0x45, 0x2, 0x311, 0x312, 0x5, 0x6c, 0x37, 0x8, 
    0x312, 0x314, 0x3, 0x2, 0x2, 0x2, 0x313, 0x2c2, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x2c7, 0x3, 0x2, 0x2, 0x2, 0x313, 0x2ca, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x313, 0x2d2, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x2d6, 0x3, 0x2, 0x2, 0x2, 0x313, 0x2db, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x2ee, 0x3, 0x2, 0x2, 0x2, 0x313, 0x2f5, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x2fd, 0x3, 0x2, 0x2, 0x2, 0x313, 0x301, 0x3, 0x2, 0x2, 0x2, 
    0x313, 0x302, 0x3, 0x2, 0x2, 0x2, 0x313, 0x310, 0x3, 0x2, 0x2, 0x2, 
    0x314, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x315, 0x316, 0xc, 0x6, 0x2, 0x2, 
    0x316, 0x317, 0x5, 0x8a, 0x46, 0x2, 0x317, 0x318, 0x5, 0x6c, 0x37, 0x7, 
    0x318, 0x33a, 0x3, 0x2, 0x2, 0x2, 0x319, 0x31a, 0xc, 0x5, 0x2, 0x2, 
    0x31a, 0x31b, 0x7, 0x82, 0x2, 0x2, 0x31b, 0x31c, 0x5, 0x6c, 0x37, 0x2, 
    0x31c, 0x31d, 0x7, 0x72, 0x2, 0x2, 0x31d, 0x31e, 0x5, 0x6c, 0x37, 0x6, 
    0x31e, 0x33a, 0x3, 0x2, 0x2, 0x2, 0x31f, 0x321, 0xc, 0x4, 0x2, 0x2, 
    0x320, 0x322, 0x7, 0x42, 0x2, 0x2, 0x321, 0x320, 0x3, 0x2, 0x2, 0x2, 
    0x321, 0x322, 0x3, 0x2, 0x2, 0x2, 0x322, 0x323, 0x3, 0x2, 0x2, 0x2, 
    0x323, 0x324, 0x7, 0xd, 0x2, 0x2, 0x324, 0x325, 0x5, 0x6c, 0x37, 0x2, 
    0x325, 0x326, 0x7, 0x6, 0x2, 0x2, 0x326, 0x327, 0x5, 0x6c, 0x37, 0x5, 
    0x327, 0x33a, 0x3, 0x2, 0x2, 0x2, 0x328, 0x329, 0xc, 0xa, 0x2, 0x2, 
    0x329, 0x32a, 0x7, 0x7b, 0x2, 0x2, 0x32a, 0x32b, 0x5, 0x6c, 0x37, 0x2, 
    0x32b, 0x32c, 0x7, 0x84, 0x2, 0x2, 0x32c, 0x33a, 0x3, 0x2, 0x2, 0x2, 
    0x32d, 0x32e, 0xc, 0x9, 0x2, 0x2, 0x32e, 0x32f, 0x7, 0x76, 0x2, 0x2, 
    0x32f, 0x33a, 0x7, 0x6c, 0x2, 0x2, 0x330, 0x331, 0xc, 0x7, 0x2, 0x2, 
    0x331, 0x333, 0x7, 0x35, 0x2, 0x2, 0x332, 0x334, 0x7, 0x42, 0x2, 0x2, 
    0x333, 0x332, 0x3, 0x2, 0x2, 0x2, 0x333, 0x334, 0x3, 0x2, 0x2, 0x2, 
    0x334, 0x335, 0x3, 0x2, 0x2, 0x2, 0x335, 0x33a, 0x7, 0x43, 0x2, 0x2, 
    0x336, 0x337, 0xc, 0x3, 0x2, 0x2, 0x337, 0x338, 0x7, 0xa, 0x2, 0x2, 
    0x338, 0x33a, 0x5, 0x86, 0x44, 0x2, 0x339, 0x315, 0x3, 0x2, 0x2, 0x2, 
    0x339, 0x319, 0x3, 0x2, 0x2, 0x2, 0x339, 0x31f, 0x3, 0x2, 0x2, 0x2, 
    0x339, 0x328, 0x3, 0x2, 0x2, 0x2, 0x339, 0x32d, 0x3, 0x2, 0x2, 0x2, 
    0x339, 0x330, 0x3, 0x2, 0x2, 0x2, 0x339, 0x336, 0x3, 0x2, 0x2, 0x2, 
    0x33a, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x33b, 0x339, 0x3, 0x2, 0x2, 0x2, 
    0x33b, 0x33c, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x33d, 
    0x33b, 0x3, 0x2, 0x2, 0x2, 0x33e, 0x343, 0x5, 0x82, 0x42, 0x2, 0x33f, 
    0x340, 0x7, 0x73, 0x2, 0x2, 0x340, 0x342, 0x5, 0x82, 0x42, 0x2, 0x341, 
    0x33f, 0x3, 0x2, 0x2, 0x2, 0x342, 0x345, 0x3, 0x2, 0x2, 0x2, 0x343, 
    0x341, 0x3, 0x2, 0x2, 0x2, 0x343, 0x344, 0x3, 0x2, 0x2, 0x2, 0x344, 
    0x6f, 0x3, 0x2, 0x2, 0x2, 0x345, 0x343, 0x3, 0x2, 0x2, 0x2, 0x346, 0x34b, 
    0x5, 0x72, 0x3a, 0x2, 0x347, 0x348, 0x7, 0x73, 0x2, 0x2, 0x348, 0x34a, 
    0x5, 0x72, 0x3a, 0x2, 0x349, 0x347, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x34d, 
    0x3, 0x2, 0x2, 0x2, 0x34b, 0x349, 0x3, 0x2, 0x2, 0x2, 0x34b, 0x34c, 
    0x3, 0x2, 0x2, 0x2, 0x34c, 0x71, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x34b, 0x3, 
    0x2, 0x2, 0x2, 0x34e, 0x351, 0x5, 0x74, 0x3b, 0x2, 0x34f, 0x351, 0x5, 
    0x6c, 0x37, 0x2, 0x350, 0x34e, 0x3, 0x2, 0x2, 0x2, 0x350, 0x34f, 0x3, 
    0x2, 0x2, 0x2, 0x351, 0x73, 0x3, 0x2, 0x2, 0x2, 0x352, 0x353, 0x7, 0x7d, 
    0x2, 0x2, 0x353, 0x358, 0x5, 0x86, 0x44, 0x2, 0x354, 0x355, 0x7, 0x73, 
    0x2, 0x2, 0x355, 0x357, 0x5, 0x86, 0x44, 0x2, 0x356, 0x354, 0x3, 0x2, 
    0x2, 0x2, 0x357, 0x35a, 0x3, 0x2, 0x2, 0x2, 0x358, 0x356, 0x3, 0x2, 
    0x2, 0x2, 0x358, 0x359, 0x3, 0x2, 0x2, 0x2, 0x359, 0x35b, 0x3, 0x2, 
    0x2, 0x2, 0x35a, 0x358, 0x3, 0x2, 0x2, 0x2, 0x35b, 0x35c, 0x7, 0x85, 
    0x2, 0x2, 0x35c, 0x366, 0x3, 0x2, 0x2, 0x2, 0x35d, 0x362, 0x5, 0x86, 
    0x44, 0x2, 0x35e, 0x35f, 0x7, 0x73, 0x2, 0x2, 0x35f, 0x361, 0x5, 0x86, 
    0x44, 0x2, 0x360, 0x35e, 0x3, 0x2, 0x2, 0x2, 0x361, 0x364, 0x3, 0x2, 
    0x2, 0x2, 0x362, 0x360, 0x3, 0x2, 0x2, 0x2, 0x362, 0x363, 0x3, 0x2, 
    0x2, 0x2, 0x363, 0x366, 0x3, 0x2, 0x2, 0x2, 0x364, 0x362, 0x3, 0x2, 
    0x2, 0x2, 0x365, 0x352, 0x3, 0x2, 0x2, 0x2, 0x365, 0x35d, 0x3, 0x2, 
    0x2, 0x2, 0x366, 0x367, 0x3, 0x2, 0x2, 0x2, 0x367, 0x368, 0x7, 0x6e, 
    0x2, 0x2, 0x368, 0x369, 0x5, 0x6c, 0x37, 0x2, 0x369, 0x75, 0x3, 0x2, 
    0x2, 0x2, 0x36a, 0x36b, 0x5, 0x7a, 0x3e, 0x2, 0x36b, 0x36c, 0x7, 0x76, 
    0x2, 0x2, 0x36c, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36d, 0x36a, 0x3, 0x2, 
    0x2, 0x2, 0x36d, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x36f, 0x3, 0x2, 
    0x2, 0x2, 0x36f, 0x372, 0x5, 0x86, 0x44, 0x2, 0x370, 0x371, 0x7, 0x76, 
    0x2, 0x2, 0x371, 0x373, 0x5, 0x86, 0x44, 0x2, 0x372, 0x370, 0x3, 0x2, 
    0x2, 0x2, 0x372, 0x373, 0x3, 0x2, 0x2, 0x2, 0x373, 0x77, 0x3, 0x2, 0x2, 
    0x2, 0x374, 0x375, 0x8, 0x3d, 0x1, 0x2, 0x375, 0x382, 0x5, 0x7a, 0x3e, 
    0x2, 0x376, 0x377, 0x5, 0x86, 0x44, 0x2, 0x377, 0x379, 0x7, 0x7d, 0x2, 
    0x2, 0x378, 0x37a, 0x5, 0x7c, 0x3f, 0x2, 0x379, 0x378, 0x3, 0x2, 0x2, 
    0x2, 0x379, 0x37a, 0x3, 0x2, 0x2, 0x2, 0x37a, 0x37b, 0x3, 0x2, 0x2, 
    0x2, 0x37b, 0x37c, 0x7, 0x85, 0x2, 0x2, 0x37c, 0x382, 0x3, 0x2, 0x2, 
    0x2, 0x37d, 0x37e, 0x7, 0x7d, 0x2, 0x2, 0x37e, 0x37f, 0x5, 0x2e, 0x18, 
    0x2, 0x37f, 0x380, 0x7, 0x85, 0x2, 0x2, 0x380, 0x382, 0x3, 0x2, 0x2, 
    0x2, 0x381, 0x374, 0x3, 0x2, 0x2, 0x2, 0x381, 0x376, 0x3, 0x2, 0x2, 
    0x2, 0x381, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x382, 0x38a, 0x3, 0x2, 0x2, 
    0x2, 0x383, 0x385, 0xc, 0x3, 0x2, 0x2, 0x384, 0x386, 0x7, 0xa, 0x2, 
    0x2, 0x385, 0x384, 0x3, 0x2, 0x2, 0x2, 0x385, 0x386, 0x3, 0x2, 0x2, 
    0x2, 0x386, 0x387, 0x3, 0x2, 0x2, 0x2, 0x387, 0x389, 0x5, 0x86, 0x44, 
    0x2, 0x388, 0x383, 0x3, 0x2, 0x2, 0x2, 0x389, 0x38c, 0x3, 0x2, 0x2, 
    0x2, 0x38a, 0x388, 0x3, 0x2, 0x2, 0x2, 0x38a, 0x38b, 0x3, 0x2, 0x2, 
    0x2, 0x38b, 0x79, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38a, 0x3, 0x2, 0x2, 0x2, 
    0x38d, 0x38e, 0x5, 0x80, 0x41, 0x2, 0x38e, 0x38f, 0x7, 0x76, 0x2, 0x2, 
    0x38f, 0x391, 0x3, 0x2, 0x2, 0x2, 0x390, 0x38d, 0x3, 0x2, 0x2, 0x2, 
    0x390, 0x391, 0x3, 0x2, 0x2, 0x2, 0x391, 0x392, 0x3, 0x2, 0x2, 0x2, 
    0x392, 0x393, 0x5, 0x86, 0x44, 0x2, 0x393, 0x7b, 0x3, 0x2, 0x2, 0x2, 
    0x394, 0x399, 0x5, 0x7e, 0x40, 0x2, 0x395, 0x396, 0x7, 0x73, 0x2, 0x2, 
    0x396, 0x398, 0x5, 0x7e, 0x40, 0x2, 0x397, 0x395, 0x3, 0x2, 0x2, 0x2, 
    0x398, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x399, 0x397, 0x3, 0x2, 0x2, 0x2, 
    0x399, 0x39a, 0x3, 0x2, 0x2, 0x2, 0x39a, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x39b, 
    0x399, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x39f, 0x5, 0x82, 0x42, 0x2, 0x39d, 
    0x39f, 0x5, 0x7a, 0x3e, 0x2, 0x39e, 0x39c, 0x3, 0x2, 0x2, 0x2, 0x39e, 
    0x39d, 0x3, 0x2, 0x2, 0x2, 0x39f, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x3a0, 0x3a1, 
    0x5, 0x86, 0x44, 0x2, 0x3a1, 0x81, 0x3, 0x2, 0x2, 0x2, 0x3a2, 0x3a4, 
    0x9, 0xb, 0x2, 0x2, 0x3a3, 0x3a2, 0x3, 0x2, 0x2, 0x2, 0x3a3, 0x3a4, 
    0x3, 0x2, 0x2, 0x2, 0x3a4, 0x3a5, 0x3, 0x2, 0x2, 0x2, 0x3a5, 0x3ad, 
    0x9, 0xc, 0x2, 0x2, 0x3a6, 0x3ad, 0x7, 0x6d, 0x2, 0x2, 0x3a7, 0x3ad, 
    0x7, 0x43, 0x2, 0x2, 0x3a8, 0x3a9, 0x5, 0x86, 0x44, 0x2, 0x3a9, 0x3aa, 
    0x7, 0x7d, 0x2, 0x2, 0x3aa, 0x3ab, 0x7, 0x85, 0x2, 0x2, 0x3ab, 0x3ad, 
    0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3a6, 
    0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3a7, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3a8, 
    0x3, 0x2, 0x2, 0x2, 0x3ad, 0x83, 0x3, 0x2, 0x2, 0x2, 0x3ae, 0x3af, 0x9, 
    0xd, 0x2, 0x2, 0x3af, 0x85, 0x3, 0x2, 0x2, 0x2, 0x3b0, 0x3b4, 0x7, 0x69, 
    0x2, 0x2, 0x3b1, 0x3b4, 0x7, 0x3, 0x2, 0x2, 0x3b2, 0x3b4, 0x5, 0x84, 
    0x43, 0x2, 0x3b3, 0x3b0, 0x3, 0x2, 0x2, 0x2, 0x3b3, 0x3b1, 0x3, 0x2, 
    0x2, 0x2, 0x3b3, 0x3b2, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x87, 0x3, 0x2, 0x2, 
    0x2, 0x3b5, 0x3b6, 0x9, 0xe, 0x2, 0x2, 0x3b6, 0x89, 0x3, 0x2, 0x2, 0x2, 
    0x3b7, 0x3d2, 0x7, 0x74, 0x2, 0x2, 0x3b8, 0x3d2, 0x7, 0x6f, 0x2, 0x2, 
    0x3b9, 0x3d2, 0x7, 0x87, 0x2, 0x2, 0x3ba, 0x3d2, 0x7, 0x81, 0x2, 0x2, 
    0x3bb, 0x3d2, 0x7, 0x75, 0x2, 0x2, 0x3bc, 0x3d2, 0x7, 0x80, 0x2, 0x2, 
    0x3bd, 0x3d2, 0x7, 0x77, 0x2, 0x2, 0x3be, 0x3d2, 0x7, 0x78, 0x2, 0x2, 
    0x3bf, 0x3d2, 0x7, 0x7f, 0x2, 0x2, 0x3c0, 0x3d2, 0x7, 0x7c, 0x2, 0x2, 
    0x3c1, 0x3d2, 0x7, 0x79, 0x2, 0x2, 0x3c2, 0x3d2, 0x7, 0x7e, 0x2, 0x2, 
    0x3c3, 0x3d2, 0x7, 0x7a, 0x2, 0x2, 0x3c4, 0x3d2, 0x7, 0x6, 0x2, 0x2, 
    0x3c5, 0x3d2, 0x7, 0x48, 0x2, 0x2, 0x3c6, 0x3c8, 0x7, 0x42, 0x2, 0x2, 
    0x3c7, 0x3c6, 0x3, 0x2, 0x2, 0x2, 0x3c7, 0x3c8, 0x3, 0x2, 0x2, 0x2, 
    0x3c8, 0x3c9, 0x3, 0x2, 0x2, 0x2, 0x3c9, 0x3d2, 0x7, 0x3b, 0x2, 0x2, 
    0x3ca, 0x3cc, 0x7, 0x29, 0x2, 0x2, 0x3cb, 0x3ca, 0x3, 0x2, 0x2, 0x2, 
    0x3cb, 0x3cc, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3ce, 0x3, 0x2, 0x2, 0x2, 
    0x3cd, 0x3cf, 0x7, 0x42, 0x2, 0x2, 0x3ce, 0x3cd, 0x3, 0x2, 0x2, 0x2, 
    0x3ce, 0x3cf, 0x3, 0x2, 0x2, 0x2, 0x3cf, 0x3d0, 0x3, 0x2, 0x2, 0x2, 
    0x3d0, 0x3d2, 0x7, 0x2f, 0x2, 0x2, 0x3d1, 0x3b7, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3b8, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3b9, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3ba, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3bb, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3bc, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3bd, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3be, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3bf, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3c0, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3c1, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3c3, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3c4, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3c5, 0x3, 0x2, 0x2, 0x2, 
    0x3d1, 0x3c7, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3cb, 0x3, 0x2, 0x2, 0x2, 
    0x3d2, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x3d3, 0x3d4, 0x7, 0x6d, 0x2, 0x2, 
    0x3d4, 0x3d5, 0x7, 0x78, 0x2, 0x2, 0x3d5, 0x3d6, 0x7, 0x6c, 0x2, 0x2, 
    0x3d6, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x84, 0x93, 0x97, 0x9f, 0xa3, 0xaa, 
    0xb0, 0xb5, 0xbc, 0xc0, 0xc7, 0xd2, 0xd9, 0xe0, 0xe6, 0xea, 0xee, 0xf1, 
    0xf4, 0xf7, 0xfa, 0xfd, 0x111, 0x116, 0x11b, 0x11e, 0x123, 0x127, 0x12b, 
    0x130, 0x132, 0x13f, 0x145, 0x14a, 0x14f, 0x152, 0x15d, 0x162, 0x169, 
    0x16e, 0x172, 0x17c, 0x17f, 0x182, 0x189, 0x191, 0x195, 0x199, 0x19d, 
    0x1a0, 0x1a3, 0x1a6, 0x1a9, 0x1ac, 0x1af, 0x1b2, 0x1b5, 0x1b8, 0x1bb, 
    0x1c3, 0x1c9, 0x1cc, 0x1dd, 0x1f7, 0x1ff, 0x206, 0x208, 0x20c, 0x211, 
    0x213, 0x216, 0x21b, 0x21d, 0x220, 0x225, 0x227, 0x229, 0x22c, 0x231, 
    0x23c, 0x241, 0x248, 0x24d, 0x251, 0x255, 0x260, 0x272, 0x279, 0x27c, 
    0x28b, 0x297, 0x2a6, 0x2ab, 0x2b2, 0x2b8, 0x2c0, 0x2c7, 0x2d8, 0x2dd, 
    0x2e6, 0x2ea, 0x305, 0x308, 0x30c, 0x313, 0x321, 0x333, 0x339, 0x33b, 
    0x343, 0x34b, 0x350, 0x358, 0x362, 0x365, 0x36d, 0x372, 0x379, 0x381, 
    0x385, 0x38a, 0x390, 0x399, 0x39e, 0x3a3, 0x3ac, 0x3b3, 0x3c7, 0x3cb, 
    0x3ce, 0x3d1, 
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
