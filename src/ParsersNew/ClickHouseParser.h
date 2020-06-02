
// Generated from ClickHouseParser.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseParser : public antlr4::Parser {
public:
  enum {
    LINE_COMMENT = 1, WHITESPACE = 2, ARROW = 3, ASTERISK = 4, BACKQUOTE = 5,
    BACKSLASH = 6, COLON = 7, COMMA = 8, CONCAT = 9, DASH = 10, DOT = 11,
    EQ = 12, EQ_DOUBLE = 13, EQ_SINGLE = 14, GE = 15, GT = 16, LBRACKET = 17,
    LE = 18, LPAREN = 19, LT = 20, NOT_EQ = 21, PERCENT = 22, PLUS = 23,
    QUERY = 24, QUOTE_SINGLE = 25, RBRACKET = 26, RPAREN = 27, SEMICOLON = 28,
    SLASH = 29, UNDERSCORE = 30, IDENTIFIER = 31, LITERAL = 32, NUMBER_LITERAL = 33,
    STRING_LITERAL = 34, ALL = 35, AND = 36, ARRAY = 37, AS = 38, ASCENDING = 39,
    BETWEEN = 40, BOTH = 41, BY = 42, CASE = 43, CAST = 44, COLLATE = 45,
    DAY = 46, DESCENDING = 47, DISTINCT = 48, ELSE = 49, END = 50, EXTRACT = 51,
    FINAL = 52, FIRST = 53, FORMAT = 54, FROM = 55, GLOBAL = 56, GROUP = 57,
    HAVING = 58, HOUR = 59, IN = 60, INTERVAL = 61, INTO = 62, IS = 63,
    JOIN = 64, LAST = 65, LEADING = 66, LEFT = 67, LIKE = 68, LIMIT = 69,
    MINUTE = 70, MONTH = 71, NOT = 72, NULL_SQL = 73, NULLS = 74, OFFSET = 75,
    OR = 76, ORDER = 77, OUTFILE = 78, PREWHERE = 79, QUARTER = 80, SAMPLE = 81,
    SECOND = 82, SELECT = 83, SETTINGS = 84, THEN = 85, TOTALS = 86, TRAILING = 87,
    TRIM = 88, UNION = 89, WEEK = 90, WHEN = 91, WHERE = 92, WITH = 93,
    YEAR = 94, INTERVAL_TYPE = 95
  };

  enum {
    RuleQueryList = 0, RuleQueryStmt = 1, RuleSelectUnionStmt = 2, RuleSelectStmt = 3,
    RuleWithClause = 4, RuleFromClause = 5, RuleSampleClause = 6, RuleArrayJoinClause = 7,
    RulePrewhereClause = 8, RuleWhereClause = 9, RuleGroupByClause = 10,
    RuleHavingClause = 11, RuleOrderByClause = 12, RuleLimitByClause = 13,
    RuleLimitClause = 14, RuleSettingsClause = 15, RuleJoinExpr = 16, RuleLimitExpr = 17,
    RuleOrderExprList = 18, RuleOrderExpr = 19, RuleRatioExpr = 20, RuleSettingExprList = 21,
    RuleSettingExpr = 22, RuleColumnExprList = 23, RuleColumnExpr = 24,
    RuleColumnFunctionExpr = 25, RuleColumnArgList = 26, RuleColumnArgExpr = 27,
    RuleColumnLambdaExpr = 28, RuleColumnIdentifier = 29, RuleTableIdentifier = 30,
    RuleDatabaseIdentifier = 31, RuleIdentifier = 32, RuleUnaryOp = 33,
    RuleBinaryOp = 34
  };

  ClickHouseParser(antlr4::TokenStream *input);
  ~ClickHouseParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


  class QueryListContext;
  class QueryStmtContext;
  class SelectUnionStmtContext;
  class SelectStmtContext;
  class WithClauseContext;
  class FromClauseContext;
  class SampleClauseContext;
  class ArrayJoinClauseContext;
  class PrewhereClauseContext;
  class WhereClauseContext;
  class GroupByClauseContext;
  class HavingClauseContext;
  class OrderByClauseContext;
  class LimitByClauseContext;
  class LimitClauseContext;
  class SettingsClauseContext;
  class JoinExprContext;
  class LimitExprContext;
  class OrderExprListContext;
  class OrderExprContext;
  class RatioExprContext;
  class SettingExprListContext;
  class SettingExprContext;
  class ColumnExprListContext;
  class ColumnExprContext;
  class ColumnFunctionExprContext;
  class ColumnArgListContext;
  class ColumnArgExprContext;
  class ColumnLambdaExprContext;
  class ColumnIdentifierContext;
  class TableIdentifierContext;
  class DatabaseIdentifierContext;
  class IdentifierContext;
  class UnaryOpContext;
  class BinaryOpContext;

  class  QueryListContext : public antlr4::ParserRuleContext {
  public:
    QueryListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<QueryStmtContext *> queryStmt();
    QueryStmtContext* queryStmt(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SEMICOLON();
    antlr4::tree::TerminalNode* SEMICOLON(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  QueryListContext* queryList();

  class  QueryStmtContext : public antlr4::ParserRuleContext {
  public:
    QueryStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectUnionStmtContext *selectUnionStmt();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  QueryStmtContext* queryStmt();

  class  SelectUnionStmtContext : public antlr4::ParserRuleContext {
  public:
    SelectUnionStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SelectStmtContext *> selectStmt();
    SelectStmtContext* selectStmt(size_t i);
    std::vector<antlr4::tree::TerminalNode *> UNION();
    antlr4::tree::TerminalNode* UNION(size_t i);
    std::vector<antlr4::tree::TerminalNode *> ALL();
    antlr4::tree::TerminalNode* ALL(size_t i);
    antlr4::tree::TerminalNode *INTO();
    antlr4::tree::TerminalNode *OUTFILE();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *FORMAT();
    IdentifierContext *identifier();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SelectUnionStmtContext* selectUnionStmt();

  class  SelectStmtContext : public antlr4::ParserRuleContext {
  public:
    SelectStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SELECT();
    ColumnExprListContext *columnExprList();
    WithClauseContext *withClause();
    antlr4::tree::TerminalNode *DISTINCT();
    FromClauseContext *fromClause();
    SampleClauseContext *sampleClause();
    ArrayJoinClauseContext *arrayJoinClause();
    PrewhereClauseContext *prewhereClause();
    WhereClauseContext *whereClause();
    GroupByClauseContext *groupByClause();
    HavingClauseContext *havingClause();
    OrderByClauseContext *orderByClause();
    LimitByClauseContext *limitByClause();
    LimitClauseContext *limitClause();
    SettingsClauseContext *settingsClause();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SelectStmtContext* selectStmt();

  class  WithClauseContext : public antlr4::ParserRuleContext {
  public:
    WithClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH();
    ColumnExprListContext *columnExprList();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  WithClauseContext* withClause();

  class  FromClauseContext : public antlr4::ParserRuleContext {
  public:
    FromClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM();
    JoinExprContext *joinExpr();
    antlr4::tree::TerminalNode *FINAL();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  FromClauseContext* fromClause();

  class  SampleClauseContext : public antlr4::ParserRuleContext {
  public:
    SampleClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SAMPLE();
    std::vector<RatioExprContext *> ratioExpr();
    RatioExprContext* ratioExpr(size_t i);
    antlr4::tree::TerminalNode *OFFSET();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SampleClauseContext* sampleClause();

  class  ArrayJoinClauseContext : public antlr4::ParserRuleContext {
  public:
    ArrayJoinClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ARRAY();
    antlr4::tree::TerminalNode *JOIN();
    ColumnExprListContext *columnExprList();
    antlr4::tree::TerminalNode *LEFT();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ArrayJoinClauseContext* arrayJoinClause();

  class  PrewhereClauseContext : public antlr4::ParserRuleContext {
  public:
    PrewhereClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PREWHERE();
    ColumnExprContext *columnExpr();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  PrewhereClauseContext* prewhereClause();

  class  WhereClauseContext : public antlr4::ParserRuleContext {
  public:
    WhereClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE();
    ColumnExprContext *columnExpr();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  WhereClauseContext* whereClause();

  class  GroupByClauseContext : public antlr4::ParserRuleContext {
  public:
    GroupByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP();
    antlr4::tree::TerminalNode *BY();
    ColumnExprListContext *columnExprList();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *TOTALS();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  GroupByClauseContext* groupByClause();

  class  HavingClauseContext : public antlr4::ParserRuleContext {
  public:
    HavingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HAVING();
    ColumnExprContext *columnExpr();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  HavingClauseContext* havingClause();

  class  OrderByClauseContext : public antlr4::ParserRuleContext {
  public:
    OrderByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *BY();
    OrderExprListContext *orderExprList();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  OrderByClauseContext* orderByClause();

  class  LimitByClauseContext : public antlr4::ParserRuleContext {
  public:
    LimitByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT();
    LimitExprContext *limitExpr();
    antlr4::tree::TerminalNode *BY();
    ColumnExprListContext *columnExprList();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  LimitByClauseContext* limitByClause();

  class  LimitClauseContext : public antlr4::ParserRuleContext {
  public:
    LimitClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT();
    LimitExprContext *limitExpr();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  LimitClauseContext* limitClause();

  class  SettingsClauseContext : public antlr4::ParserRuleContext {
  public:
    SettingsClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SETTINGS();
    SettingExprListContext *settingExprList();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SettingsClauseContext* settingsClause();

  class  JoinExprContext : public antlr4::ParserRuleContext {
  public:
    JoinExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableIdentifierContext *tableIdentifier();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  JoinExprContext* joinExpr();

  class  LimitExprContext : public antlr4::ParserRuleContext {
  public:
    LimitExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> NUMBER_LITERAL();
    antlr4::tree::TerminalNode* NUMBER_LITERAL(size_t i);
    antlr4::tree::TerminalNode *COMMA();
    antlr4::tree::TerminalNode *OFFSET();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  LimitExprContext* limitExpr();

  class  OrderExprListContext : public antlr4::ParserRuleContext {
  public:
    OrderExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OrderExprContext *> orderExpr();
    OrderExprContext* orderExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  OrderExprListContext* orderExprList();

  class  OrderExprContext : public antlr4::ParserRuleContext {
  public:
    OrderExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *NULLS();
    antlr4::tree::TerminalNode *COLLATE();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *FIRST();
    antlr4::tree::TerminalNode *LAST();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  OrderExprContext* orderExpr();

  class  RatioExprContext : public antlr4::ParserRuleContext {
  public:
    RatioExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> NUMBER_LITERAL();
    antlr4::tree::TerminalNode* NUMBER_LITERAL(size_t i);
    antlr4::tree::TerminalNode *SLASH();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  RatioExprContext* ratioExpr();

  class  SettingExprListContext : public antlr4::ParserRuleContext {
  public:
    SettingExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SettingExprContext *> settingExpr();
    SettingExprContext* settingExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SettingExprListContext* settingExprList();

  class  SettingExprContext : public antlr4::ParserRuleContext {
  public:
    SettingExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *EQ_SINGLE();
    antlr4::tree::TerminalNode *LITERAL();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  SettingExprContext* settingExpr();

  class  ColumnExprListContext : public antlr4::ParserRuleContext {
  public:
    ColumnExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnExprListContext* columnExprList();

  class  ColumnExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LITERAL();
    antlr4::tree::TerminalNode *ASTERISK();
    ColumnIdentifierContext *columnIdentifier();
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    SelectStmtContext *selectStmt();
    antlr4::tree::TerminalNode *LBRACKET();
    antlr4::tree::TerminalNode *RBRACKET();
    ColumnExprListContext *columnExprList();
    UnaryOpContext *unaryOp();
    antlr4::tree::TerminalNode *CASE();
    antlr4::tree::TerminalNode *END();
    std::vector<antlr4::tree::TerminalNode *> WHEN();
    antlr4::tree::TerminalNode* WHEN(size_t i);
    std::vector<antlr4::tree::TerminalNode *> THEN();
    antlr4::tree::TerminalNode* THEN(size_t i);
    antlr4::tree::TerminalNode *ELSE();
    antlr4::tree::TerminalNode *INTERVAL();
    antlr4::tree::TerminalNode *INTERVAL_TYPE();
    ColumnFunctionExprContext *columnFunctionExpr();
    BinaryOpContext *binaryOp();
    antlr4::tree::TerminalNode *QUERY();
    antlr4::tree::TerminalNode *COLON();
    antlr4::tree::TerminalNode *BETWEEN();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *DOT();
    antlr4::tree::TerminalNode *NUMBER_LITERAL();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *NULL_SQL();
    antlr4::tree::TerminalNode *AS();
    IdentifierContext *identifier();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnExprContext* columnExpr();
  ColumnExprContext* columnExpr(int precedence);
  class  ColumnFunctionExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnFunctionExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    std::vector<antlr4::tree::TerminalNode *> LPAREN();
    antlr4::tree::TerminalNode* LPAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode *> RPAREN();
    antlr4::tree::TerminalNode* RPAREN(size_t i);
    ColumnArgListContext *columnArgList();
    std::vector<antlr4::tree::TerminalNode *> LITERAL();
    antlr4::tree::TerminalNode* LITERAL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *EXTRACT();
    antlr4::tree::TerminalNode *INTERVAL_TYPE();
    antlr4::tree::TerminalNode *FROM();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *CAST();
    antlr4::tree::TerminalNode *AS();
    antlr4::tree::TerminalNode *TRIM();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *BOTH();
    antlr4::tree::TerminalNode *LEADING();
    antlr4::tree::TerminalNode *TRAILING();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnFunctionExprContext* columnFunctionExpr();

  class  ColumnArgListContext : public antlr4::ParserRuleContext {
  public:
    ColumnArgListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColumnArgExprContext *> columnArgExpr();
    ColumnArgExprContext* columnArgExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnArgListContext* columnArgList();

  class  ColumnArgExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnArgExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnExprContext *columnExpr();
    ColumnLambdaExprContext *columnLambdaExpr();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnArgExprContext* columnArgExpr();

  class  ColumnLambdaExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnLambdaExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ARROW();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnLambdaExprContext* columnLambdaExpr();

  class  ColumnIdentifierContext : public antlr4::ParserRuleContext {
  public:
    ColumnIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *DOT();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  ColumnIdentifierContext* columnIdentifier();

  class  TableIdentifierContext : public antlr4::ParserRuleContext {
  public:
    TableIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    DatabaseIdentifierContext *databaseIdentifier();
    antlr4::tree::TerminalNode *DOT();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  TableIdentifierContext* tableIdentifier();

  class  DatabaseIdentifierContext : public antlr4::ParserRuleContext {
  public:
    DatabaseIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  DatabaseIdentifierContext* databaseIdentifier();

  class  IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  IdentifierContext* identifier();

  class  UnaryOpContext : public antlr4::ParserRuleContext {
  public:
    UnaryOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DASH();
    antlr4::tree::TerminalNode *NOT();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  UnaryOpContext* unaryOp();

  class  BinaryOpContext : public antlr4::ParserRuleContext {
  public:
    BinaryOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASTERISK();
    antlr4::tree::TerminalNode *SLASH();
    antlr4::tree::TerminalNode *PERCENT();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *DASH();
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *NOT_EQ();
    antlr4::tree::TerminalNode *LE();
    antlr4::tree::TerminalNode *GE();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *CONCAT();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *OR();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *GLOBAL();


    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;

  };

  BinaryOpContext* binaryOp();


  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;
  bool columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex);

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace DB
