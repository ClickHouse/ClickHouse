
// Generated from ClickHouseParser.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseParser : public antlr4::Parser {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, ANALYZE = 6, AND = 7, 
    ANTI = 8, ANY = 9, ARRAY = 10, AS = 11, ASCENDING = 12, ASOF = 13, ATTACH = 14, 
    BETWEEN = 15, BOTH = 16, BY = 17, CASE = 18, CAST = 19, CHECK = 20, 
    CLEAR = 21, CLUSTER = 22, CODEC = 23, COLLATE = 24, COLUMN = 25, COMMENT = 26, 
    CONSTRAINT = 27, CREATE = 28, CROSS = 29, DATABASE = 30, DATABASES = 31, 
    DATE = 32, DAY = 33, DEDUPLICATE = 34, DEFAULT = 35, DELAY = 36, DELETE = 37, 
    DESC = 38, DESCENDING = 39, DESCRIBE = 40, DETACH = 41, DISK = 42, DISTINCT = 43, 
    DISTRIBUTED = 44, DROP = 45, ELSE = 46, END = 47, ENGINE = 48, EXISTS = 49, 
    EXTRACT = 50, FETCHES = 51, FINAL = 52, FIRST = 53, FLUSH = 54, FOR = 55, 
    FORMAT = 56, FROM = 57, FULL = 58, FUNCTION = 59, GLOBAL = 60, GRANULARITY = 61, 
    GROUP = 62, HAVING = 63, HOUR = 64, ID = 65, IF = 66, IN = 67, INDEX = 68, 
    INF = 69, INNER = 70, INSERT = 71, INTERVAL = 72, INTO = 73, IS = 74, 
    JOIN = 75, KEY = 76, LAST = 77, LEADING = 78, LEFT = 79, LIKE = 80, 
    LIMIT = 81, LOCAL = 82, LOGS = 83, MATERIALIZED = 84, MERGES = 85, MINUTE = 86, 
    MODIFY = 87, MONTH = 88, NAN_SQL = 89, NO = 90, NOT = 91, NULL_SQL = 92, 
    NULLS = 93, OFFSET = 94, ON = 95, OPTIMIZE = 96, OR = 97, ORDER = 98, 
    OUTER = 99, OUTFILE = 100, PARTITION = 101, POPULATE = 102, PREWHERE = 103, 
    PRIMARY = 104, QUARTER = 105, REMOVE = 106, RENAME = 107, REPLACE = 108, 
    REPLICA = 109, RIGHT = 110, SAMPLE = 111, SECOND = 112, SELECT = 113, 
    SEMI = 114, SENDS = 115, SET = 116, SETTINGS = 117, SHOW = 118, START = 119, 
    STOP = 120, SUBSTRING = 121, SYNC = 122, SYSTEM = 123, TABLE = 124, 
    TABLES = 125, TEMPORARY = 126, THEN = 127, TIES = 128, TIMESTAMP = 129, 
    TO = 130, TOTALS = 131, TRAILING = 132, TRIM = 133, TRUNCATE = 134, 
    TTL = 135, TYPE = 136, UNION = 137, USE = 138, USING = 139, VALUES = 140, 
    VIEW = 141, VOLUME = 142, WEEK = 143, WHEN = 144, WHERE = 145, WITH = 146, 
    YEAR = 147, JSON_FALSE = 148, JSON_TRUE = 149, IDENTIFIER = 150, FLOATING_LITERAL = 151, 
    OCTAL_LITERAL = 152, DECIMAL_LITERAL = 153, HEXADECIMAL_LITERAL = 154, 
    STRING_LITERAL = 155, ARROW = 156, ASTERISK = 157, BACKQUOTE = 158, 
    BACKSLASH = 159, COLON = 160, COMMA = 161, CONCAT = 162, DASH = 163, 
    DOT = 164, EQ_DOUBLE = 165, EQ_SINGLE = 166, GE = 167, GT = 168, LBRACE = 169, 
    LBRACKET = 170, LE = 171, LPAREN = 172, LT = 173, NOT_EQ = 174, PERCENT = 175, 
    PLUS = 176, QUERY = 177, QUOTE_DOUBLE = 178, QUOTE_SINGLE = 179, RBRACE = 180, 
    RBRACKET = 181, RPAREN = 182, SEMICOLON = 183, SLASH = 184, UNDERSCORE = 185, 
    MULTI_LINE_COMMENT = 186, SINGLE_LINE_COMMENT = 187, WHITESPACE = 188
  };

  enum {
    RuleQueryStmt = 0, RuleQuery = 1, RuleAlterStmt = 2, RuleAlterTableClause = 3, 
    RuleTableColumnPropertyType = 4, RulePartitionClause = 5, RuleAnalyzeStmt = 6, 
    RuleCheckStmt = 7, RuleCreateStmt = 8, RuleDestinationClause = 9, RuleSubqueryClause = 10, 
    RuleSchemaClause = 11, RuleEngineClause = 12, RulePartitionByClause = 13, 
    RulePrimaryKeyClause = 14, RuleSampleByClause = 15, RuleTtlClause = 16, 
    RuleEngineExpr = 17, RuleTableElementExpr = 18, RuleTableColumnDfnt = 19, 
    RuleTableColumnPropertyExpr = 20, RuleCodecExpr = 21, RuleTtlExpr = 22, 
    RuleDescribeStmt = 23, RuleDropStmt = 24, RuleExistsStmt = 25, RuleInsertStmt = 26, 
    RuleColumnsClause = 27, RuleDataClause = 28, RuleOptimizeStmt = 29, 
    RuleRenameStmt = 30, RuleSelectUnionStmt = 31, RuleSelectStmtWithParens = 32, 
    RuleSelectStmt = 33, RuleWithClause = 34, RuleFromClause = 35, RuleArrayJoinClause = 36, 
    RulePrewhereClause = 37, RuleWhereClause = 38, RuleGroupByClause = 39, 
    RuleHavingClause = 40, RuleOrderByClause = 41, RuleLimitByClause = 42, 
    RuleLimitClause = 43, RuleSettingsClause = 44, RuleJoinExpr = 45, RuleJoinOp = 46, 
    RuleJoinOpCross = 47, RuleJoinConstraintClause = 48, RuleSampleClause = 49, 
    RuleLimitExpr = 50, RuleOrderExprList = 51, RuleOrderExpr = 52, RuleRatioExpr = 53, 
    RuleSettingExprList = 54, RuleSettingExpr = 55, RuleSetStmt = 56, RuleShowStmt = 57, 
    RuleSystemStmt = 58, RuleTruncateStmt = 59, RuleUseStmt = 60, RuleColumnTypeExpr = 61, 
    RuleColumnExprList = 62, RuleColumnsExpr = 63, RuleColumnExpr = 64, 
    RuleColumnArgList = 65, RuleColumnArgExpr = 66, RuleColumnLambdaExpr = 67, 
    RuleColumnIdentifier = 68, RuleNestedIdentifier = 69, RuleTableExpr = 70, 
    RuleTableFunctionExpr = 71, RuleTableIdentifier = 72, RuleTableArgList = 73, 
    RuleTableArgExpr = 74, RuleDatabaseIdentifier = 75, RuleFloatingLiteral = 76, 
    RuleNumberLiteral = 77, RuleLiteral = 78, RuleInterval = 79, RuleKeyword = 80, 
    RuleKeywordForAlias = 81, RuleAlias = 82, RuleIdentifier = 83, RuleIdentifierOrNull = 84, 
    RuleUnaryOp = 85, RuleEnumValue = 86
  };

  ClickHouseParser(antlr4::TokenStream *input);
  ~ClickHouseParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


  class QueryStmtContext;
  class QueryContext;
  class AlterStmtContext;
  class AlterTableClauseContext;
  class TableColumnPropertyTypeContext;
  class PartitionClauseContext;
  class AnalyzeStmtContext;
  class CheckStmtContext;
  class CreateStmtContext;
  class DestinationClauseContext;
  class SubqueryClauseContext;
  class SchemaClauseContext;
  class EngineClauseContext;
  class PartitionByClauseContext;
  class PrimaryKeyClauseContext;
  class SampleByClauseContext;
  class TtlClauseContext;
  class EngineExprContext;
  class TableElementExprContext;
  class TableColumnDfntContext;
  class TableColumnPropertyExprContext;
  class CodecExprContext;
  class TtlExprContext;
  class DescribeStmtContext;
  class DropStmtContext;
  class ExistsStmtContext;
  class InsertStmtContext;
  class ColumnsClauseContext;
  class DataClauseContext;
  class OptimizeStmtContext;
  class RenameStmtContext;
  class SelectUnionStmtContext;
  class SelectStmtWithParensContext;
  class SelectStmtContext;
  class WithClauseContext;
  class FromClauseContext;
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
  class JoinOpContext;
  class JoinOpCrossContext;
  class JoinConstraintClauseContext;
  class SampleClauseContext;
  class LimitExprContext;
  class OrderExprListContext;
  class OrderExprContext;
  class RatioExprContext;
  class SettingExprListContext;
  class SettingExprContext;
  class SetStmtContext;
  class ShowStmtContext;
  class SystemStmtContext;
  class TruncateStmtContext;
  class UseStmtContext;
  class ColumnTypeExprContext;
  class ColumnExprListContext;
  class ColumnsExprContext;
  class ColumnExprContext;
  class ColumnArgListContext;
  class ColumnArgExprContext;
  class ColumnLambdaExprContext;
  class ColumnIdentifierContext;
  class NestedIdentifierContext;
  class TableExprContext;
  class TableFunctionExprContext;
  class TableIdentifierContext;
  class TableArgListContext;
  class TableArgExprContext;
  class DatabaseIdentifierContext;
  class FloatingLiteralContext;
  class NumberLiteralContext;
  class LiteralContext;
  class IntervalContext;
  class KeywordContext;
  class KeywordForAliasContext;
  class AliasContext;
  class IdentifierContext;
  class IdentifierOrNullContext;
  class UnaryOpContext;
  class EnumValueContext; 

  class  QueryStmtContext : public antlr4::ParserRuleContext {
  public:
    QueryStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryContext *query();
    antlr4::tree::TerminalNode *INTO();
    antlr4::tree::TerminalNode *OUTFILE();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *FORMAT();
    IdentifierOrNullContext *identifierOrNull();
    antlr4::tree::TerminalNode *SEMICOLON();
    InsertStmtContext *insertStmt();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  QueryStmtContext* queryStmt();

  class  QueryContext : public antlr4::ParserRuleContext {
  public:
    QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterStmtContext *alterStmt();
    AnalyzeStmtContext *analyzeStmt();
    CheckStmtContext *checkStmt();
    CreateStmtContext *createStmt();
    DescribeStmtContext *describeStmt();
    DropStmtContext *dropStmt();
    ExistsStmtContext *existsStmt();
    OptimizeStmtContext *optimizeStmt();
    RenameStmtContext *renameStmt();
    SelectUnionStmtContext *selectUnionStmt();
    SetStmtContext *setStmt();
    ShowStmtContext *showStmt();
    SystemStmtContext *systemStmt();
    TruncateStmtContext *truncateStmt();
    UseStmtContext *useStmt();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  QueryContext* query();

  class  AlterStmtContext : public antlr4::ParserRuleContext {
  public:
    AlterStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    AlterStmtContext() = default;
    void copyFrom(AlterStmtContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  AlterTableStmtContext : public AlterStmtContext {
  public:
    AlterTableStmtContext(AlterStmtContext *ctx);

    antlr4::tree::TerminalNode *ALTER();
    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    std::vector<AlterTableClauseContext *> alterTableClause();
    AlterTableClauseContext* alterTableClause(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  AlterStmtContext* alterStmt();

  class  AlterTableClauseContext : public antlr4::ParserRuleContext {
  public:
    AlterTableClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    AlterTableClauseContext() = default;
    void copyFrom(AlterTableClauseContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  AlterTableClauseReplaceContext : public AlterTableClauseContext {
  public:
    AlterTableClauseReplaceContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *REPLACE();
    PartitionClauseContext *partitionClause();
    antlr4::tree::TerminalNode *FROM();
    TableIdentifierContext *tableIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseRenameContext : public AlterTableClauseContext {
  public:
    AlterTableClauseRenameContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *RENAME();
    antlr4::tree::TerminalNode *COLUMN();
    std::vector<NestedIdentifierContext *> nestedIdentifier();
    NestedIdentifierContext* nestedIdentifier(size_t i);
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseAddContext : public AlterTableClauseContext {
  public:
    AlterTableClauseAddContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *ADD();
    antlr4::tree::TerminalNode *COLUMN();
    TableColumnDfntContext *tableColumnDfnt();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *AFTER();
    NestedIdentifierContext *nestedIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseOrderByContext : public AlterTableClauseContext {
  public:
    AlterTableClauseOrderByContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *MODIFY();
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *BY();
    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseModifyContext : public AlterTableClauseContext {
  public:
    AlterTableClauseModifyContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *MODIFY();
    antlr4::tree::TerminalNode *COLUMN();
    TableColumnDfntContext *tableColumnDfnt();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseRemoveTTLContext : public AlterTableClauseContext {
  public:
    AlterTableClauseRemoveTTLContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *REMOVE();
    antlr4::tree::TerminalNode *TTL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseDeleteContext : public AlterTableClauseContext {
  public:
    AlterTableClauseDeleteContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *WHERE();
    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseCommentContext : public AlterTableClauseContext {
  public:
    AlterTableClauseCommentContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *COMMENT();
    antlr4::tree::TerminalNode *COLUMN();
    NestedIdentifierContext *nestedIdentifier();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseRemoveContext : public AlterTableClauseContext {
  public:
    AlterTableClauseRemoveContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *MODIFY();
    antlr4::tree::TerminalNode *COLUMN();
    NestedIdentifierContext *nestedIdentifier();
    antlr4::tree::TerminalNode *REMOVE();
    TableColumnPropertyTypeContext *tableColumnPropertyType();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseAttachContext : public AlterTableClauseContext {
  public:
    AlterTableClauseAttachContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *ATTACH();
    PartitionClauseContext *partitionClause();
    antlr4::tree::TerminalNode *FROM();
    TableIdentifierContext *tableIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseDropColumnContext : public AlterTableClauseContext {
  public:
    AlterTableClauseDropColumnContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *COLUMN();
    NestedIdentifierContext *nestedIdentifier();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseClearContext : public AlterTableClauseContext {
  public:
    AlterTableClauseClearContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *CLEAR();
    antlr4::tree::TerminalNode *COLUMN();
    NestedIdentifierContext *nestedIdentifier();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *IN();
    PartitionClauseContext *partitionClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseDetachContext : public AlterTableClauseContext {
  public:
    AlterTableClauseDetachContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *DETACH();
    PartitionClauseContext *partitionClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseDropPartitionContext : public AlterTableClauseContext {
  public:
    AlterTableClauseDropPartitionContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *DROP();
    PartitionClauseContext *partitionClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  AlterTableClauseTTLContext : public AlterTableClauseContext {
  public:
    AlterTableClauseTTLContext(AlterTableClauseContext *ctx);

    antlr4::tree::TerminalNode *MODIFY();
    antlr4::tree::TerminalNode *TTL();
    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  AlterTableClauseContext* alterTableClause();

  class  TableColumnPropertyTypeContext : public antlr4::ParserRuleContext {
  public:
    TableColumnPropertyTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALIAS();
    antlr4::tree::TerminalNode *CODEC();
    antlr4::tree::TerminalNode *COMMENT();
    antlr4::tree::TerminalNode *DEFAULT();
    antlr4::tree::TerminalNode *MATERIALIZED();
    antlr4::tree::TerminalNode *TTL();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableColumnPropertyTypeContext* tableColumnPropertyType();

  class  PartitionClauseContext : public antlr4::ParserRuleContext {
  public:
    PartitionClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *ID();
    antlr4::tree::TerminalNode *STRING_LITERAL();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PartitionClauseContext* partitionClause();

  class  AnalyzeStmtContext : public antlr4::ParserRuleContext {
  public:
    AnalyzeStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ANALYZE();
    QueryContext *query();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AnalyzeStmtContext* analyzeStmt();

  class  CheckStmtContext : public antlr4::ParserRuleContext {
  public:
    CheckStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHECK();
    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CheckStmtContext* checkStmt();

  class  CreateStmtContext : public antlr4::ParserRuleContext {
  public:
    CreateStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    CreateStmtContext() = default;
    void copyFrom(CreateStmtContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  CreateViewStmtContext : public CreateStmtContext {
  public:
    CreateViewStmtContext(CreateStmtContext *ctx);

    antlr4::tree::TerminalNode *VIEW();
    TableIdentifierContext *tableIdentifier();
    SubqueryClauseContext *subqueryClause();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();
    SchemaClauseContext *schemaClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  CreateDatabaseStmtContext : public CreateStmtContext {
  public:
    CreateDatabaseStmtContext(CreateStmtContext *ctx);

    antlr4::tree::TerminalNode *DATABASE();
    DatabaseIdentifierContext *databaseIdentifier();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();
    EngineExprContext *engineExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  CreateMaterializedViewStmtContext : public CreateStmtContext {
  public:
    CreateMaterializedViewStmtContext(CreateStmtContext *ctx);

    antlr4::tree::TerminalNode *MATERIALIZED();
    antlr4::tree::TerminalNode *VIEW();
    TableIdentifierContext *tableIdentifier();
    SubqueryClauseContext *subqueryClause();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *CREATE();
    DestinationClauseContext *destinationClause();
    EngineClauseContext *engineClause();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();
    SchemaClauseContext *schemaClause();
    antlr4::tree::TerminalNode *POPULATE();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  CreateTableStmtContext : public CreateStmtContext {
  public:
    CreateTableStmtContext(CreateStmtContext *ctx);

    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *TEMPORARY();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();
    SchemaClauseContext *schemaClause();
    EngineClauseContext *engineClause();
    SubqueryClauseContext *subqueryClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  CreateStmtContext* createStmt();

  class  DestinationClauseContext : public antlr4::ParserRuleContext {
  public:
    DestinationClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TO();
    TableIdentifierContext *tableIdentifier();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DestinationClauseContext* destinationClause();

  class  SubqueryClauseContext : public antlr4::ParserRuleContext {
  public:
    SubqueryClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS();
    SelectUnionStmtContext *selectUnionStmt();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SubqueryClauseContext* subqueryClause();

  class  SchemaClauseContext : public antlr4::ParserRuleContext {
  public:
    SchemaClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    SchemaClauseContext() = default;
    void copyFrom(SchemaClauseContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  SchemaAsTableClauseContext : public SchemaClauseContext {
  public:
    SchemaAsTableClauseContext(SchemaClauseContext *ctx);

    antlr4::tree::TerminalNode *AS();
    TableIdentifierContext *tableIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  SchemaAsFunctionClauseContext : public SchemaClauseContext {
  public:
    SchemaAsFunctionClauseContext(SchemaClauseContext *ctx);

    antlr4::tree::TerminalNode *AS();
    TableFunctionExprContext *tableFunctionExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  SchemaDescriptionClauseContext : public SchemaClauseContext {
  public:
    SchemaDescriptionClauseContext(SchemaClauseContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    std::vector<TableElementExprContext *> tableElementExpr();
    TableElementExprContext* tableElementExpr(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  SchemaClauseContext* schemaClause();

  class  EngineClauseContext : public antlr4::ParserRuleContext {
  public:
    std::set<std::string> clauses;
    EngineClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    EngineExprContext *engineExpr();
    std::vector<OrderByClauseContext *> orderByClause();
    OrderByClauseContext* orderByClause(size_t i);
    std::vector<PartitionByClauseContext *> partitionByClause();
    PartitionByClauseContext* partitionByClause(size_t i);
    std::vector<PrimaryKeyClauseContext *> primaryKeyClause();
    PrimaryKeyClauseContext* primaryKeyClause(size_t i);
    std::vector<SampleByClauseContext *> sampleByClause();
    SampleByClauseContext* sampleByClause(size_t i);
    std::vector<TtlClauseContext *> ttlClause();
    TtlClauseContext* ttlClause(size_t i);
    std::vector<SettingsClauseContext *> settingsClause();
    SettingsClauseContext* settingsClause(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  EngineClauseContext* engineClause();

  class  PartitionByClauseContext : public antlr4::ParserRuleContext {
  public:
    PartitionByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION();
    antlr4::tree::TerminalNode *BY();
    ColumnExprContext *columnExpr();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PartitionByClauseContext* partitionByClause();

  class  PrimaryKeyClauseContext : public antlr4::ParserRuleContext {
  public:
    PrimaryKeyClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PRIMARY();
    antlr4::tree::TerminalNode *KEY();
    ColumnExprContext *columnExpr();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PrimaryKeyClauseContext* primaryKeyClause();

  class  SampleByClauseContext : public antlr4::ParserRuleContext {
  public:
    SampleByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SAMPLE();
    antlr4::tree::TerminalNode *BY();
    ColumnExprContext *columnExpr();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SampleByClauseContext* sampleByClause();

  class  TtlClauseContext : public antlr4::ParserRuleContext {
  public:
    TtlClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TTL();
    std::vector<TtlExprContext *> ttlExpr();
    TtlExprContext* ttlExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TtlClauseContext* ttlClause();

  class  EngineExprContext : public antlr4::ParserRuleContext {
  public:
    EngineExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENGINE();
    IdentifierOrNullContext *identifierOrNull();
    antlr4::tree::TerminalNode *EQ_SINGLE();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    ColumnExprListContext *columnExprList();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  EngineExprContext* engineExpr();

  class  TableElementExprContext : public antlr4::ParserRuleContext {
  public:
    TableElementExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    TableElementExprContext() = default;
    void copyFrom(TableElementExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  TableElementExprConstraintContext : public TableElementExprContext {
  public:
    TableElementExprConstraintContext(TableElementExprContext *ctx);

    antlr4::tree::TerminalNode *CONSTRAINT();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *CHECK();
    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  TableElementExprColumnContext : public TableElementExprContext {
  public:
    TableElementExprColumnContext(TableElementExprContext *ctx);

    TableColumnDfntContext *tableColumnDfnt();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  TableElementExprIndexContext : public TableElementExprContext {
  public:
    TableElementExprIndexContext(TableElementExprContext *ctx);

    antlr4::tree::TerminalNode *INDEX();
    IdentifierContext *identifier();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *TYPE();
    ColumnTypeExprContext *columnTypeExpr();
    antlr4::tree::TerminalNode *GRANULARITY();
    antlr4::tree::TerminalNode *DECIMAL_LITERAL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  TableElementExprContext* tableElementExpr();

  class  TableColumnDfntContext : public antlr4::ParserRuleContext {
  public:
    TableColumnDfntContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NestedIdentifierContext *nestedIdentifier();
    ColumnTypeExprContext *columnTypeExpr();
    TableColumnPropertyExprContext *tableColumnPropertyExpr();
    antlr4::tree::TerminalNode *COMMENT();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    CodecExprContext *codecExpr();
    antlr4::tree::TerminalNode *TTL();
    ColumnExprContext *columnExpr();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableColumnDfntContext* tableColumnDfnt();

  class  TableColumnPropertyExprContext : public antlr4::ParserRuleContext {
  public:
    TableColumnPropertyExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *DEFAULT();
    antlr4::tree::TerminalNode *MATERIALIZED();
    antlr4::tree::TerminalNode *ALIAS();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableColumnPropertyExprContext* tableColumnPropertyExpr();

  class  CodecExprContext : public antlr4::ParserRuleContext {
  public:
    CodecExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CODEC();
    std::vector<antlr4::tree::TerminalNode *> LPAREN();
    antlr4::tree::TerminalNode* LPAREN(size_t i);
    IdentifierContext *identifier();
    std::vector<antlr4::tree::TerminalNode *> RPAREN();
    antlr4::tree::TerminalNode* RPAREN(size_t i);
    ColumnExprListContext *columnExprList();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CodecExprContext* codecExpr();

  class  TtlExprContext : public antlr4::ParserRuleContext {
  public:
    TtlExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *DISK();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *VOLUME();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TtlExprContext* ttlExpr();

  class  DescribeStmtContext : public antlr4::ParserRuleContext {
  public:
    DescribeStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableExprContext *tableExpr();
    antlr4::tree::TerminalNode *DESCRIBE();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *TABLE();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DescribeStmtContext* describeStmt();

  class  DropStmtContext : public antlr4::ParserRuleContext {
  public:
    DropStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    DropStmtContext() = default;
    void copyFrom(DropStmtContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  DropDatabaseStmtContext : public DropStmtContext {
  public:
    DropDatabaseStmtContext(DropStmtContext *ctx);

    antlr4::tree::TerminalNode *DATABASE();
    DatabaseIdentifierContext *databaseIdentifier();
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  DropTableStmtContext : public DropStmtContext {
  public:
    DropTableStmtContext(DropStmtContext *ctx);

    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *TEMPORARY();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *DELAY();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  DropStmtContext* dropStmt();

  class  ExistsStmtContext : public antlr4::ParserRuleContext {
  public:
    ExistsStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *TEMPORARY();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExistsStmtContext* existsStmt();

  class  InsertStmtContext : public antlr4::ParserRuleContext {
  public:
    InsertStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INSERT();
    antlr4::tree::TerminalNode *INTO();
    DataClauseContext *dataClause();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *FUNCTION();
    TableFunctionExprContext *tableFunctionExpr();
    antlr4::tree::TerminalNode *TABLE();
    ColumnsClauseContext *columnsClause();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  InsertStmtContext* insertStmt();

  class  ColumnsClauseContext : public antlr4::ParserRuleContext {
  public:
    ColumnsClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<NestedIdentifierContext *> nestedIdentifier();
    NestedIdentifierContext* nestedIdentifier(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ColumnsClauseContext* columnsClause();

  class  DataClauseContext : public antlr4::ParserRuleContext {
  public:
    DataClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    DataClauseContext() = default;
    void copyFrom(DataClauseContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  DataClauseValuesContext : public DataClauseContext {
  public:
    DataClauseValuesContext(DataClauseContext *ctx);

    antlr4::tree::TerminalNode *VALUES();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  DataClauseFormatContext : public DataClauseContext {
  public:
    DataClauseFormatContext(DataClauseContext *ctx);

    antlr4::tree::TerminalNode *FORMAT();
    IdentifierContext *identifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  DataClauseSelectContext : public DataClauseContext {
  public:
    DataClauseSelectContext(DataClauseContext *ctx);

    SelectUnionStmtContext *selectUnionStmt();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  DataClauseContext* dataClause();

  class  OptimizeStmtContext : public antlr4::ParserRuleContext {
  public:
    OptimizeStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPTIMIZE();
    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    PartitionClauseContext *partitionClause();
    antlr4::tree::TerminalNode *FINAL();
    antlr4::tree::TerminalNode *DEDUPLICATE();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OptimizeStmtContext* optimizeStmt();

  class  RenameStmtContext : public antlr4::ParserRuleContext {
  public:
    RenameStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME();
    antlr4::tree::TerminalNode *TABLE();
    std::vector<TableIdentifierContext *> tableIdentifier();
    TableIdentifierContext* tableIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> TO();
    antlr4::tree::TerminalNode* TO(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RenameStmtContext* renameStmt();

  class  SelectUnionStmtContext : public antlr4::ParserRuleContext {
  public:
    SelectUnionStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SelectStmtWithParensContext *> selectStmtWithParens();
    SelectStmtWithParensContext* selectStmtWithParens(size_t i);
    std::vector<antlr4::tree::TerminalNode *> UNION();
    antlr4::tree::TerminalNode* UNION(size_t i);
    std::vector<antlr4::tree::TerminalNode *> ALL();
    antlr4::tree::TerminalNode* ALL(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SelectUnionStmtContext* selectUnionStmt();

  class  SelectStmtWithParensContext : public antlr4::ParserRuleContext {
  public:
    SelectStmtWithParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectStmtContext *selectStmt();
    antlr4::tree::TerminalNode *LPAREN();
    SelectUnionStmtContext *selectUnionStmt();
    antlr4::tree::TerminalNode *RPAREN();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SelectStmtWithParensContext* selectStmtWithParens();

  class  SelectStmtContext : public antlr4::ParserRuleContext {
  public:
    SelectStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SELECT();
    ColumnExprListContext *columnExprList();
    WithClauseContext *withClause();
    antlr4::tree::TerminalNode *DISTINCT();
    FromClauseContext *fromClause();
    ArrayJoinClauseContext *arrayJoinClause();
    PrewhereClauseContext *prewhereClause();
    WhereClauseContext *whereClause();
    GroupByClauseContext *groupByClause();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *TOTALS();
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

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FromClauseContext* fromClause();

  class  ArrayJoinClauseContext : public antlr4::ParserRuleContext {
  public:
    ArrayJoinClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ARRAY();
    antlr4::tree::TerminalNode *JOIN();
    ColumnExprListContext *columnExprList();
    antlr4::tree::TerminalNode *LEFT();
    antlr4::tree::TerminalNode *INNER();

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
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *TIES();

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
   
    JoinExprContext() = default;
    void copyFrom(JoinExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  JoinExprOpContext : public JoinExprContext {
  public:
    JoinExprOpContext(JoinExprContext *ctx);

    std::vector<JoinExprContext *> joinExpr();
    JoinExprContext* joinExpr(size_t i);
    antlr4::tree::TerminalNode *JOIN();
    JoinConstraintClauseContext *joinConstraintClause();
    JoinOpContext *joinOp();
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *LOCAL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  JoinExprTableContext : public JoinExprContext {
  public:
    JoinExprTableContext(JoinExprContext *ctx);

    TableExprContext *tableExpr();
    antlr4::tree::TerminalNode *FINAL();
    SampleClauseContext *sampleClause();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  JoinExprParensContext : public JoinExprContext {
  public:
    JoinExprParensContext(JoinExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    JoinExprContext *joinExpr();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  JoinExprCrossOpContext : public JoinExprContext {
  public:
    JoinExprCrossOpContext(JoinExprContext *ctx);

    std::vector<JoinExprContext *> joinExpr();
    JoinExprContext* joinExpr(size_t i);
    JoinOpCrossContext *joinOpCross();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  JoinExprContext* joinExpr();
  JoinExprContext* joinExpr(int precedence);
  class  JoinOpContext : public antlr4::ParserRuleContext {
  public:
    JoinOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    JoinOpContext() = default;
    void copyFrom(JoinOpContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  JoinOpFullContext : public JoinOpContext {
  public:
    JoinOpFullContext(JoinOpContext *ctx);

    antlr4::tree::TerminalNode *FULL();
    antlr4::tree::TerminalNode *OUTER();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ANY();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  JoinOpInnerContext : public JoinOpContext {
  public:
    JoinOpInnerContext(JoinOpContext *ctx);

    antlr4::tree::TerminalNode *INNER();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *ASOF();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  JoinOpLeftRightContext : public JoinOpContext {
  public:
    JoinOpLeftRightContext(JoinOpContext *ctx);

    antlr4::tree::TerminalNode *LEFT();
    antlr4::tree::TerminalNode *RIGHT();
    antlr4::tree::TerminalNode *OUTER();
    antlr4::tree::TerminalNode *SEMI();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ANTI();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *ASOF();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  JoinOpContext* joinOp();

  class  JoinOpCrossContext : public antlr4::ParserRuleContext {
  public:
    JoinOpCrossContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CROSS();
    antlr4::tree::TerminalNode *JOIN();
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *LOCAL();
    antlr4::tree::TerminalNode *COMMA();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  JoinOpCrossContext* joinOpCross();

  class  JoinConstraintClauseContext : public antlr4::ParserRuleContext {
  public:
    JoinConstraintClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    ColumnExprListContext *columnExprList();
    antlr4::tree::TerminalNode *USING();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  JoinConstraintClauseContext* joinConstraintClause();

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

  class  LimitExprContext : public antlr4::ParserRuleContext {
  public:
    LimitExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> DECIMAL_LITERAL();
    antlr4::tree::TerminalNode* DECIMAL_LITERAL(size_t i);
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
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *FIRST();
    antlr4::tree::TerminalNode *LAST();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OrderExprContext* orderExpr();

  class  RatioExprContext : public antlr4::ParserRuleContext {
  public:
    RatioExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NumberLiteralContext *> numberLiteral();
    NumberLiteralContext* numberLiteral(size_t i);
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
    LiteralContext *literal();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SettingExprContext* settingExpr();

  class  SetStmtContext : public antlr4::ParserRuleContext {
  public:
    SetStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    SettingExprListContext *settingExprList();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetStmtContext* setStmt();

  class  ShowStmtContext : public antlr4::ParserRuleContext {
  public:
    ShowStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ShowStmtContext() = default;
    void copyFrom(ShowStmtContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ShowCreateDatabaseStmtContext : public ShowStmtContext {
  public:
    ShowCreateDatabaseStmtContext(ShowStmtContext *ctx);

    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *DATABASE();
    DatabaseIdentifierContext *databaseIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ShowDatabasesStmtContext : public ShowStmtContext {
  public:
    ShowDatabasesStmtContext(ShowStmtContext *ctx);

    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *DATABASES();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ShowCreateTableStmtContext : public ShowStmtContext {
  public:
    ShowCreateTableStmtContext(ShowStmtContext *ctx);

    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *CREATE();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *TEMPORARY();
    antlr4::tree::TerminalNode *TABLE();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ShowTablesStmtContext : public ShowStmtContext {
  public:
    ShowTablesStmtContext(ShowStmtContext *ctx);

    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *TABLES();
    antlr4::tree::TerminalNode *TEMPORARY();
    DatabaseIdentifierContext *databaseIdentifier();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    WhereClauseContext *whereClause();
    LimitClauseContext *limitClause();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *IN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  ShowStmtContext* showStmt();

  class  SystemStmtContext : public antlr4::ParserRuleContext {
  public:
    SystemStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SYSTEM();
    antlr4::tree::TerminalNode *FLUSH();
    antlr4::tree::TerminalNode *DISTRIBUTED();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *LOGS();
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *STOP();
    antlr4::tree::TerminalNode *SENDS();
    antlr4::tree::TerminalNode *FETCHES();
    antlr4::tree::TerminalNode *MERGES();
    antlr4::tree::TerminalNode *SYNC();
    antlr4::tree::TerminalNode *REPLICA();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SystemStmtContext* systemStmt();

  class  TruncateStmtContext : public antlr4::ParserRuleContext {
  public:
    TruncateStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUNCATE();
    antlr4::tree::TerminalNode *TABLE();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *TEMPORARY();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *EXISTS();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TruncateStmtContext* truncateStmt();

  class  UseStmtContext : public antlr4::ParserRuleContext {
  public:
    UseStmtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *USE();
    DatabaseIdentifierContext *databaseIdentifier();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UseStmtContext* useStmt();

  class  ColumnTypeExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnTypeExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ColumnTypeExprContext() = default;
    void copyFrom(ColumnTypeExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ColumnTypeExprNestedContext : public ColumnTypeExprContext {
  public:
    ColumnTypeExprNestedContext(ColumnTypeExprContext *ctx);

    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<ColumnTypeExprContext *> columnTypeExpr();
    ColumnTypeExprContext* columnTypeExpr(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnTypeExprParamContext : public ColumnTypeExprContext {
  public:
    ColumnTypeExprParamContext(ColumnTypeExprContext *ctx);

    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    ColumnExprListContext *columnExprList();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnTypeExprSimpleContext : public ColumnTypeExprContext {
  public:
    ColumnTypeExprSimpleContext(ColumnTypeExprContext *ctx);

    IdentifierContext *identifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnTypeExprComplexContext : public ColumnTypeExprContext {
  public:
    ColumnTypeExprComplexContext(ColumnTypeExprContext *ctx);

    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<ColumnTypeExprContext *> columnTypeExpr();
    ColumnTypeExprContext* columnTypeExpr(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnTypeExprEnumContext : public ColumnTypeExprContext {
  public:
    ColumnTypeExprEnumContext(ColumnTypeExprContext *ctx);

    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<EnumValueContext *> enumValue();
    EnumValueContext* enumValue(size_t i);
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  ColumnTypeExprContext* columnTypeExpr();

  class  ColumnExprListContext : public antlr4::ParserRuleContext {
  public:
    ColumnExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColumnsExprContext *> columnsExpr();
    ColumnsExprContext* columnsExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ColumnExprListContext* columnExprList();

  class  ColumnsExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnsExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ColumnsExprContext() = default;
    void copyFrom(ColumnsExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ColumnsExprColumnContext : public ColumnsExprContext {
  public:
    ColumnsExprColumnContext(ColumnsExprContext *ctx);

    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnsExprAsteriskContext : public ColumnsExprContext {
  public:
    ColumnsExprAsteriskContext(ColumnsExprContext *ctx);

    antlr4::tree::TerminalNode *ASTERISK();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *DOT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnsExprSubqueryContext : public ColumnsExprContext {
  public:
    ColumnsExprSubqueryContext(ColumnsExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    SelectUnionStmtContext *selectUnionStmt();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  ColumnsExprContext* columnsExpr();

  class  ColumnExprContext : public antlr4::ParserRuleContext {
  public:
    ColumnExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ColumnExprContext() = default;
    void copyFrom(ColumnExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ColumnExprTernaryOpContext : public ColumnExprContext {
  public:
    ColumnExprTernaryOpContext(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *QUERY();
    antlr4::tree::TerminalNode *COLON();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprAliasContext : public ColumnExprContext {
  public:
    ColumnExprAliasContext(ColumnExprContext *ctx);

    ColumnExprContext *columnExpr();
    AliasContext *alias();
    antlr4::tree::TerminalNode *AS();
    IdentifierContext *identifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprExtractContext : public ColumnExprContext {
  public:
    ColumnExprExtractContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *EXTRACT();
    antlr4::tree::TerminalNode *LPAREN();
    IntervalContext *interval();
    antlr4::tree::TerminalNode *FROM();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprSubqueryContext : public ColumnExprContext {
  public:
    ColumnExprSubqueryContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    SelectUnionStmtContext *selectUnionStmt();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprLiteralContext : public ColumnExprContext {
  public:
    ColumnExprLiteralContext(ColumnExprContext *ctx);

    LiteralContext *literal();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprArrayContext : public ColumnExprContext {
  public:
    ColumnExprArrayContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *LBRACKET();
    antlr4::tree::TerminalNode *RBRACKET();
    ColumnExprListContext *columnExprList();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprSubstringContext : public ColumnExprContext {
  public:
    ColumnExprSubstringContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *SUBSTRING();
    antlr4::tree::TerminalNode *LPAREN();
    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *RPAREN();
    antlr4::tree::TerminalNode *FOR();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprCastContext : public ColumnExprContext {
  public:
    ColumnExprCastContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *CAST();
    antlr4::tree::TerminalNode *LPAREN();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *AS();
    ColumnTypeExprContext *columnTypeExpr();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprOrContext : public ColumnExprContext {
  public:
    ColumnExprOrContext(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *OR();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprPrecedence1Context : public ColumnExprContext {
  public:
    ColumnExprPrecedence1Context(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *ASTERISK();
    antlr4::tree::TerminalNode *SLASH();
    antlr4::tree::TerminalNode *PERCENT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprPrecedence2Context : public ColumnExprContext {
  public:
    ColumnExprPrecedence2Context(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *DASH();
    antlr4::tree::TerminalNode *CONCAT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprPrecedence3Context : public ColumnExprContext {
  public:
    ColumnExprPrecedence3Context(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *EQ_DOUBLE();
    antlr4::tree::TerminalNode *EQ_SINGLE();
    antlr4::tree::TerminalNode *NOT_EQ();
    antlr4::tree::TerminalNode *LE();
    antlr4::tree::TerminalNode *GE();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *NOT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprUnaryOpContext : public ColumnExprContext {
  public:
    ColumnExprUnaryOpContext(ColumnExprContext *ctx);

    UnaryOpContext *unaryOp();
    ColumnExprContext *columnExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprIntervalContext : public ColumnExprContext {
  public:
    ColumnExprIntervalContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *INTERVAL();
    ColumnExprContext *columnExpr();
    IntervalContext *interval();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprIsNullContext : public ColumnExprContext {
  public:
    ColumnExprIsNullContext(ColumnExprContext *ctx);

    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *NULL_SQL();
    antlr4::tree::TerminalNode *NOT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprTrimContext : public ColumnExprContext {
  public:
    ColumnExprTrimContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *TRIM();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *FROM();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *RPAREN();
    antlr4::tree::TerminalNode *BOTH();
    antlr4::tree::TerminalNode *LEADING();
    antlr4::tree::TerminalNode *TRAILING();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprTupleContext : public ColumnExprContext {
  public:
    ColumnExprTupleContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    ColumnExprListContext *columnExprList();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprArrayAccessContext : public ColumnExprContext {
  public:
    ColumnExprArrayAccessContext(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *LBRACKET();
    antlr4::tree::TerminalNode *RBRACKET();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprBetweenContext : public ColumnExprContext {
  public:
    ColumnExprBetweenContext(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *BETWEEN();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *NOT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprParensContext : public ColumnExprContext {
  public:
    ColumnExprParensContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprTimestampContext : public ColumnExprContext {
  public:
    ColumnExprTimestampContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *TIMESTAMP();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprAndContext : public ColumnExprContext {
  public:
    ColumnExprAndContext(ColumnExprContext *ctx);

    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    antlr4::tree::TerminalNode *AND();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprTupleAccessContext : public ColumnExprContext {
  public:
    ColumnExprTupleAccessContext(ColumnExprContext *ctx);

    ColumnExprContext *columnExpr();
    antlr4::tree::TerminalNode *DOT();
    antlr4::tree::TerminalNode *DECIMAL_LITERAL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprCaseContext : public ColumnExprContext {
  public:
    ColumnExprCaseContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *CASE();
    antlr4::tree::TerminalNode *END();
    std::vector<ColumnExprContext *> columnExpr();
    ColumnExprContext* columnExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> WHEN();
    antlr4::tree::TerminalNode* WHEN(size_t i);
    std::vector<antlr4::tree::TerminalNode *> THEN();
    antlr4::tree::TerminalNode* THEN(size_t i);
    antlr4::tree::TerminalNode *ELSE();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprDateContext : public ColumnExprContext {
  public:
    ColumnExprDateContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *DATE();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprIdentifierContext : public ColumnExprContext {
  public:
    ColumnExprIdentifierContext(ColumnExprContext *ctx);

    ColumnIdentifierContext *columnIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprFunctionContext : public ColumnExprContext {
  public:
    ColumnExprFunctionContext(ColumnExprContext *ctx);

    IdentifierContext *identifier();
    std::vector<antlr4::tree::TerminalNode *> LPAREN();
    antlr4::tree::TerminalNode* LPAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode *> RPAREN();
    antlr4::tree::TerminalNode* RPAREN(size_t i);
    antlr4::tree::TerminalNode *DISTINCT();
    ColumnArgListContext *columnArgList();
    ColumnExprListContext *columnExprList();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  ColumnExprAsteriskContext : public ColumnExprContext {
  public:
    ColumnExprAsteriskContext(ColumnExprContext *ctx);

    antlr4::tree::TerminalNode *ASTERISK();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *DOT();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  ColumnExprContext* columnExpr();
  ColumnExprContext* columnExpr(int precedence);
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
    ColumnLambdaExprContext *columnLambdaExpr();
    ColumnExprContext *columnExpr();

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
    NestedIdentifierContext *nestedIdentifier();
    TableIdentifierContext *tableIdentifier();
    antlr4::tree::TerminalNode *DOT();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ColumnIdentifierContext* columnIdentifier();

  class  NestedIdentifierContext : public antlr4::ParserRuleContext {
  public:
    NestedIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode *DOT();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NestedIdentifierContext* nestedIdentifier();

  class  TableExprContext : public antlr4::ParserRuleContext {
  public:
    TableExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    TableExprContext() = default;
    void copyFrom(TableExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  TableExprIdentifierContext : public TableExprContext {
  public:
    TableExprIdentifierContext(TableExprContext *ctx);

    TableIdentifierContext *tableIdentifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  TableExprSubqueryContext : public TableExprContext {
  public:
    TableExprSubqueryContext(TableExprContext *ctx);

    antlr4::tree::TerminalNode *LPAREN();
    SelectUnionStmtContext *selectUnionStmt();
    antlr4::tree::TerminalNode *RPAREN();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  TableExprAliasContext : public TableExprContext {
  public:
    TableExprAliasContext(TableExprContext *ctx);

    TableExprContext *tableExpr();
    AliasContext *alias();
    antlr4::tree::TerminalNode *AS();
    IdentifierContext *identifier();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  class  TableExprFunctionContext : public TableExprContext {
  public:
    TableExprFunctionContext(TableExprContext *ctx);

    TableFunctionExprContext *tableFunctionExpr();
    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
  };

  TableExprContext* tableExpr();
  TableExprContext* tableExpr(int precedence);
  class  TableFunctionExprContext : public antlr4::ParserRuleContext {
  public:
    TableFunctionExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    TableArgListContext *tableArgList();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableFunctionExprContext* tableFunctionExpr();

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

  class  TableArgListContext : public antlr4::ParserRuleContext {
  public:
    TableArgListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TableArgExprContext *> tableArgExpr();
    TableArgExprContext* tableArgExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableArgListContext* tableArgList();

  class  TableArgExprContext : public antlr4::ParserRuleContext {
  public:
    TableArgExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableIdentifierContext *tableIdentifier();
    TableFunctionExprContext *tableFunctionExpr();
    LiteralContext *literal();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TableArgExprContext* tableArgExpr();

  class  DatabaseIdentifierContext : public antlr4::ParserRuleContext {
  public:
    DatabaseIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DatabaseIdentifierContext* databaseIdentifier();

  class  FloatingLiteralContext : public antlr4::ParserRuleContext {
  public:
    FloatingLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FLOATING_LITERAL();
    antlr4::tree::TerminalNode *DOT();
    antlr4::tree::TerminalNode *DECIMAL_LITERAL();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FloatingLiteralContext* floatingLiteral();

  class  NumberLiteralContext : public antlr4::ParserRuleContext {
  public:
    NumberLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FloatingLiteralContext *floatingLiteral();
    antlr4::tree::TerminalNode *OCTAL_LITERAL();
    antlr4::tree::TerminalNode *DECIMAL_LITERAL();
    antlr4::tree::TerminalNode *HEXADECIMAL_LITERAL();
    antlr4::tree::TerminalNode *INF();
    antlr4::tree::TerminalNode *NAN_SQL();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *DASH();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NumberLiteralContext* numberLiteral();

  class  LiteralContext : public antlr4::ParserRuleContext {
  public:
    LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NumberLiteralContext *numberLiteral();
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *NULL_SQL();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LiteralContext* literal();

  class  IntervalContext : public antlr4::ParserRuleContext {
  public:
    IntervalContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SECOND();
    antlr4::tree::TerminalNode *MINUTE();
    antlr4::tree::TerminalNode *HOUR();
    antlr4::tree::TerminalNode *DAY();
    antlr4::tree::TerminalNode *WEEK();
    antlr4::tree::TerminalNode *MONTH();
    antlr4::tree::TerminalNode *QUARTER();
    antlr4::tree::TerminalNode *YEAR();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IntervalContext* interval();

  class  KeywordContext : public antlr4::ParserRuleContext {
  public:
    KeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AFTER();
    antlr4::tree::TerminalNode *ALIAS();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ALTER();
    antlr4::tree::TerminalNode *ANALYZE();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *ANTI();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *ARRAY();
    antlr4::tree::TerminalNode *AS();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *ASOF();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *BETWEEN();
    antlr4::tree::TerminalNode *BOTH();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *CASE();
    antlr4::tree::TerminalNode *CAST();
    antlr4::tree::TerminalNode *CHECK();
    antlr4::tree::TerminalNode *CLEAR();
    antlr4::tree::TerminalNode *CLUSTER();
    antlr4::tree::TerminalNode *CODEC();
    antlr4::tree::TerminalNode *COLLATE();
    antlr4::tree::TerminalNode *COLUMN();
    antlr4::tree::TerminalNode *COMMENT();
    antlr4::tree::TerminalNode *CONSTRAINT();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *CROSS();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *DATABASES();
    antlr4::tree::TerminalNode *DATE();
    antlr4::tree::TerminalNode *DAY();
    antlr4::tree::TerminalNode *DEDUPLICATE();
    antlr4::tree::TerminalNode *DEFAULT();
    antlr4::tree::TerminalNode *DELAY();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *DESCRIBE();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *DISK();
    antlr4::tree::TerminalNode *DISTINCT();
    antlr4::tree::TerminalNode *DISTRIBUTED();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *ELSE();
    antlr4::tree::TerminalNode *END();
    antlr4::tree::TerminalNode *ENGINE();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *EXTRACT();
    antlr4::tree::TerminalNode *FETCHES();
    antlr4::tree::TerminalNode *FINAL();
    antlr4::tree::TerminalNode *FIRST();
    antlr4::tree::TerminalNode *FLUSH();
    antlr4::tree::TerminalNode *FOR();
    antlr4::tree::TerminalNode *FORMAT();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *FULL();
    antlr4::tree::TerminalNode *FUNCTION();
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *GRANULARITY();
    antlr4::tree::TerminalNode *GROUP();
    antlr4::tree::TerminalNode *HAVING();
    antlr4::tree::TerminalNode *HOUR();
    antlr4::tree::TerminalNode *ID();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *INNER();
    antlr4::tree::TerminalNode *INSERT();
    antlr4::tree::TerminalNode *INTERVAL();
    antlr4::tree::TerminalNode *INTO();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *JOIN();
    antlr4::tree::TerminalNode *JSON_FALSE();
    antlr4::tree::TerminalNode *JSON_TRUE();
    antlr4::tree::TerminalNode *KEY();
    antlr4::tree::TerminalNode *LAST();
    antlr4::tree::TerminalNode *LEADING();
    antlr4::tree::TerminalNode *LEFT();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *LOCAL();
    antlr4::tree::TerminalNode *LOGS();
    antlr4::tree::TerminalNode *MATERIALIZED();
    antlr4::tree::TerminalNode *MERGES();
    antlr4::tree::TerminalNode *MINUTE();
    antlr4::tree::TerminalNode *MODIFY();
    antlr4::tree::TerminalNode *MONTH();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *NULLS();
    antlr4::tree::TerminalNode *OFFSET();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *OPTIMIZE();
    antlr4::tree::TerminalNode *OR();
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *OUTER();
    antlr4::tree::TerminalNode *OUTFILE();
    antlr4::tree::TerminalNode *PARTITION();
    antlr4::tree::TerminalNode *POPULATE();
    antlr4::tree::TerminalNode *PREWHERE();
    antlr4::tree::TerminalNode *PRIMARY();
    antlr4::tree::TerminalNode *QUARTER();
    antlr4::tree::TerminalNode *REMOVE();
    antlr4::tree::TerminalNode *RENAME();
    antlr4::tree::TerminalNode *REPLACE();
    antlr4::tree::TerminalNode *REPLICA();
    antlr4::tree::TerminalNode *RIGHT();
    antlr4::tree::TerminalNode *SAMPLE();
    antlr4::tree::TerminalNode *SECOND();
    antlr4::tree::TerminalNode *SELECT();
    antlr4::tree::TerminalNode *SEMI();
    antlr4::tree::TerminalNode *SENDS();
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *SETTINGS();
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *STOP();
    antlr4::tree::TerminalNode *SUBSTRING();
    antlr4::tree::TerminalNode *SYNC();
    antlr4::tree::TerminalNode *SYSTEM();
    antlr4::tree::TerminalNode *TABLE();
    antlr4::tree::TerminalNode *TABLES();
    antlr4::tree::TerminalNode *TEMPORARY();
    antlr4::tree::TerminalNode *THEN();
    antlr4::tree::TerminalNode *TIES();
    antlr4::tree::TerminalNode *TIMESTAMP();
    antlr4::tree::TerminalNode *TOTALS();
    antlr4::tree::TerminalNode *TRAILING();
    antlr4::tree::TerminalNode *TRIM();
    antlr4::tree::TerminalNode *TRUNCATE();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *TTL();
    antlr4::tree::TerminalNode *TYPE();
    antlr4::tree::TerminalNode *UNION();
    antlr4::tree::TerminalNode *USE();
    antlr4::tree::TerminalNode *USING();
    antlr4::tree::TerminalNode *VALUES();
    antlr4::tree::TerminalNode *VIEW();
    antlr4::tree::TerminalNode *VOLUME();
    antlr4::tree::TerminalNode *WEEK();
    antlr4::tree::TerminalNode *WHEN();
    antlr4::tree::TerminalNode *WHERE();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *YEAR();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KeywordContext* keyword();

  class  KeywordForAliasContext : public antlr4::ParserRuleContext {
  public:
    KeywordForAliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ID();
    antlr4::tree::TerminalNode *KEY();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KeywordForAliasContext* keywordForAlias();

  class  AliasContext : public antlr4::ParserRuleContext {
  public:
    AliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
    KeywordForAliasContext *keywordForAlias();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AliasContext* alias();

  class  IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
    IntervalContext *interval();
    KeywordContext *keyword();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IdentifierContext* identifier();

  class  IdentifierOrNullContext : public antlr4::ParserRuleContext {
  public:
    IdentifierOrNullContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *NULL_SQL();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IdentifierOrNullContext* identifierOrNull();

  class  UnaryOpContext : public antlr4::ParserRuleContext {
  public:
    UnaryOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DASH();
    antlr4::tree::TerminalNode *NOT();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UnaryOpContext* unaryOp();

  class  EnumValueContext : public antlr4::ParserRuleContext {
  public:
    EnumValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STRING_LITERAL();
    antlr4::tree::TerminalNode *EQ_SINGLE();
    NumberLiteralContext *numberLiteral();

    virtual antlrcpp::Any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  EnumValueContext* enumValue();


  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;
  bool engineClauseSempred(EngineClauseContext *_localctx, size_t predicateIndex);
  bool joinExprSempred(JoinExprContext *_localctx, size_t predicateIndex);
  bool columnExprSempred(ColumnExprContext *_localctx, size_t predicateIndex);
  bool tableExprSempred(TableExprContext *_localctx, size_t predicateIndex);

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
