
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, AND = 6, ANTI = 7, 
    ANY = 8, ARRAY = 9, AS = 10, ASCENDING = 11, ASOF = 12, ATTACH = 13, 
    BETWEEN = 14, BOTH = 15, BY = 16, CASE = 17, CAST = 18, CHECK = 19, 
    CLEAR = 20, CLUSTER = 21, CODEC = 22, COLLATE = 23, COLUMN = 24, COMMENT = 25, 
    CONSTRAINT = 26, CREATE = 27, CROSS = 28, DATABASE = 29, DATABASES = 30, 
    DATE = 31, DAY = 32, DEDUPLICATE = 33, DEFAULT = 34, DELAY = 35, DELETE = 36, 
    DESC = 37, DESCENDING = 38, DESCRIBE = 39, DETACH = 40, DISK = 41, DISTINCT = 42, 
    DISTRIBUTED = 43, DROP = 44, ELSE = 45, END = 46, ENGINE = 47, EXISTS = 48, 
    EXPLAIN = 49, EXTRACT = 50, FETCHES = 51, FINAL = 52, FIRST = 53, FLUSH = 54, 
    FOR = 55, FORMAT = 56, FROM = 57, FULL = 58, FUNCTION = 59, GLOBAL = 60, 
    GRANULARITY = 61, GROUP = 62, HAVING = 63, HOUR = 64, ID = 65, IF = 66, 
    IN = 67, INDEX = 68, INF = 69, INNER = 70, INSERT = 71, INTERVAL = 72, 
    INTO = 73, IS = 74, JOIN = 75, KEY = 76, LAST = 77, LEADING = 78, LEFT = 79, 
    LIKE = 80, LIMIT = 81, LOCAL = 82, LOGS = 83, MATERIALIZED = 84, MERGES = 85, 
    MINUTE = 86, MODIFY = 87, MONTH = 88, NAN_SQL = 89, NO = 90, NOT = 91, 
    NULL_SQL = 92, NULLS = 93, OFFSET = 94, ON = 95, OPTIMIZE = 96, OR = 97, 
    ORDER = 98, OUTER = 99, OUTFILE = 100, PARTITION = 101, POPULATE = 102, 
    PREWHERE = 103, PRIMARY = 104, QUARTER = 105, REMOVE = 106, RENAME = 107, 
    REPLACE = 108, REPLICA = 109, RIGHT = 110, SAMPLE = 111, SECOND = 112, 
    SELECT = 113, SEMI = 114, SENDS = 115, SET = 116, SETTINGS = 117, SHOW = 118, 
    START = 119, STOP = 120, SUBSTRING = 121, SYNC = 122, SYNTAX = 123, 
    SYSTEM = 124, TABLE = 125, TABLES = 126, TEMPORARY = 127, THEN = 128, 
    TIES = 129, TIMESTAMP = 130, TO = 131, TOTALS = 132, TRAILING = 133, 
    TRIM = 134, TRUNCATE = 135, TTL = 136, TYPE = 137, UNION = 138, USE = 139, 
    USING = 140, VALUES = 141, VIEW = 142, VOLUME = 143, WEEK = 144, WHEN = 145, 
    WHERE = 146, WITH = 147, YEAR = 148, JSON_FALSE = 149, JSON_TRUE = 150, 
    IDENTIFIER = 151, FLOATING_LITERAL = 152, OCTAL_LITERAL = 153, DECIMAL_LITERAL = 154, 
    HEXADECIMAL_LITERAL = 155, STRING_LITERAL = 156, ARROW = 157, ASTERISK = 158, 
    BACKQUOTE = 159, BACKSLASH = 160, COLON = 161, COMMA = 162, CONCAT = 163, 
    DASH = 164, DOT = 165, EQ_DOUBLE = 166, EQ_SINGLE = 167, GE = 168, GT = 169, 
    LBRACE = 170, LBRACKET = 171, LE = 172, LPAREN = 173, LT = 174, NOT_EQ = 175, 
    PERCENT = 176, PLUS = 177, QUERY = 178, QUOTE_DOUBLE = 179, QUOTE_SINGLE = 180, 
    RBRACE = 181, RBRACKET = 182, RPAREN = 183, SEMICOLON = 184, SLASH = 185, 
    UNDERSCORE = 186, MULTI_LINE_COMMENT = 187, SINGLE_LINE_COMMENT = 188, 
    WHITESPACE = 189
  };

  ClickHouseLexer(antlr4::CharStream *input);
  ~ClickHouseLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace DB
