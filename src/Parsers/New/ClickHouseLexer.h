
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, ANALYZE = 6, AND = 7, 
    ANTI = 8, ANY = 9, ARRAY = 10, AS = 11, ASCENDING = 12, ASOF = 13, ATTACH = 14, 
    BETWEEN = 15, BOTH = 16, BY = 17, CASE = 18, CAST = 19, CHECK = 20, 
    CLEAR = 21, CLUSTER = 22, CODEC = 23, COLLATE = 24, COLUMN = 25, COMMENT = 26, 
    CONSTRAINT = 27, CREATE = 28, CROSS = 29, DATABASE = 30, DATABASES = 31, 
    DAY = 32, DEDUPLICATE = 33, DEFAULT = 34, DELAY = 35, DELETE = 36, DESC = 37, 
    DESCENDING = 38, DESCRIBE = 39, DETACH = 40, DISK = 41, DISTINCT = 42, 
    DISTRIBUTED = 43, DROP = 44, ELSE = 45, END = 46, ENGINE = 47, EXISTS = 48, 
    EXTRACT = 49, FETCHES = 50, FINAL = 51, FIRST = 52, FLUSH = 53, FOR = 54, 
    FORMAT = 55, FROM = 56, FULL = 57, FUNCTION = 58, GLOBAL = 59, GRANULARITY = 60, 
    GROUP = 61, HAVING = 62, HOUR = 63, ID = 64, IF = 65, IN = 66, INDEX = 67, 
    INF = 68, INNER = 69, INSERT = 70, INTERVAL = 71, INTO = 72, IS = 73, 
    JOIN = 74, KEY = 75, LAST = 76, LEADING = 77, LEFT = 78, LIKE = 79, 
    LIMIT = 80, LOCAL = 81, MATERIALIZED = 82, MERGES = 83, MINUTE = 84, 
    MODIFY = 85, MONTH = 86, NAN_SQL = 87, NO = 88, NOT = 89, NULL_SQL = 90, 
    NULLS = 91, OFFSET = 92, ON = 93, OPTIMIZE = 94, OR = 95, ORDER = 96, 
    OUTER = 97, OUTFILE = 98, PARTITION = 99, POPULATE = 100, PREWHERE = 101, 
    PRIMARY = 102, QUARTER = 103, REMOVE = 104, RENAME = 105, REPLACE = 106, 
    REPLICA = 107, RIGHT = 108, SAMPLE = 109, SECOND = 110, SELECT = 111, 
    SEMI = 112, SENDS = 113, SET = 114, SETTINGS = 115, SHOW = 116, START = 117, 
    STOP = 118, SUBSTRING = 119, SYNC = 120, SYSTEM = 121, TABLE = 122, 
    TABLES = 123, TEMPORARY = 124, THEN = 125, TIES = 126, TO = 127, TOTALS = 128, 
    TRAILING = 129, TRIM = 130, TRUNCATE = 131, TTL = 132, TYPE = 133, UNION = 134, 
    USE = 135, USING = 136, VALUES = 137, VIEW = 138, VOLUME = 139, WEEK = 140, 
    WHEN = 141, WHERE = 142, WITH = 143, YEAR = 144, JSON_FALSE = 145, JSON_TRUE = 146, 
    IDENTIFIER = 147, FLOATING_LITERAL = 148, HEXADECIMAL_LITERAL = 149, 
    INTEGER_LITERAL = 150, STRING_LITERAL = 151, ARROW = 152, ASTERISK = 153, 
    BACKQUOTE = 154, BACKSLASH = 155, COLON = 156, COMMA = 157, CONCAT = 158, 
    DASH = 159, DOT = 160, EQ_DOUBLE = 161, EQ_SINGLE = 162, GE = 163, GT = 164, 
    LBRACE = 165, LBRACKET = 166, LE = 167, LPAREN = 168, LT = 169, NOT_EQ = 170, 
    PERCENT = 171, PLUS = 172, QUERY = 173, QUOTE_DOUBLE = 174, QUOTE_SINGLE = 175, 
    RBRACE = 176, RBRACKET = 177, RPAREN = 178, SEMICOLON = 179, SLASH = 180, 
    UNDERSCORE = 181, MULTI_LINE_COMMENT = 182, SINGLE_LINE_COMMENT = 183, 
    WHITESPACE = 184
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
