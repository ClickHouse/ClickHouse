
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ADD = 2, AFTER = 3, ALIAS = 4, ALL = 5, ALTER = 6, 
    ANALYZE = 7, AND = 8, ANTI = 9, ANY = 10, ARRAY = 11, AS = 12, ASCENDING = 13, 
    ASOF = 14, ATTACH = 15, BETWEEN = 16, BOTH = 17, BY = 18, CASE = 19, 
    CAST = 20, CHECK = 21, CLEAR = 22, CLUSTER = 23, COLLATE = 24, COLUMN = 25, 
    COMMENT = 26, CREATE = 27, CROSS = 28, DATABASE = 29, DATABASES = 30, 
    DAY = 31, DEDUPLICATE = 32, DEFAULT = 33, DELAY = 34, DELETE = 35, DESC = 36, 
    DESCENDING = 37, DESCRIBE = 38, DETACH = 39, DISK = 40, DISTINCT = 41, 
    DISTRIBUTED = 42, DROP = 43, ELSE = 44, END = 45, ENGINE = 46, EXISTS = 47, 
    EXTRACT = 48, FETCHES = 49, FINAL = 50, FIRST = 51, FLUSH = 52, FOR = 53, 
    FORMAT = 54, FROM = 55, FULL = 56, FUNCTION = 57, GLOBAL = 58, GRANULARITY = 59, 
    GROUP = 60, HAVING = 61, HOUR = 62, ID = 63, IF = 64, IN = 65, INDEX = 66, 
    INF = 67, INNER = 68, INSERT = 69, INTERVAL = 70, INTO = 71, IS = 72, 
    JOIN = 73, KEY = 74, LAST = 75, LEADING = 76, LEFT = 77, LIKE = 78, 
    LIMIT = 79, LOCAL = 80, MATERIALIZED = 81, MERGES = 82, MINUTE = 83, 
    MODIFY = 84, MONTH = 85, NAN_SQL = 86, NO = 87, NOT = 88, NULL_SQL = 89, 
    NULLS = 90, OFFSET = 91, ON = 92, OPTIMIZE = 93, OR = 94, ORDER = 95, 
    OUTER = 96, OUTFILE = 97, PARTITION = 98, POPULATE = 99, PREWHERE = 100, 
    PRIMARY = 101, QUARTER = 102, RENAME = 103, REPLACE = 104, REPLICA = 105, 
    RIGHT = 106, SAMPLE = 107, SECOND = 108, SELECT = 109, SEMI = 110, SENDS = 111, 
    SET = 112, SETTINGS = 113, SHOW = 114, START = 115, STOP = 116, SUBSTRING = 117, 
    SYNC = 118, SYSTEM = 119, TABLE = 120, TABLES = 121, TEMPORARY = 122, 
    THEN = 123, TIES = 124, TO = 125, TOTALS = 126, TRAILING = 127, TRIM = 128, 
    TRUNCATE = 129, TTL = 130, TYPE = 131, UNION = 132, USE = 133, USING = 134, 
    VALUES = 135, VIEW = 136, VOLUME = 137, WEEK = 138, WHEN = 139, WHERE = 140, 
    WITH = 141, YEAR = 142, JSON_FALSE = 143, JSON_TRUE = 144, IDENTIFIER = 145, 
    FLOATING_LITERAL = 146, HEXADECIMAL_LITERAL = 147, INTEGER_LITERAL = 148, 
    STRING_LITERAL = 149, ARROW = 150, ASTERISK = 151, BACKQUOTE = 152, 
    BACKSLASH = 153, COLON = 154, COMMA = 155, CONCAT = 156, DASH = 157, 
    DOT = 158, EQ_DOUBLE = 159, EQ_SINGLE = 160, GE = 161, GT = 162, LBRACE = 163, 
    LBRACKET = 164, LE = 165, LPAREN = 166, LT = 167, NOT_EQ = 168, PERCENT = 169, 
    PLUS = 170, QUERY = 171, QUOTE_DOUBLE = 172, QUOTE_SINGLE = 173, RBRACE = 174, 
    RBRACKET = 175, RPAREN = 176, SEMICOLON = 177, SLASH = 178, UNDERSCORE = 179, 
    MULTI_LINE_COMMENT = 180, SINGLE_LINE_COMMENT = 181, WHITESPACE = 182
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
