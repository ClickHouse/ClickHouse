
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
    DATE = 32, DAY = 33, DEDUPLICATE = 34, DEFAULT = 35, DELAY = 36, DELETE = 37, 
    DESC = 38, DESCENDING = 39, DESCRIBE = 40, DETACH = 41, DISK = 42, DISTINCT = 43, 
    DISTRIBUTED = 44, DROP = 45, ELSE = 46, END = 47, ENGINE = 48, EXISTS = 49, 
    EXTRACT = 50, FETCHES = 51, FINAL = 52, FIRST = 53, FLUSH = 54, FOR = 55, 
    FORMAT = 56, FROM = 57, FULL = 58, FUNCTION = 59, GLOBAL = 60, GRANULARITY = 61, 
    GROUP = 62, HAVING = 63, HOUR = 64, ID = 65, IF = 66, IN = 67, INDEX = 68, 
    INF = 69, INNER = 70, INSERT = 71, INTERVAL = 72, INTO = 73, IS = 74, 
    JOIN = 75, KEY = 76, LAST = 77, LEADING = 78, LEFT = 79, LIKE = 80, 
    LIMIT = 81, LOCAL = 82, MATERIALIZED = 83, MERGES = 84, MINUTE = 85, 
    MODIFY = 86, MONTH = 87, NAN_SQL = 88, NO = 89, NOT = 90, NULL_SQL = 91, 
    NULLS = 92, OFFSET = 93, ON = 94, OPTIMIZE = 95, OR = 96, ORDER = 97, 
    OUTER = 98, OUTFILE = 99, PARTITION = 100, POPULATE = 101, PREWHERE = 102, 
    PRIMARY = 103, QUARTER = 104, REMOVE = 105, RENAME = 106, REPLACE = 107, 
    REPLICA = 108, RIGHT = 109, SAMPLE = 110, SECOND = 111, SELECT = 112, 
    SEMI = 113, SENDS = 114, SET = 115, SETTINGS = 116, SHOW = 117, START = 118, 
    STOP = 119, SUBSTRING = 120, SYNC = 121, SYSTEM = 122, TABLE = 123, 
    TABLES = 124, TEMPORARY = 125, THEN = 126, TIES = 127, TIMESTAMP = 128, 
    TO = 129, TOTALS = 130, TRAILING = 131, TRIM = 132, TRUNCATE = 133, 
    TTL = 134, TYPE = 135, UNION = 136, USE = 137, USING = 138, VALUES = 139, 
    VIEW = 140, VOLUME = 141, WEEK = 142, WHEN = 143, WHERE = 144, WITH = 145, 
    YEAR = 146, JSON_FALSE = 147, JSON_TRUE = 148, IDENTIFIER = 149, FLOATING_LITERAL = 150, 
    HEXADECIMAL_LITERAL = 151, INTEGER_LITERAL = 152, STRING_LITERAL = 153, 
    ARROW = 154, ASTERISK = 155, BACKQUOTE = 156, BACKSLASH = 157, COLON = 158, 
    COMMA = 159, CONCAT = 160, DASH = 161, DOT = 162, EQ_DOUBLE = 163, EQ_SINGLE = 164, 
    GE = 165, GT = 166, LBRACE = 167, LBRACKET = 168, LE = 169, LPAREN = 170, 
    LT = 171, NOT_EQ = 172, PERCENT = 173, PLUS = 174, QUERY = 175, QUOTE_DOUBLE = 176, 
    QUOTE_SINGLE = 177, RBRACE = 178, RBRACKET = 179, RPAREN = 180, SEMICOLON = 181, 
    SLASH = 182, UNDERSCORE = 183, MULTI_LINE_COMMENT = 184, SINGLE_LINE_COMMENT = 185, 
    WHITESPACE = 186
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
