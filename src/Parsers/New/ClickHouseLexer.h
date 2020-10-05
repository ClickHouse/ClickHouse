
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
    CLEAR = 21, CLUSTER = 22, COLLATE = 23, COLUMN = 24, COMMENT = 25, CREATE = 26, 
    CROSS = 27, DATABASE = 28, DATABASES = 29, DAY = 30, DEDUPLICATE = 31, 
    DEFAULT = 32, DELAY = 33, DELETE = 34, DESC = 35, DESCENDING = 36, DESCRIBE = 37, 
    DETACH = 38, DISK = 39, DISTINCT = 40, DISTRIBUTED = 41, DROP = 42, 
    ELSE = 43, END = 44, ENGINE = 45, EXISTS = 46, EXTRACT = 47, FETCHES = 48, 
    FINAL = 49, FIRST = 50, FLUSH = 51, FOR = 52, FORMAT = 53, FROM = 54, 
    FULL = 55, FUNCTION = 56, GLOBAL = 57, GRANULARITY = 58, GROUP = 59, 
    HAVING = 60, HOUR = 61, ID = 62, IF = 63, IN = 64, INDEX = 65, INF = 66, 
    INNER = 67, INSERT = 68, INTERVAL = 69, INTO = 70, IS = 71, JOIN = 72, 
    KEY = 73, LAST = 74, LEADING = 75, LEFT = 76, LIKE = 77, LIMIT = 78, 
    LOCAL = 79, MATERIALIZED = 80, MERGES = 81, MINUTE = 82, MODIFY = 83, 
    MONTH = 84, NAN_SQL = 85, NO = 86, NOT = 87, NULL_SQL = 88, NULLS = 89, 
    OFFSET = 90, ON = 91, OPTIMIZE = 92, OR = 93, ORDER = 94, OUTER = 95, 
    OUTFILE = 96, PARTITION = 97, POPULATE = 98, PREWHERE = 99, PRIMARY = 100, 
    QUARTER = 101, RENAME = 102, REPLACE = 103, REPLICA = 104, RIGHT = 105, 
    SAMPLE = 106, SECOND = 107, SELECT = 108, SEMI = 109, SENDS = 110, SET = 111, 
    SETTINGS = 112, SHOW = 113, START = 114, STOP = 115, SUBSTRING = 116, 
    SYNC = 117, SYSTEM = 118, TABLE = 119, TABLES = 120, TEMPORARY = 121, 
    THEN = 122, TIES = 123, TO = 124, TOTALS = 125, TRAILING = 126, TRIM = 127, 
    TRUNCATE = 128, TTL = 129, TYPE = 130, UNION = 131, USE = 132, USING = 133, 
    VALUES = 134, VIEW = 135, VOLUME = 136, WEEK = 137, WHEN = 138, WHERE = 139, 
    WITH = 140, YEAR = 141, JSON_FALSE = 142, JSON_TRUE = 143, IDENTIFIER = 144, 
    FLOATING_LITERAL = 145, HEXADECIMAL_LITERAL = 146, INTEGER_LITERAL = 147, 
    STRING_LITERAL = 148, ARROW = 149, ASTERISK = 150, BACKQUOTE = 151, 
    BACKSLASH = 152, COLON = 153, COMMA = 154, CONCAT = 155, DASH = 156, 
    DOT = 157, EQ_DOUBLE = 158, EQ_SINGLE = 159, GE = 160, GT = 161, LBRACE = 162, 
    LBRACKET = 163, LE = 164, LPAREN = 165, LT = 166, NOT_EQ = 167, PERCENT = 168, 
    PLUS = 169, QUERY = 170, QUOTE_DOUBLE = 171, QUOTE_SINGLE = 172, RBRACE = 173, 
    RBRACKET = 174, RPAREN = 175, SEMICOLON = 176, SLASH = 177, UNDERSCORE = 178, 
    MULTI_LINE_COMMENT = 179, SINGLE_LINE_COMMENT = 180, WHITESPACE = 181
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
