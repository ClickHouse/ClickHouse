
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
    DROP = 42, ELSE = 43, END = 44, ENGINE = 45, EXISTS = 46, EXTRACT = 47, 
    FETCHES = 48, FINAL = 49, FIRST = 50, FOR = 51, FORMAT = 52, FROM = 53, 
    FULL = 54, FUNCTION = 55, GLOBAL = 56, GROUP = 57, HAVING = 58, HOUR = 59, 
    ID = 60, IF = 61, IN = 62, INF = 63, INNER = 64, INSERT = 65, INTERVAL = 66, 
    INTO = 67, IS = 68, JOIN = 69, KEY = 70, LAST = 71, LEADING = 72, LEFT = 73, 
    LIKE = 74, LIMIT = 75, LOCAL = 76, MATERIALIZED = 77, MERGES = 78, MINUTE = 79, 
    MODIFY = 80, MONTH = 81, NAN_SQL = 82, NO = 83, NOT = 84, NULL_SQL = 85, 
    NULLS = 86, OFFSET = 87, ON = 88, OPTIMIZE = 89, OR = 90, ORDER = 91, 
    OUTER = 92, OUTFILE = 93, PARTITION = 94, POPULATE = 95, PREWHERE = 96, 
    PRIMARY = 97, QUARTER = 98, RENAME = 99, REPLACE = 100, REPLICA = 101, 
    RIGHT = 102, SAMPLE = 103, SECOND = 104, SELECT = 105, SEMI = 106, SET = 107, 
    SETTINGS = 108, SHOW = 109, START = 110, STOP = 111, SUBSTRING = 112, 
    SYNC = 113, SYSTEM = 114, TABLE = 115, TABLES = 116, TEMPORARY = 117, 
    THEN = 118, TIES = 119, TO = 120, TOTALS = 121, TRAILING = 122, TRIM = 123, 
    TRUNCATE = 124, TTL = 125, UNION = 126, USE = 127, USING = 128, VALUES = 129, 
    VIEW = 130, VOLUME = 131, WEEK = 132, WHEN = 133, WHERE = 134, WITH = 135, 
    YEAR = 136, JSON_FALSE = 137, JSON_TRUE = 138, IDENTIFIER = 139, FLOATING_LITERAL = 140, 
    HEXADECIMAL_LITERAL = 141, INTEGER_LITERAL = 142, STRING_LITERAL = 143, 
    ARROW = 144, ASTERISK = 145, BACKQUOTE = 146, BACKSLASH = 147, COLON = 148, 
    COMMA = 149, CONCAT = 150, DASH = 151, DOT = 152, EQ_DOUBLE = 153, EQ_SINGLE = 154, 
    GE = 155, GT = 156, LBRACE = 157, LBRACKET = 158, LE = 159, LPAREN = 160, 
    LT = 161, NOT_EQ = 162, PERCENT = 163, PLUS = 164, QUERY = 165, QUOTE_DOUBLE = 166, 
    QUOTE_SINGLE = 167, RBRACE = 168, RBRACKET = 169, RPAREN = 170, SEMICOLON = 171, 
    SLASH = 172, UNDERSCORE = 173, MULTI_LINE_COMMENT = 174, SINGLE_LINE_COMMENT = 175, 
    WHITESPACE = 176
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
