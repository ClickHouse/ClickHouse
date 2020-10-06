
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
    CREATE = 27, CROSS = 28, DATABASE = 29, DATABASES = 30, DAY = 31, DEDUPLICATE = 32, 
    DEFAULT = 33, DELAY = 34, DELETE = 35, DESC = 36, DESCENDING = 37, DESCRIBE = 38, 
    DETACH = 39, DISK = 40, DISTINCT = 41, DISTRIBUTED = 42, DROP = 43, 
    ELSE = 44, END = 45, ENGINE = 46, EXISTS = 47, EXTRACT = 48, FETCHES = 49, 
    FINAL = 50, FIRST = 51, FLUSH = 52, FOR = 53, FORMAT = 54, FROM = 55, 
    FULL = 56, FUNCTION = 57, GLOBAL = 58, GRANULARITY = 59, GROUP = 60, 
    HAVING = 61, HOUR = 62, ID = 63, IF = 64, IN = 65, INDEX = 66, INF = 67, 
    INNER = 68, INSERT = 69, INTERVAL = 70, INTO = 71, IS = 72, JOIN = 73, 
    KEY = 74, LAST = 75, LEADING = 76, LEFT = 77, LIKE = 78, LIMIT = 79, 
    LOCAL = 80, MATERIALIZED = 81, MERGES = 82, MINUTE = 83, MODIFY = 84, 
    MONTH = 85, NAN_SQL = 86, NO = 87, NOT = 88, NULL_SQL = 89, NULLS = 90, 
    OFFSET = 91, ON = 92, OPTIMIZE = 93, OR = 94, ORDER = 95, OUTER = 96, 
    OUTFILE = 97, PARTITION = 98, POPULATE = 99, PREWHERE = 100, PRIMARY = 101, 
    QUARTER = 102, REMOVE = 103, RENAME = 104, REPLACE = 105, REPLICA = 106, 
    RIGHT = 107, SAMPLE = 108, SECOND = 109, SELECT = 110, SEMI = 111, SENDS = 112, 
    SET = 113, SETTINGS = 114, SHOW = 115, START = 116, STOP = 117, SUBSTRING = 118, 
    SYNC = 119, SYSTEM = 120, TABLE = 121, TABLES = 122, TEMPORARY = 123, 
    THEN = 124, TIES = 125, TO = 126, TOTALS = 127, TRAILING = 128, TRIM = 129, 
    TRUNCATE = 130, TTL = 131, TYPE = 132, UNION = 133, USE = 134, USING = 135, 
    VALUES = 136, VIEW = 137, VOLUME = 138, WEEK = 139, WHEN = 140, WHERE = 141, 
    WITH = 142, YEAR = 143, JSON_FALSE = 144, JSON_TRUE = 145, IDENTIFIER = 146, 
    FLOATING_LITERAL = 147, HEXADECIMAL_LITERAL = 148, INTEGER_LITERAL = 149, 
    STRING_LITERAL = 150, ARROW = 151, ASTERISK = 152, BACKQUOTE = 153, 
    BACKSLASH = 154, COLON = 155, COMMA = 156, CONCAT = 157, DASH = 158, 
    DOT = 159, EQ_DOUBLE = 160, EQ_SINGLE = 161, GE = 162, GT = 163, LBRACE = 164, 
    LBRACKET = 165, LE = 166, LPAREN = 167, LT = 168, NOT_EQ = 169, PERCENT = 170, 
    PLUS = 171, QUERY = 172, QUOTE_DOUBLE = 173, QUOTE_SINGLE = 174, RBRACE = 175, 
    RBRACKET = 176, RPAREN = 177, SEMICOLON = 178, SLASH = 179, UNDERSCORE = 180, 
    MULTI_LINE_COMMENT = 181, SINGLE_LINE_COMMENT = 182, WHITESPACE = 183
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
