
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
    CONSTRAINT = 26, CREATE = 27, CROSS = 28, CUBE = 29, DATABASE = 30, 
    DATABASES = 31, DATE = 32, DAY = 33, DEDUPLICATE = 34, DEFAULT = 35, 
    DELAY = 36, DELETE = 37, DESC = 38, DESCENDING = 39, DESCRIBE = 40, 
    DETACH = 41, DICTIONARY = 42, DISK = 43, DISTINCT = 44, DISTRIBUTED = 45, 
    DROP = 46, ELSE = 47, END = 48, ENGINE = 49, EVENTS = 50, EXISTS = 51, 
    EXPLAIN = 52, EXPRESSION = 53, EXTRACT = 54, FETCHES = 55, FINAL = 56, 
    FIRST = 57, FLUSH = 58, FOR = 59, FORMAT = 60, FREEZE = 61, FROM = 62, 
    FULL = 63, FUNCTION = 64, GLOBAL = 65, GRANULARITY = 66, GROUP = 67, 
    HAVING = 68, HIERARCHICAL = 69, HOUR = 70, ID = 71, IF = 72, ILIKE = 73, 
    IN = 74, INDEX = 75, INF = 76, INJECTIVE = 77, INNER = 78, INSERT = 79, 
    INTERVAL = 80, INTO = 81, IS = 82, IS_OBJECT_ID = 83, JOIN = 84, KEY = 85, 
    LAST = 86, LAYOUT = 87, LEADING = 88, LEFT = 89, LIFETIME = 90, LIKE = 91, 
    LIMIT = 92, LIVE = 93, LOCAL = 94, LOGS = 95, MATERIALIZED = 96, MAX = 97, 
    MERGES = 98, MIN = 99, MINUTE = 100, MODIFY = 101, MONTH = 102, MOVE = 103, 
    NAN_SQL = 104, NO = 105, NOT = 106, NULL_SQL = 107, NULLS = 108, OFFSET = 109, 
    ON = 110, OPTIMIZE = 111, OR = 112, ORDER = 113, OUTER = 114, OUTFILE = 115, 
    PARTITION = 116, POPULATE = 117, PREWHERE = 118, PRIMARY = 119, QUARTER = 120, 
    RANGE = 121, REMOVE = 122, RENAME = 123, REPLACE = 124, REPLICA = 125, 
    REPLICATED = 126, RIGHT = 127, ROLLUP = 128, SAMPLE = 129, SECOND = 130, 
    SELECT = 131, SEMI = 132, SENDS = 133, SET = 134, SETTINGS = 135, SHOW = 136, 
    SOURCE = 137, START = 138, STOP = 139, SUBSTRING = 140, SYNC = 141, 
    SYNTAX = 142, SYSTEM = 143, TABLE = 144, TABLES = 145, TEMPORARY = 146, 
    THEN = 147, TIES = 148, TIMEOUT = 149, TIMESTAMP = 150, TO = 151, TOP = 152, 
    TOTALS = 153, TRAILING = 154, TRIM = 155, TRUNCATE = 156, TTL = 157, 
    TYPE = 158, UNION = 159, UPDATE = 160, USE = 161, USING = 162, UUID = 163, 
    VALUES = 164, VIEW = 165, VOLUME = 166, WATCH = 167, WEEK = 168, WHEN = 169, 
    WHERE = 170, WITH = 171, YEAR = 172, JSON_FALSE = 173, JSON_TRUE = 174, 
    IDENTIFIER = 175, FLOATING_LITERAL = 176, OCTAL_LITERAL = 177, DECIMAL_LITERAL = 178, 
    HEXADECIMAL_LITERAL = 179, STRING_LITERAL = 180, ARROW = 181, ASTERISK = 182, 
    BACKQUOTE = 183, BACKSLASH = 184, COLON = 185, COMMA = 186, CONCAT = 187, 
    DASH = 188, DOT = 189, EQ_DOUBLE = 190, EQ_SINGLE = 191, GE = 192, GT = 193, 
    LBRACE = 194, LBRACKET = 195, LE = 196, LPAREN = 197, LT = 198, NOT_EQ = 199, 
    PERCENT = 200, PLUS = 201, QUERY = 202, QUOTE_DOUBLE = 203, QUOTE_SINGLE = 204, 
    RBRACE = 205, RBRACKET = 206, RPAREN = 207, SEMICOLON = 208, SLASH = 209, 
    UNDERSCORE = 210, MULTI_LINE_COMMENT = 211, SINGLE_LINE_COMMENT = 212, 
    WHITESPACE = 213
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
