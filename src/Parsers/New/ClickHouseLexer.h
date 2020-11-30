
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
    RANGE = 121, RELOAD = 122, REMOVE = 123, RENAME = 124, REPLACE = 125, 
    REPLICA = 126, REPLICATED = 127, RIGHT = 128, ROLLUP = 129, SAMPLE = 130, 
    SECOND = 131, SELECT = 132, SEMI = 133, SENDS = 134, SET = 135, SETTINGS = 136, 
    SHOW = 137, SOURCE = 138, START = 139, STOP = 140, SUBSTRING = 141, 
    SYNC = 142, SYNTAX = 143, SYSTEM = 144, TABLE = 145, TABLES = 146, TEMPORARY = 147, 
    THEN = 148, TIES = 149, TIMEOUT = 150, TIMESTAMP = 151, TO = 152, TOP = 153, 
    TOTALS = 154, TRAILING = 155, TRIM = 156, TRUNCATE = 157, TTL = 158, 
    TYPE = 159, UNION = 160, UPDATE = 161, USE = 162, USING = 163, UUID = 164, 
    VALUES = 165, VIEW = 166, VOLUME = 167, WATCH = 168, WEEK = 169, WHEN = 170, 
    WHERE = 171, WITH = 172, YEAR = 173, JSON_FALSE = 174, JSON_TRUE = 175, 
    IDENTIFIER = 176, FLOATING_LITERAL = 177, OCTAL_LITERAL = 178, DECIMAL_LITERAL = 179, 
    HEXADECIMAL_LITERAL = 180, STRING_LITERAL = 181, ARROW = 182, ASTERISK = 183, 
    BACKQUOTE = 184, BACKSLASH = 185, COLON = 186, COMMA = 187, CONCAT = 188, 
    DASH = 189, DOT = 190, EQ_DOUBLE = 191, EQ_SINGLE = 192, GE = 193, GT = 194, 
    LBRACE = 195, LBRACKET = 196, LE = 197, LPAREN = 198, LT = 199, NOT_EQ = 200, 
    PERCENT = 201, PLUS = 202, QUERY = 203, QUOTE_DOUBLE = 204, QUOTE_SINGLE = 205, 
    RBRACE = 206, RBRACKET = 207, RPAREN = 208, SEMICOLON = 209, SLASH = 210, 
    UNDERSCORE = 211, MULTI_LINE_COMMENT = 212, SINGLE_LINE_COMMENT = 213, 
    WHITESPACE = 214
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
