
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
    DETACH = 41, DICTIONARIES = 42, DICTIONARY = 43, DISK = 44, DISTINCT = 45, 
    DISTRIBUTED = 46, DROP = 47, ELSE = 48, END = 49, ENGINE = 50, EVENTS = 51, 
    EXISTS = 52, EXPLAIN = 53, EXPRESSION = 54, EXTRACT = 55, FETCHES = 56, 
    FINAL = 57, FIRST = 58, FLUSH = 59, FOR = 60, FORMAT = 61, FREEZE = 62, 
    FROM = 63, FULL = 64, FUNCTION = 65, GLOBAL = 66, GRANULARITY = 67, 
    GROUP = 68, HAVING = 69, HIERARCHICAL = 70, HOUR = 71, ID = 72, IF = 73, 
    ILIKE = 74, IN = 75, INDEX = 76, INF = 77, INJECTIVE = 78, INNER = 79, 
    INSERT = 80, INTERVAL = 81, INTO = 82, IS = 83, IS_OBJECT_ID = 84, JOIN = 85, 
    KEY = 86, LAST = 87, LAYOUT = 88, LEADING = 89, LEFT = 90, LIFETIME = 91, 
    LIKE = 92, LIMIT = 93, LIVE = 94, LOCAL = 95, LOGS = 96, MATERIALIZED = 97, 
    MAX = 98, MERGES = 99, MIN = 100, MINUTE = 101, MODIFY = 102, MONTH = 103, 
    MOVE = 104, NAN_SQL = 105, NO = 106, NOT = 107, NULL_SQL = 108, NULLS = 109, 
    OFFSET = 110, ON = 111, OPTIMIZE = 112, OR = 113, ORDER = 114, OUTER = 115, 
    OUTFILE = 116, PARTITION = 117, POPULATE = 118, PREWHERE = 119, PRIMARY = 120, 
    QUARTER = 121, RANGE = 122, RELOAD = 123, REMOVE = 124, RENAME = 125, 
    REPLACE = 126, REPLICA = 127, REPLICATED = 128, RIGHT = 129, ROLLUP = 130, 
    SAMPLE = 131, SECOND = 132, SELECT = 133, SEMI = 134, SENDS = 135, SET = 136, 
    SETTINGS = 137, SHOW = 138, SOURCE = 139, START = 140, STOP = 141, SUBSTRING = 142, 
    SYNC = 143, SYNTAX = 144, SYSTEM = 145, TABLE = 146, TABLES = 147, TEMPORARY = 148, 
    THEN = 149, TIES = 150, TIMEOUT = 151, TIMESTAMP = 152, TO = 153, TOP = 154, 
    TOTALS = 155, TRAILING = 156, TRIM = 157, TRUNCATE = 158, TTL = 159, 
    TYPE = 160, UNION = 161, UPDATE = 162, USE = 163, USING = 164, UUID = 165, 
    VALUES = 166, VIEW = 167, VOLUME = 168, WATCH = 169, WEEK = 170, WHEN = 171, 
    WHERE = 172, WITH = 173, YEAR = 174, JSON_FALSE = 175, JSON_TRUE = 176, 
    IDENTIFIER = 177, FLOATING_LITERAL = 178, OCTAL_LITERAL = 179, DECIMAL_LITERAL = 180, 
    HEXADECIMAL_LITERAL = 181, STRING_LITERAL = 182, ARROW = 183, ASTERISK = 184, 
    BACKQUOTE = 185, BACKSLASH = 186, COLON = 187, COMMA = 188, CONCAT = 189, 
    DASH = 190, DOT = 191, EQ_DOUBLE = 192, EQ_SINGLE = 193, GE = 194, GT = 195, 
    LBRACE = 196, LBRACKET = 197, LE = 198, LPAREN = 199, LT = 200, NOT_EQ = 201, 
    PERCENT = 202, PLUS = 203, QUERY = 204, QUOTE_DOUBLE = 205, QUOTE_SINGLE = 206, 
    RBRACE = 207, RBRACKET = 208, RPAREN = 209, SEMICOLON = 210, SLASH = 211, 
    UNDERSCORE = 212, MULTI_LINE_COMMENT = 213, SINGLE_LINE_COMMENT = 214, 
    WHITESPACE = 215
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
