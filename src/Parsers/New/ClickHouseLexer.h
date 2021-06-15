
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, AND = 6, ANTI = 7, 
    ANY = 8, ARRAY = 9, AS = 10, ASCENDING = 11, ASOF = 12, ASYNC = 13, 
    ATTACH = 14, BETWEEN = 15, BOTH = 16, BY = 17, CASE = 18, CAST = 19, 
    CHECK = 20, CLEAR = 21, CLUSTER = 22, CODEC = 23, COLLATE = 24, COLUMN = 25, 
    COMMENT = 26, CONSTRAINT = 27, CREATE = 28, CROSS = 29, CUBE = 30, DATABASE = 31, 
    DATABASES = 32, DATE = 33, DAY = 34, DEDUPLICATE = 35, DEFAULT = 36, 
    DELAY = 37, DELETE = 38, DESC = 39, DESCENDING = 40, DESCRIBE = 41, 
    DETACH = 42, DICTIONARIES = 43, DICTIONARY = 44, DISK = 45, DISTINCT = 46, 
    DISTRIBUTED = 47, DROP = 48, ELSE = 49, END = 50, ENGINE = 51, EVENTS = 52, 
    EXISTS = 53, EXPLAIN = 54, EXPRESSION = 55, EXTRACT = 56, FETCHES = 57, 
    FINAL = 58, FIRST = 59, FLUSH = 60, FOR = 61, FORMAT = 62, FREEZE = 63, 
    FROM = 64, FULL = 65, FUNCTION = 66, GLOBAL = 67, GRANULARITY = 68, 
    GROUP = 69, HAVING = 70, HIERARCHICAL = 71, HOUR = 72, ID = 73, IF = 74, 
    ILIKE = 75, IN = 76, INDEX = 77, INF = 78, INJECTIVE = 79, INNER = 80, 
    INSERT = 81, INTERVAL = 82, INTO = 83, IS = 84, IS_OBJECT_ID = 85, JOIN = 86, 
    KEY = 87, KILL = 88, LAST = 89, LAYOUT = 90, LEADING = 91, LEFT = 92, 
    LIFETIME = 93, LIKE = 94, LIMIT = 95, LIVE = 96, LOCAL = 97, LOGS = 98, 
    MATERIALIZED = 99, MATERIALIZE = 100, MAX = 101, MERGES = 102, MIN = 103, 
    MINUTE = 104, MODIFY = 105, MONTH = 106, MOVE = 107, MUTATION = 108, 
    NAN_SQL = 109, NO = 110, NOT = 111, NULL_SQL = 112, NULLS = 113, OFFSET = 114, 
    ON = 115, OPTIMIZE = 116, OR = 117, ORDER = 118, OUTER = 119, OUTFILE = 120, 
    PARTITION = 121, POPULATE = 122, PREWHERE = 123, PRIMARY = 124, PROJECTION = 125, 
    QUARTER = 126, RANGE = 127, RELOAD = 128, REMOVE = 129, RENAME = 130, 
    REPLACE = 131, REPLICA = 132, REPLICATED = 133, RIGHT = 134, ROLLUP = 135, 
    SAMPLE = 136, SECOND = 137, SELECT = 138, SEMI = 139, SENDS = 140, SET = 141, 
    SETTINGS = 142, SHOW = 143, SOURCE = 144, START = 145, STOP = 146, SUBSTRING = 147, 
    SYNC = 148, SYNTAX = 149, SYSTEM = 150, TABLE = 151, TABLES = 152, TEMPORARY = 153, 
    TEST = 154, THEN = 155, TIES = 156, TIMEOUT = 157, TIMESTAMP = 158, 
    TO = 159, TOP = 160, TOTALS = 161, TRAILING = 162, TRIM = 163, TRUNCATE = 164, 
    TTL = 165, TYPE = 166, UNION = 167, UPDATE = 168, USE = 169, USING = 170, 
    UUID = 171, VALUES = 172, VIEW = 173, VOLUME = 174, WATCH = 175, WEEK = 176, 
    WHEN = 177, WHERE = 178, WITH = 179, YEAR = 180, JSON_FALSE = 181, JSON_TRUE = 182, 
    IDENTIFIER = 183, FLOATING_LITERAL = 184, OCTAL_LITERAL = 185, DECIMAL_LITERAL = 186, 
    HEXADECIMAL_LITERAL = 187, STRING_LITERAL = 188, ARROW = 189, ASTERISK = 190, 
    BACKQUOTE = 191, BACKSLASH = 192, COLON = 193, COMMA = 194, CONCAT = 195, 
    DASH = 196, DOT = 197, EQ_DOUBLE = 198, EQ_SINGLE = 199, GE = 200, GT = 201, 
    LBRACE = 202, LBRACKET = 203, LE = 204, LPAREN = 205, LT = 206, NOT_EQ = 207, 
    PERCENT = 208, PLUS = 209, QUERY = 210, QUOTE_DOUBLE = 211, QUOTE_SINGLE = 212, 
    RBRACE = 213, RBRACKET = 214, RPAREN = 215, SEMICOLON = 216, SLASH = 217, 
    UNDERSCORE = 218, MULTI_LINE_COMMENT = 219, SINGLE_LINE_COMMENT = 220, 
    WHITESPACE = 221
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
