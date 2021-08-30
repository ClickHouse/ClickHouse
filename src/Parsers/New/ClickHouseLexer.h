
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, AND = 6, ANTI = 7, 
    ANY = 8, ARRAY = 9, AS = 10, ASCENDING = 11, ASOF = 12, AST = 13, ASYNC = 14, 
    ATTACH = 15, BETWEEN = 16, BOTH = 17, BY = 18, CASE = 19, CAST = 20, 
    CHECK = 21, CLEAR = 22, CLUSTER = 23, CODEC = 24, COLLATE = 25, COLUMN = 26, 
    COMMENT = 27, CONSTRAINT = 28, CREATE = 29, CROSS = 30, CUBE = 31, DATABASE = 32, 
    DATABASES = 33, DATE = 34, DAY = 35, DEDUPLICATE = 36, DEFAULT = 37, 
    DELAY = 38, DELETE = 39, DESC = 40, DESCENDING = 41, DESCRIBE = 42, 
    DETACH = 43, DICTIONARIES = 44, DICTIONARY = 45, DISK = 46, DISTINCT = 47, 
    DISTRIBUTED = 48, DROP = 49, ELSE = 50, END = 51, ENGINE = 52, EVENTS = 53, 
    EXISTS = 54, EXPLAIN = 55, EXPRESSION = 56, EXTRACT = 57, FETCHES = 58, 
    FINAL = 59, FIRST = 60, FLUSH = 61, FOR = 62, FORMAT = 63, FREEZE = 64, 
    FROM = 65, FULL = 66, FUNCTION = 67, GLOBAL = 68, GRANULARITY = 69, 
    GROUP = 70, HAVING = 71, HIERARCHICAL = 72, HOUR = 73, ID = 74, IF = 75, 
    ILIKE = 76, IN = 77, INDEX = 78, INF = 79, INJECTIVE = 80, INNER = 81, 
    INSERT = 82, INTERVAL = 83, INTO = 84, IS = 85, IS_OBJECT_ID = 86, JOIN = 87, 
    KEY = 88, KILL = 89, LAST = 90, LAYOUT = 91, LEADING = 92, LEFT = 93, 
    LIFETIME = 94, LIKE = 95, LIMIT = 96, LIVE = 97, LOCAL = 98, LOGS = 99, 
    MATERIALIZE = 100, MATERIALIZED = 101, MAX = 102, MERGES = 103, MIN = 104, 
    MINUTE = 105, MODIFY = 106, MONTH = 107, MOVE = 108, MUTATION = 109, 
    NAN_SQL = 110, NO = 111, NOT = 112, NULL_SQL = 113, NULLS = 114, OFFSET = 115, 
    ON = 116, OPTIMIZE = 117, OR = 118, ORDER = 119, OUTER = 120, OUTFILE = 121, 
    PARTITION = 122, POPULATE = 123, PREWHERE = 124, PRIMARY = 125, PROJECTION = 126, 
    QUARTER = 127, RANGE = 128, RELOAD = 129, REMOVE = 130, RENAME = 131, 
    REPLACE = 132, REPLICA = 133, REPLICATED = 134, RIGHT = 135, ROLLUP = 136, 
    SAMPLE = 137, SECOND = 138, SELECT = 139, SEMI = 140, SENDS = 141, SET = 142, 
    SETTINGS = 143, SHOW = 144, SOURCE = 145, START = 146, STOP = 147, SUBSTRING = 148, 
    SYNC = 149, SYNTAX = 150, SYSTEM = 151, TABLE = 152, TABLES = 153, TEMPORARY = 154, 
    TEST = 155, THEN = 156, TIES = 157, TIMEOUT = 158, TIMESTAMP = 159, 
    TO = 160, TOP = 161, TOTALS = 162, TRAILING = 163, TRIM = 164, TRUNCATE = 165, 
    TTL = 166, TYPE = 167, UNION = 168, UPDATE = 169, USE = 170, USING = 171, 
    UUID = 172, VALUES = 173, VIEW = 174, VOLUME = 175, WATCH = 176, WEEK = 177, 
    WHEN = 178, WHERE = 179, WITH = 180, YEAR = 181, JSON_FALSE = 182, JSON_TRUE = 183, 
    IDENTIFIER = 184, FLOATING_LITERAL = 185, OCTAL_LITERAL = 186, DECIMAL_LITERAL = 187, 
    HEXADECIMAL_LITERAL = 188, STRING_LITERAL = 189, ARROW = 190, ASTERISK = 191, 
    BACKQUOTE = 192, BACKSLASH = 193, COLON = 194, COMMA = 195, CONCAT = 196, 
    DASH = 197, DOT = 198, EQ_DOUBLE = 199, EQ_SINGLE = 200, GE = 201, GT = 202, 
    LBRACE = 203, LBRACKET = 204, LE = 205, LPAREN = 206, LT = 207, NOT_EQ = 208, 
    PERCENT = 209, PLUS = 210, QUERY = 211, QUOTE_DOUBLE = 212, QUOTE_SINGLE = 213, 
    RBRACE = 214, RBRACKET = 215, RPAREN = 216, SEMICOLON = 217, SLASH = 218, 
    UNDERSCORE = 219, MULTI_LINE_COMMENT = 220, SINGLE_LINE_COMMENT = 221, 
    WHITESPACE = 222
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
