
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
    QUARTER = 127, QUERY = 128, QUEUES = 129, RANGE = 130, RELOAD = 131, 
    REMOVE = 132, RENAME = 133, REPLACE = 134, REPLICA = 135, REPLICATED = 136, 
    REPLICATION = 137, RIGHT = 138, ROLLUP = 139, SAMPLE = 140, SECOND = 141, 
    SELECT = 142, SEMI = 143, SENDS = 144, SET = 145, SETTINGS = 146, SHOW = 147, 
    SOURCE = 148, START = 149, STOP = 150, SUBSTRING = 151, SYNC = 152, 
    SYNTAX = 153, SYSTEM = 154, TABLE = 155, TABLES = 156, TEMPORARY = 157, 
    TEST = 158, THEN = 159, TIES = 160, TIMEOUT = 161, TIMESTAMP = 162, 
    TO = 163, TOP = 164, TOTALS = 165, TRAILING = 166, TRIM = 167, TRUNCATE = 168, 
    TTL = 169, TYPE = 170, UNION = 171, UPDATE = 172, USE = 173, USING = 174, 
    UUID = 175, VALUES = 176, VIEW = 177, VOLUME = 178, WATCH = 179, WEEK = 180, 
    WHEN = 181, WHERE = 182, WITH = 183, YEAR = 184, JSON_FALSE = 185, JSON_TRUE = 186, 
    IDENTIFIER = 187, FLOATING_LITERAL = 188, OCTAL_LITERAL = 189, DECIMAL_LITERAL = 190, 
    HEXADECIMAL_LITERAL = 191, STRING_LITERAL = 192, ARROW = 193, ASTERISK = 194, 
    BACKQUOTE = 195, BACKSLASH = 196, COLON = 197, COMMA = 198, CONCAT = 199, 
    DASH = 200, DOT = 201, EQ_DOUBLE = 202, EQ_SINGLE = 203, GE = 204, GT = 205, 
    LBRACE = 206, LBRACKET = 207, LE = 208, LPAREN = 209, LT = 210, NOT_EQ = 211, 
    PERCENT = 212, PLUS = 213, QUERY_SIGN = 214, QUOTE_DOUBLE = 215, QUOTE_SINGLE = 216, 
    RBRACE = 217, RBRACKET = 218, RPAREN = 219, SEMICOLON = 220, SLASH = 221, 
    UNDERSCORE = 222, MULTI_LINE_COMMENT = 223, SINGLE_LINE_COMMENT = 224, 
    WHITESPACE = 225
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
