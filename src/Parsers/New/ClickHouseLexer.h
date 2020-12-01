
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
    MATERIALIZED = 99, MAX = 100, MERGES = 101, MIN = 102, MINUTE = 103, 
    MODIFY = 104, MONTH = 105, MOVE = 106, MUTATION = 107, NAN_SQL = 108, 
    NO = 109, NOT = 110, NULL_SQL = 111, NULLS = 112, OFFSET = 113, ON = 114, 
    OPTIMIZE = 115, OR = 116, ORDER = 117, OUTER = 118, OUTFILE = 119, PARTITION = 120, 
    POPULATE = 121, PREWHERE = 122, PRIMARY = 123, QUARTER = 124, RANGE = 125, 
    RELOAD = 126, REMOVE = 127, RENAME = 128, REPLACE = 129, REPLICA = 130, 
    REPLICATED = 131, RIGHT = 132, ROLLUP = 133, SAMPLE = 134, SECOND = 135, 
    SELECT = 136, SEMI = 137, SENDS = 138, SET = 139, SETTINGS = 140, SHOW = 141, 
    SOURCE = 142, START = 143, STOP = 144, SUBSTRING = 145, SYNC = 146, 
    SYNTAX = 147, SYSTEM = 148, TABLE = 149, TABLES = 150, TEMPORARY = 151, 
    TEST = 152, THEN = 153, TIES = 154, TIMEOUT = 155, TIMESTAMP = 156, 
    TO = 157, TOP = 158, TOTALS = 159, TRAILING = 160, TRIM = 161, TRUNCATE = 162, 
    TTL = 163, TYPE = 164, UNION = 165, UPDATE = 166, USE = 167, USING = 168, 
    UUID = 169, VALUES = 170, VIEW = 171, VOLUME = 172, WATCH = 173, WEEK = 174, 
    WHEN = 175, WHERE = 176, WITH = 177, YEAR = 178, JSON_FALSE = 179, JSON_TRUE = 180, 
    IDENTIFIER = 181, FLOATING_LITERAL = 182, OCTAL_LITERAL = 183, DECIMAL_LITERAL = 184, 
    HEXADECIMAL_LITERAL = 185, STRING_LITERAL = 186, ARROW = 187, ASTERISK = 188, 
    BACKQUOTE = 189, BACKSLASH = 190, COLON = 191, COMMA = 192, CONCAT = 193, 
    DASH = 194, DOT = 195, EQ_DOUBLE = 196, EQ_SINGLE = 197, GE = 198, GT = 199, 
    LBRACE = 200, LBRACKET = 201, LE = 202, LPAREN = 203, LT = 204, NOT_EQ = 205, 
    PERCENT = 206, PLUS = 207, QUERY = 208, QUOTE_DOUBLE = 209, QUOTE_SINGLE = 210, 
    RBRACE = 211, RBRACKET = 212, RPAREN = 213, SEMICOLON = 214, SLASH = 215, 
    UNDERSCORE = 216, MULTI_LINE_COMMENT = 217, SINGLE_LINE_COMMENT = 218, 
    WHITESPACE = 219
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
