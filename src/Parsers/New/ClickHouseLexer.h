
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
    EXPLAIN = 52, EXTRACT = 53, FETCHES = 54, FINAL = 55, FIRST = 56, FLUSH = 57, 
    FOR = 58, FORMAT = 59, FREEZE = 60, FROM = 61, FULL = 62, FUNCTION = 63, 
    GLOBAL = 64, GRANULARITY = 65, GROUP = 66, HAVING = 67, HOUR = 68, ID = 69, 
    IF = 70, ILIKE = 71, IN = 72, INDEX = 73, INF = 74, INNER = 75, INSERT = 76, 
    INTERVAL = 77, INTO = 78, IS = 79, JOIN = 80, KEY = 81, LAST = 82, LEADING = 83, 
    LEFT = 84, LIKE = 85, LIMIT = 86, LIVE = 87, LOCAL = 88, LOGS = 89, 
    MATERIALIZED = 90, MERGES = 91, MINUTE = 92, MODIFY = 93, MONTH = 94, 
    MOVE = 95, NAN_SQL = 96, NO = 97, NOT = 98, NULL_SQL = 99, NULLS = 100, 
    OFFSET = 101, ON = 102, OPTIMIZE = 103, OR = 104, ORDER = 105, OUTER = 106, 
    OUTFILE = 107, PARTITION = 108, POPULATE = 109, PREWHERE = 110, PRIMARY = 111, 
    QUARTER = 112, REMOVE = 113, RENAME = 114, REPLACE = 115, REPLICA = 116, 
    REPLICATED = 117, RIGHT = 118, ROLLUP = 119, SAMPLE = 120, SECOND = 121, 
    SELECT = 122, SEMI = 123, SENDS = 124, SET = 125, SETTINGS = 126, SHOW = 127, 
    START = 128, STOP = 129, SUBSTRING = 130, SYNC = 131, SYNTAX = 132, 
    SYSTEM = 133, TABLE = 134, TABLES = 135, TEMPORARY = 136, THEN = 137, 
    TIES = 138, TIMEOUT = 139, TIMESTAMP = 140, TO = 141, TOP = 142, TOTALS = 143, 
    TRAILING = 144, TRIM = 145, TRUNCATE = 146, TTL = 147, TYPE = 148, UNION = 149, 
    UPDATE = 150, USE = 151, USING = 152, UUID = 153, VALUES = 154, VIEW = 155, 
    VOLUME = 156, WATCH = 157, WEEK = 158, WHEN = 159, WHERE = 160, WITH = 161, 
    YEAR = 162, JSON_FALSE = 163, JSON_TRUE = 164, IDENTIFIER = 165, FLOATING_LITERAL = 166, 
    OCTAL_LITERAL = 167, DECIMAL_LITERAL = 168, HEXADECIMAL_LITERAL = 169, 
    STRING_LITERAL = 170, ARROW = 171, ASTERISK = 172, BACKQUOTE = 173, 
    BACKSLASH = 174, COLON = 175, COMMA = 176, CONCAT = 177, DASH = 178, 
    DOT = 179, EQ_DOUBLE = 180, EQ_SINGLE = 181, GE = 182, GT = 183, LBRACE = 184, 
    LBRACKET = 185, LE = 186, LPAREN = 187, LT = 188, NOT_EQ = 189, PERCENT = 190, 
    PLUS = 191, QUERY = 192, QUOTE_DOUBLE = 193, QUOTE_SINGLE = 194, RBRACE = 195, 
    RBRACKET = 196, RPAREN = 197, SEMICOLON = 198, SLASH = 199, UNDERSCORE = 200, 
    MULTI_LINE_COMMENT = 201, SINGLE_LINE_COMMENT = 202, WHITESPACE = 203
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
