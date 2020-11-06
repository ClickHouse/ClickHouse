
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
    DETACH = 41, DISK = 42, DISTINCT = 43, DISTRIBUTED = 44, DROP = 45, 
    ELSE = 46, END = 47, ENGINE = 48, EXISTS = 49, EXPLAIN = 50, EXTRACT = 51, 
    FETCHES = 52, FINAL = 53, FIRST = 54, FLUSH = 55, FOR = 56, FORMAT = 57, 
    FROM = 58, FULL = 59, FUNCTION = 60, GLOBAL = 61, GRANULARITY = 62, 
    GROUP = 63, HAVING = 64, HOUR = 65, ID = 66, IF = 67, ILIKE = 68, IN = 69, 
    INDEX = 70, INF = 71, INNER = 72, INSERT = 73, INTERVAL = 74, INTO = 75, 
    IS = 76, JOIN = 77, KEY = 78, LAST = 79, LEADING = 80, LEFT = 81, LIKE = 82, 
    LIMIT = 83, LOCAL = 84, LOGS = 85, MATERIALIZED = 86, MERGES = 87, MINUTE = 88, 
    MODIFY = 89, MONTH = 90, NAN_SQL = 91, NO = 92, NOT = 93, NULL_SQL = 94, 
    NULLS = 95, OFFSET = 96, ON = 97, OPTIMIZE = 98, OR = 99, ORDER = 100, 
    OUTER = 101, OUTFILE = 102, PARTITION = 103, POPULATE = 104, PREWHERE = 105, 
    PRIMARY = 106, QUARTER = 107, REMOVE = 108, RENAME = 109, REPLACE = 110, 
    REPLICA = 111, REPLICATED = 112, RIGHT = 113, ROLLUP = 114, SAMPLE = 115, 
    SECOND = 116, SELECT = 117, SEMI = 118, SENDS = 119, SET = 120, SETTINGS = 121, 
    SHOW = 122, START = 123, STOP = 124, SUBSTRING = 125, SYNC = 126, SYNTAX = 127, 
    SYSTEM = 128, TABLE = 129, TABLES = 130, TEMPORARY = 131, THEN = 132, 
    TIES = 133, TIMESTAMP = 134, TO = 135, TOTALS = 136, TRAILING = 137, 
    TRIM = 138, TRUNCATE = 139, TTL = 140, TYPE = 141, UNION = 142, USE = 143, 
    USING = 144, UUID = 145, VALUES = 146, VIEW = 147, VOLUME = 148, WEEK = 149, 
    WHEN = 150, WHERE = 151, WITH = 152, YEAR = 153, JSON_FALSE = 154, JSON_TRUE = 155, 
    IDENTIFIER = 156, FLOATING_LITERAL = 157, OCTAL_LITERAL = 158, DECIMAL_LITERAL = 159, 
    HEXADECIMAL_LITERAL = 160, STRING_LITERAL = 161, ARROW = 162, ASTERISK = 163, 
    BACKQUOTE = 164, BACKSLASH = 165, COLON = 166, COMMA = 167, CONCAT = 168, 
    DASH = 169, DOT = 170, EQ_DOUBLE = 171, EQ_SINGLE = 172, GE = 173, GT = 174, 
    LBRACE = 175, LBRACKET = 176, LE = 177, LPAREN = 178, LT = 179, NOT_EQ = 180, 
    PERCENT = 181, PLUS = 182, QUERY = 183, QUOTE_DOUBLE = 184, QUOTE_SINGLE = 185, 
    RBRACE = 186, RBRACKET = 187, RPAREN = 188, SEMICOLON = 189, SLASH = 190, 
    UNDERSCORE = 191, MULTI_LINE_COMMENT = 192, SINGLE_LINE_COMMENT = 193, 
    WHITESPACE = 194
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
