
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
    CONSTRAINT = 27, CREATE = 28, CROSS = 29, DATABASE = 30, DATABASES = 31, 
    DATE = 32, DAY = 33, DEDUPLICATE = 34, DEFAULT = 35, DELAY = 36, DELETE = 37, 
    DESC = 38, DESCENDING = 39, DESCRIBE = 40, DETACH = 41, DISK = 42, DISTINCT = 43, 
    DISTRIBUTED = 44, DROP = 45, ELSE = 46, END = 47, ENGINE = 48, EXISTS = 49, 
    EXTRACT = 50, FETCHES = 51, FINAL = 52, FIRST = 53, FLUSH = 54, FOR = 55, 
    FORMAT = 56, FROM = 57, FULL = 58, FUNCTION = 59, GLOBAL = 60, GRANULARITY = 61, 
    GROUP = 62, HAVING = 63, HOUR = 64, ID = 65, IF = 66, IN = 67, INDEX = 68, 
    INF = 69, INNER = 70, INSERT = 71, INTERVAL = 72, INTO = 73, IS = 74, 
    JOIN = 75, KEY = 76, LAST = 77, LEADING = 78, LEFT = 79, LIKE = 80, 
    LIMIT = 81, LOCAL = 82, LOGS = 83, MATERIALIZED = 84, MERGES = 85, MINUTE = 86, 
    MODIFY = 87, MONTH = 88, NAN_SQL = 89, NO = 90, NOT = 91, NULL_SQL = 92, 
    NULLS = 93, OFFSET = 94, ON = 95, OPTIMIZE = 96, OR = 97, ORDER = 98, 
    OUTER = 99, OUTFILE = 100, PARTITION = 101, POPULATE = 102, PREWHERE = 103, 
    PRIMARY = 104, QUARTER = 105, REMOVE = 106, RENAME = 107, REPLACE = 108, 
    REPLICA = 109, RIGHT = 110, SAMPLE = 111, SECOND = 112, SELECT = 113, 
    SEMI = 114, SENDS = 115, SET = 116, SETTINGS = 117, SHOW = 118, START = 119, 
    STOP = 120, SUBSTRING = 121, SYNC = 122, SYSTEM = 123, TABLE = 124, 
    TABLES = 125, TEMPORARY = 126, THEN = 127, TIES = 128, TIMESTAMP = 129, 
    TO = 130, TOTALS = 131, TRAILING = 132, TRIM = 133, TRUNCATE = 134, 
    TTL = 135, TYPE = 136, UNION = 137, USE = 138, USING = 139, VALUES = 140, 
    VIEW = 141, VOLUME = 142, WEEK = 143, WHEN = 144, WHERE = 145, WITH = 146, 
    YEAR = 147, JSON_FALSE = 148, JSON_TRUE = 149, IDENTIFIER = 150, FLOATING_LITERAL = 151, 
    OCTAL_LITERAL = 152, DECIMAL_LITERAL = 153, HEXADECIMAL_LITERAL = 154, 
    STRING_LITERAL = 155, ARROW = 156, ASTERISK = 157, BACKQUOTE = 158, 
    BACKSLASH = 159, COLON = 160, COMMA = 161, CONCAT = 162, DASH = 163, 
    DOT = 164, EQ_DOUBLE = 165, EQ_SINGLE = 166, GE = 167, GT = 168, LBRACE = 169, 
    LBRACKET = 170, LE = 171, LPAREN = 172, LT = 173, NOT_EQ = 174, PERCENT = 175, 
    PLUS = 176, QUERY = 177, QUOTE_DOUBLE = 178, QUOTE_SINGLE = 179, RBRACE = 180, 
    RBRACKET = 181, RPAREN = 182, SEMICOLON = 183, SLASH = 184, UNDERSCORE = 185, 
    MULTI_LINE_COMMENT = 186, SINGLE_LINE_COMMENT = 187, WHITESPACE = 188
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
