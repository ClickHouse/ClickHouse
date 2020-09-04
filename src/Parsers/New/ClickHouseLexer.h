
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ADD = 2, AFTER = 3, ALIAS = 4, ALL = 5, ALTER = 6, 
    ANALYZE = 7, AND = 8, ANTI = 9, ANY = 10, ARRAY = 11, AS = 12, ASCENDING = 13, 
    ASOF = 14, ATTACH = 15, BETWEEN = 16, BOTH = 17, BY = 18, CASE = 19, 
    CAST = 20, CHECK = 21, CLEAR = 22, CLUSTER = 23, COLLATE = 24, COLUMN = 25, 
    COMMENT = 26, CREATE = 27, CROSS = 28, DATABASE = 29, DAY = 30, DEDUPLICATE = 31, 
    DEFAULT = 32, DELAY = 33, DELETE = 34, DESC = 35, DESCENDING = 36, DESCRIBE = 37, 
    DETACH = 38, DISK = 39, DISTINCT = 40, DROP = 41, ELSE = 42, END = 43, 
    ENGINE = 44, EXISTS = 45, EXTRACT = 46, FETCHES = 47, FINAL = 48, FIRST = 49, 
    FOR = 50, FORMAT = 51, FROM = 52, FULL = 53, FUNCTION = 54, GLOBAL = 55, 
    GROUP = 56, HAVING = 57, HOUR = 58, ID = 59, IF = 60, IN = 61, INF = 62, 
    INNER = 63, INSERT = 64, INTERVAL = 65, INTO = 66, IS = 67, JOIN = 68, 
    KEY = 69, LAST = 70, LEADING = 71, LEFT = 72, LIKE = 73, LIMIT = 74, 
    LOCAL = 75, MATERIALIZED = 76, MERGES = 77, MINUTE = 78, MODIFY = 79, 
    MONTH = 80, NAN_SQL = 81, NO = 82, NOT = 83, NULL_SQL = 84, NULLS = 85, 
    OFFSET = 86, ON = 87, OPTIMIZE = 88, OR = 89, ORDER = 90, OUTER = 91, 
    OUTFILE = 92, PARTITION = 93, POPULATE = 94, PREWHERE = 95, PRIMARY = 96, 
    QUARTER = 97, RENAME = 98, REPLACE = 99, REPLICA = 100, RIGHT = 101, 
    SAMPLE = 102, SECOND = 103, SELECT = 104, SEMI = 105, SET = 106, SETTINGS = 107, 
    SHOW = 108, START = 109, STOP = 110, SUBSTRING = 111, SYNC = 112, SYSTEM = 113, 
    TABLE = 114, TABLES = 115, TEMPORARY = 116, THEN = 117, TIES = 118, 
    TO = 119, TOTALS = 120, TRAILING = 121, TRIM = 122, TRUNCATE = 123, 
    TTL = 124, UNION = 125, USE = 126, USING = 127, VALUES = 128, VIEW = 129, 
    VOLUME = 130, WEEK = 131, WHEN = 132, WHERE = 133, WITH = 134, YEAR = 135, 
    IDENTIFIER = 136, FLOATING_LITERAL = 137, HEXADECIMAL_LITERAL = 138, 
    INTEGER_LITERAL = 139, STRING_LITERAL = 140, ARROW = 141, ASTERISK = 142, 
    BACKQUOTE = 143, BACKSLASH = 144, COLON = 145, COMMA = 146, CONCAT = 147, 
    DASH = 148, DOT = 149, EQ_DOUBLE = 150, EQ_SINGLE = 151, GE = 152, GT = 153, 
    LBRACKET = 154, LE = 155, LPAREN = 156, LT = 157, NOT_EQ = 158, PERCENT = 159, 
    PLUS = 160, QUERY = 161, QUOTE_DOUBLE = 162, QUOTE_SINGLE = 163, RBRACKET = 164, 
    RPAREN = 165, SEMICOLON = 166, SLASH = 167, UNDERSCORE = 168, SINGLE_LINE_COMMENT = 169, 
    MULTI_LINE_COMMENT = 170, WHITESPACE = 171
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
