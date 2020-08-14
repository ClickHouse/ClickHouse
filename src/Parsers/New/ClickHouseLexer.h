
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ALIAS = 2, ALL = 3, AND = 4, ANTI = 5, ANY = 6, ARRAY = 7, 
    AS = 8, ASCENDING = 9, ASOF = 10, BETWEEN = 11, BOTH = 12, BY = 13, 
    CASE = 14, CAST = 15, CLUSTER = 16, COLLATE = 17, CREATE = 18, CROSS = 19, 
    DATABASE = 20, DAY = 21, DEFAULT = 22, DELETE = 23, DESCENDING = 24, 
    DISK = 25, DISTINCT = 26, DROP = 27, ELSE = 28, END = 29, ENGINE = 30, 
    EXISTS = 31, EXTRACT = 32, FINAL = 33, FIRST = 34, FORMAT = 35, FROM = 36, 
    FULL = 37, GLOBAL = 38, GROUP = 39, HAVING = 40, HOUR = 41, IF = 42, 
    IN = 43, INF = 44, INNER = 45, INSERT = 46, INTERVAL = 47, INTO = 48, 
    IS = 49, JOIN = 50, KEY = 51, LAST = 52, LEADING = 53, LEFT = 54, LIKE = 55, 
    LIMIT = 56, LOCAL = 57, MATERIALIZED = 58, MINUTE = 59, MONTH = 60, 
    NAN_SQL = 61, NOT = 62, NULL_SQL = 63, NULLS = 64, OFFSET = 65, ON = 66, 
    OR = 67, ORDER = 68, OUTER = 69, OUTFILE = 70, PARTITION = 71, PREWHERE = 72, 
    PRIMARY = 73, QUARTER = 74, RIGHT = 75, SAMPLE = 76, SECOND = 77, SELECT = 78, 
    SEMI = 79, SET = 80, SETTINGS = 81, TABLE = 82, TEMPORARY = 83, THEN = 84, 
    TO = 85, TOTALS = 86, TRAILING = 87, TRIM = 88, TTL = 89, UNION = 90, 
    USING = 91, VALUES = 92, VOLUME = 93, WEEK = 94, WHEN = 95, WHERE = 96, 
    WITH = 97, YEAR = 98, IDENTIFIER = 99, FLOATING_LITERAL = 100, HEXADECIMAL_LITERAL = 101, 
    INTEGER_LITERAL = 102, STRING_LITERAL = 103, ARROW = 104, ASTERISK = 105, 
    BACKQUOTE = 106, BACKSLASH = 107, COLON = 108, COMMA = 109, CONCAT = 110, 
    DASH = 111, DOT = 112, EQ_DOUBLE = 113, EQ_SINGLE = 114, GE = 115, GT = 116, 
    LBRACKET = 117, LE = 118, LPAREN = 119, LT = 120, NOT_EQ = 121, PERCENT = 122, 
    PLUS = 123, QUERY = 124, QUOTE_SINGLE = 125, RBRACKET = 126, RPAREN = 127, 
    SEMICOLON = 128, SLASH = 129, UNDERSCORE = 130, SINGLE_LINE_COMMENT = 131, 
    MULTI_LINE_COMMENT = 132, WHITESPACE = 133
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
