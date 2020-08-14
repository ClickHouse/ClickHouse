
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
    IN = 43, INNER = 44, INSERT = 45, INTERVAL = 46, INTO = 47, IS = 48, 
    JOIN = 49, KEY = 50, LAST = 51, LEADING = 52, LEFT = 53, LIKE = 54, 
    LIMIT = 55, LOCAL = 56, MATERIALIZED = 57, MINUTE = 58, MONTH = 59, 
    NOT = 60, NULL_SQL = 61, NULLS = 62, OFFSET = 63, ON = 64, OR = 65, 
    ORDER = 66, OUTER = 67, OUTFILE = 68, PARTITION = 69, PREWHERE = 70, 
    PRIMARY = 71, QUARTER = 72, RIGHT = 73, SAMPLE = 74, SECOND = 75, SELECT = 76, 
    SEMI = 77, SET = 78, SETTINGS = 79, TABLE = 80, TEMPORARY = 81, THEN = 82, 
    TO = 83, TOTALS = 84, TRAILING = 85, TRIM = 86, TTL = 87, UNION = 88, 
    USING = 89, VALUES = 90, VOLUME = 91, WEEK = 92, WHEN = 93, WHERE = 94, 
    WITH = 95, YEAR = 96, IDENTIFIER = 97, NUMBER_LITERAL = 98, STRING_LITERAL = 99, 
    ARROW = 100, ASTERISK = 101, BACKQUOTE = 102, BACKSLASH = 103, COLON = 104, 
    COMMA = 105, CONCAT = 106, DASH = 107, DOT = 108, EQ_DOUBLE = 109, EQ_SINGLE = 110, 
    GE = 111, GT = 112, LBRACKET = 113, LE = 114, LPAREN = 115, LT = 116, 
    NOT_EQ = 117, PERCENT = 118, PLUS = 119, QUERY = 120, QUOTE_SINGLE = 121, 
    RBRACKET = 122, RPAREN = 123, SEMICOLON = 124, SLASH = 125, UNDERSCORE = 126, 
    LINE_COMMENT = 127, WHITESPACE = 128
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
