
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ALL = 2, AND = 3, ANTI = 4, ANY = 5, ARRAY = 6, AS = 7, 
    ASCENDING = 8, ASOF = 9, BETWEEN = 10, BOTH = 11, BY = 12, CASE = 13, 
    CAST = 14, CLUSTER = 15, COLLATE = 16, CROSS = 17, DATABASE = 18, DAY = 19, 
    DESCENDING = 20, DISTINCT = 21, DROP = 22, ELSE = 23, END = 24, EXISTS = 25, 
    EXTRACT = 26, FINAL = 27, FIRST = 28, FORMAT = 29, FROM = 30, FULL = 31, 
    GLOBAL = 32, GROUP = 33, HAVING = 34, HOUR = 35, IF = 36, IN = 37, INNER = 38, 
    INSERT = 39, INTERVAL = 40, INTO = 41, IS = 42, JOIN = 43, LAST = 44, 
    LEADING = 45, LEFT = 46, LIKE = 47, LIMIT = 48, LOCAL = 49, MINUTE = 50, 
    MONTH = 51, NOT = 52, NULL_SQL = 53, NULLS = 54, OFFSET = 55, ON = 56, 
    OR = 57, ORDER = 58, OUTER = 59, OUTFILE = 60, PREWHERE = 61, QUARTER = 62, 
    RIGHT = 63, SAMPLE = 64, SECOND = 65, SELECT = 66, SEMI = 67, SET = 68, 
    SETTINGS = 69, TABLE = 70, TEMPORARY = 71, THEN = 72, TOTALS = 73, TRAILING = 74, 
    TRIM = 75, UNION = 76, USING = 77, WEEK = 78, WHEN = 79, WHERE = 80, 
    WITH = 81, YEAR = 82, IDENTIFIER = 83, NUMBER_LITERAL = 84, STRING_LITERAL = 85, 
    ARROW = 86, ASTERISK = 87, BACKQUOTE = 88, BACKSLASH = 89, COLON = 90, 
    COMMA = 91, CONCAT = 92, DASH = 93, DOT = 94, EQ_DOUBLE = 95, EQ_SINGLE = 96, 
    GE = 97, GT = 98, LBRACKET = 99, LE = 100, LPAREN = 101, LT = 102, NOT_EQ = 103, 
    PERCENT = 104, PLUS = 105, QUERY = 106, QUOTE_SINGLE = 107, RBRACKET = 108, 
    RPAREN = 109, SEMICOLON = 110, SLASH = 111, UNDERSCORE = 112, LINE_COMMENT = 113, 
    WHITESPACE = 114
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
