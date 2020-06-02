
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    LINE_COMMENT = 1, WHITESPACE = 2, ARROW = 3, ASTERISK = 4, BACKQUOTE = 5,
    BACKSLASH = 6, COLON = 7, COMMA = 8, CONCAT = 9, DASH = 10, DOT = 11,
    EQ = 12, EQ_DOUBLE = 13, EQ_SINGLE = 14, GE = 15, GT = 16, LBRACKET = 17,
    LE = 18, LPAREN = 19, LT = 20, NOT_EQ = 21, PERCENT = 22, PLUS = 23,
    QUERY = 24, QUOTE_SINGLE = 25, RBRACKET = 26, RPAREN = 27, SEMICOLON = 28,
    SLASH = 29, UNDERSCORE = 30, IDENTIFIER = 31, LITERAL = 32, NUMBER_LITERAL = 33,
    STRING_LITERAL = 34, ALL = 35, AND = 36, ARRAY = 37, AS = 38, ASCENDING = 39,
    BETWEEN = 40, BOTH = 41, BY = 42, CASE = 43, CAST = 44, COLLATE = 45,
    DAY = 46, DESCENDING = 47, DISTINCT = 48, ELSE = 49, END = 50, EXTRACT = 51,
    FINAL = 52, FIRST = 53, FORMAT = 54, FROM = 55, GLOBAL = 56, GROUP = 57,
    HAVING = 58, HOUR = 59, IN = 60, INTERVAL = 61, INTO = 62, IS = 63,
    JOIN = 64, LAST = 65, LEADING = 66, LEFT = 67, LIKE = 68, LIMIT = 69,
    MINUTE = 70, MONTH = 71, NOT = 72, NULL_SQL = 73, NULLS = 74, OFFSET = 75,
    OR = 76, ORDER = 77, OUTFILE = 78, PREWHERE = 79, QUARTER = 80, SAMPLE = 81,
    SECOND = 82, SELECT = 83, SETTINGS = 84, THEN = 85, TOTALS = 86, TRAILING = 87,
    TRIM = 88, UNION = 89, WEEK = 90, WHEN = 91, WHERE = 92, WITH = 93,
    YEAR = 94, INTERVAL_TYPE = 95
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
