
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ALL = 2, AND = 3, ANTI = 4, ANY = 5, ARRAY = 6, AS = 7, 
    ASCENDING = 8, ASOF = 9, BETWEEN = 10, BOTH = 11, BY = 12, CASE = 13, 
    CAST = 14, COLLATE = 15, CROSS = 16, DAY = 17, DESCENDING = 18, DISTINCT = 19, 
    ELSE = 20, END = 21, EXTRACT = 22, FINAL = 23, FIRST = 24, FORMAT = 25, 
    FROM = 26, FULL = 27, GLOBAL = 28, GROUP = 29, HAVING = 30, HOUR = 31, 
    IN = 32, INNER = 33, INSERT = 34, INTERVAL = 35, INTO = 36, IS = 37, 
    JOIN = 38, LAST = 39, LEADING = 40, LEFT = 41, LIKE = 42, LIMIT = 43, 
    LOCAL = 44, MINUTE = 45, MONTH = 46, NOT = 47, NULL_SQL = 48, NULLS = 49, 
    OFFSET = 50, ON = 51, OR = 52, ORDER = 53, OUTER = 54, OUTFILE = 55, 
    PREWHERE = 56, QUARTER = 57, RIGHT = 58, SAMPLE = 59, SECOND = 60, SELECT = 61, 
    SEMI = 62, SETTINGS = 63, THEN = 64, TOTALS = 65, TRAILING = 66, TRIM = 67, 
    UNION = 68, USING = 69, WEEK = 70, WHEN = 71, WHERE = 72, WITH = 73, 
    YEAR = 74, IDENTIFIER = 75, NUMBER_LITERAL = 76, STRING_LITERAL = 77, 
    ARROW = 78, ASTERISK = 79, BACKQUOTE = 80, BACKSLASH = 81, COLON = 82, 
    COMMA = 83, CONCAT = 84, DASH = 85, DOT = 86, EQ_DOUBLE = 87, EQ_SINGLE = 88, 
    GE = 89, GT = 90, LBRACKET = 91, LE = 92, LPAREN = 93, LT = 94, NOT_EQ = 95, 
    PERCENT = 96, PLUS = 97, QUERY = 98, QUOTE_SINGLE = 99, RBRACKET = 100, 
    RPAREN = 101, SEMICOLON = 102, SLASH = 103, UNDERSCORE = 104, LINE_COMMENT = 105, 
    WHITESPACE = 106
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
