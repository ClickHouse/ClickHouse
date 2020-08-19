
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
    DATABASE = 20, DAY = 21, DEDUPLICATE = 22, DEFAULT = 23, DELETE = 24, 
    DESCENDING = 25, DISK = 26, DISTINCT = 27, DROP = 28, ELSE = 29, END = 30, 
    ENGINE = 31, EXISTS = 32, EXTRACT = 33, FINAL = 34, FIRST = 35, FORMAT = 36, 
    FROM = 37, FULL = 38, GLOBAL = 39, GROUP = 40, HAVING = 41, HOUR = 42, 
    ID = 43, IF = 44, IN = 45, INF = 46, INNER = 47, INSERT = 48, INTERVAL = 49, 
    INTO = 50, IS = 51, JOIN = 52, KEY = 53, LAST = 54, LEADING = 55, LEFT = 56, 
    LIKE = 57, LIMIT = 58, LOCAL = 59, MATERIALIZED = 60, MINUTE = 61, MONTH = 62, 
    NAN_SQL = 63, NOT = 64, NULL_SQL = 65, NULLS = 66, OFFSET = 67, ON = 68, 
    OPTIMIZE = 69, OR = 70, ORDER = 71, OUTER = 72, OUTFILE = 73, PARTITION = 74, 
    PREWHERE = 75, PRIMARY = 76, QUARTER = 77, RIGHT = 78, SAMPLE = 79, 
    SECOND = 80, SELECT = 81, SEMI = 82, SET = 83, SETTINGS = 84, TABLE = 85, 
    TEMPORARY = 86, THEN = 87, TO = 88, TOTALS = 89, TRAILING = 90, TRIM = 91, 
    TTL = 92, UNION = 93, USE = 94, USING = 95, VALUES = 96, VOLUME = 97, 
    WEEK = 98, WHEN = 99, WHERE = 100, WITH = 101, YEAR = 102, IDENTIFIER = 103, 
    FLOATING_LITERAL = 104, HEXADECIMAL_LITERAL = 105, INTEGER_LITERAL = 106, 
    STRING_LITERAL = 107, ARROW = 108, ASTERISK = 109, BACKQUOTE = 110, 
    BACKSLASH = 111, COLON = 112, COMMA = 113, CONCAT = 114, DASH = 115, 
    DOT = 116, EQ_DOUBLE = 117, EQ_SINGLE = 118, GE = 119, GT = 120, LBRACKET = 121, 
    LE = 122, LPAREN = 123, LT = 124, NOT_EQ = 125, PERCENT = 126, PLUS = 127, 
    QUERY = 128, QUOTE_SINGLE = 129, RBRACKET = 130, RPAREN = 131, SEMICOLON = 132, 
    SLASH = 133, UNDERSCORE = 134, SINGLE_LINE_COMMENT = 135, MULTI_LINE_COMMENT = 136, 
    WHITESPACE = 137
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
