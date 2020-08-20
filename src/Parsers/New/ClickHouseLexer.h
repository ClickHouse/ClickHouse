
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ADD = 2, AFTER = 3, ALIAS = 4, ALL = 5, ALTER = 6, 
    AND = 7, ANTI = 8, ANY = 9, ARRAY = 10, AS = 11, ASCENDING = 12, ASOF = 13, 
    BETWEEN = 14, BOTH = 15, BY = 16, CASE = 17, CAST = 18, CHECK = 19, 
    CLUSTER = 20, COLLATE = 21, COLUMN = 22, CREATE = 23, CROSS = 24, DATABASE = 25, 
    DAY = 26, DEDUPLICATE = 27, DEFAULT = 28, DELETE = 29, DESC = 30, DESCENDING = 31, 
    DESCRIBE = 32, DISK = 33, DISTINCT = 34, DROP = 35, ELSE = 36, END = 37, 
    ENGINE = 38, EXISTS = 39, EXTRACT = 40, FINAL = 41, FIRST = 42, FORMAT = 43, 
    FROM = 44, FULL = 45, GLOBAL = 46, GROUP = 47, HAVING = 48, HOUR = 49, 
    ID = 50, IF = 51, IN = 52, INF = 53, INNER = 54, INSERT = 55, INTERVAL = 56, 
    INTO = 57, IS = 58, JOIN = 59, KEY = 60, LAST = 61, LEADING = 62, LEFT = 63, 
    LIKE = 64, LIMIT = 65, LOCAL = 66, MATERIALIZED = 67, MINUTE = 68, MONTH = 69, 
    NAN_SQL = 70, NOT = 71, NULL_SQL = 72, NULLS = 73, OFFSET = 74, ON = 75, 
    OPTIMIZE = 76, OR = 77, ORDER = 78, OUTER = 79, OUTFILE = 80, PARTITION = 81, 
    PREWHERE = 82, PRIMARY = 83, QUARTER = 84, RIGHT = 85, SAMPLE = 86, 
    SECOND = 87, SELECT = 88, SEMI = 89, SET = 90, SETTINGS = 91, SHOW = 92, 
    TABLE = 93, TABLES = 94, TEMPORARY = 95, THEN = 96, TIES = 97, TO = 98, 
    TOTALS = 99, TRAILING = 100, TRIM = 101, TTL = 102, UNION = 103, USE = 104, 
    USING = 105, VALUES = 106, VOLUME = 107, WEEK = 108, WHEN = 109, WHERE = 110, 
    WITH = 111, YEAR = 112, IDENTIFIER = 113, FLOATING_LITERAL = 114, HEXADECIMAL_LITERAL = 115, 
    INTEGER_LITERAL = 116, STRING_LITERAL = 117, ARROW = 118, ASTERISK = 119, 
    BACKQUOTE = 120, BACKSLASH = 121, COLON = 122, COMMA = 123, CONCAT = 124, 
    DASH = 125, DOT = 126, EQ_DOUBLE = 127, EQ_SINGLE = 128, GE = 129, GT = 130, 
    LBRACKET = 131, LE = 132, LPAREN = 133, LT = 134, NOT_EQ = 135, PERCENT = 136, 
    PLUS = 137, QUERY = 138, QUOTE_SINGLE = 139, RBRACKET = 140, RPAREN = 141, 
    SEMICOLON = 142, SLASH = 143, UNDERSCORE = 144, SINGLE_LINE_COMMENT = 145, 
    MULTI_LINE_COMMENT = 146, WHITESPACE = 147
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
