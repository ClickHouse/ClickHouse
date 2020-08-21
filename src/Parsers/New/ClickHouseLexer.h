
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
    LIKE = 64, LIMIT = 65, LOCAL = 66, MATERIALIZED = 67, MINUTE = 68, MODIFY = 69, 
    MONTH = 70, NAN_SQL = 71, NOT = 72, NULL_SQL = 73, NULLS = 74, OFFSET = 75, 
    ON = 76, OPTIMIZE = 77, OR = 78, ORDER = 79, OUTER = 80, OUTFILE = 81, 
    PARTITION = 82, PREWHERE = 83, PRIMARY = 84, QUARTER = 85, RIGHT = 86, 
    SAMPLE = 87, SECOND = 88, SELECT = 89, SEMI = 90, SET = 91, SETTINGS = 92, 
    SHOW = 93, TABLE = 94, TABLES = 95, TEMPORARY = 96, THEN = 97, TIES = 98, 
    TO = 99, TOTALS = 100, TRAILING = 101, TRIM = 102, TTL = 103, UNION = 104, 
    USE = 105, USING = 106, VALUES = 107, VOLUME = 108, WEEK = 109, WHEN = 110, 
    WHERE = 111, WITH = 112, YEAR = 113, IDENTIFIER = 114, FLOATING_LITERAL = 115, 
    HEXADECIMAL_LITERAL = 116, INTEGER_LITERAL = 117, STRING_LITERAL = 118, 
    ARROW = 119, ASTERISK = 120, BACKQUOTE = 121, BACKSLASH = 122, COLON = 123, 
    COMMA = 124, CONCAT = 125, DASH = 126, DOT = 127, EQ_DOUBLE = 128, EQ_SINGLE = 129, 
    GE = 130, GT = 131, LBRACKET = 132, LE = 133, LPAREN = 134, LT = 135, 
    NOT_EQ = 136, PERCENT = 137, PLUS = 138, QUERY = 139, QUOTE_SINGLE = 140, 
    RBRACKET = 141, RPAREN = 142, SEMICOLON = 143, SLASH = 144, UNDERSCORE = 145, 
    SINGLE_LINE_COMMENT = 146, MULTI_LINE_COMMENT = 147, WHITESPACE = 148
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
