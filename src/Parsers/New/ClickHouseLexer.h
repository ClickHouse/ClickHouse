
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
    CLUSTER = 20, COLLATE = 21, COLUMN = 22, COMMENT = 23, CREATE = 24, 
    CROSS = 25, DATABASE = 26, DAY = 27, DEDUPLICATE = 28, DEFAULT = 29, 
    DELETE = 30, DESC = 31, DESCENDING = 32, DESCRIBE = 33, DISK = 34, DISTINCT = 35, 
    DROP = 36, ELSE = 37, END = 38, ENGINE = 39, EXISTS = 40, EXTRACT = 41, 
    FINAL = 42, FIRST = 43, FORMAT = 44, FROM = 45, FULL = 46, GLOBAL = 47, 
    GROUP = 48, HAVING = 49, HOUR = 50, ID = 51, IF = 52, IN = 53, INF = 54, 
    INNER = 55, INSERT = 56, INTERVAL = 57, INTO = 58, IS = 59, JOIN = 60, 
    KEY = 61, LAST = 62, LEADING = 63, LEFT = 64, LIKE = 65, LIMIT = 66, 
    LOCAL = 67, MATERIALIZED = 68, MINUTE = 69, MODIFY = 70, MONTH = 71, 
    NAN_SQL = 72, NOT = 73, NULL_SQL = 74, NULLS = 75, OFFSET = 76, ON = 77, 
    OPTIMIZE = 78, OR = 79, ORDER = 80, OUTER = 81, OUTFILE = 82, PARTITION = 83, 
    PREWHERE = 84, PRIMARY = 85, QUARTER = 86, RIGHT = 87, SAMPLE = 88, 
    SECOND = 89, SELECT = 90, SEMI = 91, SET = 92, SETTINGS = 93, SHOW = 94, 
    TABLE = 95, TABLES = 96, TEMPORARY = 97, THEN = 98, TIES = 99, TO = 100, 
    TOTALS = 101, TRAILING = 102, TRIM = 103, TTL = 104, UNION = 105, USE = 106, 
    USING = 107, VALUES = 108, VIEW = 109, VOLUME = 110, WEEK = 111, WHEN = 112, 
    WHERE = 113, WITH = 114, YEAR = 115, IDENTIFIER = 116, FLOATING_LITERAL = 117, 
    HEXADECIMAL_LITERAL = 118, INTEGER_LITERAL = 119, STRING_LITERAL = 120, 
    ARROW = 121, ASTERISK = 122, BACKQUOTE = 123, BACKSLASH = 124, COLON = 125, 
    COMMA = 126, CONCAT = 127, DASH = 128, DOT = 129, EQ_DOUBLE = 130, EQ_SINGLE = 131, 
    GE = 132, GT = 133, LBRACKET = 134, LE = 135, LPAREN = 136, LT = 137, 
    NOT_EQ = 138, PERCENT = 139, PLUS = 140, QUERY = 141, QUOTE_SINGLE = 142, 
    RBRACKET = 143, RPAREN = 144, SEMICOLON = 145, SLASH = 146, UNDERSCORE = 147, 
    SINGLE_LINE_COMMENT = 148, MULTI_LINE_COMMENT = 149, WHITESPACE = 150
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
