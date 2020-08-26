
// Generated from ClickHouseLexer.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    INTERVAL_TYPE = 1, ADD = 2, AFTER = 3, ALIAS = 4, ALL = 5, ALTER = 6, 
    AND = 7, ANTI = 8, ANY = 9, ARRAY = 10, AS = 11, ASCENDING = 12, ASOF = 13, 
    ATTACH = 14, BETWEEN = 15, BOTH = 16, BY = 17, CASE = 18, CAST = 19, 
    CHECK = 20, CLUSTER = 21, COLLATE = 22, COLUMN = 23, COMMENT = 24, CREATE = 25, 
    CROSS = 26, DATABASE = 27, DAY = 28, DEDUPLICATE = 29, DEFAULT = 30, 
    DELAY = 31, DELETE = 32, DESC = 33, DESCENDING = 34, DESCRIBE = 35, 
    DETACH = 36, DISK = 37, DISTINCT = 38, DROP = 39, ELSE = 40, END = 41, 
    ENGINE = 42, EXISTS = 43, EXTRACT = 44, FINAL = 45, FIRST = 46, FORMAT = 47, 
    FROM = 48, FULL = 49, GLOBAL = 50, GROUP = 51, HAVING = 52, HOUR = 53, 
    ID = 54, IF = 55, IN = 56, INF = 57, INNER = 58, INSERT = 59, INTERVAL = 60, 
    INTO = 61, IS = 62, JOIN = 63, KEY = 64, LAST = 65, LEADING = 66, LEFT = 67, 
    LIKE = 68, LIMIT = 69, LOCAL = 70, MATERIALIZED = 71, MINUTE = 72, MODIFY = 73, 
    MONTH = 74, NAN_SQL = 75, NO = 76, NOT = 77, NULL_SQL = 78, NULLS = 79, 
    OFFSET = 80, ON = 81, OPTIMIZE = 82, OR = 83, ORDER = 84, OUTER = 85, 
    OUTFILE = 86, PARTITION = 87, POPULATE = 88, PREWHERE = 89, PRIMARY = 90, 
    QUARTER = 91, RENAME = 92, RIGHT = 93, SAMPLE = 94, SECOND = 95, SELECT = 96, 
    SEMI = 97, SET = 98, SETTINGS = 99, SHOW = 100, TABLE = 101, TABLES = 102, 
    TEMPORARY = 103, THEN = 104, TIES = 105, TO = 106, TOTALS = 107, TRAILING = 108, 
    TRIM = 109, TTL = 110, UNION = 111, USE = 112, USING = 113, VALUES = 114, 
    VIEW = 115, VOLUME = 116, WEEK = 117, WHEN = 118, WHERE = 119, WITH = 120, 
    YEAR = 121, IDENTIFIER = 122, FLOATING_LITERAL = 123, HEXADECIMAL_LITERAL = 124, 
    INTEGER_LITERAL = 125, STRING_LITERAL = 126, ARROW = 127, ASTERISK = 128, 
    BACKQUOTE = 129, BACKSLASH = 130, COLON = 131, COMMA = 132, CONCAT = 133, 
    DASH = 134, DOT = 135, EQ_DOUBLE = 136, EQ_SINGLE = 137, GE = 138, GT = 139, 
    LBRACKET = 140, LE = 141, LPAREN = 142, LT = 143, NOT_EQ = 144, PERCENT = 145, 
    PLUS = 146, QUERY = 147, QUOTE_SINGLE = 148, RBRACKET = 149, RPAREN = 150, 
    SEMICOLON = 151, SLASH = 152, UNDERSCORE = 153, SINGLE_LINE_COMMENT = 154, 
    MULTI_LINE_COMMENT = 155, WHITESPACE = 156
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
