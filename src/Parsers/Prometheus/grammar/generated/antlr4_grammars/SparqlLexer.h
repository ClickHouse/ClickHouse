
// Generated from SparqlLexer.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace antlr4_grammars {


class  SparqlLexer : public antlr4::Lexer {
public:
  enum {
    A = 1, ASC = 2, ASK = 3, BASE = 4, BOUND = 5, BY = 6, CONSTRUCT = 7, 
    DATATYPE = 8, DESC = 9, DESCRIBE = 10, DISTINCT = 11, FILTER = 12, FROM = 13, 
    GRAPH = 14, LANG = 15, LANGMATCHES = 16, LIMIT = 17, NAMED = 18, OFFSET = 19, 
    OPTIONAL = 20, ORDER = 21, PREFIX = 22, REDUCED = 23, REGEX = 24, SELECT = 25, 
    STR = 26, UNION = 27, WHERE = 28, TRUE = 29, FALSE = 30, IS_LITERAL = 31, 
    IS_BLANK = 32, IS_URI = 33, IS_IRI = 34, SAME_TERM = 35, COMMA = 36, 
    DOT = 37, DOUBLE_AMP = 38, DOUBLE_BAR = 39, DOUBLE_CARET = 40, EQUAL = 41, 
    EXCLAMATION = 42, GREATER = 43, GREATER_OR_EQUAL = 44, LESS = 45, LESS_OR_EQUAL = 46, 
    L_CURLY = 47, L_PAREN = 48, L_SQUARE = 49, MINUS = 50, NOT_EQUAL = 51, 
    PLUS = 52, R_CURLY = 53, R_PAREN = 54, R_SQUARE = 55, SEMICOLON = 56, 
    SLASH = 57, STAR = 58, IRI_REF = 59, PNAME_NS = 60, PNAME_LN = 61, BLANK_NODE_LABEL = 62, 
    VAR1 = 63, VAR2 = 64, LANGTAG = 65, INTEGER = 66, DECIMAL = 67, DOUBLE = 68, 
    INTEGER_POSITIVE = 69, DECIMAL_POSITIVE = 70, DOUBLE_POSITIVE = 71, 
    INTEGER_NEGATIVE = 72, DECIMAL_NEGATIVE = 73, DOUBLE_NEGATIVE = 74, 
    EXPONENT = 75, STRING_LITERAL1 = 76, STRING_LITERAL2 = 77, STRING_LITERAL_LONG1 = 78, 
    STRING_LITERAL_LONG2 = 79, ECHAR = 80, NIL = 81, ANON = 82, PN_CHARS_U = 83, 
    VARNAME = 84, PN_PREFIX = 85, PN_LOCAL = 86, WS = 87
  };

  explicit SparqlLexer(antlr4::CharStream *input);

  ~SparqlLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

}  // namespace antlr4_grammars
