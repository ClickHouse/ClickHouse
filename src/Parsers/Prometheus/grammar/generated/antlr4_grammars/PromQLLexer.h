
// Generated from PromQLLexer.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace antlr4_grammars {


class  PromQLLexer : public antlr4::Lexer {
public:
  enum {
    NUMBER = 1, STRING = 2, ADD = 3, SUB = 4, MULT = 5, DIV = 6, MOD = 7, 
    POW = 8, AND = 9, OR = 10, UNLESS = 11, ATAN2 = 12, EQ = 13, DEQ = 14, 
    NE = 15, GT = 16, LT = 17, GE = 18, LE = 19, RE = 20, NRE = 21, BY = 22, 
    WITHOUT = 23, ON = 24, IGNORING = 25, GROUP_LEFT = 26, GROUP_RIGHT = 27, 
    GROUP_LEFT_PAREN = 28, GROUP_RIGHT_PAREN = 29, OFFSET = 30, BOOL = 31, 
    START = 32, END = 33, AGGREGATION_OPERATOR = 34, FUNCTION = 35, LEFT_BRACE = 36, 
    RIGHT_BRACE = 37, LEFT_PAREN = 38, RIGHT_PAREN = 39, LEFT_BRACKET = 40, 
    RIGHT_BRACKET = 41, COMMA = 42, AT = 43, SUBQUERY_RANGE = 44, SELECTOR_RANGE = 45, 
    METRIC_NAME = 46, LABEL_NAME = 47, WS = 48, SL_COMMENT = 49
  };

  enum {
    WHITESPACE = 2, COMMENTS = 3
  };

  explicit PromQLLexer(antlr4::CharStream *input);

  ~PromQLLexer() override;


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
