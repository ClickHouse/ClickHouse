
// Generated from PromQLLexer.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace antlr4_grammars {


class  PromQLLexer : public antlr4::Lexer {
public:
  enum {
    NUMBER = 1, STRING = 2, ADD = 3, SUB = 4, MULT = 5, DIV = 6, MOD = 7, 
    POW = 8, ATAN2 = 9, AND = 10, OR = 11, UNLESS = 12, EQ = 13, DEQ = 14, 
    NE = 15, GT = 16, LT = 17, GE = 18, LE = 19, RE = 20, NRE = 21, BY = 22, 
    WITHOUT = 23, ON = 24, IGNORING = 25, GROUP_LEFT = 26, GROUP_RIGHT = 27, 
    OFFSET = 28, BOOL = 29, AGGREGATION_OPERATOR = 30, FUNCTION = 31, LEFT_BRACE = 32, 
    RIGHT_BRACE = 33, LEFT_PAREN = 34, RIGHT_PAREN = 35, LEFT_BRACKET = 36, 
    RIGHT_BRACKET = 37, COMMA = 38, AT = 39, SUBQUERY_RANGE = 40, TIME_RANGE = 41, 
    METRIC_NAME = 42, LABEL_NAME = 43, WS = 44, SL_COMMENT = 45
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
