
// Generated from PromQLLexer.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace antlr4_grammars {


class  PromQLLexer : public antlr4::Lexer {
public:
  enum {
    NUMBER = 1, STRING = 2, ADD = 3, SUB = 4, MULT = 5, DIV = 6, MOD = 7, 
    POW = 8, AND = 9, OR = 10, UNLESS = 11, EQ = 12, DEQ = 13, NE = 14, 
    GT = 15, LT = 16, GE = 17, LE = 18, RE = 19, NRE = 20, BY = 21, WITHOUT = 22, 
    ON = 23, IGNORING = 24, GROUP_LEFT = 25, GROUP_RIGHT = 26, OFFSET = 27, 
    BOOL = 28, AGGREGATION_OPERATOR = 29, FUNCTION = 30, LEFT_BRACE = 31, 
    RIGHT_BRACE = 32, LEFT_PAREN = 33, RIGHT_PAREN = 34, LEFT_BRACKET = 35, 
    RIGHT_BRACKET = 36, COMMA = 37, AT = 38, SUBQUERY_RANGE = 39, TIME_RANGE = 40, 
    METRIC_NAME = 41, LABEL_NAME = 42, WS = 43, SL_COMMENT = 44
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
