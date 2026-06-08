
// Generated from PromQLParser.g4 by ANTLR 4.13.2


#include "PromQLParserListener.h"
#include "PromQLParserVisitor.h"

#include "PromQLParser.h"


using namespace antlrcpp;
using namespace antlr4_grammars;

using namespace antlr4;

namespace {

struct PromQLParserStaticData final {
  PromQLParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  PromQLParserStaticData(const PromQLParserStaticData&) = delete;
  PromQLParserStaticData(PromQLParserStaticData&&) = delete;
  PromQLParserStaticData& operator=(const PromQLParserStaticData&) = delete;
  PromQLParserStaticData& operator=(PromQLParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag promqlparserParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
std::unique_ptr<PromQLParserStaticData> promqlparserParserStaticData = nullptr;

void promqlparserParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (promqlparserParserStaticData != nullptr) {
    return;
  }
#else
  assert(promqlparserParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<PromQLParserStaticData>(
    std::vector<std::string>{
      "expression", "vectorOperation", "unaryOp", "powOp", "multOp", "addOp", 
      "compareOp", "andUnlessOp", "orOp", "subqueryOp", "offsetOp", "vector", 
      "parens", "instantSelector", "labelMatcher", "labelMatcherOperator", 
      "labelMatcherList", "matrixSelector", "offset", "function_", "parameter", 
      "parameterList", "aggregation", "by", "without", "grouping", "on_", 
      "ignoring", "groupLeft", "groupRight", "labelName", "labelNameList", 
      "metricName", "keyword", "literal"
    },
    std::vector<std::string>{
      "", "", "", "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'and'", "'or'", 
      "'unless'", "'='", "'=='", "'!='", "'>'", "'<'", "'>='", "'<='", "'=~'", 
      "'!~'", "'by'", "'without'", "'on'", "'ignoring'", "'group_left'", 
      "'group_right'", "'offset'", "'bool'", "", "", "'{'", "'}'", "'('", 
      "')'", "'['", "']'", "','", "'@'"
    },
    std::vector<std::string>{
      "", "NUMBER", "STRING", "ADD", "SUB", "MULT", "DIV", "MOD", "POW", 
      "AND", "OR", "UNLESS", "EQ", "DEQ", "NE", "GT", "LT", "GE", "LE", 
      "RE", "NRE", "BY", "WITHOUT", "ON", "IGNORING", "GROUP_LEFT", "GROUP_RIGHT", 
      "OFFSET", "BOOL", "AGGREGATION_OPERATOR", "FUNCTION", "LEFT_BRACE", 
      "RIGHT_BRACE", "LEFT_PAREN", "RIGHT_PAREN", "LEFT_BRACKET", "RIGHT_BRACKET", 
      "COMMA", "AT", "SUBQUERY_RANGE", "TIME_RANGE", "METRIC_NAME", "LABEL_NAME", 
      "WS", "SL_COMMENT"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,44,322,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,
  	21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,
  	28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,1,0,1,
  	0,1,0,1,1,1,1,1,1,1,1,1,1,3,1,79,8,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
  	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
  	1,5,1,107,8,1,10,1,12,1,110,9,1,1,2,1,2,1,3,1,3,3,3,116,8,3,1,4,1,4,3,
  	4,120,8,4,1,5,1,5,3,5,124,8,5,1,6,1,6,3,6,128,8,6,1,6,3,6,131,8,6,1,7,
  	1,7,3,7,135,8,7,1,8,1,8,3,8,139,8,8,1,9,1,9,3,9,143,8,9,1,10,1,10,1,10,
  	1,10,3,10,149,8,10,1,10,3,10,152,8,10,1,10,1,10,3,10,156,8,10,1,10,1,
  	10,1,10,3,10,161,8,10,3,10,163,8,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,
  	3,11,172,8,11,1,12,1,12,1,12,1,12,1,13,1,13,1,13,3,13,181,8,13,1,13,3,
  	13,184,8,13,1,13,1,13,1,13,1,13,3,13,190,8,13,1,14,1,14,1,14,1,14,1,15,
  	1,15,1,16,1,16,1,16,5,16,201,8,16,10,16,12,16,204,9,16,1,16,3,16,207,
  	8,16,1,17,1,17,1,17,1,18,1,18,1,18,1,18,1,18,1,18,3,18,218,8,18,1,19,
  	1,19,1,19,1,19,1,19,5,19,225,8,19,10,19,12,19,228,9,19,3,19,230,8,19,
  	1,19,1,19,1,20,1,20,3,20,236,8,20,1,21,1,21,1,21,1,21,5,21,242,8,21,10,
  	21,12,21,245,9,21,3,21,247,8,21,1,21,1,21,1,22,1,22,1,22,1,22,1,22,3,
  	22,256,8,22,1,22,1,22,1,22,1,22,1,22,1,22,3,22,264,8,22,3,22,266,8,22,
  	1,23,1,23,1,23,1,24,1,24,1,24,1,25,1,25,3,25,276,8,25,1,25,1,25,3,25,
  	280,8,25,1,26,1,26,1,26,1,27,1,27,1,27,1,28,1,28,3,28,290,8,28,1,29,1,
  	29,3,29,294,8,29,1,30,1,30,1,30,3,30,299,8,30,1,31,1,31,1,31,1,31,5,31,
  	305,8,31,10,31,12,31,308,9,31,3,31,310,8,31,1,31,1,31,1,32,1,32,3,32,
  	316,8,32,1,33,1,33,1,34,1,34,1,34,0,1,2,35,0,2,4,6,8,10,12,14,16,18,20,
  	22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,
  	68,0,7,1,0,3,4,1,0,5,7,1,0,13,18,2,0,9,9,11,11,3,0,12,12,14,14,19,20,
  	2,0,9,11,21,30,1,0,1,2,338,0,70,1,0,0,0,2,78,1,0,0,0,4,111,1,0,0,0,6,
  	113,1,0,0,0,8,117,1,0,0,0,10,121,1,0,0,0,12,125,1,0,0,0,14,132,1,0,0,
  	0,16,136,1,0,0,0,18,140,1,0,0,0,20,162,1,0,0,0,22,171,1,0,0,0,24,173,
  	1,0,0,0,26,189,1,0,0,0,28,191,1,0,0,0,30,195,1,0,0,0,32,197,1,0,0,0,34,
  	208,1,0,0,0,36,217,1,0,0,0,38,219,1,0,0,0,40,235,1,0,0,0,42,237,1,0,0,
  	0,44,265,1,0,0,0,46,267,1,0,0,0,48,270,1,0,0,0,50,275,1,0,0,0,52,281,
  	1,0,0,0,54,284,1,0,0,0,56,287,1,0,0,0,58,291,1,0,0,0,60,298,1,0,0,0,62,
  	300,1,0,0,0,64,315,1,0,0,0,66,317,1,0,0,0,68,319,1,0,0,0,70,71,3,2,1,
  	0,71,72,5,0,0,1,72,1,1,0,0,0,73,74,6,1,-1,0,74,75,3,4,2,0,75,76,3,2,1,
  	7,76,79,1,0,0,0,77,79,3,22,11,0,78,73,1,0,0,0,78,77,1,0,0,0,79,108,1,
  	0,0,0,80,81,10,9,0,0,81,82,3,6,3,0,82,83,3,2,1,9,83,107,1,0,0,0,84,85,
  	10,6,0,0,85,86,3,8,4,0,86,87,3,2,1,7,87,107,1,0,0,0,88,89,10,5,0,0,89,
  	90,3,10,5,0,90,91,3,2,1,6,91,107,1,0,0,0,92,93,10,4,0,0,93,94,3,12,6,
  	0,94,95,3,2,1,5,95,107,1,0,0,0,96,97,10,3,0,0,97,98,3,14,7,0,98,99,3,
  	2,1,4,99,107,1,0,0,0,100,101,10,2,0,0,101,102,3,16,8,0,102,103,3,2,1,
  	3,103,107,1,0,0,0,104,105,10,8,0,0,105,107,3,18,9,0,106,80,1,0,0,0,106,
  	84,1,0,0,0,106,88,1,0,0,0,106,92,1,0,0,0,106,96,1,0,0,0,106,100,1,0,0,
  	0,106,104,1,0,0,0,107,110,1,0,0,0,108,106,1,0,0,0,108,109,1,0,0,0,109,
  	3,1,0,0,0,110,108,1,0,0,0,111,112,7,0,0,0,112,5,1,0,0,0,113,115,5,8,0,
  	0,114,116,3,50,25,0,115,114,1,0,0,0,115,116,1,0,0,0,116,7,1,0,0,0,117,
  	119,7,1,0,0,118,120,3,50,25,0,119,118,1,0,0,0,119,120,1,0,0,0,120,9,1,
  	0,0,0,121,123,7,0,0,0,122,124,3,50,25,0,123,122,1,0,0,0,123,124,1,0,0,
  	0,124,11,1,0,0,0,125,127,7,2,0,0,126,128,5,28,0,0,127,126,1,0,0,0,127,
  	128,1,0,0,0,128,130,1,0,0,0,129,131,3,50,25,0,130,129,1,0,0,0,130,131,
  	1,0,0,0,131,13,1,0,0,0,132,134,7,3,0,0,133,135,3,50,25,0,134,133,1,0,
  	0,0,134,135,1,0,0,0,135,15,1,0,0,0,136,138,5,10,0,0,137,139,3,50,25,0,
  	138,137,1,0,0,0,138,139,1,0,0,0,139,17,1,0,0,0,140,142,5,39,0,0,141,143,
  	3,20,10,0,142,141,1,0,0,0,142,143,1,0,0,0,143,19,1,0,0,0,144,145,5,38,
  	0,0,145,151,5,1,0,0,146,148,5,27,0,0,147,149,7,0,0,0,148,147,1,0,0,0,
  	148,149,1,0,0,0,149,150,1,0,0,0,150,152,5,1,0,0,151,146,1,0,0,0,151,152,
  	1,0,0,0,152,163,1,0,0,0,153,155,5,27,0,0,154,156,7,0,0,0,155,154,1,0,
  	0,0,155,156,1,0,0,0,156,157,1,0,0,0,157,160,5,1,0,0,158,159,5,38,0,0,
  	159,161,5,1,0,0,160,158,1,0,0,0,160,161,1,0,0,0,161,163,1,0,0,0,162,144,
  	1,0,0,0,162,153,1,0,0,0,163,21,1,0,0,0,164,172,3,38,19,0,165,172,3,44,
  	22,0,166,172,3,26,13,0,167,172,3,34,17,0,168,172,3,36,18,0,169,172,3,
  	68,34,0,170,172,3,24,12,0,171,164,1,0,0,0,171,165,1,0,0,0,171,166,1,0,
  	0,0,171,167,1,0,0,0,171,168,1,0,0,0,171,169,1,0,0,0,171,170,1,0,0,0,172,
  	23,1,0,0,0,173,174,5,33,0,0,174,175,3,2,1,0,175,176,5,34,0,0,176,25,1,
  	0,0,0,177,183,3,64,32,0,178,180,5,31,0,0,179,181,3,32,16,0,180,179,1,
  	0,0,0,180,181,1,0,0,0,181,182,1,0,0,0,182,184,5,32,0,0,183,178,1,0,0,
  	0,183,184,1,0,0,0,184,190,1,0,0,0,185,186,5,31,0,0,186,187,3,32,16,0,
  	187,188,5,32,0,0,188,190,1,0,0,0,189,177,1,0,0,0,189,185,1,0,0,0,190,
  	27,1,0,0,0,191,192,3,60,30,0,192,193,3,30,15,0,193,194,5,2,0,0,194,29,
  	1,0,0,0,195,196,7,4,0,0,196,31,1,0,0,0,197,202,3,28,14,0,198,199,5,37,
  	0,0,199,201,3,28,14,0,200,198,1,0,0,0,201,204,1,0,0,0,202,200,1,0,0,0,
  	202,203,1,0,0,0,203,206,1,0,0,0,204,202,1,0,0,0,205,207,5,37,0,0,206,
  	205,1,0,0,0,206,207,1,0,0,0,207,33,1,0,0,0,208,209,3,26,13,0,209,210,
  	5,40,0,0,210,35,1,0,0,0,211,212,3,26,13,0,212,213,3,20,10,0,213,218,1,
  	0,0,0,214,215,3,34,17,0,215,216,3,20,10,0,216,218,1,0,0,0,217,211,1,0,
  	0,0,217,214,1,0,0,0,218,37,1,0,0,0,219,220,5,30,0,0,220,229,5,33,0,0,
  	221,226,3,40,20,0,222,223,5,37,0,0,223,225,3,40,20,0,224,222,1,0,0,0,
  	225,228,1,0,0,0,226,224,1,0,0,0,226,227,1,0,0,0,227,230,1,0,0,0,228,226,
  	1,0,0,0,229,221,1,0,0,0,229,230,1,0,0,0,230,231,1,0,0,0,231,232,5,34,
  	0,0,232,39,1,0,0,0,233,236,3,68,34,0,234,236,3,2,1,0,235,233,1,0,0,0,
  	235,234,1,0,0,0,236,41,1,0,0,0,237,246,5,33,0,0,238,243,3,40,20,0,239,
  	240,5,37,0,0,240,242,3,40,20,0,241,239,1,0,0,0,242,245,1,0,0,0,243,241,
  	1,0,0,0,243,244,1,0,0,0,244,247,1,0,0,0,245,243,1,0,0,0,246,238,1,0,0,
  	0,246,247,1,0,0,0,247,248,1,0,0,0,248,249,5,34,0,0,249,43,1,0,0,0,250,
  	251,5,29,0,0,251,266,3,42,21,0,252,255,5,29,0,0,253,256,3,46,23,0,254,
  	256,3,48,24,0,255,253,1,0,0,0,255,254,1,0,0,0,256,257,1,0,0,0,257,258,
  	3,42,21,0,258,266,1,0,0,0,259,260,5,29,0,0,260,263,3,42,21,0,261,264,
  	3,46,23,0,262,264,3,48,24,0,263,261,1,0,0,0,263,262,1,0,0,0,264,266,1,
  	0,0,0,265,250,1,0,0,0,265,252,1,0,0,0,265,259,1,0,0,0,266,45,1,0,0,0,
  	267,268,5,21,0,0,268,269,3,62,31,0,269,47,1,0,0,0,270,271,5,22,0,0,271,
  	272,3,62,31,0,272,49,1,0,0,0,273,276,3,52,26,0,274,276,3,54,27,0,275,
  	273,1,0,0,0,275,274,1,0,0,0,276,279,1,0,0,0,277,280,3,56,28,0,278,280,
  	3,58,29,0,279,277,1,0,0,0,279,278,1,0,0,0,279,280,1,0,0,0,280,51,1,0,
  	0,0,281,282,5,23,0,0,282,283,3,62,31,0,283,53,1,0,0,0,284,285,5,24,0,
  	0,285,286,3,62,31,0,286,55,1,0,0,0,287,289,5,25,0,0,288,290,3,62,31,0,
  	289,288,1,0,0,0,289,290,1,0,0,0,290,57,1,0,0,0,291,293,5,26,0,0,292,294,
  	3,62,31,0,293,292,1,0,0,0,293,294,1,0,0,0,294,59,1,0,0,0,295,299,3,66,
  	33,0,296,299,5,41,0,0,297,299,5,42,0,0,298,295,1,0,0,0,298,296,1,0,0,
  	0,298,297,1,0,0,0,299,61,1,0,0,0,300,309,5,33,0,0,301,306,3,60,30,0,302,
  	303,5,37,0,0,303,305,3,60,30,0,304,302,1,0,0,0,305,308,1,0,0,0,306,304,
  	1,0,0,0,306,307,1,0,0,0,307,310,1,0,0,0,308,306,1,0,0,0,309,301,1,0,0,
  	0,309,310,1,0,0,0,310,311,1,0,0,0,311,312,5,34,0,0,312,63,1,0,0,0,313,
  	316,5,41,0,0,314,316,3,66,33,0,315,313,1,0,0,0,315,314,1,0,0,0,316,65,
  	1,0,0,0,317,318,7,5,0,0,318,67,1,0,0,0,319,320,7,6,0,0,320,69,1,0,0,0,
  	39,78,106,108,115,119,123,127,130,134,138,142,148,151,155,160,162,171,
  	180,183,189,202,206,217,226,229,235,243,246,255,263,265,275,279,289,293,
  	298,306,309,315
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  promqlparserParserStaticData = std::move(staticData);
}

}

PromQLParser::PromQLParser(TokenStream *input) : PromQLParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

PromQLParser::PromQLParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  PromQLParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *promqlparserParserStaticData->atn, promqlparserParserStaticData->decisionToDFA, promqlparserParserStaticData->sharedContextCache, options);
}

PromQLParser::~PromQLParser() {
  delete _interpreter;
}

const atn::ATN& PromQLParser::getATN() const {
  return *promqlparserParserStaticData->atn;
}

std::string PromQLParser::getGrammarFileName() const {
  return "PromQLParser.g4";
}

const std::vector<std::string>& PromQLParser::getRuleNames() const {
  return promqlparserParserStaticData->ruleNames;
}

const dfa::Vocabulary& PromQLParser::getVocabulary() const {
  return promqlparserParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView PromQLParser::getSerializedATN() const {
  return promqlparserParserStaticData->serializedATN;
}


//----------------- ExpressionContext ------------------------------------------------------------------

PromQLParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::VectorOperationContext* PromQLParser::ExpressionContext::vectorOperation() {
  return getRuleContext<PromQLParser::VectorOperationContext>(0);
}

tree::TerminalNode* PromQLParser::ExpressionContext::EOF() {
  return getToken(PromQLParser::EOF, 0);
}


size_t PromQLParser::ExpressionContext::getRuleIndex() const {
  return PromQLParser::RuleExpression;
}

void PromQLParser::ExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpression(this);
}

void PromQLParser::ExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpression(this);
}


std::any PromQLParser::ExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::ExpressionContext* PromQLParser::expression() {
  ExpressionContext *_localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 0, PromQLParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(70);
    vectorOperation(0);
    setState(71);
    match(PromQLParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VectorOperationContext ------------------------------------------------------------------

PromQLParser::VectorOperationContext::VectorOperationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::UnaryOpContext* PromQLParser::VectorOperationContext::unaryOp() {
  return getRuleContext<PromQLParser::UnaryOpContext>(0);
}

std::vector<PromQLParser::VectorOperationContext *> PromQLParser::VectorOperationContext::vectorOperation() {
  return getRuleContexts<PromQLParser::VectorOperationContext>();
}

PromQLParser::VectorOperationContext* PromQLParser::VectorOperationContext::vectorOperation(size_t i) {
  return getRuleContext<PromQLParser::VectorOperationContext>(i);
}

PromQLParser::VectorContext* PromQLParser::VectorOperationContext::vector() {
  return getRuleContext<PromQLParser::VectorContext>(0);
}

PromQLParser::PowOpContext* PromQLParser::VectorOperationContext::powOp() {
  return getRuleContext<PromQLParser::PowOpContext>(0);
}

PromQLParser::MultOpContext* PromQLParser::VectorOperationContext::multOp() {
  return getRuleContext<PromQLParser::MultOpContext>(0);
}

PromQLParser::AddOpContext* PromQLParser::VectorOperationContext::addOp() {
  return getRuleContext<PromQLParser::AddOpContext>(0);
}

PromQLParser::CompareOpContext* PromQLParser::VectorOperationContext::compareOp() {
  return getRuleContext<PromQLParser::CompareOpContext>(0);
}

PromQLParser::AndUnlessOpContext* PromQLParser::VectorOperationContext::andUnlessOp() {
  return getRuleContext<PromQLParser::AndUnlessOpContext>(0);
}

PromQLParser::OrOpContext* PromQLParser::VectorOperationContext::orOp() {
  return getRuleContext<PromQLParser::OrOpContext>(0);
}

PromQLParser::SubqueryOpContext* PromQLParser::VectorOperationContext::subqueryOp() {
  return getRuleContext<PromQLParser::SubqueryOpContext>(0);
}


size_t PromQLParser::VectorOperationContext::getRuleIndex() const {
  return PromQLParser::RuleVectorOperation;
}

void PromQLParser::VectorOperationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVectorOperation(this);
}

void PromQLParser::VectorOperationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVectorOperation(this);
}


std::any PromQLParser::VectorOperationContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitVectorOperation(this);
  else
    return visitor->visitChildren(this);
}


PromQLParser::VectorOperationContext* PromQLParser::vectorOperation() {
   return vectorOperation(0);
}

PromQLParser::VectorOperationContext* PromQLParser::vectorOperation(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  PromQLParser::VectorOperationContext *_localctx = _tracker.createInstance<VectorOperationContext>(_ctx, parentState);
  PromQLParser::VectorOperationContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 2;
  enterRecursionRule(_localctx, 2, PromQLParser::RuleVectorOperation, precedence);

    

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(78);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ADD:
      case PromQLParser::SUB: {
        setState(74);
        unaryOp();
        setState(75);
        vectorOperation(7);
        break;
      }

      case PromQLParser::NUMBER:
      case PromQLParser::STRING:
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION:
      case PromQLParser::LEFT_BRACE:
      case PromQLParser::LEFT_PAREN:
      case PromQLParser::METRIC_NAME: {
        setState(77);
        vector();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(108);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(106);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx)) {
        case 1: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(80);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(81);
          powOp();
          setState(82);
          vectorOperation(9);
          break;
        }

        case 2: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(84);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(85);
          multOp();
          setState(86);
          vectorOperation(7);
          break;
        }

        case 3: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(88);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(89);
          addOp();
          setState(90);
          vectorOperation(6);
          break;
        }

        case 4: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(92);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(93);
          compareOp();
          setState(94);
          vectorOperation(5);
          break;
        }

        case 5: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(96);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(97);
          andUnlessOp();
          setState(98);
          vectorOperation(4);
          break;
        }

        case 6: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(100);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(101);
          orOp();
          setState(102);
          vectorOperation(3);
          break;
        }

        case 7: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(104);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(105);
          subqueryOp();
          break;
        }

        default:
          break;
        } 
      }
      setState(110);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- UnaryOpContext ------------------------------------------------------------------

PromQLParser::UnaryOpContext::UnaryOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::UnaryOpContext::ADD() {
  return getToken(PromQLParser::ADD, 0);
}

tree::TerminalNode* PromQLParser::UnaryOpContext::SUB() {
  return getToken(PromQLParser::SUB, 0);
}


size_t PromQLParser::UnaryOpContext::getRuleIndex() const {
  return PromQLParser::RuleUnaryOp;
}

void PromQLParser::UnaryOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnaryOp(this);
}

void PromQLParser::UnaryOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnaryOp(this);
}


std::any PromQLParser::UnaryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitUnaryOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::UnaryOpContext* PromQLParser::unaryOp() {
  UnaryOpContext *_localctx = _tracker.createInstance<UnaryOpContext>(_ctx, getState());
  enterRule(_localctx, 4, PromQLParser::RuleUnaryOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(111);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::ADD

    || _la == PromQLParser::SUB)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PowOpContext ------------------------------------------------------------------

PromQLParser::PowOpContext::PowOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::PowOpContext::POW() {
  return getToken(PromQLParser::POW, 0);
}

PromQLParser::GroupingContext* PromQLParser::PowOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::PowOpContext::getRuleIndex() const {
  return PromQLParser::RulePowOp;
}

void PromQLParser::PowOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPowOp(this);
}

void PromQLParser::PowOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPowOp(this);
}


std::any PromQLParser::PowOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitPowOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::PowOpContext* PromQLParser::powOp() {
  PowOpContext *_localctx = _tracker.createInstance<PowOpContext>(_ctx, getState());
  enterRule(_localctx, 6, PromQLParser::RulePowOp);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(113);
    match(PromQLParser::POW);
    setState(115);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
    case 1: {
      setState(114);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultOpContext ------------------------------------------------------------------

PromQLParser::MultOpContext::MultOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::MultOpContext::MULT() {
  return getToken(PromQLParser::MULT, 0);
}

tree::TerminalNode* PromQLParser::MultOpContext::DIV() {
  return getToken(PromQLParser::DIV, 0);
}

tree::TerminalNode* PromQLParser::MultOpContext::MOD() {
  return getToken(PromQLParser::MOD, 0);
}

PromQLParser::GroupingContext* PromQLParser::MultOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::MultOpContext::getRuleIndex() const {
  return PromQLParser::RuleMultOp;
}

void PromQLParser::MultOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultOp(this);
}

void PromQLParser::MultOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultOp(this);
}


std::any PromQLParser::MultOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitMultOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::MultOpContext* PromQLParser::multOp() {
  MultOpContext *_localctx = _tracker.createInstance<MultOpContext>(_ctx, getState());
  enterRule(_localctx, 8, PromQLParser::RuleMultOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(117);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 224) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(119);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      setState(118);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AddOpContext ------------------------------------------------------------------

PromQLParser::AddOpContext::AddOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::AddOpContext::ADD() {
  return getToken(PromQLParser::ADD, 0);
}

tree::TerminalNode* PromQLParser::AddOpContext::SUB() {
  return getToken(PromQLParser::SUB, 0);
}

PromQLParser::GroupingContext* PromQLParser::AddOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::AddOpContext::getRuleIndex() const {
  return PromQLParser::RuleAddOp;
}

void PromQLParser::AddOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAddOp(this);
}

void PromQLParser::AddOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAddOp(this);
}


std::any PromQLParser::AddOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitAddOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::AddOpContext* PromQLParser::addOp() {
  AddOpContext *_localctx = _tracker.createInstance<AddOpContext>(_ctx, getState());
  enterRule(_localctx, 10, PromQLParser::RuleAddOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(121);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::ADD

    || _la == PromQLParser::SUB)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(123);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(122);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CompareOpContext ------------------------------------------------------------------

PromQLParser::CompareOpContext::CompareOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::CompareOpContext::DEQ() {
  return getToken(PromQLParser::DEQ, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::NE() {
  return getToken(PromQLParser::NE, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::GT() {
  return getToken(PromQLParser::GT, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::LT() {
  return getToken(PromQLParser::LT, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::GE() {
  return getToken(PromQLParser::GE, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::LE() {
  return getToken(PromQLParser::LE, 0);
}

tree::TerminalNode* PromQLParser::CompareOpContext::BOOL() {
  return getToken(PromQLParser::BOOL, 0);
}

PromQLParser::GroupingContext* PromQLParser::CompareOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::CompareOpContext::getRuleIndex() const {
  return PromQLParser::RuleCompareOp;
}

void PromQLParser::CompareOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCompareOp(this);
}

void PromQLParser::CompareOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCompareOp(this);
}


std::any PromQLParser::CompareOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitCompareOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::CompareOpContext* PromQLParser::compareOp() {
  CompareOpContext *_localctx = _tracker.createInstance<CompareOpContext>(_ctx, getState());
  enterRule(_localctx, 12, PromQLParser::RuleCompareOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(125);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 516096) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(127);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx)) {
    case 1: {
      setState(126);
      match(PromQLParser::BOOL);
      break;
    }

    default:
      break;
    }
    setState(130);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
    case 1: {
      setState(129);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AndUnlessOpContext ------------------------------------------------------------------

PromQLParser::AndUnlessOpContext::AndUnlessOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::AndUnlessOpContext::AND() {
  return getToken(PromQLParser::AND, 0);
}

tree::TerminalNode* PromQLParser::AndUnlessOpContext::UNLESS() {
  return getToken(PromQLParser::UNLESS, 0);
}

PromQLParser::GroupingContext* PromQLParser::AndUnlessOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::AndUnlessOpContext::getRuleIndex() const {
  return PromQLParser::RuleAndUnlessOp;
}

void PromQLParser::AndUnlessOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAndUnlessOp(this);
}

void PromQLParser::AndUnlessOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAndUnlessOp(this);
}


std::any PromQLParser::AndUnlessOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitAndUnlessOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::AndUnlessOpContext* PromQLParser::andUnlessOp() {
  AndUnlessOpContext *_localctx = _tracker.createInstance<AndUnlessOpContext>(_ctx, getState());
  enterRule(_localctx, 14, PromQLParser::RuleAndUnlessOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(132);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::AND

    || _la == PromQLParser::UNLESS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(134);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
    case 1: {
      setState(133);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrOpContext ------------------------------------------------------------------

PromQLParser::OrOpContext::OrOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::OrOpContext::OR() {
  return getToken(PromQLParser::OR, 0);
}

PromQLParser::GroupingContext* PromQLParser::OrOpContext::grouping() {
  return getRuleContext<PromQLParser::GroupingContext>(0);
}


size_t PromQLParser::OrOpContext::getRuleIndex() const {
  return PromQLParser::RuleOrOp;
}

void PromQLParser::OrOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrOp(this);
}

void PromQLParser::OrOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrOp(this);
}


std::any PromQLParser::OrOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitOrOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::OrOpContext* PromQLParser::orOp() {
  OrOpContext *_localctx = _tracker.createInstance<OrOpContext>(_ctx, getState());
  enterRule(_localctx, 16, PromQLParser::RuleOrOp);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(136);
    match(PromQLParser::OR);
    setState(138);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(137);
      grouping();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubqueryOpContext ------------------------------------------------------------------

PromQLParser::SubqueryOpContext::SubqueryOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::SubqueryOpContext::SUBQUERY_RANGE() {
  return getToken(PromQLParser::SUBQUERY_RANGE, 0);
}

PromQLParser::OffsetOpContext* PromQLParser::SubqueryOpContext::offsetOp() {
  return getRuleContext<PromQLParser::OffsetOpContext>(0);
}


size_t PromQLParser::SubqueryOpContext::getRuleIndex() const {
  return PromQLParser::RuleSubqueryOp;
}

void PromQLParser::SubqueryOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubqueryOp(this);
}

void PromQLParser::SubqueryOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubqueryOp(this);
}


std::any PromQLParser::SubqueryOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitSubqueryOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::SubqueryOpContext* PromQLParser::subqueryOp() {
  SubqueryOpContext *_localctx = _tracker.createInstance<SubqueryOpContext>(_ctx, getState());
  enterRule(_localctx, 18, PromQLParser::RuleSubqueryOp);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(140);
    match(PromQLParser::SUBQUERY_RANGE);
    setState(142);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(141);
      offsetOp();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetOpContext ------------------------------------------------------------------

PromQLParser::OffsetOpContext::OffsetOpContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::OffsetOpContext::AT() {
  return getToken(PromQLParser::AT, 0);
}

std::vector<tree::TerminalNode *> PromQLParser::OffsetOpContext::NUMBER() {
  return getTokens(PromQLParser::NUMBER);
}

tree::TerminalNode* PromQLParser::OffsetOpContext::NUMBER(size_t i) {
  return getToken(PromQLParser::NUMBER, i);
}

tree::TerminalNode* PromQLParser::OffsetOpContext::OFFSET() {
  return getToken(PromQLParser::OFFSET, 0);
}

tree::TerminalNode* PromQLParser::OffsetOpContext::ADD() {
  return getToken(PromQLParser::ADD, 0);
}

tree::TerminalNode* PromQLParser::OffsetOpContext::SUB() {
  return getToken(PromQLParser::SUB, 0);
}


size_t PromQLParser::OffsetOpContext::getRuleIndex() const {
  return PromQLParser::RuleOffsetOp;
}

void PromQLParser::OffsetOpContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOffsetOp(this);
}

void PromQLParser::OffsetOpContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOffsetOp(this);
}


std::any PromQLParser::OffsetOpContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitOffsetOp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::OffsetOpContext* PromQLParser::offsetOp() {
  OffsetOpContext *_localctx = _tracker.createInstance<OffsetOpContext>(_ctx, getState());
  enterRule(_localctx, 20, PromQLParser::RuleOffsetOp);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(162);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AT: {
        enterOuterAlt(_localctx, 1);
        setState(144);
        match(PromQLParser::AT);
        setState(145);
        match(PromQLParser::NUMBER);
        setState(151);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
        case 1: {
          setState(146);
          match(PromQLParser::OFFSET);
          setState(148);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PromQLParser::ADD

          || _la == PromQLParser::SUB) {
            setState(147);
            _la = _input->LA(1);
            if (!(_la == PromQLParser::ADD

            || _la == PromQLParser::SUB)) {
            _errHandler->recoverInline(this);
            }
            else {
              _errHandler->reportMatch(this);
              consume();
            }
          }
          setState(150);
          match(PromQLParser::NUMBER);
          break;
        }

        default:
          break;
        }
        break;
      }

      case PromQLParser::OFFSET: {
        enterOuterAlt(_localctx, 2);
        setState(153);
        match(PromQLParser::OFFSET);
        setState(155);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PromQLParser::ADD

        || _la == PromQLParser::SUB) {
          setState(154);
          _la = _input->LA(1);
          if (!(_la == PromQLParser::ADD

          || _la == PromQLParser::SUB)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(157);
        match(PromQLParser::NUMBER);
        setState(160);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
        case 1: {
          setState(158);
          match(PromQLParser::AT);
          setState(159);
          match(PromQLParser::NUMBER);
          break;
        }

        default:
          break;
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VectorContext ------------------------------------------------------------------

PromQLParser::VectorContext::VectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::Function_Context* PromQLParser::VectorContext::function_() {
  return getRuleContext<PromQLParser::Function_Context>(0);
}

PromQLParser::AggregationContext* PromQLParser::VectorContext::aggregation() {
  return getRuleContext<PromQLParser::AggregationContext>(0);
}

PromQLParser::InstantSelectorContext* PromQLParser::VectorContext::instantSelector() {
  return getRuleContext<PromQLParser::InstantSelectorContext>(0);
}

PromQLParser::MatrixSelectorContext* PromQLParser::VectorContext::matrixSelector() {
  return getRuleContext<PromQLParser::MatrixSelectorContext>(0);
}

PromQLParser::OffsetContext* PromQLParser::VectorContext::offset() {
  return getRuleContext<PromQLParser::OffsetContext>(0);
}

PromQLParser::LiteralContext* PromQLParser::VectorContext::literal() {
  return getRuleContext<PromQLParser::LiteralContext>(0);
}

PromQLParser::ParensContext* PromQLParser::VectorContext::parens() {
  return getRuleContext<PromQLParser::ParensContext>(0);
}


size_t PromQLParser::VectorContext::getRuleIndex() const {
  return PromQLParser::RuleVector;
}

void PromQLParser::VectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVector(this);
}

void PromQLParser::VectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVector(this);
}


std::any PromQLParser::VectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitVector(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::VectorContext* PromQLParser::vector() {
  VectorContext *_localctx = _tracker.createInstance<VectorContext>(_ctx, getState());
  enterRule(_localctx, 22, PromQLParser::RuleVector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(171);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(164);
      function_();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(165);
      aggregation();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(166);
      instantSelector();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(167);
      matrixSelector();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(168);
      offset();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(169);
      literal();
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(170);
      parens();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParensContext ------------------------------------------------------------------

PromQLParser::ParensContext::ParensContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::ParensContext::LEFT_PAREN() {
  return getToken(PromQLParser::LEFT_PAREN, 0);
}

PromQLParser::VectorOperationContext* PromQLParser::ParensContext::vectorOperation() {
  return getRuleContext<PromQLParser::VectorOperationContext>(0);
}

tree::TerminalNode* PromQLParser::ParensContext::RIGHT_PAREN() {
  return getToken(PromQLParser::RIGHT_PAREN, 0);
}


size_t PromQLParser::ParensContext::getRuleIndex() const {
  return PromQLParser::RuleParens;
}

void PromQLParser::ParensContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParens(this);
}

void PromQLParser::ParensContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParens(this);
}


std::any PromQLParser::ParensContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitParens(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::ParensContext* PromQLParser::parens() {
  ParensContext *_localctx = _tracker.createInstance<ParensContext>(_ctx, getState());
  enterRule(_localctx, 24, PromQLParser::RuleParens);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(173);
    match(PromQLParser::LEFT_PAREN);
    setState(174);
    vectorOperation(0);
    setState(175);
    match(PromQLParser::RIGHT_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InstantSelectorContext ------------------------------------------------------------------

PromQLParser::InstantSelectorContext::InstantSelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::MetricNameContext* PromQLParser::InstantSelectorContext::metricName() {
  return getRuleContext<PromQLParser::MetricNameContext>(0);
}

tree::TerminalNode* PromQLParser::InstantSelectorContext::LEFT_BRACE() {
  return getToken(PromQLParser::LEFT_BRACE, 0);
}

tree::TerminalNode* PromQLParser::InstantSelectorContext::RIGHT_BRACE() {
  return getToken(PromQLParser::RIGHT_BRACE, 0);
}

PromQLParser::LabelMatcherListContext* PromQLParser::InstantSelectorContext::labelMatcherList() {
  return getRuleContext<PromQLParser::LabelMatcherListContext>(0);
}


size_t PromQLParser::InstantSelectorContext::getRuleIndex() const {
  return PromQLParser::RuleInstantSelector;
}

void PromQLParser::InstantSelectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInstantSelector(this);
}

void PromQLParser::InstantSelectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInstantSelector(this);
}


std::any PromQLParser::InstantSelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitInstantSelector(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::InstantSelectorContext* PromQLParser::instantSelector() {
  InstantSelectorContext *_localctx = _tracker.createInstance<InstantSelectorContext>(_ctx, getState());
  enterRule(_localctx, 26, PromQLParser::RuleInstantSelector);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(189);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION:
      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(177);
        metricName();
        setState(183);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
        case 1: {
          setState(178);
          match(PromQLParser::LEFT_BRACE);
          setState(180);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if ((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & 6599215156736) != 0)) {
            setState(179);
            labelMatcherList();
          }
          setState(182);
          match(PromQLParser::RIGHT_BRACE);
          break;
        }

        default:
          break;
        }
        break;
      }

      case PromQLParser::LEFT_BRACE: {
        enterOuterAlt(_localctx, 2);
        setState(185);
        match(PromQLParser::LEFT_BRACE);
        setState(186);
        labelMatcherList();
        setState(187);
        match(PromQLParser::RIGHT_BRACE);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelMatcherContext ------------------------------------------------------------------

PromQLParser::LabelMatcherContext::LabelMatcherContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::LabelNameContext* PromQLParser::LabelMatcherContext::labelName() {
  return getRuleContext<PromQLParser::LabelNameContext>(0);
}

PromQLParser::LabelMatcherOperatorContext* PromQLParser::LabelMatcherContext::labelMatcherOperator() {
  return getRuleContext<PromQLParser::LabelMatcherOperatorContext>(0);
}

tree::TerminalNode* PromQLParser::LabelMatcherContext::STRING() {
  return getToken(PromQLParser::STRING, 0);
}


size_t PromQLParser::LabelMatcherContext::getRuleIndex() const {
  return PromQLParser::RuleLabelMatcher;
}

void PromQLParser::LabelMatcherContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLabelMatcher(this);
}

void PromQLParser::LabelMatcherContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLabelMatcher(this);
}


std::any PromQLParser::LabelMatcherContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLabelMatcher(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LabelMatcherContext* PromQLParser::labelMatcher() {
  LabelMatcherContext *_localctx = _tracker.createInstance<LabelMatcherContext>(_ctx, getState());
  enterRule(_localctx, 28, PromQLParser::RuleLabelMatcher);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(191);
    labelName();
    setState(192);
    labelMatcherOperator();
    setState(193);
    match(PromQLParser::STRING);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelMatcherOperatorContext ------------------------------------------------------------------

PromQLParser::LabelMatcherOperatorContext::LabelMatcherOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::LabelMatcherOperatorContext::EQ() {
  return getToken(PromQLParser::EQ, 0);
}

tree::TerminalNode* PromQLParser::LabelMatcherOperatorContext::NE() {
  return getToken(PromQLParser::NE, 0);
}

tree::TerminalNode* PromQLParser::LabelMatcherOperatorContext::RE() {
  return getToken(PromQLParser::RE, 0);
}

tree::TerminalNode* PromQLParser::LabelMatcherOperatorContext::NRE() {
  return getToken(PromQLParser::NRE, 0);
}


size_t PromQLParser::LabelMatcherOperatorContext::getRuleIndex() const {
  return PromQLParser::RuleLabelMatcherOperator;
}

void PromQLParser::LabelMatcherOperatorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLabelMatcherOperator(this);
}

void PromQLParser::LabelMatcherOperatorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLabelMatcherOperator(this);
}


std::any PromQLParser::LabelMatcherOperatorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLabelMatcherOperator(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LabelMatcherOperatorContext* PromQLParser::labelMatcherOperator() {
  LabelMatcherOperatorContext *_localctx = _tracker.createInstance<LabelMatcherOperatorContext>(_ctx, getState());
  enterRule(_localctx, 30, PromQLParser::RuleLabelMatcherOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(195);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 1593344) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelMatcherListContext ------------------------------------------------------------------

PromQLParser::LabelMatcherListContext::LabelMatcherListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<PromQLParser::LabelMatcherContext *> PromQLParser::LabelMatcherListContext::labelMatcher() {
  return getRuleContexts<PromQLParser::LabelMatcherContext>();
}

PromQLParser::LabelMatcherContext* PromQLParser::LabelMatcherListContext::labelMatcher(size_t i) {
  return getRuleContext<PromQLParser::LabelMatcherContext>(i);
}

std::vector<tree::TerminalNode *> PromQLParser::LabelMatcherListContext::COMMA() {
  return getTokens(PromQLParser::COMMA);
}

tree::TerminalNode* PromQLParser::LabelMatcherListContext::COMMA(size_t i) {
  return getToken(PromQLParser::COMMA, i);
}


size_t PromQLParser::LabelMatcherListContext::getRuleIndex() const {
  return PromQLParser::RuleLabelMatcherList;
}

void PromQLParser::LabelMatcherListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLabelMatcherList(this);
}

void PromQLParser::LabelMatcherListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLabelMatcherList(this);
}


std::any PromQLParser::LabelMatcherListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLabelMatcherList(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LabelMatcherListContext* PromQLParser::labelMatcherList() {
  LabelMatcherListContext *_localctx = _tracker.createInstance<LabelMatcherListContext>(_ctx, getState());
  enterRule(_localctx, 32, PromQLParser::RuleLabelMatcherList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(197);
    labelMatcher();
    setState(202);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(198);
        match(PromQLParser::COMMA);
        setState(199);
        labelMatcher(); 
      }
      setState(204);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx);
    }
    setState(206);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PromQLParser::COMMA) {
      setState(205);
      match(PromQLParser::COMMA);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MatrixSelectorContext ------------------------------------------------------------------

PromQLParser::MatrixSelectorContext::MatrixSelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::InstantSelectorContext* PromQLParser::MatrixSelectorContext::instantSelector() {
  return getRuleContext<PromQLParser::InstantSelectorContext>(0);
}

tree::TerminalNode* PromQLParser::MatrixSelectorContext::TIME_RANGE() {
  return getToken(PromQLParser::TIME_RANGE, 0);
}


size_t PromQLParser::MatrixSelectorContext::getRuleIndex() const {
  return PromQLParser::RuleMatrixSelector;
}

void PromQLParser::MatrixSelectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMatrixSelector(this);
}

void PromQLParser::MatrixSelectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMatrixSelector(this);
}


std::any PromQLParser::MatrixSelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitMatrixSelector(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::MatrixSelectorContext* PromQLParser::matrixSelector() {
  MatrixSelectorContext *_localctx = _tracker.createInstance<MatrixSelectorContext>(_ctx, getState());
  enterRule(_localctx, 34, PromQLParser::RuleMatrixSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(208);
    instantSelector();
    setState(209);
    match(PromQLParser::TIME_RANGE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetContext ------------------------------------------------------------------

PromQLParser::OffsetContext::OffsetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::InstantSelectorContext* PromQLParser::OffsetContext::instantSelector() {
  return getRuleContext<PromQLParser::InstantSelectorContext>(0);
}

PromQLParser::OffsetOpContext* PromQLParser::OffsetContext::offsetOp() {
  return getRuleContext<PromQLParser::OffsetOpContext>(0);
}

PromQLParser::MatrixSelectorContext* PromQLParser::OffsetContext::matrixSelector() {
  return getRuleContext<PromQLParser::MatrixSelectorContext>(0);
}


size_t PromQLParser::OffsetContext::getRuleIndex() const {
  return PromQLParser::RuleOffset;
}

void PromQLParser::OffsetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOffset(this);
}

void PromQLParser::OffsetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOffset(this);
}


std::any PromQLParser::OffsetContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitOffset(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::OffsetContext* PromQLParser::offset() {
  OffsetContext *_localctx = _tracker.createInstance<OffsetContext>(_ctx, getState());
  enterRule(_localctx, 36, PromQLParser::RuleOffset);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(217);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(211);
      instantSelector();
      setState(212);
      offsetOp();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(214);
      matrixSelector();
      setState(215);
      offsetOp();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Function_Context ------------------------------------------------------------------

PromQLParser::Function_Context::Function_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::Function_Context::FUNCTION() {
  return getToken(PromQLParser::FUNCTION, 0);
}

tree::TerminalNode* PromQLParser::Function_Context::LEFT_PAREN() {
  return getToken(PromQLParser::LEFT_PAREN, 0);
}

tree::TerminalNode* PromQLParser::Function_Context::RIGHT_PAREN() {
  return getToken(PromQLParser::RIGHT_PAREN, 0);
}

std::vector<PromQLParser::ParameterContext *> PromQLParser::Function_Context::parameter() {
  return getRuleContexts<PromQLParser::ParameterContext>();
}

PromQLParser::ParameterContext* PromQLParser::Function_Context::parameter(size_t i) {
  return getRuleContext<PromQLParser::ParameterContext>(i);
}

std::vector<tree::TerminalNode *> PromQLParser::Function_Context::COMMA() {
  return getTokens(PromQLParser::COMMA);
}

tree::TerminalNode* PromQLParser::Function_Context::COMMA(size_t i) {
  return getToken(PromQLParser::COMMA, i);
}


size_t PromQLParser::Function_Context::getRuleIndex() const {
  return PromQLParser::RuleFunction_;
}

void PromQLParser::Function_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunction_(this);
}

void PromQLParser::Function_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunction_(this);
}


std::any PromQLParser::Function_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitFunction_(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::Function_Context* PromQLParser::function_() {
  Function_Context *_localctx = _tracker.createInstance<Function_Context>(_ctx, getState());
  enterRule(_localctx, 38, PromQLParser::RuleFunction_);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(219);
    match(PromQLParser::FUNCTION);
    setState(220);
    match(PromQLParser::LEFT_PAREN);
    setState(229);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2211906063902) != 0)) {
      setState(221);
      parameter();
      setState(226);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(222);
        match(PromQLParser::COMMA);
        setState(223);
        parameter();
        setState(228);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(231);
    match(PromQLParser::RIGHT_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParameterContext ------------------------------------------------------------------

PromQLParser::ParameterContext::ParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::LiteralContext* PromQLParser::ParameterContext::literal() {
  return getRuleContext<PromQLParser::LiteralContext>(0);
}

PromQLParser::VectorOperationContext* PromQLParser::ParameterContext::vectorOperation() {
  return getRuleContext<PromQLParser::VectorOperationContext>(0);
}


size_t PromQLParser::ParameterContext::getRuleIndex() const {
  return PromQLParser::RuleParameter;
}

void PromQLParser::ParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParameter(this);
}

void PromQLParser::ParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParameter(this);
}


std::any PromQLParser::ParameterContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitParameter(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::ParameterContext* PromQLParser::parameter() {
  ParameterContext *_localctx = _tracker.createInstance<ParameterContext>(_ctx, getState());
  enterRule(_localctx, 40, PromQLParser::RuleParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(235);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(233);
      literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(234);
      vectorOperation(0);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParameterListContext ------------------------------------------------------------------

PromQLParser::ParameterListContext::ParameterListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::ParameterListContext::LEFT_PAREN() {
  return getToken(PromQLParser::LEFT_PAREN, 0);
}

tree::TerminalNode* PromQLParser::ParameterListContext::RIGHT_PAREN() {
  return getToken(PromQLParser::RIGHT_PAREN, 0);
}

std::vector<PromQLParser::ParameterContext *> PromQLParser::ParameterListContext::parameter() {
  return getRuleContexts<PromQLParser::ParameterContext>();
}

PromQLParser::ParameterContext* PromQLParser::ParameterListContext::parameter(size_t i) {
  return getRuleContext<PromQLParser::ParameterContext>(i);
}

std::vector<tree::TerminalNode *> PromQLParser::ParameterListContext::COMMA() {
  return getTokens(PromQLParser::COMMA);
}

tree::TerminalNode* PromQLParser::ParameterListContext::COMMA(size_t i) {
  return getToken(PromQLParser::COMMA, i);
}


size_t PromQLParser::ParameterListContext::getRuleIndex() const {
  return PromQLParser::RuleParameterList;
}

void PromQLParser::ParameterListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParameterList(this);
}

void PromQLParser::ParameterListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParameterList(this);
}


std::any PromQLParser::ParameterListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitParameterList(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::ParameterListContext* PromQLParser::parameterList() {
  ParameterListContext *_localctx = _tracker.createInstance<ParameterListContext>(_ctx, getState());
  enterRule(_localctx, 42, PromQLParser::RuleParameterList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(237);
    match(PromQLParser::LEFT_PAREN);
    setState(246);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2211906063902) != 0)) {
      setState(238);
      parameter();
      setState(243);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(239);
        match(PromQLParser::COMMA);
        setState(240);
        parameter();
        setState(245);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(248);
    match(PromQLParser::RIGHT_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AggregationContext ------------------------------------------------------------------

PromQLParser::AggregationContext::AggregationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::AggregationContext::AGGREGATION_OPERATOR() {
  return getToken(PromQLParser::AGGREGATION_OPERATOR, 0);
}

PromQLParser::ParameterListContext* PromQLParser::AggregationContext::parameterList() {
  return getRuleContext<PromQLParser::ParameterListContext>(0);
}

PromQLParser::ByContext* PromQLParser::AggregationContext::by() {
  return getRuleContext<PromQLParser::ByContext>(0);
}

PromQLParser::WithoutContext* PromQLParser::AggregationContext::without() {
  return getRuleContext<PromQLParser::WithoutContext>(0);
}


size_t PromQLParser::AggregationContext::getRuleIndex() const {
  return PromQLParser::RuleAggregation;
}

void PromQLParser::AggregationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAggregation(this);
}

void PromQLParser::AggregationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAggregation(this);
}


std::any PromQLParser::AggregationContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitAggregation(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::AggregationContext* PromQLParser::aggregation() {
  AggregationContext *_localctx = _tracker.createInstance<AggregationContext>(_ctx, getState());
  enterRule(_localctx, 44, PromQLParser::RuleAggregation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(265);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(250);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(251);
      parameterList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(252);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(255);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(253);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(254);
          without();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(257);
      parameterList();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(259);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(260);
      parameterList();
      setState(263);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(261);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(262);
          without();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ByContext ------------------------------------------------------------------

PromQLParser::ByContext::ByContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::ByContext::BY() {
  return getToken(PromQLParser::BY, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::ByContext::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::ByContext::getRuleIndex() const {
  return PromQLParser::RuleBy;
}

void PromQLParser::ByContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBy(this);
}

void PromQLParser::ByContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBy(this);
}


std::any PromQLParser::ByContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitBy(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::ByContext* PromQLParser::by() {
  ByContext *_localctx = _tracker.createInstance<ByContext>(_ctx, getState());
  enterRule(_localctx, 46, PromQLParser::RuleBy);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(267);
    match(PromQLParser::BY);
    setState(268);
    labelNameList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WithoutContext ------------------------------------------------------------------

PromQLParser::WithoutContext::WithoutContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::WithoutContext::WITHOUT() {
  return getToken(PromQLParser::WITHOUT, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::WithoutContext::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::WithoutContext::getRuleIndex() const {
  return PromQLParser::RuleWithout;
}

void PromQLParser::WithoutContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWithout(this);
}

void PromQLParser::WithoutContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWithout(this);
}


std::any PromQLParser::WithoutContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitWithout(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::WithoutContext* PromQLParser::without() {
  WithoutContext *_localctx = _tracker.createInstance<WithoutContext>(_ctx, getState());
  enterRule(_localctx, 48, PromQLParser::RuleWithout);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(270);
    match(PromQLParser::WITHOUT);
    setState(271);
    labelNameList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupingContext ------------------------------------------------------------------

PromQLParser::GroupingContext::GroupingContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::On_Context* PromQLParser::GroupingContext::on_() {
  return getRuleContext<PromQLParser::On_Context>(0);
}

PromQLParser::IgnoringContext* PromQLParser::GroupingContext::ignoring() {
  return getRuleContext<PromQLParser::IgnoringContext>(0);
}

PromQLParser::GroupLeftContext* PromQLParser::GroupingContext::groupLeft() {
  return getRuleContext<PromQLParser::GroupLeftContext>(0);
}

PromQLParser::GroupRightContext* PromQLParser::GroupingContext::groupRight() {
  return getRuleContext<PromQLParser::GroupRightContext>(0);
}


size_t PromQLParser::GroupingContext::getRuleIndex() const {
  return PromQLParser::RuleGrouping;
}

void PromQLParser::GroupingContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGrouping(this);
}

void PromQLParser::GroupingContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGrouping(this);
}


std::any PromQLParser::GroupingContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitGrouping(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::GroupingContext* PromQLParser::grouping() {
  GroupingContext *_localctx = _tracker.createInstance<GroupingContext>(_ctx, getState());
  enterRule(_localctx, 50, PromQLParser::RuleGrouping);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(275);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ON: {
        setState(273);
        on_();
        break;
      }

      case PromQLParser::IGNORING: {
        setState(274);
        ignoring();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(279);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
    case 1: {
      setState(277);
      groupLeft();
      break;
    }

    case 2: {
      setState(278);
      groupRight();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- On_Context ------------------------------------------------------------------

PromQLParser::On_Context::On_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::On_Context::ON() {
  return getToken(PromQLParser::ON, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::On_Context::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::On_Context::getRuleIndex() const {
  return PromQLParser::RuleOn_;
}

void PromQLParser::On_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOn_(this);
}

void PromQLParser::On_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOn_(this);
}


std::any PromQLParser::On_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitOn_(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::On_Context* PromQLParser::on_() {
  On_Context *_localctx = _tracker.createInstance<On_Context>(_ctx, getState());
  enterRule(_localctx, 52, PromQLParser::RuleOn_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(281);
    match(PromQLParser::ON);
    setState(282);
    labelNameList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IgnoringContext ------------------------------------------------------------------

PromQLParser::IgnoringContext::IgnoringContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::IgnoringContext::IGNORING() {
  return getToken(PromQLParser::IGNORING, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::IgnoringContext::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::IgnoringContext::getRuleIndex() const {
  return PromQLParser::RuleIgnoring;
}

void PromQLParser::IgnoringContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIgnoring(this);
}

void PromQLParser::IgnoringContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIgnoring(this);
}


std::any PromQLParser::IgnoringContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitIgnoring(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::IgnoringContext* PromQLParser::ignoring() {
  IgnoringContext *_localctx = _tracker.createInstance<IgnoringContext>(_ctx, getState());
  enterRule(_localctx, 54, PromQLParser::RuleIgnoring);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(284);
    match(PromQLParser::IGNORING);
    setState(285);
    labelNameList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupLeftContext ------------------------------------------------------------------

PromQLParser::GroupLeftContext::GroupLeftContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::GroupLeftContext::GROUP_LEFT() {
  return getToken(PromQLParser::GROUP_LEFT, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::GroupLeftContext::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::GroupLeftContext::getRuleIndex() const {
  return PromQLParser::RuleGroupLeft;
}

void PromQLParser::GroupLeftContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupLeft(this);
}

void PromQLParser::GroupLeftContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupLeft(this);
}


std::any PromQLParser::GroupLeftContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitGroupLeft(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::GroupLeftContext* PromQLParser::groupLeft() {
  GroupLeftContext *_localctx = _tracker.createInstance<GroupLeftContext>(_ctx, getState());
  enterRule(_localctx, 56, PromQLParser::RuleGroupLeft);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(287);
    match(PromQLParser::GROUP_LEFT);
    setState(289);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
    case 1: {
      setState(288);
      labelNameList();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupRightContext ------------------------------------------------------------------

PromQLParser::GroupRightContext::GroupRightContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::GroupRightContext::GROUP_RIGHT() {
  return getToken(PromQLParser::GROUP_RIGHT, 0);
}

PromQLParser::LabelNameListContext* PromQLParser::GroupRightContext::labelNameList() {
  return getRuleContext<PromQLParser::LabelNameListContext>(0);
}


size_t PromQLParser::GroupRightContext::getRuleIndex() const {
  return PromQLParser::RuleGroupRight;
}

void PromQLParser::GroupRightContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupRight(this);
}

void PromQLParser::GroupRightContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupRight(this);
}


std::any PromQLParser::GroupRightContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitGroupRight(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::GroupRightContext* PromQLParser::groupRight() {
  GroupRightContext *_localctx = _tracker.createInstance<GroupRightContext>(_ctx, getState());
  enterRule(_localctx, 58, PromQLParser::RuleGroupRight);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(291);
    match(PromQLParser::GROUP_RIGHT);
    setState(293);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      setState(292);
      labelNameList();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelNameContext ------------------------------------------------------------------

PromQLParser::LabelNameContext::LabelNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::KeywordContext* PromQLParser::LabelNameContext::keyword() {
  return getRuleContext<PromQLParser::KeywordContext>(0);
}

tree::TerminalNode* PromQLParser::LabelNameContext::METRIC_NAME() {
  return getToken(PromQLParser::METRIC_NAME, 0);
}

tree::TerminalNode* PromQLParser::LabelNameContext::LABEL_NAME() {
  return getToken(PromQLParser::LABEL_NAME, 0);
}


size_t PromQLParser::LabelNameContext::getRuleIndex() const {
  return PromQLParser::RuleLabelName;
}

void PromQLParser::LabelNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLabelName(this);
}

void PromQLParser::LabelNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLabelName(this);
}


std::any PromQLParser::LabelNameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLabelName(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LabelNameContext* PromQLParser::labelName() {
  LabelNameContext *_localctx = _tracker.createInstance<LabelNameContext>(_ctx, getState());
  enterRule(_localctx, 60, PromQLParser::RuleLabelName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(298);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION: {
        enterOuterAlt(_localctx, 1);
        setState(295);
        keyword();
        break;
      }

      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 2);
        setState(296);
        match(PromQLParser::METRIC_NAME);
        break;
      }

      case PromQLParser::LABEL_NAME: {
        enterOuterAlt(_localctx, 3);
        setState(297);
        match(PromQLParser::LABEL_NAME);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LabelNameListContext ------------------------------------------------------------------

PromQLParser::LabelNameListContext::LabelNameListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::LabelNameListContext::LEFT_PAREN() {
  return getToken(PromQLParser::LEFT_PAREN, 0);
}

tree::TerminalNode* PromQLParser::LabelNameListContext::RIGHT_PAREN() {
  return getToken(PromQLParser::RIGHT_PAREN, 0);
}

std::vector<PromQLParser::LabelNameContext *> PromQLParser::LabelNameListContext::labelName() {
  return getRuleContexts<PromQLParser::LabelNameContext>();
}

PromQLParser::LabelNameContext* PromQLParser::LabelNameListContext::labelName(size_t i) {
  return getRuleContext<PromQLParser::LabelNameContext>(i);
}

std::vector<tree::TerminalNode *> PromQLParser::LabelNameListContext::COMMA() {
  return getTokens(PromQLParser::COMMA);
}

tree::TerminalNode* PromQLParser::LabelNameListContext::COMMA(size_t i) {
  return getToken(PromQLParser::COMMA, i);
}


size_t PromQLParser::LabelNameListContext::getRuleIndex() const {
  return PromQLParser::RuleLabelNameList;
}

void PromQLParser::LabelNameListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLabelNameList(this);
}

void PromQLParser::LabelNameListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLabelNameList(this);
}


std::any PromQLParser::LabelNameListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLabelNameList(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LabelNameListContext* PromQLParser::labelNameList() {
  LabelNameListContext *_localctx = _tracker.createInstance<LabelNameListContext>(_ctx, getState());
  enterRule(_localctx, 62, PromQLParser::RuleLabelNameList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(300);
    match(PromQLParser::LEFT_PAREN);
    setState(309);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 6599215156736) != 0)) {
      setState(301);
      labelName();
      setState(306);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(302);
        match(PromQLParser::COMMA);
        setState(303);
        labelName();
        setState(308);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(311);
    match(PromQLParser::RIGHT_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MetricNameContext ------------------------------------------------------------------

PromQLParser::MetricNameContext::MetricNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::MetricNameContext::METRIC_NAME() {
  return getToken(PromQLParser::METRIC_NAME, 0);
}

PromQLParser::KeywordContext* PromQLParser::MetricNameContext::keyword() {
  return getRuleContext<PromQLParser::KeywordContext>(0);
}


size_t PromQLParser::MetricNameContext::getRuleIndex() const {
  return PromQLParser::RuleMetricName;
}

void PromQLParser::MetricNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMetricName(this);
}

void PromQLParser::MetricNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMetricName(this);
}


std::any PromQLParser::MetricNameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitMetricName(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::MetricNameContext* PromQLParser::metricName() {
  MetricNameContext *_localctx = _tracker.createInstance<MetricNameContext>(_ctx, getState());
  enterRule(_localctx, 64, PromQLParser::RuleMetricName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(315);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(313);
        match(PromQLParser::METRIC_NAME);
        break;
      }

      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION: {
        enterOuterAlt(_localctx, 2);
        setState(314);
        keyword();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KeywordContext ------------------------------------------------------------------

PromQLParser::KeywordContext::KeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::KeywordContext::AND() {
  return getToken(PromQLParser::AND, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::OR() {
  return getToken(PromQLParser::OR, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::UNLESS() {
  return getToken(PromQLParser::UNLESS, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::BY() {
  return getToken(PromQLParser::BY, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::WITHOUT() {
  return getToken(PromQLParser::WITHOUT, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::ON() {
  return getToken(PromQLParser::ON, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::IGNORING() {
  return getToken(PromQLParser::IGNORING, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::GROUP_LEFT() {
  return getToken(PromQLParser::GROUP_LEFT, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::GROUP_RIGHT() {
  return getToken(PromQLParser::GROUP_RIGHT, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::OFFSET() {
  return getToken(PromQLParser::OFFSET, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::BOOL() {
  return getToken(PromQLParser::BOOL, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::AGGREGATION_OPERATOR() {
  return getToken(PromQLParser::AGGREGATION_OPERATOR, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::FUNCTION() {
  return getToken(PromQLParser::FUNCTION, 0);
}


size_t PromQLParser::KeywordContext::getRuleIndex() const {
  return PromQLParser::RuleKeyword;
}

void PromQLParser::KeywordContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKeyword(this);
}

void PromQLParser::KeywordContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKeyword(this);
}


std::any PromQLParser::KeywordContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitKeyword(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::KeywordContext* PromQLParser::keyword() {
  KeywordContext *_localctx = _tracker.createInstance<KeywordContext>(_ctx, getState());
  enterRule(_localctx, 66, PromQLParser::RuleKeyword);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(317);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2145390080) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LiteralContext ------------------------------------------------------------------

PromQLParser::LiteralContext::LiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::LiteralContext::NUMBER() {
  return getToken(PromQLParser::NUMBER, 0);
}

tree::TerminalNode* PromQLParser::LiteralContext::STRING() {
  return getToken(PromQLParser::STRING, 0);
}


size_t PromQLParser::LiteralContext::getRuleIndex() const {
  return PromQLParser::RuleLiteral;
}

void PromQLParser::LiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral(this);
}

void PromQLParser::LiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral(this);
}


std::any PromQLParser::LiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitLiteral(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::LiteralContext* PromQLParser::literal() {
  LiteralContext *_localctx = _tracker.createInstance<LiteralContext>(_ctx, getState());
  enterRule(_localctx, 68, PromQLParser::RuleLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(319);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::NUMBER

    || _la == PromQLParser::STRING)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool PromQLParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 1: return vectorOperationSempred(antlrcpp::downCast<VectorOperationContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool PromQLParser::vectorOperationSempred(VectorOperationContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 9);
    case 1: return precpred(_ctx, 6);
    case 2: return precpred(_ctx, 5);
    case 3: return precpred(_ctx, 4);
    case 4: return precpred(_ctx, 3);
    case 5: return precpred(_ctx, 2);
    case 6: return precpred(_ctx, 8);

  default:
    break;
  }
  return true;
}

void PromQLParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  promqlparserParserInitialize();
#else
  ::antlr4::internal::call_once(promqlparserParserOnceFlag, promqlparserParserInitialize);
#endif
}
