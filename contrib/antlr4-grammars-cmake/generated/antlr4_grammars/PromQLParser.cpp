
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
      "parens", "timestamp", "duration", "offsetValue", "instantSelector", 
      "labelMatcher", "labelMatcherOperator", "labelMatcherList", "rangeSelector", 
      "selectorWithOffset", "function_", "parameter", "parameterList", "aggregation", 
      "by", "without", "grouping", "on_", "ignoring", "groupLeft", "groupRight", 
      "labelName", "labelNameList", "metricName", "keyword", "literal"
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
      "COMMA", "AT", "SUBQUERY_RANGE", "SELECTOR_RANGE", "METRIC_NAME", 
      "LABEL_NAME", "WS", "SL_COMMENT"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,44,331,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,
  	21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,
  	28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,7,
  	35,2,36,7,36,2,37,7,37,1,0,1,0,1,0,1,1,1,1,1,1,1,1,1,1,3,1,85,8,1,1,1,
  	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
  	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,5,1,113,8,1,10,1,12,1,116,9,1,1,2,1,2,1,
  	3,1,3,3,3,122,8,3,1,4,1,4,3,4,126,8,4,1,5,1,5,3,5,130,8,5,1,6,1,6,3,6,
  	134,8,6,1,6,3,6,137,8,6,1,7,1,7,3,7,141,8,7,1,8,1,8,3,8,145,8,8,1,9,1,
  	9,3,9,149,8,9,1,10,1,10,1,10,1,10,3,10,155,8,10,1,10,1,10,1,10,1,10,3,
  	10,161,8,10,3,10,163,8,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,3,11,172,
  	8,11,1,12,1,12,1,12,1,12,1,13,1,13,1,14,1,14,1,15,3,15,183,8,15,1,15,
  	1,15,1,16,1,16,1,16,3,16,190,8,16,1,16,3,16,193,8,16,1,16,1,16,1,16,1,
  	16,3,16,199,8,16,1,17,1,17,1,17,1,17,1,18,1,18,1,19,1,19,1,19,5,19,210,
  	8,19,10,19,12,19,213,9,19,1,19,3,19,216,8,19,1,20,1,20,1,20,1,21,1,21,
  	1,21,1,21,1,21,1,21,3,21,227,8,21,1,22,1,22,1,22,1,22,1,22,5,22,234,8,
  	22,10,22,12,22,237,9,22,3,22,239,8,22,1,22,1,22,1,23,1,23,3,23,245,8,
  	23,1,24,1,24,1,24,1,24,5,24,251,8,24,10,24,12,24,254,9,24,3,24,256,8,
  	24,1,24,1,24,1,25,1,25,1,25,1,25,1,25,3,25,265,8,25,1,25,1,25,1,25,1,
  	25,1,25,1,25,3,25,273,8,25,3,25,275,8,25,1,26,1,26,1,26,1,27,1,27,1,27,
  	1,28,1,28,3,28,285,8,28,1,28,1,28,3,28,289,8,28,1,29,1,29,1,29,1,30,1,
  	30,1,30,1,31,1,31,3,31,299,8,31,1,32,1,32,3,32,303,8,32,1,33,1,33,1,33,
  	3,33,308,8,33,1,34,1,34,1,34,1,34,5,34,314,8,34,10,34,12,34,317,9,34,
  	3,34,319,8,34,1,34,1,34,1,35,1,35,3,35,325,8,35,1,36,1,36,1,37,1,37,1,
  	37,0,1,2,38,0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,
  	42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,0,7,1,0,3,4,1,0,5,
  	7,1,0,13,18,2,0,9,9,11,11,3,0,12,12,14,14,19,20,2,0,9,11,21,30,1,0,1,
  	2,343,0,76,1,0,0,0,2,84,1,0,0,0,4,117,1,0,0,0,6,119,1,0,0,0,8,123,1,0,
  	0,0,10,127,1,0,0,0,12,131,1,0,0,0,14,138,1,0,0,0,16,142,1,0,0,0,18,146,
  	1,0,0,0,20,162,1,0,0,0,22,171,1,0,0,0,24,173,1,0,0,0,26,177,1,0,0,0,28,
  	179,1,0,0,0,30,182,1,0,0,0,32,198,1,0,0,0,34,200,1,0,0,0,36,204,1,0,0,
  	0,38,206,1,0,0,0,40,217,1,0,0,0,42,226,1,0,0,0,44,228,1,0,0,0,46,244,
  	1,0,0,0,48,246,1,0,0,0,50,274,1,0,0,0,52,276,1,0,0,0,54,279,1,0,0,0,56,
  	284,1,0,0,0,58,290,1,0,0,0,60,293,1,0,0,0,62,296,1,0,0,0,64,300,1,0,0,
  	0,66,307,1,0,0,0,68,309,1,0,0,0,70,324,1,0,0,0,72,326,1,0,0,0,74,328,
  	1,0,0,0,76,77,3,2,1,0,77,78,5,0,0,1,78,1,1,0,0,0,79,80,6,1,-1,0,80,81,
  	3,4,2,0,81,82,3,2,1,7,82,85,1,0,0,0,83,85,3,22,11,0,84,79,1,0,0,0,84,
  	83,1,0,0,0,85,114,1,0,0,0,86,87,10,8,0,0,87,88,3,6,3,0,88,89,3,2,1,8,
  	89,113,1,0,0,0,90,91,10,6,0,0,91,92,3,8,4,0,92,93,3,2,1,7,93,113,1,0,
  	0,0,94,95,10,5,0,0,95,96,3,10,5,0,96,97,3,2,1,6,97,113,1,0,0,0,98,99,
  	10,4,0,0,99,100,3,12,6,0,100,101,3,2,1,5,101,113,1,0,0,0,102,103,10,3,
  	0,0,103,104,3,14,7,0,104,105,3,2,1,4,105,113,1,0,0,0,106,107,10,2,0,0,
  	107,108,3,16,8,0,108,109,3,2,1,3,109,113,1,0,0,0,110,111,10,9,0,0,111,
  	113,3,18,9,0,112,86,1,0,0,0,112,90,1,0,0,0,112,94,1,0,0,0,112,98,1,0,
  	0,0,112,102,1,0,0,0,112,106,1,0,0,0,112,110,1,0,0,0,113,116,1,0,0,0,114,
  	112,1,0,0,0,114,115,1,0,0,0,115,3,1,0,0,0,116,114,1,0,0,0,117,118,7,0,
  	0,0,118,5,1,0,0,0,119,121,5,8,0,0,120,122,3,56,28,0,121,120,1,0,0,0,121,
  	122,1,0,0,0,122,7,1,0,0,0,123,125,7,1,0,0,124,126,3,56,28,0,125,124,1,
  	0,0,0,125,126,1,0,0,0,126,9,1,0,0,0,127,129,7,0,0,0,128,130,3,56,28,0,
  	129,128,1,0,0,0,129,130,1,0,0,0,130,11,1,0,0,0,131,133,7,2,0,0,132,134,
  	5,28,0,0,133,132,1,0,0,0,133,134,1,0,0,0,134,136,1,0,0,0,135,137,3,56,
  	28,0,136,135,1,0,0,0,136,137,1,0,0,0,137,13,1,0,0,0,138,140,7,3,0,0,139,
  	141,3,56,28,0,140,139,1,0,0,0,140,141,1,0,0,0,141,15,1,0,0,0,142,144,
  	5,10,0,0,143,145,3,56,28,0,144,143,1,0,0,0,144,145,1,0,0,0,145,17,1,0,
  	0,0,146,148,5,39,0,0,147,149,3,20,10,0,148,147,1,0,0,0,148,149,1,0,0,
  	0,149,19,1,0,0,0,150,151,5,38,0,0,151,154,3,26,13,0,152,153,5,27,0,0,
  	153,155,3,30,15,0,154,152,1,0,0,0,154,155,1,0,0,0,155,163,1,0,0,0,156,
  	157,5,27,0,0,157,160,3,30,15,0,158,159,5,38,0,0,159,161,3,26,13,0,160,
  	158,1,0,0,0,160,161,1,0,0,0,161,163,1,0,0,0,162,150,1,0,0,0,162,156,1,
  	0,0,0,163,21,1,0,0,0,164,172,3,44,22,0,165,172,3,50,25,0,166,172,3,32,
  	16,0,167,172,3,40,20,0,168,172,3,42,21,0,169,172,3,74,37,0,170,172,3,
  	24,12,0,171,164,1,0,0,0,171,165,1,0,0,0,171,166,1,0,0,0,171,167,1,0,0,
  	0,171,168,1,0,0,0,171,169,1,0,0,0,171,170,1,0,0,0,172,23,1,0,0,0,173,
  	174,5,33,0,0,174,175,3,2,1,0,175,176,5,34,0,0,176,25,1,0,0,0,177,178,
  	5,1,0,0,178,27,1,0,0,0,179,180,5,1,0,0,180,29,1,0,0,0,181,183,7,0,0,0,
  	182,181,1,0,0,0,182,183,1,0,0,0,183,184,1,0,0,0,184,185,5,1,0,0,185,31,
  	1,0,0,0,186,192,3,70,35,0,187,189,5,31,0,0,188,190,3,38,19,0,189,188,
  	1,0,0,0,189,190,1,0,0,0,190,191,1,0,0,0,191,193,5,32,0,0,192,187,1,0,
  	0,0,192,193,1,0,0,0,193,199,1,0,0,0,194,195,5,31,0,0,195,196,3,38,19,
  	0,196,197,5,32,0,0,197,199,1,0,0,0,198,186,1,0,0,0,198,194,1,0,0,0,199,
  	33,1,0,0,0,200,201,3,66,33,0,201,202,3,36,18,0,202,203,5,2,0,0,203,35,
  	1,0,0,0,204,205,7,4,0,0,205,37,1,0,0,0,206,211,3,34,17,0,207,208,5,37,
  	0,0,208,210,3,34,17,0,209,207,1,0,0,0,210,213,1,0,0,0,211,209,1,0,0,0,
  	211,212,1,0,0,0,212,215,1,0,0,0,213,211,1,0,0,0,214,216,5,37,0,0,215,
  	214,1,0,0,0,215,216,1,0,0,0,216,39,1,0,0,0,217,218,3,32,16,0,218,219,
  	5,40,0,0,219,41,1,0,0,0,220,221,3,32,16,0,221,222,3,20,10,0,222,227,1,
  	0,0,0,223,224,3,40,20,0,224,225,3,20,10,0,225,227,1,0,0,0,226,220,1,0,
  	0,0,226,223,1,0,0,0,227,43,1,0,0,0,228,229,5,30,0,0,229,238,5,33,0,0,
  	230,235,3,46,23,0,231,232,5,37,0,0,232,234,3,46,23,0,233,231,1,0,0,0,
  	234,237,1,0,0,0,235,233,1,0,0,0,235,236,1,0,0,0,236,239,1,0,0,0,237,235,
  	1,0,0,0,238,230,1,0,0,0,238,239,1,0,0,0,239,240,1,0,0,0,240,241,5,34,
  	0,0,241,45,1,0,0,0,242,245,3,74,37,0,243,245,3,2,1,0,244,242,1,0,0,0,
  	244,243,1,0,0,0,245,47,1,0,0,0,246,255,5,33,0,0,247,252,3,46,23,0,248,
  	249,5,37,0,0,249,251,3,46,23,0,250,248,1,0,0,0,251,254,1,0,0,0,252,250,
  	1,0,0,0,252,253,1,0,0,0,253,256,1,0,0,0,254,252,1,0,0,0,255,247,1,0,0,
  	0,255,256,1,0,0,0,256,257,1,0,0,0,257,258,5,34,0,0,258,49,1,0,0,0,259,
  	260,5,29,0,0,260,275,3,48,24,0,261,264,5,29,0,0,262,265,3,52,26,0,263,
  	265,3,54,27,0,264,262,1,0,0,0,264,263,1,0,0,0,265,266,1,0,0,0,266,267,
  	3,48,24,0,267,275,1,0,0,0,268,269,5,29,0,0,269,272,3,48,24,0,270,273,
  	3,52,26,0,271,273,3,54,27,0,272,270,1,0,0,0,272,271,1,0,0,0,273,275,1,
  	0,0,0,274,259,1,0,0,0,274,261,1,0,0,0,274,268,1,0,0,0,275,51,1,0,0,0,
  	276,277,5,21,0,0,277,278,3,68,34,0,278,53,1,0,0,0,279,280,5,22,0,0,280,
  	281,3,68,34,0,281,55,1,0,0,0,282,285,3,58,29,0,283,285,3,60,30,0,284,
  	282,1,0,0,0,284,283,1,0,0,0,285,288,1,0,0,0,286,289,3,62,31,0,287,289,
  	3,64,32,0,288,286,1,0,0,0,288,287,1,0,0,0,288,289,1,0,0,0,289,57,1,0,
  	0,0,290,291,5,23,0,0,291,292,3,68,34,0,292,59,1,0,0,0,293,294,5,24,0,
  	0,294,295,3,68,34,0,295,61,1,0,0,0,296,298,5,25,0,0,297,299,3,68,34,0,
  	298,297,1,0,0,0,298,299,1,0,0,0,299,63,1,0,0,0,300,302,5,26,0,0,301,303,
  	3,68,34,0,302,301,1,0,0,0,302,303,1,0,0,0,303,65,1,0,0,0,304,308,3,72,
  	36,0,305,308,5,41,0,0,306,308,5,42,0,0,307,304,1,0,0,0,307,305,1,0,0,
  	0,307,306,1,0,0,0,308,67,1,0,0,0,309,318,5,33,0,0,310,315,3,66,33,0,311,
  	312,5,37,0,0,312,314,3,66,33,0,313,311,1,0,0,0,314,317,1,0,0,0,315,313,
  	1,0,0,0,315,316,1,0,0,0,316,319,1,0,0,0,317,315,1,0,0,0,318,310,1,0,0,
  	0,318,319,1,0,0,0,319,320,1,0,0,0,320,321,5,34,0,0,321,69,1,0,0,0,322,
  	325,5,41,0,0,323,325,3,72,36,0,324,322,1,0,0,0,324,323,1,0,0,0,325,71,
  	1,0,0,0,326,327,7,5,0,0,327,73,1,0,0,0,328,329,7,6,0,0,329,75,1,0,0,0,
  	38,84,112,114,121,125,129,133,136,140,144,148,154,160,162,171,182,189,
  	192,198,211,215,226,235,238,244,252,255,264,272,274,284,288,298,302,307,
  	315,318,324
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
    setState(76);
    vectorOperation(0);
    setState(77);
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
    setState(84);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ADD:
      case PromQLParser::SUB: {
        setState(80);
        unaryOp();
        setState(81);
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
        setState(83);
        vector();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(114);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(112);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx)) {
        case 1: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(86);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(87);
          powOp();
          setState(88);
          vectorOperation(8);
          break;
        }

        case 2: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(90);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(91);
          multOp();
          setState(92);
          vectorOperation(7);
          break;
        }

        case 3: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(94);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(95);
          addOp();
          setState(96);
          vectorOperation(6);
          break;
        }

        case 4: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(98);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(99);
          compareOp();
          setState(100);
          vectorOperation(5);
          break;
        }

        case 5: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(102);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(103);
          andUnlessOp();
          setState(104);
          vectorOperation(4);
          break;
        }

        case 6: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(106);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(107);
          orOp();
          setState(108);
          vectorOperation(3);
          break;
        }

        case 7: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(110);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(111);
          subqueryOp();
          break;
        }

        default:
          break;
        } 
      }
      setState(116);
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
    setState(117);
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
    setState(119);
    match(PromQLParser::POW);
    setState(121);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
    case 1: {
      setState(120);
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
    setState(123);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 224) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(125);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      setState(124);
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
    setState(127);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::ADD

    || _la == PromQLParser::SUB)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(129);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(128);
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
    setState(131);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 516096) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(133);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx)) {
    case 1: {
      setState(132);
      match(PromQLParser::BOOL);
      break;
    }

    default:
      break;
    }
    setState(136);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
    case 1: {
      setState(135);
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
    setState(138);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::AND

    || _la == PromQLParser::UNLESS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(140);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
    case 1: {
      setState(139);
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
    setState(142);
    match(PromQLParser::OR);
    setState(144);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(143);
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
    setState(146);
    match(PromQLParser::SUBQUERY_RANGE);
    setState(148);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(147);
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

PromQLParser::TimestampContext* PromQLParser::OffsetOpContext::timestamp() {
  return getRuleContext<PromQLParser::TimestampContext>(0);
}

tree::TerminalNode* PromQLParser::OffsetOpContext::OFFSET() {
  return getToken(PromQLParser::OFFSET, 0);
}

PromQLParser::OffsetValueContext* PromQLParser::OffsetOpContext::offsetValue() {
  return getRuleContext<PromQLParser::OffsetValueContext>(0);
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
        setState(150);
        match(PromQLParser::AT);
        setState(151);
        timestamp();
        setState(154);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
        case 1: {
          setState(152);
          match(PromQLParser::OFFSET);
          setState(153);
          offsetValue();
          break;
        }

        default:
          break;
        }
        break;
      }

      case PromQLParser::OFFSET: {
        enterOuterAlt(_localctx, 2);
        setState(156);
        match(PromQLParser::OFFSET);
        setState(157);
        offsetValue();
        setState(160);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
        case 1: {
          setState(158);
          match(PromQLParser::AT);
          setState(159);
          timestamp();
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

PromQLParser::RangeSelectorContext* PromQLParser::VectorContext::rangeSelector() {
  return getRuleContext<PromQLParser::RangeSelectorContext>(0);
}

PromQLParser::SelectorWithOffsetContext* PromQLParser::VectorContext::selectorWithOffset() {
  return getRuleContext<PromQLParser::SelectorWithOffsetContext>(0);
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
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
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
      rangeSelector();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(168);
      selectorWithOffset();
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

//----------------- TimestampContext ------------------------------------------------------------------

PromQLParser::TimestampContext::TimestampContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::TimestampContext::NUMBER() {
  return getToken(PromQLParser::NUMBER, 0);
}


size_t PromQLParser::TimestampContext::getRuleIndex() const {
  return PromQLParser::RuleTimestamp;
}

void PromQLParser::TimestampContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimestamp(this);
}

void PromQLParser::TimestampContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimestamp(this);
}


std::any PromQLParser::TimestampContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitTimestamp(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::TimestampContext* PromQLParser::timestamp() {
  TimestampContext *_localctx = _tracker.createInstance<TimestampContext>(_ctx, getState());
  enterRule(_localctx, 26, PromQLParser::RuleTimestamp);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(177);
    match(PromQLParser::NUMBER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DurationContext ------------------------------------------------------------------

PromQLParser::DurationContext::DurationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::DurationContext::NUMBER() {
  return getToken(PromQLParser::NUMBER, 0);
}


size_t PromQLParser::DurationContext::getRuleIndex() const {
  return PromQLParser::RuleDuration;
}

void PromQLParser::DurationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDuration(this);
}

void PromQLParser::DurationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDuration(this);
}


std::any PromQLParser::DurationContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitDuration(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::DurationContext* PromQLParser::duration() {
  DurationContext *_localctx = _tracker.createInstance<DurationContext>(_ctx, getState());
  enterRule(_localctx, 28, PromQLParser::RuleDuration);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(179);
    match(PromQLParser::NUMBER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetValueContext ------------------------------------------------------------------

PromQLParser::OffsetValueContext::OffsetValueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* PromQLParser::OffsetValueContext::NUMBER() {
  return getToken(PromQLParser::NUMBER, 0);
}

tree::TerminalNode* PromQLParser::OffsetValueContext::ADD() {
  return getToken(PromQLParser::ADD, 0);
}

tree::TerminalNode* PromQLParser::OffsetValueContext::SUB() {
  return getToken(PromQLParser::SUB, 0);
}


size_t PromQLParser::OffsetValueContext::getRuleIndex() const {
  return PromQLParser::RuleOffsetValue;
}

void PromQLParser::OffsetValueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOffsetValue(this);
}

void PromQLParser::OffsetValueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOffsetValue(this);
}


std::any PromQLParser::OffsetValueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitOffsetValue(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::OffsetValueContext* PromQLParser::offsetValue() {
  OffsetValueContext *_localctx = _tracker.createInstance<OffsetValueContext>(_ctx, getState());
  enterRule(_localctx, 30, PromQLParser::RuleOffsetValue);
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
    setState(182);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PromQLParser::ADD

    || _la == PromQLParser::SUB) {
      setState(181);
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
    setState(184);
    match(PromQLParser::NUMBER);
   
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
  enterRule(_localctx, 32, PromQLParser::RuleInstantSelector);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(198);
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
        setState(186);
        metricName();
        setState(192);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
        case 1: {
          setState(187);
          match(PromQLParser::LEFT_BRACE);
          setState(189);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if ((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & 6599215156736) != 0)) {
            setState(188);
            labelMatcherList();
          }
          setState(191);
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
        setState(194);
        match(PromQLParser::LEFT_BRACE);
        setState(195);
        labelMatcherList();
        setState(196);
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
  enterRule(_localctx, 34, PromQLParser::RuleLabelMatcher);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(200);
    labelName();
    setState(201);
    labelMatcherOperator();
    setState(202);
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
  enterRule(_localctx, 36, PromQLParser::RuleLabelMatcherOperator);
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
    setState(204);
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
  enterRule(_localctx, 38, PromQLParser::RuleLabelMatcherList);
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
    setState(206);
    labelMatcher();
    setState(211);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 19, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(207);
        match(PromQLParser::COMMA);
        setState(208);
        labelMatcher(); 
      }
      setState(213);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 19, _ctx);
    }
    setState(215);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PromQLParser::COMMA) {
      setState(214);
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

//----------------- RangeSelectorContext ------------------------------------------------------------------

PromQLParser::RangeSelectorContext::RangeSelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::InstantSelectorContext* PromQLParser::RangeSelectorContext::instantSelector() {
  return getRuleContext<PromQLParser::InstantSelectorContext>(0);
}

tree::TerminalNode* PromQLParser::RangeSelectorContext::SELECTOR_RANGE() {
  return getToken(PromQLParser::SELECTOR_RANGE, 0);
}


size_t PromQLParser::RangeSelectorContext::getRuleIndex() const {
  return PromQLParser::RuleRangeSelector;
}

void PromQLParser::RangeSelectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRangeSelector(this);
}

void PromQLParser::RangeSelectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRangeSelector(this);
}


std::any PromQLParser::RangeSelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitRangeSelector(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::RangeSelectorContext* PromQLParser::rangeSelector() {
  RangeSelectorContext *_localctx = _tracker.createInstance<RangeSelectorContext>(_ctx, getState());
  enterRule(_localctx, 40, PromQLParser::RuleRangeSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(217);
    instantSelector();
    setState(218);
    match(PromQLParser::SELECTOR_RANGE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectorWithOffsetContext ------------------------------------------------------------------

PromQLParser::SelectorWithOffsetContext::SelectorWithOffsetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::InstantSelectorContext* PromQLParser::SelectorWithOffsetContext::instantSelector() {
  return getRuleContext<PromQLParser::InstantSelectorContext>(0);
}

PromQLParser::OffsetOpContext* PromQLParser::SelectorWithOffsetContext::offsetOp() {
  return getRuleContext<PromQLParser::OffsetOpContext>(0);
}

PromQLParser::RangeSelectorContext* PromQLParser::SelectorWithOffsetContext::rangeSelector() {
  return getRuleContext<PromQLParser::RangeSelectorContext>(0);
}


size_t PromQLParser::SelectorWithOffsetContext::getRuleIndex() const {
  return PromQLParser::RuleSelectorWithOffset;
}

void PromQLParser::SelectorWithOffsetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectorWithOffset(this);
}

void PromQLParser::SelectorWithOffsetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectorWithOffset(this);
}


std::any PromQLParser::SelectorWithOffsetContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitSelectorWithOffset(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::SelectorWithOffsetContext* PromQLParser::selectorWithOffset() {
  SelectorWithOffsetContext *_localctx = _tracker.createInstance<SelectorWithOffsetContext>(_ctx, getState());
  enterRule(_localctx, 42, PromQLParser::RuleSelectorWithOffset);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(226);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(220);
      instantSelector();
      setState(221);
      offsetOp();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(223);
      rangeSelector();
      setState(224);
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
  enterRule(_localctx, 44, PromQLParser::RuleFunction_);
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
    setState(228);
    match(PromQLParser::FUNCTION);
    setState(229);
    match(PromQLParser::LEFT_PAREN);
    setState(238);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2211906063902) != 0)) {
      setState(230);
      parameter();
      setState(235);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(231);
        match(PromQLParser::COMMA);
        setState(232);
        parameter();
        setState(237);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(240);
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
  enterRule(_localctx, 46, PromQLParser::RuleParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(244);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(242);
      literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(243);
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
  enterRule(_localctx, 48, PromQLParser::RuleParameterList);
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
    setState(246);
    match(PromQLParser::LEFT_PAREN);
    setState(255);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2211906063902) != 0)) {
      setState(247);
      parameter();
      setState(252);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(248);
        match(PromQLParser::COMMA);
        setState(249);
        parameter();
        setState(254);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(257);
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
  enterRule(_localctx, 50, PromQLParser::RuleAggregation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(274);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(259);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(260);
      parameterList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(261);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(264);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(262);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(263);
          without();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(266);
      parameterList();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(268);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(269);
      parameterList();
      setState(272);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(270);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(271);
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
  enterRule(_localctx, 52, PromQLParser::RuleBy);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(276);
    match(PromQLParser::BY);
    setState(277);
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
  enterRule(_localctx, 54, PromQLParser::RuleWithout);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(279);
    match(PromQLParser::WITHOUT);
    setState(280);
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
  enterRule(_localctx, 56, PromQLParser::RuleGrouping);

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
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ON: {
        setState(282);
        on_();
        break;
      }

      case PromQLParser::IGNORING: {
        setState(283);
        ignoring();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(288);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
    case 1: {
      setState(286);
      groupLeft();
      break;
    }

    case 2: {
      setState(287);
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
  enterRule(_localctx, 58, PromQLParser::RuleOn_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(290);
    match(PromQLParser::ON);
    setState(291);
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
  enterRule(_localctx, 60, PromQLParser::RuleIgnoring);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(293);
    match(PromQLParser::IGNORING);
    setState(294);
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
  enterRule(_localctx, 62, PromQLParser::RuleGroupLeft);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(296);
    match(PromQLParser::GROUP_LEFT);
    setState(298);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
    case 1: {
      setState(297);
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
  enterRule(_localctx, 64, PromQLParser::RuleGroupRight);

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
    match(PromQLParser::GROUP_RIGHT);
    setState(302);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
    case 1: {
      setState(301);
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
  enterRule(_localctx, 66, PromQLParser::RuleLabelName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(307);
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
        setState(304);
        keyword();
        break;
      }

      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 2);
        setState(305);
        match(PromQLParser::METRIC_NAME);
        break;
      }

      case PromQLParser::LABEL_NAME: {
        enterOuterAlt(_localctx, 3);
        setState(306);
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
  enterRule(_localctx, 68, PromQLParser::RuleLabelNameList);
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
    setState(309);
    match(PromQLParser::LEFT_PAREN);
    setState(318);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 6599215156736) != 0)) {
      setState(310);
      labelName();
      setState(315);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(311);
        match(PromQLParser::COMMA);
        setState(312);
        labelName();
        setState(317);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(320);
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
  enterRule(_localctx, 70, PromQLParser::RuleMetricName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(324);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(322);
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
        setState(323);
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
  enterRule(_localctx, 72, PromQLParser::RuleKeyword);
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
    setState(326);
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
  enterRule(_localctx, 74, PromQLParser::RuleLiteral);
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
    setState(328);
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
    case 0: return precpred(_ctx, 8);
    case 1: return precpred(_ctx, 6);
    case 2: return precpred(_ctx, 5);
    case 3: return precpred(_ctx, 4);
    case 4: return precpred(_ctx, 3);
    case 5: return precpred(_ctx, 2);
    case 6: return precpred(_ctx, 9);

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
