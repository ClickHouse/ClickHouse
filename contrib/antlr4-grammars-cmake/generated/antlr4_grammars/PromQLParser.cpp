
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
      "labelMatcher", "selectorIdentifier", "labelMatcherOperator", "labelMatcherList", 
      "rangeSelector", "selectorWithOffset", "function_", "parameter", "parameterList", 
      "aggregation", "by", "without", "grouping", "on_", "ignoring", "groupLeft", 
      "groupRight", "labelName", "labelNameList", "metricName", "keyword", 
      "literal"
    },
    std::vector<std::string>{
      "", "", "", "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'and'", "'or'", 
      "'unless'", "'atan2'", "'='", "'=='", "'!='", "'>'", "'<'", "'>='", 
      "'<='", "'=~'", "'!~'", "'by'", "'without'", "'on'", "'ignoring'", 
      "'group_left'", "'group_right'", "'offset'", "'bool'", "'start'", 
      "'end'", "", "", "'{'", "'}'", "'('", "')'", "'['", "']'", "','", 
      "'@'"
    },
    std::vector<std::string>{
      "", "NUMBER", "STRING", "ADD", "SUB", "MULT", "DIV", "MOD", "POW", 
      "AND", "OR", "UNLESS", "ATAN2", "EQ", "DEQ", "NE", "GT", "LT", "GE", 
      "LE", "RE", "NRE", "BY", "WITHOUT", "ON", "IGNORING", "GROUP_LEFT", 
      "GROUP_RIGHT", "OFFSET", "BOOL", "START", "END", "AGGREGATION_OPERATOR", 
      "FUNCTION", "LEFT_BRACE", "RIGHT_BRACE", "LEFT_PAREN", "RIGHT_PAREN", 
      "LEFT_BRACKET", "RIGHT_BRACKET", "COMMA", "AT", "SUBQUERY_RANGE", 
      "SELECTOR_RANGE", "METRIC_NAME", "LABEL_NAME", "WS", "SL_COMMENT"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,47,350,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,
  	21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,
  	28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,7,
  	35,2,36,7,36,2,37,7,37,2,38,7,38,1,0,1,0,1,0,1,1,1,1,1,1,1,1,1,1,3,1,
  	87,8,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
  	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,5,1,115,8,1,10,1,12,1,118,9,
  	1,1,2,1,2,1,3,1,3,3,3,124,8,3,1,4,1,4,3,4,128,8,4,1,5,1,5,3,5,132,8,5,
  	1,6,1,6,3,6,136,8,6,1,6,3,6,139,8,6,1,7,1,7,3,7,143,8,7,1,8,1,8,3,8,147,
  	8,8,1,9,1,9,3,9,151,8,9,1,10,1,10,1,10,1,10,3,10,157,8,10,1,10,1,10,1,
  	10,1,10,3,10,163,8,10,3,10,165,8,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,
  	3,11,174,8,11,1,12,1,12,1,12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,1,13,
  	3,13,187,8,13,1,14,1,14,1,15,3,15,192,8,15,1,15,1,15,1,16,1,16,1,16,3,
  	16,199,8,16,1,16,3,16,202,8,16,1,16,1,16,1,16,1,16,3,16,208,8,16,1,17,
  	1,17,1,17,1,17,1,17,3,17,215,8,17,1,18,1,18,3,18,219,8,18,1,19,1,19,1,
  	20,1,20,1,20,5,20,226,8,20,10,20,12,20,229,9,20,1,20,3,20,232,8,20,1,
  	21,1,21,1,21,1,22,1,22,1,22,1,22,1,22,1,22,3,22,243,8,22,1,23,1,23,1,
  	23,1,23,1,23,5,23,250,8,23,10,23,12,23,253,9,23,3,23,255,8,23,1,23,1,
  	23,1,24,1,24,3,24,261,8,24,1,25,1,25,1,25,1,25,5,25,267,8,25,10,25,12,
  	25,270,9,25,3,25,272,8,25,1,25,1,25,1,26,1,26,1,26,1,26,1,26,3,26,281,
  	8,26,1,26,1,26,1,26,1,26,1,26,1,26,3,26,289,8,26,3,26,291,8,26,1,27,1,
  	27,1,27,1,28,1,28,1,28,1,29,1,29,3,29,301,8,29,1,29,1,29,3,29,305,8,29,
  	1,30,1,30,1,30,1,31,1,31,1,31,1,32,1,32,3,32,315,8,32,1,33,1,33,3,33,
  	319,8,33,1,34,1,34,1,34,3,34,324,8,34,1,35,1,35,1,35,1,35,5,35,330,8,
  	35,10,35,12,35,333,9,35,1,35,3,35,336,8,35,3,35,338,8,35,1,35,1,35,1,
  	36,1,36,3,36,344,8,36,1,37,1,37,1,38,1,38,1,38,0,1,2,39,0,2,4,6,8,10,
  	12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,
  	58,60,62,64,66,68,70,72,74,76,0,7,1,0,3,4,2,0,5,7,12,12,1,0,14,19,2,0,
  	9,9,11,11,3,0,13,13,15,15,20,21,2,0,9,12,22,33,1,0,1,2,366,0,78,1,0,0,
  	0,2,86,1,0,0,0,4,119,1,0,0,0,6,121,1,0,0,0,8,125,1,0,0,0,10,129,1,0,0,
  	0,12,133,1,0,0,0,14,140,1,0,0,0,16,144,1,0,0,0,18,148,1,0,0,0,20,164,
  	1,0,0,0,22,173,1,0,0,0,24,175,1,0,0,0,26,186,1,0,0,0,28,188,1,0,0,0,30,
  	191,1,0,0,0,32,207,1,0,0,0,34,214,1,0,0,0,36,218,1,0,0,0,38,220,1,0,0,
  	0,40,222,1,0,0,0,42,233,1,0,0,0,44,242,1,0,0,0,46,244,1,0,0,0,48,260,
  	1,0,0,0,50,262,1,0,0,0,52,290,1,0,0,0,54,292,1,0,0,0,56,295,1,0,0,0,58,
  	300,1,0,0,0,60,306,1,0,0,0,62,309,1,0,0,0,64,312,1,0,0,0,66,316,1,0,0,
  	0,68,323,1,0,0,0,70,325,1,0,0,0,72,343,1,0,0,0,74,345,1,0,0,0,76,347,
  	1,0,0,0,78,79,3,2,1,0,79,80,5,0,0,1,80,1,1,0,0,0,81,82,6,1,-1,0,82,83,
  	3,4,2,0,83,84,3,2,1,7,84,87,1,0,0,0,85,87,3,22,11,0,86,81,1,0,0,0,86,
  	85,1,0,0,0,87,116,1,0,0,0,88,89,10,8,0,0,89,90,3,6,3,0,90,91,3,2,1,8,
  	91,115,1,0,0,0,92,93,10,6,0,0,93,94,3,8,4,0,94,95,3,2,1,7,95,115,1,0,
  	0,0,96,97,10,5,0,0,97,98,3,10,5,0,98,99,3,2,1,6,99,115,1,0,0,0,100,101,
  	10,4,0,0,101,102,3,12,6,0,102,103,3,2,1,5,103,115,1,0,0,0,104,105,10,
  	3,0,0,105,106,3,14,7,0,106,107,3,2,1,4,107,115,1,0,0,0,108,109,10,2,0,
  	0,109,110,3,16,8,0,110,111,3,2,1,3,111,115,1,0,0,0,112,113,10,9,0,0,113,
  	115,3,18,9,0,114,88,1,0,0,0,114,92,1,0,0,0,114,96,1,0,0,0,114,100,1,0,
  	0,0,114,104,1,0,0,0,114,108,1,0,0,0,114,112,1,0,0,0,115,118,1,0,0,0,116,
  	114,1,0,0,0,116,117,1,0,0,0,117,3,1,0,0,0,118,116,1,0,0,0,119,120,7,0,
  	0,0,120,5,1,0,0,0,121,123,5,8,0,0,122,124,3,58,29,0,123,122,1,0,0,0,123,
  	124,1,0,0,0,124,7,1,0,0,0,125,127,7,1,0,0,126,128,3,58,29,0,127,126,1,
  	0,0,0,127,128,1,0,0,0,128,9,1,0,0,0,129,131,7,0,0,0,130,132,3,58,29,0,
  	131,130,1,0,0,0,131,132,1,0,0,0,132,11,1,0,0,0,133,135,7,2,0,0,134,136,
  	5,29,0,0,135,134,1,0,0,0,135,136,1,0,0,0,136,138,1,0,0,0,137,139,3,58,
  	29,0,138,137,1,0,0,0,138,139,1,0,0,0,139,13,1,0,0,0,140,142,7,3,0,0,141,
  	143,3,58,29,0,142,141,1,0,0,0,142,143,1,0,0,0,143,15,1,0,0,0,144,146,
  	5,10,0,0,145,147,3,58,29,0,146,145,1,0,0,0,146,147,1,0,0,0,147,17,1,0,
  	0,0,148,150,5,42,0,0,149,151,3,20,10,0,150,149,1,0,0,0,150,151,1,0,0,
  	0,151,19,1,0,0,0,152,153,5,41,0,0,153,156,3,26,13,0,154,155,5,28,0,0,
  	155,157,3,30,15,0,156,154,1,0,0,0,156,157,1,0,0,0,157,165,1,0,0,0,158,
  	159,5,28,0,0,159,162,3,30,15,0,160,161,5,41,0,0,161,163,3,26,13,0,162,
  	160,1,0,0,0,162,163,1,0,0,0,163,165,1,0,0,0,164,152,1,0,0,0,164,158,1,
  	0,0,0,165,21,1,0,0,0,166,174,3,46,23,0,167,174,3,52,26,0,168,174,3,32,
  	16,0,169,174,3,42,21,0,170,174,3,44,22,0,171,174,3,76,38,0,172,174,3,
  	24,12,0,173,166,1,0,0,0,173,167,1,0,0,0,173,168,1,0,0,0,173,169,1,0,0,
  	0,173,170,1,0,0,0,173,171,1,0,0,0,173,172,1,0,0,0,174,23,1,0,0,0,175,
  	176,5,36,0,0,176,177,3,2,1,0,177,178,5,37,0,0,178,25,1,0,0,0,179,187,
  	5,1,0,0,180,181,5,30,0,0,181,182,5,36,0,0,182,187,5,37,0,0,183,184,5,
  	31,0,0,184,185,5,36,0,0,185,187,5,37,0,0,186,179,1,0,0,0,186,180,1,0,
  	0,0,186,183,1,0,0,0,187,27,1,0,0,0,188,189,5,1,0,0,189,29,1,0,0,0,190,
  	192,7,0,0,0,191,190,1,0,0,0,191,192,1,0,0,0,192,193,1,0,0,0,193,194,5,
  	1,0,0,194,31,1,0,0,0,195,201,3,72,36,0,196,198,5,34,0,0,197,199,3,40,
  	20,0,198,197,1,0,0,0,198,199,1,0,0,0,199,200,1,0,0,0,200,202,5,35,0,0,
  	201,196,1,0,0,0,201,202,1,0,0,0,202,208,1,0,0,0,203,204,5,34,0,0,204,
  	205,3,40,20,0,205,206,5,35,0,0,206,208,1,0,0,0,207,195,1,0,0,0,207,203,
  	1,0,0,0,208,33,1,0,0,0,209,210,3,36,18,0,210,211,3,38,19,0,211,212,5,
  	2,0,0,212,215,1,0,0,0,213,215,5,2,0,0,214,209,1,0,0,0,214,213,1,0,0,0,
  	215,35,1,0,0,0,216,219,3,68,34,0,217,219,5,2,0,0,218,216,1,0,0,0,218,
  	217,1,0,0,0,219,37,1,0,0,0,220,221,7,4,0,0,221,39,1,0,0,0,222,227,3,34,
  	17,0,223,224,5,40,0,0,224,226,3,34,17,0,225,223,1,0,0,0,226,229,1,0,0,
  	0,227,225,1,0,0,0,227,228,1,0,0,0,228,231,1,0,0,0,229,227,1,0,0,0,230,
  	232,5,40,0,0,231,230,1,0,0,0,231,232,1,0,0,0,232,41,1,0,0,0,233,234,3,
  	32,16,0,234,235,5,43,0,0,235,43,1,0,0,0,236,237,3,32,16,0,237,238,3,20,
  	10,0,238,243,1,0,0,0,239,240,3,42,21,0,240,241,3,20,10,0,241,243,1,0,
  	0,0,242,236,1,0,0,0,242,239,1,0,0,0,243,45,1,0,0,0,244,245,5,33,0,0,245,
  	254,5,36,0,0,246,251,3,48,24,0,247,248,5,40,0,0,248,250,3,48,24,0,249,
  	247,1,0,0,0,250,253,1,0,0,0,251,249,1,0,0,0,251,252,1,0,0,0,252,255,1,
  	0,0,0,253,251,1,0,0,0,254,246,1,0,0,0,254,255,1,0,0,0,255,256,1,0,0,0,
  	256,257,5,37,0,0,257,47,1,0,0,0,258,261,3,76,38,0,259,261,3,2,1,0,260,
  	258,1,0,0,0,260,259,1,0,0,0,261,49,1,0,0,0,262,271,5,36,0,0,263,268,3,
  	48,24,0,264,265,5,40,0,0,265,267,3,48,24,0,266,264,1,0,0,0,267,270,1,
  	0,0,0,268,266,1,0,0,0,268,269,1,0,0,0,269,272,1,0,0,0,270,268,1,0,0,0,
  	271,263,1,0,0,0,271,272,1,0,0,0,272,273,1,0,0,0,273,274,5,37,0,0,274,
  	51,1,0,0,0,275,276,5,32,0,0,276,291,3,50,25,0,277,280,5,32,0,0,278,281,
  	3,54,27,0,279,281,3,56,28,0,280,278,1,0,0,0,280,279,1,0,0,0,281,282,1,
  	0,0,0,282,283,3,50,25,0,283,291,1,0,0,0,284,285,5,32,0,0,285,288,3,50,
  	25,0,286,289,3,54,27,0,287,289,3,56,28,0,288,286,1,0,0,0,288,287,1,0,
  	0,0,289,291,1,0,0,0,290,275,1,0,0,0,290,277,1,0,0,0,290,284,1,0,0,0,291,
  	53,1,0,0,0,292,293,5,22,0,0,293,294,3,70,35,0,294,55,1,0,0,0,295,296,
  	5,23,0,0,296,297,3,70,35,0,297,57,1,0,0,0,298,301,3,60,30,0,299,301,3,
  	62,31,0,300,298,1,0,0,0,300,299,1,0,0,0,301,304,1,0,0,0,302,305,3,64,
  	32,0,303,305,3,66,33,0,304,302,1,0,0,0,304,303,1,0,0,0,304,305,1,0,0,
  	0,305,59,1,0,0,0,306,307,5,24,0,0,307,308,3,70,35,0,308,61,1,0,0,0,309,
  	310,5,25,0,0,310,311,3,70,35,0,311,63,1,0,0,0,312,314,5,26,0,0,313,315,
  	3,70,35,0,314,313,1,0,0,0,314,315,1,0,0,0,315,65,1,0,0,0,316,318,5,27,
  	0,0,317,319,3,70,35,0,318,317,1,0,0,0,318,319,1,0,0,0,319,67,1,0,0,0,
  	320,324,3,74,37,0,321,324,5,44,0,0,322,324,5,45,0,0,323,320,1,0,0,0,323,
  	321,1,0,0,0,323,322,1,0,0,0,324,69,1,0,0,0,325,337,5,36,0,0,326,331,3,
  	68,34,0,327,328,5,40,0,0,328,330,3,68,34,0,329,327,1,0,0,0,330,333,1,
  	0,0,0,331,329,1,0,0,0,331,332,1,0,0,0,332,335,1,0,0,0,333,331,1,0,0,0,
  	334,336,5,40,0,0,335,334,1,0,0,0,335,336,1,0,0,0,336,338,1,0,0,0,337,
  	326,1,0,0,0,337,338,1,0,0,0,338,339,1,0,0,0,339,340,5,37,0,0,340,71,1,
  	0,0,0,341,344,5,44,0,0,342,344,3,74,37,0,343,341,1,0,0,0,343,342,1,0,
  	0,0,344,73,1,0,0,0,345,346,7,5,0,0,346,75,1,0,0,0,347,348,7,6,0,0,348,
  	77,1,0,0,0,42,86,114,116,123,127,131,135,138,142,146,150,156,162,164,
  	173,186,191,198,201,207,214,218,227,231,242,251,254,260,268,271,280,288,
  	290,300,304,314,318,323,331,335,337,343
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
    setState(78);
    vectorOperation(0);
    setState(79);
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
    setState(86);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ADD:
      case PromQLParser::SUB: {
        setState(82);
        unaryOp();
        setState(83);
        vectorOperation(7);
        break;
      }

      case PromQLParser::NUMBER:
      case PromQLParser::STRING:
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::ATAN2:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::START:
      case PromQLParser::END:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION:
      case PromQLParser::LEFT_BRACE:
      case PromQLParser::LEFT_PAREN:
      case PromQLParser::METRIC_NAME: {
        setState(85);
        vector();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(116);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(114);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx)) {
        case 1: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(88);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(89);
          powOp();
          setState(90);
          vectorOperation(8);
          break;
        }

        case 2: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(92);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(93);
          multOp();
          setState(94);
          vectorOperation(7);
          break;
        }

        case 3: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(96);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(97);
          addOp();
          setState(98);
          vectorOperation(6);
          break;
        }

        case 4: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(100);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(101);
          compareOp();
          setState(102);
          vectorOperation(5);
          break;
        }

        case 5: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(104);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(105);
          andUnlessOp();
          setState(106);
          vectorOperation(4);
          break;
        }

        case 6: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(108);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(109);
          orOp();
          setState(110);
          vectorOperation(3);
          break;
        }

        case 7: {
          _localctx = _tracker.createInstance<VectorOperationContext>(parentContext, parentState);
          pushNewRecursionContext(_localctx, startState, RuleVectorOperation);
          setState(112);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(113);
          subqueryOp();
          break;
        }

        default:
          break;
        } 
      }
      setState(118);
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
    setState(119);
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
    setState(121);
    match(PromQLParser::POW);
    setState(123);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
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

tree::TerminalNode* PromQLParser::MultOpContext::ATAN2() {
  return getToken(PromQLParser::ATAN2, 0);
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
    setState(125);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 4320) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(127);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx)) {
    case 1: {
      setState(126);
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
    setState(129);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::ADD

    || _la == PromQLParser::SUB)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(131);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(130);
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
    setState(133);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 1032192) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(135);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx)) {
    case 1: {
      setState(134);
      match(PromQLParser::BOOL);
      break;
    }

    default:
      break;
    }
    setState(138);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
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
    setState(140);
    _la = _input->LA(1);
    if (!(_la == PromQLParser::AND

    || _la == PromQLParser::UNLESS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(142);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
    case 1: {
      setState(141);
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
    setState(144);
    match(PromQLParser::OR);
    setState(146);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(145);
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
    setState(148);
    match(PromQLParser::SUBQUERY_RANGE);
    setState(150);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(149);
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
    setState(164);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AT: {
        enterOuterAlt(_localctx, 1);
        setState(152);
        match(PromQLParser::AT);
        setState(153);
        timestamp();
        setState(156);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
        case 1: {
          setState(154);
          match(PromQLParser::OFFSET);
          setState(155);
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
        setState(158);
        match(PromQLParser::OFFSET);
        setState(159);
        offsetValue();
        setState(162);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
        case 1: {
          setState(160);
          match(PromQLParser::AT);
          setState(161);
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
    setState(173);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(166);
      function_();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(167);
      aggregation();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(168);
      instantSelector();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(169);
      rangeSelector();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(170);
      selectorWithOffset();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(171);
      literal();
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(172);
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
    setState(175);
    match(PromQLParser::LEFT_PAREN);
    setState(176);
    vectorOperation(0);
    setState(177);
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

tree::TerminalNode* PromQLParser::TimestampContext::START() {
  return getToken(PromQLParser::START, 0);
}

tree::TerminalNode* PromQLParser::TimestampContext::LEFT_PAREN() {
  return getToken(PromQLParser::LEFT_PAREN, 0);
}

tree::TerminalNode* PromQLParser::TimestampContext::RIGHT_PAREN() {
  return getToken(PromQLParser::RIGHT_PAREN, 0);
}

tree::TerminalNode* PromQLParser::TimestampContext::END() {
  return getToken(PromQLParser::END, 0);
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
    setState(186);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::NUMBER: {
        enterOuterAlt(_localctx, 1);
        setState(179);
        match(PromQLParser::NUMBER);
        break;
      }

      case PromQLParser::START: {
        enterOuterAlt(_localctx, 2);
        setState(180);
        match(PromQLParser::START);
        setState(181);
        match(PromQLParser::LEFT_PAREN);
        setState(182);
        match(PromQLParser::RIGHT_PAREN);
        break;
      }

      case PromQLParser::END: {
        enterOuterAlt(_localctx, 3);
        setState(183);
        match(PromQLParser::END);
        setState(184);
        match(PromQLParser::LEFT_PAREN);
        setState(185);
        match(PromQLParser::RIGHT_PAREN);
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
    setState(188);
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
    setState(191);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PromQLParser::ADD

    || _la == PromQLParser::SUB) {
      setState(190);
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
    setState(193);
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
    setState(207);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::ATAN2:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::START:
      case PromQLParser::END:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION:
      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(195);
        metricName();
        setState(201);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
        case 1: {
          setState(196);
          match(PromQLParser::LEFT_BRACE);
          setState(198);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if ((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & 52793733815812) != 0)) {
            setState(197);
            labelMatcherList();
          }
          setState(200);
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
        setState(203);
        match(PromQLParser::LEFT_BRACE);
        setState(204);
        labelMatcherList();
        setState(205);
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

PromQLParser::SelectorIdentifierContext* PromQLParser::LabelMatcherContext::selectorIdentifier() {
  return getRuleContext<PromQLParser::SelectorIdentifierContext>(0);
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
    setState(214);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(209);
      selectorIdentifier();
      setState(210);
      labelMatcherOperator();
      setState(211);
      match(PromQLParser::STRING);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(213);
      match(PromQLParser::STRING);
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

//----------------- SelectorIdentifierContext ------------------------------------------------------------------

PromQLParser::SelectorIdentifierContext::SelectorIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

PromQLParser::LabelNameContext* PromQLParser::SelectorIdentifierContext::labelName() {
  return getRuleContext<PromQLParser::LabelNameContext>(0);
}

tree::TerminalNode* PromQLParser::SelectorIdentifierContext::STRING() {
  return getToken(PromQLParser::STRING, 0);
}


size_t PromQLParser::SelectorIdentifierContext::getRuleIndex() const {
  return PromQLParser::RuleSelectorIdentifier;
}

void PromQLParser::SelectorIdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectorIdentifier(this);
}

void PromQLParser::SelectorIdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<PromQLParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectorIdentifier(this);
}


std::any PromQLParser::SelectorIdentifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<PromQLParserVisitor*>(visitor))
    return parserVisitor->visitSelectorIdentifier(this);
  else
    return visitor->visitChildren(this);
}

PromQLParser::SelectorIdentifierContext* PromQLParser::selectorIdentifier() {
  SelectorIdentifierContext *_localctx = _tracker.createInstance<SelectorIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 36, PromQLParser::RuleSelectorIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(218);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::ATAN2:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::START:
      case PromQLParser::END:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION:
      case PromQLParser::METRIC_NAME:
      case PromQLParser::LABEL_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(216);
        labelName();
        break;
      }

      case PromQLParser::STRING: {
        enterOuterAlt(_localctx, 2);
        setState(217);
        match(PromQLParser::STRING);
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
  enterRule(_localctx, 38, PromQLParser::RuleLabelMatcherOperator);
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
    setState(220);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 3186688) != 0))) {
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
  enterRule(_localctx, 40, PromQLParser::RuleLabelMatcherList);
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
    setState(222);
    labelMatcher();
    setState(227);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(223);
        match(PromQLParser::COMMA);
        setState(224);
        labelMatcher(); 
      }
      setState(229);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx);
    }
    setState(231);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PromQLParser::COMMA) {
      setState(230);
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
  enterRule(_localctx, 42, PromQLParser::RuleRangeSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(233);
    instantSelector();
    setState(234);
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
  enterRule(_localctx, 44, PromQLParser::RuleSelectorWithOffset);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(242);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(236);
      instantSelector();
      setState(237);
      offsetOp();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(239);
      rangeSelector();
      setState(240);
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
  enterRule(_localctx, 46, PromQLParser::RuleFunction_);
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
    setState(244);
    match(PromQLParser::FUNCTION);
    setState(245);
    match(PromQLParser::LEFT_PAREN);
    setState(254);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 17695261072926) != 0)) {
      setState(246);
      parameter();
      setState(251);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(247);
        match(PromQLParser::COMMA);
        setState(248);
        parameter();
        setState(253);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(256);
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
  enterRule(_localctx, 48, PromQLParser::RuleParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(260);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(258);
      literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(259);
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
  enterRule(_localctx, 50, PromQLParser::RuleParameterList);
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
    setState(262);
    match(PromQLParser::LEFT_PAREN);
    setState(271);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 17695261072926) != 0)) {
      setState(263);
      parameter();
      setState(268);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PromQLParser::COMMA) {
        setState(264);
        match(PromQLParser::COMMA);
        setState(265);
        parameter();
        setState(270);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(273);
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
  enterRule(_localctx, 52, PromQLParser::RuleAggregation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(290);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(275);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(276);
      parameterList();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(277);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(280);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(278);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(279);
          without();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(282);
      parameterList();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(284);
      match(PromQLParser::AGGREGATION_OPERATOR);
      setState(285);
      parameterList();
      setState(288);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PromQLParser::BY: {
          setState(286);
          by();
          break;
        }

        case PromQLParser::WITHOUT: {
          setState(287);
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
  enterRule(_localctx, 54, PromQLParser::RuleBy);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(292);
    match(PromQLParser::BY);
    setState(293);
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
  enterRule(_localctx, 56, PromQLParser::RuleWithout);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(295);
    match(PromQLParser::WITHOUT);
    setState(296);
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
  enterRule(_localctx, 58, PromQLParser::RuleGrouping);

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
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::ON: {
        setState(298);
        on_();
        break;
      }

      case PromQLParser::IGNORING: {
        setState(299);
        ignoring();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(304);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      setState(302);
      groupLeft();
      break;
    }

    case 2: {
      setState(303);
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
  enterRule(_localctx, 60, PromQLParser::RuleOn_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(306);
    match(PromQLParser::ON);
    setState(307);
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
  enterRule(_localctx, 62, PromQLParser::RuleIgnoring);

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
    match(PromQLParser::IGNORING);
    setState(310);
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
  enterRule(_localctx, 64, PromQLParser::RuleGroupLeft);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(312);
    match(PromQLParser::GROUP_LEFT);
    setState(314);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx)) {
    case 1: {
      setState(313);
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
  enterRule(_localctx, 66, PromQLParser::RuleGroupRight);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(316);
    match(PromQLParser::GROUP_RIGHT);
    setState(318);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx)) {
    case 1: {
      setState(317);
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
  enterRule(_localctx, 68, PromQLParser::RuleLabelName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(323);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::ATAN2:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::START:
      case PromQLParser::END:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION: {
        enterOuterAlt(_localctx, 1);
        setState(320);
        keyword();
        break;
      }

      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 2);
        setState(321);
        match(PromQLParser::METRIC_NAME);
        break;
      }

      case PromQLParser::LABEL_NAME: {
        enterOuterAlt(_localctx, 3);
        setState(322);
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
  enterRule(_localctx, 70, PromQLParser::RuleLabelNameList);
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
    setState(325);
    match(PromQLParser::LEFT_PAREN);
    setState(337);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 52793733815808) != 0)) {
      setState(326);
      labelName();
      setState(331);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(327);
          match(PromQLParser::COMMA);
          setState(328);
          labelName(); 
        }
        setState(333);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
      }
      setState(335);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == PromQLParser::COMMA) {
        setState(334);
        match(PromQLParser::COMMA);
      }
    }
    setState(339);
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
  enterRule(_localctx, 72, PromQLParser::RuleMetricName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(343);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PromQLParser::METRIC_NAME: {
        enterOuterAlt(_localctx, 1);
        setState(341);
        match(PromQLParser::METRIC_NAME);
        break;
      }

      case PromQLParser::AND:
      case PromQLParser::OR:
      case PromQLParser::UNLESS:
      case PromQLParser::ATAN2:
      case PromQLParser::BY:
      case PromQLParser::WITHOUT:
      case PromQLParser::ON:
      case PromQLParser::IGNORING:
      case PromQLParser::GROUP_LEFT:
      case PromQLParser::GROUP_RIGHT:
      case PromQLParser::OFFSET:
      case PromQLParser::BOOL:
      case PromQLParser::START:
      case PromQLParser::END:
      case PromQLParser::AGGREGATION_OPERATOR:
      case PromQLParser::FUNCTION: {
        enterOuterAlt(_localctx, 2);
        setState(342);
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

tree::TerminalNode* PromQLParser::KeywordContext::ATAN2() {
  return getToken(PromQLParser::ATAN2, 0);
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

tree::TerminalNode* PromQLParser::KeywordContext::START() {
  return getToken(PromQLParser::START, 0);
}

tree::TerminalNode* PromQLParser::KeywordContext::END() {
  return getToken(PromQLParser::END, 0);
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
  enterRule(_localctx, 74, PromQLParser::RuleKeyword);
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
    setState(345);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 17175682560) != 0))) {
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
  enterRule(_localctx, 76, PromQLParser::RuleLiteral);
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
    setState(347);
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
