
// Generated from SparqlParser.g4 by ANTLR 4.13.2


#include "SparqlParserListener.h"
#include "SparqlParserVisitor.h"

#include "SparqlParser.h"


using namespace antlrcpp;
using namespace antlr4_grammars;

using namespace antlr4;

namespace {

struct SparqlParserStaticData final {
  SparqlParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  SparqlParserStaticData(const SparqlParserStaticData&) = delete;
  SparqlParserStaticData(SparqlParserStaticData&&) = delete;
  SparqlParserStaticData& operator=(const SparqlParserStaticData&) = delete;
  SparqlParserStaticData& operator=(SparqlParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag sparqlparserParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
std::unique_ptr<SparqlParserStaticData> sparqlparserParserStaticData = nullptr;

void sparqlparserParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (sparqlparserParserStaticData != nullptr) {
    return;
  }
#else
  assert(sparqlparserParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<SparqlParserStaticData>(
    std::vector<std::string>{
      "query", "prologue", "baseDecl", "prefixDecl", "selectQuery", "constructQuery", 
      "describeQuery", "askQuery", "datasetClause", "defaultGraphClause", 
      "namedGraphClause", "sourceSelector", "whereClause", "solutionModifier", 
      "limitOffsetClauses", "orderClause", "orderCondition", "limitClause", 
      "offsetClause", "groupGraphPattern", "triplesBlock", "graphPatternNotTriples", 
      "optionalGraphPattern", "graphGraphPattern", "groupOrUnionGraphPattern", 
      "filter_", "constraint", "functionCall", "argList", "constructTemplate", 
      "constructTriples", "triplesSameSubject", "propertyListNotEmpty", 
      "propertyList", "objectList", "object_", "verb", "triplesNode", "blankNodePropertyList", 
      "collection", "graphNode", "varOrTerm", "varOrIRIref", "var_", "graphTerm", 
      "expression", "conditionalOrExpression", "conditionalAndExpression", 
      "valueLogical", "relationalExpression", "numericExpression", "additiveExpression", 
      "multiplicativeExpression", "unaryExpression", "primaryExpression", 
      "brackettedExpression", "builtInCall", "regexExpression", "iriRefOrFunction", 
      "rdfLiteral", "numericLiteral", "numericLiteralUnsigned", "numericLiteralPositive", 
      "numericLiteralNegative", "booleanLiteral", "string_", "iriRef", "prefixedName", 
      "blankNode"
    },
    std::vector<std::string>{
      "", "", "'ASC'", "'ASK'", "'BASE'", "'BOUND'", "'BY'", "'CONSTRUCT'", 
      "'DATATYPE'", "'DESC'", "'DESCRIBE'", "'DISTINCT'", "'FILTER'", "'FROM'", 
      "'GRAPH'", "'LANG'", "'LANGMATCHES'", "'LIMIT'", "'NAMED'", "'OFFSET'", 
      "'OPTIONAL'", "'ORDER'", "'PREFIX'", "'REDUCED'", "'REGEX'", "'SELECT'", 
      "'STR'", "'UNION'", "'WHERE'", "'true'", "'false'", "'isLITERAL'", 
      "'isBLANK'", "'isURI'", "'isIRI'", "'sameTerm'", "','", "'.'", "'&&'", 
      "'||'", "'^^'", "'='", "'!'", "'>'", "'>='", "'<'", "'<='", "'{'", 
      "'('", "'['", "'-'", "'!='", "'+'", "'}'", "')'", "']'", "';'", "'/'", 
      "'*'"
    },
    std::vector<std::string>{
      "", "A", "ASC", "ASK", "BASE", "BOUND", "BY", "CONSTRUCT", "DATATYPE", 
      "DESC", "DESCRIBE", "DISTINCT", "FILTER", "FROM", "GRAPH", "LANG", 
      "LANGMATCHES", "LIMIT", "NAMED", "OFFSET", "OPTIONAL", "ORDER", "PREFIX", 
      "REDUCED", "REGEX", "SELECT", "STR", "UNION", "WHERE", "TRUE", "FALSE", 
      "IS_LITERAL", "IS_BLANK", "IS_URI", "IS_IRI", "SAME_TERM", "COMMA", 
      "DOT", "DOUBLE_AMP", "DOUBLE_BAR", "DOUBLE_CARET", "EQUAL", "EXCLAMATION", 
      "GREATER", "GREATER_OR_EQUAL", "LESS", "LESS_OR_EQUAL", "L_CURLY", 
      "L_PAREN", "L_SQUARE", "MINUS", "NOT_EQUAL", "PLUS", "R_CURLY", "R_PAREN", 
      "R_SQUARE", "SEMICOLON", "SLASH", "STAR", "IRI_REF", "PNAME_NS", "PNAME_LN", 
      "BLANK_NODE_LABEL", "VAR1", "VAR2", "LANGTAG", "INTEGER", "DECIMAL", 
      "DOUBLE", "INTEGER_POSITIVE", "DECIMAL_POSITIVE", "DOUBLE_POSITIVE", 
      "INTEGER_NEGATIVE", "DECIMAL_NEGATIVE", "DOUBLE_NEGATIVE", "EXPONENT", 
      "STRING_LITERAL1", "STRING_LITERAL2", "STRING_LITERAL_LONG1", "STRING_LITERAL_LONG2", 
      "ECHAR", "NIL", "ANON", "PN_CHARS_U", "VARNAME", "PN_PREFIX", "PN_LOCAL", 
      "WS"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,87,603,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,
  	21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,
  	28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,7,
  	35,2,36,7,36,2,37,7,37,2,38,7,38,2,39,7,39,2,40,7,40,2,41,7,41,2,42,7,
  	42,2,43,7,43,2,44,7,44,2,45,7,45,2,46,7,46,2,47,7,47,2,48,7,48,2,49,7,
  	49,2,50,7,50,2,51,7,51,2,52,7,52,2,53,7,53,2,54,7,54,2,55,7,55,2,56,7,
  	56,2,57,7,57,2,58,7,58,2,59,7,59,2,60,7,60,2,61,7,61,2,62,7,62,2,63,7,
  	63,2,64,7,64,2,65,7,65,2,66,7,66,2,67,7,67,2,68,7,68,1,0,1,0,1,0,1,0,
  	1,0,3,0,144,8,0,1,0,1,0,1,1,3,1,149,8,1,1,1,5,1,152,8,1,10,1,12,1,155,
  	9,1,1,2,1,2,1,2,1,3,1,3,1,3,1,3,1,4,1,4,3,4,166,8,4,1,4,4,4,169,8,4,11,
  	4,12,4,170,1,4,3,4,174,8,4,1,4,5,4,177,8,4,10,4,12,4,180,9,4,1,4,1,4,
  	1,4,1,5,1,5,1,5,5,5,188,8,5,10,5,12,5,191,9,5,1,5,1,5,1,5,1,6,1,6,4,6,
  	198,8,6,11,6,12,6,199,1,6,3,6,203,8,6,1,6,5,6,206,8,6,10,6,12,6,209,9,
  	6,1,6,3,6,212,8,6,1,6,1,6,1,7,1,7,5,7,218,8,7,10,7,12,7,221,9,7,1,7,1,
  	7,1,8,1,8,1,8,3,8,228,8,8,1,9,1,9,1,10,1,10,1,10,1,11,1,11,1,12,3,12,
  	238,8,12,1,12,1,12,1,13,3,13,243,8,13,1,13,3,13,246,8,13,1,14,1,14,3,
  	14,250,8,14,1,14,1,14,3,14,254,8,14,3,14,256,8,14,1,15,1,15,1,15,4,15,
  	261,8,15,11,15,12,15,262,1,16,1,16,1,16,1,16,3,16,269,8,16,1,17,1,17,
  	1,17,1,18,1,18,1,18,1,19,1,19,3,19,279,8,19,1,19,1,19,3,19,283,8,19,1,
  	19,3,19,286,8,19,1,19,3,19,289,8,19,5,19,291,8,19,10,19,12,19,294,9,19,
  	1,19,1,19,1,20,1,20,1,20,3,20,301,8,20,3,20,303,8,20,1,21,1,21,1,21,3,
  	21,308,8,21,1,22,1,22,1,22,1,23,1,23,1,23,1,23,1,24,1,24,1,24,5,24,320,
  	8,24,10,24,12,24,323,9,24,1,25,1,25,1,25,1,26,1,26,1,26,3,26,331,8,26,
  	1,27,1,27,1,27,1,28,1,28,1,28,1,28,1,28,5,28,341,8,28,10,28,12,28,344,
  	9,28,1,28,1,28,3,28,348,8,28,1,29,1,29,3,29,352,8,29,1,29,1,29,1,30,1,
  	30,1,30,3,30,359,8,30,3,30,361,8,30,1,31,1,31,1,31,1,31,1,31,1,31,3,31,
  	369,8,31,1,32,1,32,1,32,1,32,1,32,1,32,3,32,377,8,32,5,32,379,8,32,10,
  	32,12,32,382,9,32,1,33,3,33,385,8,33,1,34,1,34,1,34,5,34,390,8,34,10,
  	34,12,34,393,9,34,1,35,1,35,1,36,1,36,3,36,399,8,36,1,37,1,37,3,37,403,
  	8,37,1,38,1,38,1,38,1,38,1,39,1,39,4,39,411,8,39,11,39,12,39,412,1,39,
  	1,39,1,40,1,40,3,40,419,8,40,1,41,1,41,3,41,423,8,41,1,42,1,42,3,42,427,
  	8,42,1,43,1,43,1,44,1,44,1,44,1,44,1,44,1,44,3,44,437,8,44,1,45,1,45,
  	1,46,1,46,1,46,5,46,444,8,46,10,46,12,46,447,9,46,1,47,1,47,1,47,5,47,
  	452,8,47,10,47,12,47,455,9,47,1,48,1,48,1,49,1,49,1,49,3,49,462,8,49,
  	1,50,1,50,1,51,1,51,1,51,1,51,1,51,5,51,471,8,51,10,51,12,51,474,9,51,
  	1,52,1,52,1,52,5,52,479,8,52,10,52,12,52,482,9,52,1,53,3,53,485,8,53,
  	1,53,1,53,1,54,1,54,1,54,1,54,1,54,1,54,1,54,3,54,496,8,54,1,55,1,55,
  	1,55,1,55,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,
  	1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,
  	1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,
  	1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,1,56,
  	1,56,3,56,557,8,56,1,57,1,57,1,57,1,57,1,57,1,57,1,57,3,57,566,8,57,1,
  	57,1,57,1,58,1,58,3,58,572,8,58,1,59,1,59,1,59,1,59,3,59,578,8,59,1,60,
  	1,60,1,60,3,60,583,8,60,1,61,1,61,1,62,1,62,1,63,1,63,1,64,1,64,1,65,
  	1,65,1,66,1,66,3,66,597,8,66,1,67,1,67,1,68,1,68,1,68,0,0,69,0,2,4,6,
  	8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,
  	56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96,98,100,
  	102,104,106,108,110,112,114,116,118,120,122,124,126,128,130,132,134,136,
  	0,14,2,0,11,11,23,23,2,0,2,2,9,9,1,0,63,64,3,0,41,41,43,46,51,51,2,0,
  	50,50,52,52,1,0,57,58,3,0,42,42,50,50,52,52,1,0,66,68,1,0,69,71,1,0,72,
  	74,1,0,29,30,1,0,76,77,1,0,60,61,2,0,62,62,82,82,622,0,138,1,0,0,0,2,
  	148,1,0,0,0,4,156,1,0,0,0,6,159,1,0,0,0,8,163,1,0,0,0,10,184,1,0,0,0,
  	12,195,1,0,0,0,14,215,1,0,0,0,16,224,1,0,0,0,18,229,1,0,0,0,20,231,1,
  	0,0,0,22,234,1,0,0,0,24,237,1,0,0,0,26,242,1,0,0,0,28,255,1,0,0,0,30,
  	257,1,0,0,0,32,268,1,0,0,0,34,270,1,0,0,0,36,273,1,0,0,0,38,276,1,0,0,
  	0,40,297,1,0,0,0,42,307,1,0,0,0,44,309,1,0,0,0,46,312,1,0,0,0,48,316,
  	1,0,0,0,50,324,1,0,0,0,52,330,1,0,0,0,54,332,1,0,0,0,56,347,1,0,0,0,58,
  	349,1,0,0,0,60,355,1,0,0,0,62,368,1,0,0,0,64,370,1,0,0,0,66,384,1,0,0,
  	0,68,386,1,0,0,0,70,394,1,0,0,0,72,398,1,0,0,0,74,402,1,0,0,0,76,404,
  	1,0,0,0,78,408,1,0,0,0,80,418,1,0,0,0,82,422,1,0,0,0,84,426,1,0,0,0,86,
  	428,1,0,0,0,88,436,1,0,0,0,90,438,1,0,0,0,92,440,1,0,0,0,94,448,1,0,0,
  	0,96,456,1,0,0,0,98,458,1,0,0,0,100,463,1,0,0,0,102,465,1,0,0,0,104,475,
  	1,0,0,0,106,484,1,0,0,0,108,495,1,0,0,0,110,497,1,0,0,0,112,556,1,0,0,
  	0,114,558,1,0,0,0,116,569,1,0,0,0,118,573,1,0,0,0,120,582,1,0,0,0,122,
  	584,1,0,0,0,124,586,1,0,0,0,126,588,1,0,0,0,128,590,1,0,0,0,130,592,1,
  	0,0,0,132,596,1,0,0,0,134,598,1,0,0,0,136,600,1,0,0,0,138,143,3,2,1,0,
  	139,144,3,8,4,0,140,144,3,10,5,0,141,144,3,12,6,0,142,144,3,14,7,0,143,
  	139,1,0,0,0,143,140,1,0,0,0,143,141,1,0,0,0,143,142,1,0,0,0,144,145,1,
  	0,0,0,145,146,5,0,0,1,146,1,1,0,0,0,147,149,3,4,2,0,148,147,1,0,0,0,148,
  	149,1,0,0,0,149,153,1,0,0,0,150,152,3,6,3,0,151,150,1,0,0,0,152,155,1,
  	0,0,0,153,151,1,0,0,0,153,154,1,0,0,0,154,3,1,0,0,0,155,153,1,0,0,0,156,
  	157,5,4,0,0,157,158,5,59,0,0,158,5,1,0,0,0,159,160,5,22,0,0,160,161,5,
  	60,0,0,161,162,5,59,0,0,162,7,1,0,0,0,163,165,5,25,0,0,164,166,7,0,0,
  	0,165,164,1,0,0,0,165,166,1,0,0,0,166,173,1,0,0,0,167,169,3,86,43,0,168,
  	167,1,0,0,0,169,170,1,0,0,0,170,168,1,0,0,0,170,171,1,0,0,0,171,174,1,
  	0,0,0,172,174,5,58,0,0,173,168,1,0,0,0,173,172,1,0,0,0,174,178,1,0,0,
  	0,175,177,3,16,8,0,176,175,1,0,0,0,177,180,1,0,0,0,178,176,1,0,0,0,178,
  	179,1,0,0,0,179,181,1,0,0,0,180,178,1,0,0,0,181,182,3,24,12,0,182,183,
  	3,26,13,0,183,9,1,0,0,0,184,185,5,7,0,0,185,189,3,58,29,0,186,188,3,16,
  	8,0,187,186,1,0,0,0,188,191,1,0,0,0,189,187,1,0,0,0,189,190,1,0,0,0,190,
  	192,1,0,0,0,191,189,1,0,0,0,192,193,3,24,12,0,193,194,3,26,13,0,194,11,
  	1,0,0,0,195,202,5,10,0,0,196,198,3,84,42,0,197,196,1,0,0,0,198,199,1,
  	0,0,0,199,197,1,0,0,0,199,200,1,0,0,0,200,203,1,0,0,0,201,203,5,58,0,
  	0,202,197,1,0,0,0,202,201,1,0,0,0,203,207,1,0,0,0,204,206,3,16,8,0,205,
  	204,1,0,0,0,206,209,1,0,0,0,207,205,1,0,0,0,207,208,1,0,0,0,208,211,1,
  	0,0,0,209,207,1,0,0,0,210,212,3,24,12,0,211,210,1,0,0,0,211,212,1,0,0,
  	0,212,213,1,0,0,0,213,214,3,26,13,0,214,13,1,0,0,0,215,219,5,3,0,0,216,
  	218,3,16,8,0,217,216,1,0,0,0,218,221,1,0,0,0,219,217,1,0,0,0,219,220,
  	1,0,0,0,220,222,1,0,0,0,221,219,1,0,0,0,222,223,3,24,12,0,223,15,1,0,
  	0,0,224,227,5,13,0,0,225,228,3,18,9,0,226,228,3,20,10,0,227,225,1,0,0,
  	0,227,226,1,0,0,0,228,17,1,0,0,0,229,230,3,22,11,0,230,19,1,0,0,0,231,
  	232,5,18,0,0,232,233,3,22,11,0,233,21,1,0,0,0,234,235,3,132,66,0,235,
  	23,1,0,0,0,236,238,5,28,0,0,237,236,1,0,0,0,237,238,1,0,0,0,238,239,1,
  	0,0,0,239,240,3,38,19,0,240,25,1,0,0,0,241,243,3,30,15,0,242,241,1,0,
  	0,0,242,243,1,0,0,0,243,245,1,0,0,0,244,246,3,28,14,0,245,244,1,0,0,0,
  	245,246,1,0,0,0,246,27,1,0,0,0,247,249,3,34,17,0,248,250,3,36,18,0,249,
  	248,1,0,0,0,249,250,1,0,0,0,250,256,1,0,0,0,251,253,3,36,18,0,252,254,
  	3,34,17,0,253,252,1,0,0,0,253,254,1,0,0,0,254,256,1,0,0,0,255,247,1,0,
  	0,0,255,251,1,0,0,0,256,29,1,0,0,0,257,258,5,21,0,0,258,260,5,6,0,0,259,
  	261,3,32,16,0,260,259,1,0,0,0,261,262,1,0,0,0,262,260,1,0,0,0,262,263,
  	1,0,0,0,263,31,1,0,0,0,264,265,7,1,0,0,265,269,3,110,55,0,266,269,3,52,
  	26,0,267,269,3,86,43,0,268,264,1,0,0,0,268,266,1,0,0,0,268,267,1,0,0,
  	0,269,33,1,0,0,0,270,271,5,17,0,0,271,272,5,66,0,0,272,35,1,0,0,0,273,
  	274,5,19,0,0,274,275,5,66,0,0,275,37,1,0,0,0,276,278,5,47,0,0,277,279,
  	3,40,20,0,278,277,1,0,0,0,278,279,1,0,0,0,279,292,1,0,0,0,280,283,3,42,
  	21,0,281,283,3,50,25,0,282,280,1,0,0,0,282,281,1,0,0,0,283,285,1,0,0,
  	0,284,286,5,37,0,0,285,284,1,0,0,0,285,286,1,0,0,0,286,288,1,0,0,0,287,
  	289,3,40,20,0,288,287,1,0,0,0,288,289,1,0,0,0,289,291,1,0,0,0,290,282,
  	1,0,0,0,291,294,1,0,0,0,292,290,1,0,0,0,292,293,1,0,0,0,293,295,1,0,0,
  	0,294,292,1,0,0,0,295,296,5,53,0,0,296,39,1,0,0,0,297,302,3,62,31,0,298,
  	300,5,37,0,0,299,301,3,40,20,0,300,299,1,0,0,0,300,301,1,0,0,0,301,303,
  	1,0,0,0,302,298,1,0,0,0,302,303,1,0,0,0,303,41,1,0,0,0,304,308,3,44,22,
  	0,305,308,3,48,24,0,306,308,3,46,23,0,307,304,1,0,0,0,307,305,1,0,0,0,
  	307,306,1,0,0,0,308,43,1,0,0,0,309,310,5,20,0,0,310,311,3,38,19,0,311,
  	45,1,0,0,0,312,313,5,14,0,0,313,314,3,84,42,0,314,315,3,38,19,0,315,47,
  	1,0,0,0,316,321,3,38,19,0,317,318,5,27,0,0,318,320,3,38,19,0,319,317,
  	1,0,0,0,320,323,1,0,0,0,321,319,1,0,0,0,321,322,1,0,0,0,322,49,1,0,0,
  	0,323,321,1,0,0,0,324,325,5,12,0,0,325,326,3,52,26,0,326,51,1,0,0,0,327,
  	331,3,110,55,0,328,331,3,112,56,0,329,331,3,54,27,0,330,327,1,0,0,0,330,
  	328,1,0,0,0,330,329,1,0,0,0,331,53,1,0,0,0,332,333,3,132,66,0,333,334,
  	3,56,28,0,334,55,1,0,0,0,335,348,5,81,0,0,336,337,5,48,0,0,337,342,3,
  	90,45,0,338,339,5,36,0,0,339,341,3,90,45,0,340,338,1,0,0,0,341,344,1,
  	0,0,0,342,340,1,0,0,0,342,343,1,0,0,0,343,345,1,0,0,0,344,342,1,0,0,0,
  	345,346,5,54,0,0,346,348,1,0,0,0,347,335,1,0,0,0,347,336,1,0,0,0,348,
  	57,1,0,0,0,349,351,5,47,0,0,350,352,3,60,30,0,351,350,1,0,0,0,351,352,
  	1,0,0,0,352,353,1,0,0,0,353,354,5,53,0,0,354,59,1,0,0,0,355,360,3,62,
  	31,0,356,358,5,37,0,0,357,359,3,60,30,0,358,357,1,0,0,0,358,359,1,0,0,
  	0,359,361,1,0,0,0,360,356,1,0,0,0,360,361,1,0,0,0,361,61,1,0,0,0,362,
  	363,3,82,41,0,363,364,3,64,32,0,364,369,1,0,0,0,365,366,3,74,37,0,366,
  	367,3,66,33,0,367,369,1,0,0,0,368,362,1,0,0,0,368,365,1,0,0,0,369,63,
  	1,0,0,0,370,371,3,72,36,0,371,380,3,68,34,0,372,376,5,56,0,0,373,374,
  	3,72,36,0,374,375,3,68,34,0,375,377,1,0,0,0,376,373,1,0,0,0,376,377,1,
  	0,0,0,377,379,1,0,0,0,378,372,1,0,0,0,379,382,1,0,0,0,380,378,1,0,0,0,
  	380,381,1,0,0,0,381,65,1,0,0,0,382,380,1,0,0,0,383,385,3,64,32,0,384,
  	383,1,0,0,0,384,385,1,0,0,0,385,67,1,0,0,0,386,391,3,70,35,0,387,388,
  	5,36,0,0,388,390,3,70,35,0,389,387,1,0,0,0,390,393,1,0,0,0,391,389,1,
  	0,0,0,391,392,1,0,0,0,392,69,1,0,0,0,393,391,1,0,0,0,394,395,3,80,40,
  	0,395,71,1,0,0,0,396,399,3,84,42,0,397,399,5,1,0,0,398,396,1,0,0,0,398,
  	397,1,0,0,0,399,73,1,0,0,0,400,403,3,78,39,0,401,403,3,76,38,0,402,400,
  	1,0,0,0,402,401,1,0,0,0,403,75,1,0,0,0,404,405,5,49,0,0,405,406,3,64,
  	32,0,406,407,5,55,0,0,407,77,1,0,0,0,408,410,5,48,0,0,409,411,3,80,40,
  	0,410,409,1,0,0,0,411,412,1,0,0,0,412,410,1,0,0,0,412,413,1,0,0,0,413,
  	414,1,0,0,0,414,415,5,54,0,0,415,79,1,0,0,0,416,419,3,82,41,0,417,419,
  	3,74,37,0,418,416,1,0,0,0,418,417,1,0,0,0,419,81,1,0,0,0,420,423,3,86,
  	43,0,421,423,3,88,44,0,422,420,1,0,0,0,422,421,1,0,0,0,423,83,1,0,0,0,
  	424,427,3,86,43,0,425,427,3,132,66,0,426,424,1,0,0,0,426,425,1,0,0,0,
  	427,85,1,0,0,0,428,429,7,2,0,0,429,87,1,0,0,0,430,437,3,132,66,0,431,
  	437,3,118,59,0,432,437,3,120,60,0,433,437,3,128,64,0,434,437,3,136,68,
  	0,435,437,5,81,0,0,436,430,1,0,0,0,436,431,1,0,0,0,436,432,1,0,0,0,436,
  	433,1,0,0,0,436,434,1,0,0,0,436,435,1,0,0,0,437,89,1,0,0,0,438,439,3,
  	92,46,0,439,91,1,0,0,0,440,445,3,94,47,0,441,442,5,39,0,0,442,444,3,94,
  	47,0,443,441,1,0,0,0,444,447,1,0,0,0,445,443,1,0,0,0,445,446,1,0,0,0,
  	446,93,1,0,0,0,447,445,1,0,0,0,448,453,3,96,48,0,449,450,5,38,0,0,450,
  	452,3,96,48,0,451,449,1,0,0,0,452,455,1,0,0,0,453,451,1,0,0,0,453,454,
  	1,0,0,0,454,95,1,0,0,0,455,453,1,0,0,0,456,457,3,98,49,0,457,97,1,0,0,
  	0,458,461,3,100,50,0,459,460,7,3,0,0,460,462,3,100,50,0,461,459,1,0,0,
  	0,461,462,1,0,0,0,462,99,1,0,0,0,463,464,3,102,51,0,464,101,1,0,0,0,465,
  	472,3,104,52,0,466,467,7,4,0,0,467,471,3,104,52,0,468,471,3,124,62,0,
  	469,471,3,126,63,0,470,466,1,0,0,0,470,468,1,0,0,0,470,469,1,0,0,0,471,
  	474,1,0,0,0,472,470,1,0,0,0,472,473,1,0,0,0,473,103,1,0,0,0,474,472,1,
  	0,0,0,475,480,3,106,53,0,476,477,7,5,0,0,477,479,3,106,53,0,478,476,1,
  	0,0,0,479,482,1,0,0,0,480,478,1,0,0,0,480,481,1,0,0,0,481,105,1,0,0,0,
  	482,480,1,0,0,0,483,485,7,6,0,0,484,483,1,0,0,0,484,485,1,0,0,0,485,486,
  	1,0,0,0,486,487,3,108,54,0,487,107,1,0,0,0,488,496,3,110,55,0,489,496,
  	3,112,56,0,490,496,3,116,58,0,491,496,3,118,59,0,492,496,3,120,60,0,493,
  	496,3,128,64,0,494,496,3,86,43,0,495,488,1,0,0,0,495,489,1,0,0,0,495,
  	490,1,0,0,0,495,491,1,0,0,0,495,492,1,0,0,0,495,493,1,0,0,0,495,494,1,
  	0,0,0,496,109,1,0,0,0,497,498,5,48,0,0,498,499,3,90,45,0,499,500,5,54,
  	0,0,500,111,1,0,0,0,501,502,5,26,0,0,502,503,5,48,0,0,503,504,3,90,45,
  	0,504,505,5,54,0,0,505,557,1,0,0,0,506,507,5,15,0,0,507,508,5,48,0,0,
  	508,509,3,90,45,0,509,510,5,54,0,0,510,557,1,0,0,0,511,512,5,16,0,0,512,
  	513,5,48,0,0,513,514,3,90,45,0,514,515,5,36,0,0,515,516,3,90,45,0,516,
  	517,5,54,0,0,517,557,1,0,0,0,518,519,5,8,0,0,519,520,5,48,0,0,520,521,
  	3,90,45,0,521,522,5,54,0,0,522,557,1,0,0,0,523,524,5,5,0,0,524,525,5,
  	48,0,0,525,526,3,86,43,0,526,527,5,54,0,0,527,557,1,0,0,0,528,529,5,35,
  	0,0,529,530,5,48,0,0,530,531,3,90,45,0,531,532,5,36,0,0,532,533,3,90,
  	45,0,533,534,5,54,0,0,534,557,1,0,0,0,535,536,5,34,0,0,536,537,5,48,0,
  	0,537,538,3,90,45,0,538,539,5,54,0,0,539,557,1,0,0,0,540,541,5,33,0,0,
  	541,542,5,48,0,0,542,543,3,90,45,0,543,544,5,54,0,0,544,557,1,0,0,0,545,
  	546,5,32,0,0,546,547,5,48,0,0,547,548,3,90,45,0,548,549,5,54,0,0,549,
  	557,1,0,0,0,550,551,5,31,0,0,551,552,5,48,0,0,552,553,3,90,45,0,553,554,
  	5,54,0,0,554,557,1,0,0,0,555,557,3,114,57,0,556,501,1,0,0,0,556,506,1,
  	0,0,0,556,511,1,0,0,0,556,518,1,0,0,0,556,523,1,0,0,0,556,528,1,0,0,0,
  	556,535,1,0,0,0,556,540,1,0,0,0,556,545,1,0,0,0,556,550,1,0,0,0,556,555,
  	1,0,0,0,557,113,1,0,0,0,558,559,5,24,0,0,559,560,5,48,0,0,560,561,3,90,
  	45,0,561,562,5,36,0,0,562,565,3,90,45,0,563,564,5,36,0,0,564,566,3,90,
  	45,0,565,563,1,0,0,0,565,566,1,0,0,0,566,567,1,0,0,0,567,568,5,54,0,0,
  	568,115,1,0,0,0,569,571,3,132,66,0,570,572,3,56,28,0,571,570,1,0,0,0,
  	571,572,1,0,0,0,572,117,1,0,0,0,573,577,3,130,65,0,574,578,5,65,0,0,575,
  	576,5,40,0,0,576,578,3,132,66,0,577,574,1,0,0,0,577,575,1,0,0,0,577,578,
  	1,0,0,0,578,119,1,0,0,0,579,583,3,122,61,0,580,583,3,124,62,0,581,583,
  	3,126,63,0,582,579,1,0,0,0,582,580,1,0,0,0,582,581,1,0,0,0,583,121,1,
  	0,0,0,584,585,7,7,0,0,585,123,1,0,0,0,586,587,7,8,0,0,587,125,1,0,0,0,
  	588,589,7,9,0,0,589,127,1,0,0,0,590,591,7,10,0,0,591,129,1,0,0,0,592,
  	593,7,11,0,0,593,131,1,0,0,0,594,597,5,59,0,0,595,597,3,134,67,0,596,
  	594,1,0,0,0,596,595,1,0,0,0,597,133,1,0,0,0,598,599,7,12,0,0,599,135,
  	1,0,0,0,600,601,7,13,0,0,601,137,1,0,0,0,63,143,148,153,165,170,173,178,
  	189,199,202,207,211,219,227,237,242,245,249,253,255,262,268,278,282,285,
  	288,292,300,302,307,321,330,342,347,351,358,360,368,376,380,384,391,398,
  	402,412,418,422,426,436,445,453,461,470,472,480,484,495,556,565,571,577,
  	582,596
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  sparqlparserParserStaticData = std::move(staticData);
}

}

SparqlParser::SparqlParser(TokenStream *input) : SparqlParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

SparqlParser::SparqlParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  SparqlParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *sparqlparserParserStaticData->atn, sparqlparserParserStaticData->decisionToDFA, sparqlparserParserStaticData->sharedContextCache, options);
}

SparqlParser::~SparqlParser() {
  delete _interpreter;
}

const atn::ATN& SparqlParser::getATN() const {
  return *sparqlparserParserStaticData->atn;
}

std::string SparqlParser::getGrammarFileName() const {
  return "SparqlParser.g4";
}

const std::vector<std::string>& SparqlParser::getRuleNames() const {
  return sparqlparserParserStaticData->ruleNames;
}

const dfa::Vocabulary& SparqlParser::getVocabulary() const {
  return sparqlparserParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView SparqlParser::getSerializedATN() const {
  return sparqlparserParserStaticData->serializedATN;
}


//----------------- QueryContext ------------------------------------------------------------------

SparqlParser::QueryContext::QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PrologueContext* SparqlParser::QueryContext::prologue() {
  return getRuleContext<SparqlParser::PrologueContext>(0);
}

tree::TerminalNode* SparqlParser::QueryContext::EOF() {
  return getToken(SparqlParser::EOF, 0);
}

SparqlParser::SelectQueryContext* SparqlParser::QueryContext::selectQuery() {
  return getRuleContext<SparqlParser::SelectQueryContext>(0);
}

SparqlParser::ConstructQueryContext* SparqlParser::QueryContext::constructQuery() {
  return getRuleContext<SparqlParser::ConstructQueryContext>(0);
}

SparqlParser::DescribeQueryContext* SparqlParser::QueryContext::describeQuery() {
  return getRuleContext<SparqlParser::DescribeQueryContext>(0);
}

SparqlParser::AskQueryContext* SparqlParser::QueryContext::askQuery() {
  return getRuleContext<SparqlParser::AskQueryContext>(0);
}


size_t SparqlParser::QueryContext::getRuleIndex() const {
  return SparqlParser::RuleQuery;
}

void SparqlParser::QueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuery(this);
}

void SparqlParser::QueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuery(this);
}


std::any SparqlParser::QueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::QueryContext* SparqlParser::query() {
  QueryContext *_localctx = _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 0, SparqlParser::RuleQuery);

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
    prologue();
    setState(143);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::SELECT: {
        setState(139);
        selectQuery();
        break;
      }

      case SparqlParser::CONSTRUCT: {
        setState(140);
        constructQuery();
        break;
      }

      case SparqlParser::DESCRIBE: {
        setState(141);
        describeQuery();
        break;
      }

      case SparqlParser::ASK: {
        setState(142);
        askQuery();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(145);
    match(SparqlParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrologueContext ------------------------------------------------------------------

SparqlParser::PrologueContext::PrologueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BaseDeclContext* SparqlParser::PrologueContext::baseDecl() {
  return getRuleContext<SparqlParser::BaseDeclContext>(0);
}

std::vector<SparqlParser::PrefixDeclContext *> SparqlParser::PrologueContext::prefixDecl() {
  return getRuleContexts<SparqlParser::PrefixDeclContext>();
}

SparqlParser::PrefixDeclContext* SparqlParser::PrologueContext::prefixDecl(size_t i) {
  return getRuleContext<SparqlParser::PrefixDeclContext>(i);
}


size_t SparqlParser::PrologueContext::getRuleIndex() const {
  return SparqlParser::RulePrologue;
}

void SparqlParser::PrologueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrologue(this);
}

void SparqlParser::PrologueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrologue(this);
}


std::any SparqlParser::PrologueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPrologue(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrologueContext* SparqlParser::prologue() {
  PrologueContext *_localctx = _tracker.createInstance<PrologueContext>(_ctx, getState());
  enterRule(_localctx, 2, SparqlParser::RulePrologue);
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
    setState(148);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::BASE) {
      setState(147);
      baseDecl();
    }
    setState(153);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::PREFIX) {
      setState(150);
      prefixDecl();
      setState(155);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BaseDeclContext ------------------------------------------------------------------

SparqlParser::BaseDeclContext::BaseDeclContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BaseDeclContext::BASE() {
  return getToken(SparqlParser::BASE, 0);
}

tree::TerminalNode* SparqlParser::BaseDeclContext::IRI_REF() {
  return getToken(SparqlParser::IRI_REF, 0);
}


size_t SparqlParser::BaseDeclContext::getRuleIndex() const {
  return SparqlParser::RuleBaseDecl;
}

void SparqlParser::BaseDeclContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBaseDecl(this);
}

void SparqlParser::BaseDeclContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBaseDecl(this);
}


std::any SparqlParser::BaseDeclContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBaseDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BaseDeclContext* SparqlParser::baseDecl() {
  BaseDeclContext *_localctx = _tracker.createInstance<BaseDeclContext>(_ctx, getState());
  enterRule(_localctx, 4, SparqlParser::RuleBaseDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(156);
    match(SparqlParser::BASE);
    setState(157);
    match(SparqlParser::IRI_REF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixDeclContext ------------------------------------------------------------------

SparqlParser::PrefixDeclContext::PrefixDeclContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::PrefixDeclContext::PREFIX() {
  return getToken(SparqlParser::PREFIX, 0);
}

tree::TerminalNode* SparqlParser::PrefixDeclContext::PNAME_NS() {
  return getToken(SparqlParser::PNAME_NS, 0);
}

tree::TerminalNode* SparqlParser::PrefixDeclContext::IRI_REF() {
  return getToken(SparqlParser::IRI_REF, 0);
}


size_t SparqlParser::PrefixDeclContext::getRuleIndex() const {
  return SparqlParser::RulePrefixDecl;
}

void SparqlParser::PrefixDeclContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrefixDecl(this);
}

void SparqlParser::PrefixDeclContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrefixDecl(this);
}


std::any SparqlParser::PrefixDeclContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPrefixDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrefixDeclContext* SparqlParser::prefixDecl() {
  PrefixDeclContext *_localctx = _tracker.createInstance<PrefixDeclContext>(_ctx, getState());
  enterRule(_localctx, 6, SparqlParser::RulePrefixDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(159);
    match(SparqlParser::PREFIX);
    setState(160);
    match(SparqlParser::PNAME_NS);
    setState(161);
    match(SparqlParser::IRI_REF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectQueryContext ------------------------------------------------------------------

SparqlParser::SelectQueryContext::SelectQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::SelectQueryContext::SELECT() {
  return getToken(SparqlParser::SELECT, 0);
}

SparqlParser::WhereClauseContext* SparqlParser::SelectQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::SelectQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

tree::TerminalNode* SparqlParser::SelectQueryContext::STAR() {
  return getToken(SparqlParser::STAR, 0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::SelectQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::SelectQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}

tree::TerminalNode* SparqlParser::SelectQueryContext::DISTINCT() {
  return getToken(SparqlParser::DISTINCT, 0);
}

tree::TerminalNode* SparqlParser::SelectQueryContext::REDUCED() {
  return getToken(SparqlParser::REDUCED, 0);
}

std::vector<SparqlParser::Var_Context *> SparqlParser::SelectQueryContext::var_() {
  return getRuleContexts<SparqlParser::Var_Context>();
}

SparqlParser::Var_Context* SparqlParser::SelectQueryContext::var_(size_t i) {
  return getRuleContext<SparqlParser::Var_Context>(i);
}


size_t SparqlParser::SelectQueryContext::getRuleIndex() const {
  return SparqlParser::RuleSelectQuery;
}

void SparqlParser::SelectQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectQuery(this);
}

void SparqlParser::SelectQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectQuery(this);
}


std::any SparqlParser::SelectQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitSelectQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SelectQueryContext* SparqlParser::selectQuery() {
  SelectQueryContext *_localctx = _tracker.createInstance<SelectQueryContext>(_ctx, getState());
  enterRule(_localctx, 8, SparqlParser::RuleSelectQuery);
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
    setState(163);
    match(SparqlParser::SELECT);
    setState(165);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::DISTINCT

    || _la == SparqlParser::REDUCED) {
      setState(164);
      _la = _input->LA(1);
      if (!(_la == SparqlParser::DISTINCT

      || _la == SparqlParser::REDUCED)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(173);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        setState(168); 
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(167);
          var_();
          setState(170); 
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == SparqlParser::VAR1

        || _la == SparqlParser::VAR2);
        break;
      }

      case SparqlParser::STAR: {
        setState(172);
        match(SparqlParser::STAR);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(178);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(175);
      datasetClause();
      setState(180);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(181);
    whereClause();
    setState(182);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructQueryContext ------------------------------------------------------------------

SparqlParser::ConstructQueryContext::ConstructQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ConstructQueryContext::CONSTRUCT() {
  return getToken(SparqlParser::CONSTRUCT, 0);
}

SparqlParser::ConstructTemplateContext* SparqlParser::ConstructQueryContext::constructTemplate() {
  return getRuleContext<SparqlParser::ConstructTemplateContext>(0);
}

SparqlParser::WhereClauseContext* SparqlParser::ConstructQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::ConstructQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::ConstructQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::ConstructQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}


size_t SparqlParser::ConstructQueryContext::getRuleIndex() const {
  return SparqlParser::RuleConstructQuery;
}

void SparqlParser::ConstructQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructQuery(this);
}

void SparqlParser::ConstructQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructQuery(this);
}


std::any SparqlParser::ConstructQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConstructQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructQueryContext* SparqlParser::constructQuery() {
  ConstructQueryContext *_localctx = _tracker.createInstance<ConstructQueryContext>(_ctx, getState());
  enterRule(_localctx, 10, SparqlParser::RuleConstructQuery);
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
    setState(184);
    match(SparqlParser::CONSTRUCT);
    setState(185);
    constructTemplate();
    setState(189);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(186);
      datasetClause();
      setState(191);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(192);
    whereClause();
    setState(193);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DescribeQueryContext ------------------------------------------------------------------

SparqlParser::DescribeQueryContext::DescribeQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::DescribeQueryContext::DESCRIBE() {
  return getToken(SparqlParser::DESCRIBE, 0);
}

SparqlParser::SolutionModifierContext* SparqlParser::DescribeQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

tree::TerminalNode* SparqlParser::DescribeQueryContext::STAR() {
  return getToken(SparqlParser::STAR, 0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::DescribeQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::DescribeQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}

SparqlParser::WhereClauseContext* SparqlParser::DescribeQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

std::vector<SparqlParser::VarOrIRIrefContext *> SparqlParser::DescribeQueryContext::varOrIRIref() {
  return getRuleContexts<SparqlParser::VarOrIRIrefContext>();
}

SparqlParser::VarOrIRIrefContext* SparqlParser::DescribeQueryContext::varOrIRIref(size_t i) {
  return getRuleContext<SparqlParser::VarOrIRIrefContext>(i);
}


size_t SparqlParser::DescribeQueryContext::getRuleIndex() const {
  return SparqlParser::RuleDescribeQuery;
}

void SparqlParser::DescribeQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDescribeQuery(this);
}

void SparqlParser::DescribeQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDescribeQuery(this);
}


std::any SparqlParser::DescribeQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitDescribeQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DescribeQueryContext* SparqlParser::describeQuery() {
  DescribeQueryContext *_localctx = _tracker.createInstance<DescribeQueryContext>(_ctx, getState());
  enterRule(_localctx, 12, SparqlParser::RuleDescribeQuery);
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
    match(SparqlParser::DESCRIBE);
    setState(202);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        setState(197); 
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(196);
          varOrIRIref();
          setState(199); 
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (((((_la - 59) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 59)) & 55) != 0));
        break;
      }

      case SparqlParser::STAR: {
        setState(201);
        match(SparqlParser::STAR);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(207);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(204);
      datasetClause();
      setState(209);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(211);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::WHERE

    || _la == SparqlParser::L_CURLY) {
      setState(210);
      whereClause();
    }
    setState(213);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AskQueryContext ------------------------------------------------------------------

SparqlParser::AskQueryContext::AskQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::AskQueryContext::ASK() {
  return getToken(SparqlParser::ASK, 0);
}

SparqlParser::WhereClauseContext* SparqlParser::AskQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::AskQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::AskQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}


size_t SparqlParser::AskQueryContext::getRuleIndex() const {
  return SparqlParser::RuleAskQuery;
}

void SparqlParser::AskQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAskQuery(this);
}

void SparqlParser::AskQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAskQuery(this);
}


std::any SparqlParser::AskQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitAskQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AskQueryContext* SparqlParser::askQuery() {
  AskQueryContext *_localctx = _tracker.createInstance<AskQueryContext>(_ctx, getState());
  enterRule(_localctx, 14, SparqlParser::RuleAskQuery);
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
    setState(215);
    match(SparqlParser::ASK);
    setState(219);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(216);
      datasetClause();
      setState(221);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(222);
    whereClause();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DatasetClauseContext ------------------------------------------------------------------

SparqlParser::DatasetClauseContext::DatasetClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::DatasetClauseContext::FROM() {
  return getToken(SparqlParser::FROM, 0);
}

SparqlParser::DefaultGraphClauseContext* SparqlParser::DatasetClauseContext::defaultGraphClause() {
  return getRuleContext<SparqlParser::DefaultGraphClauseContext>(0);
}

SparqlParser::NamedGraphClauseContext* SparqlParser::DatasetClauseContext::namedGraphClause() {
  return getRuleContext<SparqlParser::NamedGraphClauseContext>(0);
}


size_t SparqlParser::DatasetClauseContext::getRuleIndex() const {
  return SparqlParser::RuleDatasetClause;
}

void SparqlParser::DatasetClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDatasetClause(this);
}

void SparqlParser::DatasetClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDatasetClause(this);
}


std::any SparqlParser::DatasetClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitDatasetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DatasetClauseContext* SparqlParser::datasetClause() {
  DatasetClauseContext *_localctx = _tracker.createInstance<DatasetClauseContext>(_ctx, getState());
  enterRule(_localctx, 16, SparqlParser::RuleDatasetClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(224);
    match(SparqlParser::FROM);
    setState(227);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        setState(225);
        defaultGraphClause();
        break;
      }

      case SparqlParser::NAMED: {
        setState(226);
        namedGraphClause();
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

//----------------- DefaultGraphClauseContext ------------------------------------------------------------------

SparqlParser::DefaultGraphClauseContext::DefaultGraphClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::SourceSelectorContext* SparqlParser::DefaultGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlParser::SourceSelectorContext>(0);
}


size_t SparqlParser::DefaultGraphClauseContext::getRuleIndex() const {
  return SparqlParser::RuleDefaultGraphClause;
}

void SparqlParser::DefaultGraphClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDefaultGraphClause(this);
}

void SparqlParser::DefaultGraphClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDefaultGraphClause(this);
}


std::any SparqlParser::DefaultGraphClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitDefaultGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DefaultGraphClauseContext* SparqlParser::defaultGraphClause() {
  DefaultGraphClauseContext *_localctx = _tracker.createInstance<DefaultGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 18, SparqlParser::RuleDefaultGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(229);
    sourceSelector();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedGraphClauseContext ------------------------------------------------------------------

SparqlParser::NamedGraphClauseContext::NamedGraphClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NamedGraphClauseContext::NAMED() {
  return getToken(SparqlParser::NAMED, 0);
}

SparqlParser::SourceSelectorContext* SparqlParser::NamedGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlParser::SourceSelectorContext>(0);
}


size_t SparqlParser::NamedGraphClauseContext::getRuleIndex() const {
  return SparqlParser::RuleNamedGraphClause;
}

void SparqlParser::NamedGraphClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedGraphClause(this);
}

void SparqlParser::NamedGraphClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedGraphClause(this);
}


std::any SparqlParser::NamedGraphClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNamedGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NamedGraphClauseContext* SparqlParser::namedGraphClause() {
  NamedGraphClauseContext *_localctx = _tracker.createInstance<NamedGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 20, SparqlParser::RuleNamedGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(231);
    match(SparqlParser::NAMED);
    setState(232);
    sourceSelector();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SourceSelectorContext ------------------------------------------------------------------

SparqlParser::SourceSelectorContext::SourceSelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriRefContext* SparqlParser::SourceSelectorContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}


size_t SparqlParser::SourceSelectorContext::getRuleIndex() const {
  return SparqlParser::RuleSourceSelector;
}

void SparqlParser::SourceSelectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSourceSelector(this);
}

void SparqlParser::SourceSelectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSourceSelector(this);
}


std::any SparqlParser::SourceSelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitSourceSelector(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SourceSelectorContext* SparqlParser::sourceSelector() {
  SourceSelectorContext *_localctx = _tracker.createInstance<SourceSelectorContext>(_ctx, getState());
  enterRule(_localctx, 22, SparqlParser::RuleSourceSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(234);
    iriRef();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereClauseContext ------------------------------------------------------------------

SparqlParser::WhereClauseContext::WhereClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GroupGraphPatternContext* SparqlParser::WhereClauseContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}

tree::TerminalNode* SparqlParser::WhereClauseContext::WHERE() {
  return getToken(SparqlParser::WHERE, 0);
}


size_t SparqlParser::WhereClauseContext::getRuleIndex() const {
  return SparqlParser::RuleWhereClause;
}

void SparqlParser::WhereClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWhereClause(this);
}

void SparqlParser::WhereClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWhereClause(this);
}


std::any SparqlParser::WhereClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitWhereClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::WhereClauseContext* SparqlParser::whereClause() {
  WhereClauseContext *_localctx = _tracker.createInstance<WhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 24, SparqlParser::RuleWhereClause);
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
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::WHERE) {
      setState(236);
      match(SparqlParser::WHERE);
    }
    setState(239);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SolutionModifierContext ------------------------------------------------------------------

SparqlParser::SolutionModifierContext::SolutionModifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::OrderClauseContext* SparqlParser::SolutionModifierContext::orderClause() {
  return getRuleContext<SparqlParser::OrderClauseContext>(0);
}

SparqlParser::LimitOffsetClausesContext* SparqlParser::SolutionModifierContext::limitOffsetClauses() {
  return getRuleContext<SparqlParser::LimitOffsetClausesContext>(0);
}


size_t SparqlParser::SolutionModifierContext::getRuleIndex() const {
  return SparqlParser::RuleSolutionModifier;
}

void SparqlParser::SolutionModifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSolutionModifier(this);
}

void SparqlParser::SolutionModifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSolutionModifier(this);
}


std::any SparqlParser::SolutionModifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitSolutionModifier(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SolutionModifierContext* SparqlParser::solutionModifier() {
  SolutionModifierContext *_localctx = _tracker.createInstance<SolutionModifierContext>(_ctx, getState());
  enterRule(_localctx, 26, SparqlParser::RuleSolutionModifier);
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
    setState(242);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::ORDER) {
      setState(241);
      orderClause();
    }
    setState(245);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::LIMIT

    || _la == SparqlParser::OFFSET) {
      setState(244);
      limitOffsetClauses();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitOffsetClausesContext ------------------------------------------------------------------

SparqlParser::LimitOffsetClausesContext::LimitOffsetClausesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::LimitClauseContext* SparqlParser::LimitOffsetClausesContext::limitClause() {
  return getRuleContext<SparqlParser::LimitClauseContext>(0);
}

SparqlParser::OffsetClauseContext* SparqlParser::LimitOffsetClausesContext::offsetClause() {
  return getRuleContext<SparqlParser::OffsetClauseContext>(0);
}


size_t SparqlParser::LimitOffsetClausesContext::getRuleIndex() const {
  return SparqlParser::RuleLimitOffsetClauses;
}

void SparqlParser::LimitOffsetClausesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLimitOffsetClauses(this);
}

void SparqlParser::LimitOffsetClausesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLimitOffsetClauses(this);
}


std::any SparqlParser::LimitOffsetClausesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitLimitOffsetClauses(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::LimitOffsetClausesContext* SparqlParser::limitOffsetClauses() {
  LimitOffsetClausesContext *_localctx = _tracker.createInstance<LimitOffsetClausesContext>(_ctx, getState());
  enterRule(_localctx, 28, SparqlParser::RuleLimitOffsetClauses);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(255);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::LIMIT: {
        enterOuterAlt(_localctx, 1);
        setState(247);
        limitClause();
        setState(249);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::OFFSET) {
          setState(248);
          offsetClause();
        }
        break;
      }

      case SparqlParser::OFFSET: {
        enterOuterAlt(_localctx, 2);
        setState(251);
        offsetClause();
        setState(253);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::LIMIT) {
          setState(252);
          limitClause();
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

//----------------- OrderClauseContext ------------------------------------------------------------------

SparqlParser::OrderClauseContext::OrderClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OrderClauseContext::ORDER() {
  return getToken(SparqlParser::ORDER, 0);
}

tree::TerminalNode* SparqlParser::OrderClauseContext::BY() {
  return getToken(SparqlParser::BY, 0);
}

std::vector<SparqlParser::OrderConditionContext *> SparqlParser::OrderClauseContext::orderCondition() {
  return getRuleContexts<SparqlParser::OrderConditionContext>();
}

SparqlParser::OrderConditionContext* SparqlParser::OrderClauseContext::orderCondition(size_t i) {
  return getRuleContext<SparqlParser::OrderConditionContext>(i);
}


size_t SparqlParser::OrderClauseContext::getRuleIndex() const {
  return SparqlParser::RuleOrderClause;
}

void SparqlParser::OrderClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrderClause(this);
}

void SparqlParser::OrderClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrderClause(this);
}


std::any SparqlParser::OrderClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitOrderClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OrderClauseContext* SparqlParser::orderClause() {
  OrderClauseContext *_localctx = _tracker.createInstance<OrderClauseContext>(_ctx, getState());
  enterRule(_localctx, 30, SparqlParser::RuleOrderClause);
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
    setState(257);
    match(SparqlParser::ORDER);
    setState(258);
    match(SparqlParser::BY);
    setState(260); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(259);
      orderCondition();
      setState(262); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (((((_la - 2) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 2)) & 7926405729580245193) != 0));
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderConditionContext ------------------------------------------------------------------

SparqlParser::OrderConditionContext::OrderConditionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::OrderConditionContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::OrderConditionContext::ASC() {
  return getToken(SparqlParser::ASC, 0);
}

tree::TerminalNode* SparqlParser::OrderConditionContext::DESC() {
  return getToken(SparqlParser::DESC, 0);
}

SparqlParser::ConstraintContext* SparqlParser::OrderConditionContext::constraint() {
  return getRuleContext<SparqlParser::ConstraintContext>(0);
}

SparqlParser::Var_Context* SparqlParser::OrderConditionContext::var_() {
  return getRuleContext<SparqlParser::Var_Context>(0);
}


size_t SparqlParser::OrderConditionContext::getRuleIndex() const {
  return SparqlParser::RuleOrderCondition;
}

void SparqlParser::OrderConditionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrderCondition(this);
}

void SparqlParser::OrderConditionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrderCondition(this);
}


std::any SparqlParser::OrderConditionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitOrderCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OrderConditionContext* SparqlParser::orderCondition() {
  OrderConditionContext *_localctx = _tracker.createInstance<OrderConditionContext>(_ctx, getState());
  enterRule(_localctx, 32, SparqlParser::RuleOrderCondition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(268);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::ASC:
      case SparqlParser::DESC: {
        enterOuterAlt(_localctx, 1);
        setState(264);
        _la = _input->LA(1);
        if (!(_la == SparqlParser::ASC

        || _la == SparqlParser::DESC)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(265);
        brackettedExpression();
        break;
      }

      case SparqlParser::BOUND:
      case SparqlParser::DATATYPE:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::REGEX:
      case SparqlParser::STR:
      case SparqlParser::IS_LITERAL:
      case SparqlParser::IS_BLANK:
      case SparqlParser::IS_URI:
      case SparqlParser::IS_IRI:
      case SparqlParser::SAME_TERM:
      case SparqlParser::L_PAREN:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 2);
        setState(266);
        constraint();
        break;
      }

      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 3);
        setState(267);
        var_();
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

//----------------- LimitClauseContext ------------------------------------------------------------------

SparqlParser::LimitClauseContext::LimitClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::LimitClauseContext::LIMIT() {
  return getToken(SparqlParser::LIMIT, 0);
}

tree::TerminalNode* SparqlParser::LimitClauseContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}


size_t SparqlParser::LimitClauseContext::getRuleIndex() const {
  return SparqlParser::RuleLimitClause;
}

void SparqlParser::LimitClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLimitClause(this);
}

void SparqlParser::LimitClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLimitClause(this);
}


std::any SparqlParser::LimitClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitLimitClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::LimitClauseContext* SparqlParser::limitClause() {
  LimitClauseContext *_localctx = _tracker.createInstance<LimitClauseContext>(_ctx, getState());
  enterRule(_localctx, 34, SparqlParser::RuleLimitClause);

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
    match(SparqlParser::LIMIT);
    setState(271);
    match(SparqlParser::INTEGER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetClauseContext ------------------------------------------------------------------

SparqlParser::OffsetClauseContext::OffsetClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OffsetClauseContext::OFFSET() {
  return getToken(SparqlParser::OFFSET, 0);
}

tree::TerminalNode* SparqlParser::OffsetClauseContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}


size_t SparqlParser::OffsetClauseContext::getRuleIndex() const {
  return SparqlParser::RuleOffsetClause;
}

void SparqlParser::OffsetClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOffsetClause(this);
}

void SparqlParser::OffsetClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOffsetClause(this);
}


std::any SparqlParser::OffsetClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitOffsetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OffsetClauseContext* SparqlParser::offsetClause() {
  OffsetClauseContext *_localctx = _tracker.createInstance<OffsetClauseContext>(_ctx, getState());
  enterRule(_localctx, 36, SparqlParser::RuleOffsetClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(273);
    match(SparqlParser::OFFSET);
    setState(274);
    match(SparqlParser::INTEGER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupGraphPatternContext ------------------------------------------------------------------

SparqlParser::GroupGraphPatternContext::GroupGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::GroupGraphPatternContext::L_CURLY() {
  return getToken(SparqlParser::L_CURLY, 0);
}

tree::TerminalNode* SparqlParser::GroupGraphPatternContext::R_CURLY() {
  return getToken(SparqlParser::R_CURLY, 0);
}

std::vector<SparqlParser::TriplesBlockContext *> SparqlParser::GroupGraphPatternContext::triplesBlock() {
  return getRuleContexts<SparqlParser::TriplesBlockContext>();
}

SparqlParser::TriplesBlockContext* SparqlParser::GroupGraphPatternContext::triplesBlock(size_t i) {
  return getRuleContext<SparqlParser::TriplesBlockContext>(i);
}

std::vector<SparqlParser::GraphPatternNotTriplesContext *> SparqlParser::GroupGraphPatternContext::graphPatternNotTriples() {
  return getRuleContexts<SparqlParser::GraphPatternNotTriplesContext>();
}

SparqlParser::GraphPatternNotTriplesContext* SparqlParser::GroupGraphPatternContext::graphPatternNotTriples(size_t i) {
  return getRuleContext<SparqlParser::GraphPatternNotTriplesContext>(i);
}

std::vector<SparqlParser::Filter_Context *> SparqlParser::GroupGraphPatternContext::filter_() {
  return getRuleContexts<SparqlParser::Filter_Context>();
}

SparqlParser::Filter_Context* SparqlParser::GroupGraphPatternContext::filter_(size_t i) {
  return getRuleContext<SparqlParser::Filter_Context>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::GroupGraphPatternContext::DOT() {
  return getTokens(SparqlParser::DOT);
}

tree::TerminalNode* SparqlParser::GroupGraphPatternContext::DOT(size_t i) {
  return getToken(SparqlParser::DOT, i);
}


size_t SparqlParser::GroupGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGroupGraphPattern;
}

void SparqlParser::GroupGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupGraphPattern(this);
}

void SparqlParser::GroupGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupGraphPattern(this);
}


std::any SparqlParser::GroupGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGroupGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::groupGraphPattern() {
  GroupGraphPatternContext *_localctx = _tracker.createInstance<GroupGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 38, SparqlParser::RuleGroupGraphPattern);
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
    setState(276);
    match(SparqlParser::L_CURLY);
    setState(278);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 29) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 29)) & 14003310299709443) != 0)) {
      setState(277);
      triplesBlock();
    }
    setState(292);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 140737489424384) != 0)) {
      setState(282);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlParser::GRAPH:
        case SparqlParser::OPTIONAL:
        case SparqlParser::L_CURLY: {
          setState(280);
          graphPatternNotTriples();
          break;
        }

        case SparqlParser::FILTER: {
          setState(281);
          filter_();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(285);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SparqlParser::DOT) {
        setState(284);
        match(SparqlParser::DOT);
      }
      setState(288);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (((((_la - 29) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 29)) & 14003310299709443) != 0)) {
        setState(287);
        triplesBlock();
      }
      setState(294);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(295);
    match(SparqlParser::R_CURLY);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesBlockContext ------------------------------------------------------------------

SparqlParser::TriplesBlockContext::TriplesBlockContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::TriplesBlockContext::triplesSameSubject() {
  return getRuleContext<SparqlParser::TriplesSameSubjectContext>(0);
}

tree::TerminalNode* SparqlParser::TriplesBlockContext::DOT() {
  return getToken(SparqlParser::DOT, 0);
}

SparqlParser::TriplesBlockContext* SparqlParser::TriplesBlockContext::triplesBlock() {
  return getRuleContext<SparqlParser::TriplesBlockContext>(0);
}


size_t SparqlParser::TriplesBlockContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesBlock;
}

void SparqlParser::TriplesBlockContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesBlock(this);
}

void SparqlParser::TriplesBlockContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesBlock(this);
}


std::any SparqlParser::TriplesBlockContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitTriplesBlock(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesBlockContext* SparqlParser::triplesBlock() {
  TriplesBlockContext *_localctx = _tracker.createInstance<TriplesBlockContext>(_ctx, getState());
  enterRule(_localctx, 40, SparqlParser::RuleTriplesBlock);
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
    setState(297);
    triplesSameSubject();
    setState(302);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::DOT) {
      setState(298);
      match(SparqlParser::DOT);
      setState(300);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (((((_la - 29) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 29)) & 14003310299709443) != 0)) {
        setState(299);
        triplesBlock();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphPatternNotTriplesContext ------------------------------------------------------------------

SparqlParser::GraphPatternNotTriplesContext::GraphPatternNotTriplesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::OptionalGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::optionalGraphPattern() {
  return getRuleContext<SparqlParser::OptionalGraphPatternContext>(0);
}

SparqlParser::GroupOrUnionGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::groupOrUnionGraphPattern() {
  return getRuleContext<SparqlParser::GroupOrUnionGraphPatternContext>(0);
}

SparqlParser::GraphGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::graphGraphPattern() {
  return getRuleContext<SparqlParser::GraphGraphPatternContext>(0);
}


size_t SparqlParser::GraphPatternNotTriplesContext::getRuleIndex() const {
  return SparqlParser::RuleGraphPatternNotTriples;
}

void SparqlParser::GraphPatternNotTriplesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphPatternNotTriples(this);
}

void SparqlParser::GraphPatternNotTriplesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphPatternNotTriples(this);
}


std::any SparqlParser::GraphPatternNotTriplesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGraphPatternNotTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphPatternNotTriplesContext* SparqlParser::graphPatternNotTriples() {
  GraphPatternNotTriplesContext *_localctx = _tracker.createInstance<GraphPatternNotTriplesContext>(_ctx, getState());
  enterRule(_localctx, 42, SparqlParser::RuleGraphPatternNotTriples);

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
      case SparqlParser::OPTIONAL: {
        enterOuterAlt(_localctx, 1);
        setState(304);
        optionalGraphPattern();
        break;
      }

      case SparqlParser::L_CURLY: {
        enterOuterAlt(_localctx, 2);
        setState(305);
        groupOrUnionGraphPattern();
        break;
      }

      case SparqlParser::GRAPH: {
        enterOuterAlt(_localctx, 3);
        setState(306);
        graphGraphPattern();
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

//----------------- OptionalGraphPatternContext ------------------------------------------------------------------

SparqlParser::OptionalGraphPatternContext::OptionalGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OptionalGraphPatternContext::OPTIONAL() {
  return getToken(SparqlParser::OPTIONAL, 0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::OptionalGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::OptionalGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleOptionalGraphPattern;
}

void SparqlParser::OptionalGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOptionalGraphPattern(this);
}

void SparqlParser::OptionalGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOptionalGraphPattern(this);
}


std::any SparqlParser::OptionalGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitOptionalGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OptionalGraphPatternContext* SparqlParser::optionalGraphPattern() {
  OptionalGraphPatternContext *_localctx = _tracker.createInstance<OptionalGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 44, SparqlParser::RuleOptionalGraphPattern);

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
    match(SparqlParser::OPTIONAL);
    setState(310);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphGraphPatternContext ------------------------------------------------------------------

SparqlParser::GraphGraphPatternContext::GraphGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::GraphGraphPatternContext::GRAPH() {
  return getToken(SparqlParser::GRAPH, 0);
}

SparqlParser::VarOrIRIrefContext* SparqlParser::GraphGraphPatternContext::varOrIRIref() {
  return getRuleContext<SparqlParser::VarOrIRIrefContext>(0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::GraphGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::GraphGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGraphGraphPattern;
}

void SparqlParser::GraphGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphGraphPattern(this);
}

void SparqlParser::GraphGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphGraphPattern(this);
}


std::any SparqlParser::GraphGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGraphGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphGraphPatternContext* SparqlParser::graphGraphPattern() {
  GraphGraphPatternContext *_localctx = _tracker.createInstance<GraphGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 46, SparqlParser::RuleGraphGraphPattern);

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
    match(SparqlParser::GRAPH);
    setState(313);
    varOrIRIref();
    setState(314);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupOrUnionGraphPatternContext ------------------------------------------------------------------

SparqlParser::GroupOrUnionGraphPatternContext::GroupOrUnionGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::GroupGraphPatternContext *> SparqlParser::GroupOrUnionGraphPatternContext::groupGraphPattern() {
  return getRuleContexts<SparqlParser::GroupGraphPatternContext>();
}

SparqlParser::GroupGraphPatternContext* SparqlParser::GroupOrUnionGraphPatternContext::groupGraphPattern(size_t i) {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::GroupOrUnionGraphPatternContext::UNION() {
  return getTokens(SparqlParser::UNION);
}

tree::TerminalNode* SparqlParser::GroupOrUnionGraphPatternContext::UNION(size_t i) {
  return getToken(SparqlParser::UNION, i);
}


size_t SparqlParser::GroupOrUnionGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGroupOrUnionGraphPattern;
}

void SparqlParser::GroupOrUnionGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupOrUnionGraphPattern(this);
}

void SparqlParser::GroupOrUnionGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupOrUnionGraphPattern(this);
}


std::any SparqlParser::GroupOrUnionGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGroupOrUnionGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupOrUnionGraphPatternContext* SparqlParser::groupOrUnionGraphPattern() {
  GroupOrUnionGraphPatternContext *_localctx = _tracker.createInstance<GroupOrUnionGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 48, SparqlParser::RuleGroupOrUnionGraphPattern);
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
    setState(316);
    groupGraphPattern();
    setState(321);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::UNION) {
      setState(317);
      match(SparqlParser::UNION);
      setState(318);
      groupGraphPattern();
      setState(323);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Filter_Context ------------------------------------------------------------------

SparqlParser::Filter_Context::Filter_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::Filter_Context::FILTER() {
  return getToken(SparqlParser::FILTER, 0);
}

SparqlParser::ConstraintContext* SparqlParser::Filter_Context::constraint() {
  return getRuleContext<SparqlParser::ConstraintContext>(0);
}


size_t SparqlParser::Filter_Context::getRuleIndex() const {
  return SparqlParser::RuleFilter_;
}

void SparqlParser::Filter_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFilter_(this);
}

void SparqlParser::Filter_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFilter_(this);
}


std::any SparqlParser::Filter_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitFilter_(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::Filter_Context* SparqlParser::filter_() {
  Filter_Context *_localctx = _tracker.createInstance<Filter_Context>(_ctx, getState());
  enterRule(_localctx, 50, SparqlParser::RuleFilter_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(324);
    match(SparqlParser::FILTER);
    setState(325);
    constraint();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintContext ------------------------------------------------------------------

SparqlParser::ConstraintContext::ConstraintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::ConstraintContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

SparqlParser::BuiltInCallContext* SparqlParser::ConstraintContext::builtInCall() {
  return getRuleContext<SparqlParser::BuiltInCallContext>(0);
}

SparqlParser::FunctionCallContext* SparqlParser::ConstraintContext::functionCall() {
  return getRuleContext<SparqlParser::FunctionCallContext>(0);
}


size_t SparqlParser::ConstraintContext::getRuleIndex() const {
  return SparqlParser::RuleConstraint;
}

void SparqlParser::ConstraintContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraint(this);
}

void SparqlParser::ConstraintContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraint(this);
}


std::any SparqlParser::ConstraintContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConstraint(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstraintContext* SparqlParser::constraint() {
  ConstraintContext *_localctx = _tracker.createInstance<ConstraintContext>(_ctx, getState());
  enterRule(_localctx, 52, SparqlParser::RuleConstraint);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(330);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::L_PAREN: {
        enterOuterAlt(_localctx, 1);
        setState(327);
        brackettedExpression();
        break;
      }

      case SparqlParser::BOUND:
      case SparqlParser::DATATYPE:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::REGEX:
      case SparqlParser::STR:
      case SparqlParser::IS_LITERAL:
      case SparqlParser::IS_BLANK:
      case SparqlParser::IS_URI:
      case SparqlParser::IS_IRI:
      case SparqlParser::SAME_TERM: {
        enterOuterAlt(_localctx, 2);
        setState(328);
        builtInCall();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 3);
        setState(329);
        functionCall();
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

//----------------- FunctionCallContext ------------------------------------------------------------------

SparqlParser::FunctionCallContext::FunctionCallContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriRefContext* SparqlParser::FunctionCallContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}

SparqlParser::ArgListContext* SparqlParser::FunctionCallContext::argList() {
  return getRuleContext<SparqlParser::ArgListContext>(0);
}


size_t SparqlParser::FunctionCallContext::getRuleIndex() const {
  return SparqlParser::RuleFunctionCall;
}

void SparqlParser::FunctionCallContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionCall(this);
}

void SparqlParser::FunctionCallContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionCall(this);
}


std::any SparqlParser::FunctionCallContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitFunctionCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::FunctionCallContext* SparqlParser::functionCall() {
  FunctionCallContext *_localctx = _tracker.createInstance<FunctionCallContext>(_ctx, getState());
  enterRule(_localctx, 54, SparqlParser::RuleFunctionCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(332);
    iriRef();
    setState(333);
    argList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ArgListContext ------------------------------------------------------------------

SparqlParser::ArgListContext::ArgListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ArgListContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

tree::TerminalNode* SparqlParser::ArgListContext::L_PAREN() {
  return getToken(SparqlParser::L_PAREN, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::ArgListContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::ArgListContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::ArgListContext::R_PAREN() {
  return getToken(SparqlParser::R_PAREN, 0);
}

std::vector<tree::TerminalNode *> SparqlParser::ArgListContext::COMMA() {
  return getTokens(SparqlParser::COMMA);
}

tree::TerminalNode* SparqlParser::ArgListContext::COMMA(size_t i) {
  return getToken(SparqlParser::COMMA, i);
}


size_t SparqlParser::ArgListContext::getRuleIndex() const {
  return SparqlParser::RuleArgList;
}

void SparqlParser::ArgListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterArgList(this);
}

void SparqlParser::ArgListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitArgList(this);
}


std::any SparqlParser::ArgListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitArgList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ArgListContext* SparqlParser::argList() {
  ArgListContext *_localctx = _tracker.createInstance<ArgListContext>(_ctx, getState());
  enterRule(_localctx, 56, SparqlParser::RuleArgList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(347);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(335);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::L_PAREN: {
        enterOuterAlt(_localctx, 2);
        setState(336);
        match(SparqlParser::L_PAREN);
        setState(337);
        expression();
        setState(342);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::COMMA) {
          setState(338);
          match(SparqlParser::COMMA);
          setState(339);
          expression();
          setState(344);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(345);
        match(SparqlParser::R_PAREN);
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

//----------------- ConstructTemplateContext ------------------------------------------------------------------

SparqlParser::ConstructTemplateContext::ConstructTemplateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ConstructTemplateContext::L_CURLY() {
  return getToken(SparqlParser::L_CURLY, 0);
}

tree::TerminalNode* SparqlParser::ConstructTemplateContext::R_CURLY() {
  return getToken(SparqlParser::R_CURLY, 0);
}

SparqlParser::ConstructTriplesContext* SparqlParser::ConstructTemplateContext::constructTriples() {
  return getRuleContext<SparqlParser::ConstructTriplesContext>(0);
}


size_t SparqlParser::ConstructTemplateContext::getRuleIndex() const {
  return SparqlParser::RuleConstructTemplate;
}

void SparqlParser::ConstructTemplateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructTemplate(this);
}

void SparqlParser::ConstructTemplateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructTemplate(this);
}


std::any SparqlParser::ConstructTemplateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConstructTemplate(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructTemplateContext* SparqlParser::constructTemplate() {
  ConstructTemplateContext *_localctx = _tracker.createInstance<ConstructTemplateContext>(_ctx, getState());
  enterRule(_localctx, 58, SparqlParser::RuleConstructTemplate);
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
    setState(349);
    match(SparqlParser::L_CURLY);
    setState(351);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 29) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 29)) & 14003310299709443) != 0)) {
      setState(350);
      constructTriples();
    }
    setState(353);
    match(SparqlParser::R_CURLY);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructTriplesContext ------------------------------------------------------------------

SparqlParser::ConstructTriplesContext::ConstructTriplesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::ConstructTriplesContext::triplesSameSubject() {
  return getRuleContext<SparqlParser::TriplesSameSubjectContext>(0);
}

tree::TerminalNode* SparqlParser::ConstructTriplesContext::DOT() {
  return getToken(SparqlParser::DOT, 0);
}

SparqlParser::ConstructTriplesContext* SparqlParser::ConstructTriplesContext::constructTriples() {
  return getRuleContext<SparqlParser::ConstructTriplesContext>(0);
}


size_t SparqlParser::ConstructTriplesContext::getRuleIndex() const {
  return SparqlParser::RuleConstructTriples;
}

void SparqlParser::ConstructTriplesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructTriples(this);
}

void SparqlParser::ConstructTriplesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructTriples(this);
}


std::any SparqlParser::ConstructTriplesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConstructTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructTriplesContext* SparqlParser::constructTriples() {
  ConstructTriplesContext *_localctx = _tracker.createInstance<ConstructTriplesContext>(_ctx, getState());
  enterRule(_localctx, 60, SparqlParser::RuleConstructTriples);
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
    setState(355);
    triplesSameSubject();
    setState(360);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::DOT) {
      setState(356);
      match(SparqlParser::DOT);
      setState(358);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (((((_la - 29) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 29)) & 14003310299709443) != 0)) {
        setState(357);
        constructTriples();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesSameSubjectContext ------------------------------------------------------------------

SparqlParser::TriplesSameSubjectContext::TriplesSameSubjectContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::TriplesSameSubjectContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::TriplesSameSubjectContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}

SparqlParser::TriplesNodeContext* SparqlParser::TriplesSameSubjectContext::triplesNode() {
  return getRuleContext<SparqlParser::TriplesNodeContext>(0);
}

SparqlParser::PropertyListContext* SparqlParser::TriplesSameSubjectContext::propertyList() {
  return getRuleContext<SparqlParser::PropertyListContext>(0);
}


size_t SparqlParser::TriplesSameSubjectContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesSameSubject;
}

void SparqlParser::TriplesSameSubjectContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesSameSubject(this);
}

void SparqlParser::TriplesSameSubjectContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesSameSubject(this);
}


std::any SparqlParser::TriplesSameSubjectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitTriplesSameSubject(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::triplesSameSubject() {
  TriplesSameSubjectContext *_localctx = _tracker.createInstance<TriplesSameSubjectContext>(_ctx, getState());
  enterRule(_localctx, 62, SparqlParser::RuleTriplesSameSubject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(368);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::TRUE:
      case SparqlParser::FALSE:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(362);
        varOrTerm();
        setState(363);
        propertyListNotEmpty();
        break;
      }

      case SparqlParser::L_PAREN:
      case SparqlParser::L_SQUARE: {
        enterOuterAlt(_localctx, 2);
        setState(365);
        triplesNode();
        setState(366);
        propertyList();
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

//----------------- PropertyListNotEmptyContext ------------------------------------------------------------------

SparqlParser::PropertyListNotEmptyContext::PropertyListNotEmptyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::VerbContext *> SparqlParser::PropertyListNotEmptyContext::verb() {
  return getRuleContexts<SparqlParser::VerbContext>();
}

SparqlParser::VerbContext* SparqlParser::PropertyListNotEmptyContext::verb(size_t i) {
  return getRuleContext<SparqlParser::VerbContext>(i);
}

std::vector<SparqlParser::ObjectListContext *> SparqlParser::PropertyListNotEmptyContext::objectList() {
  return getRuleContexts<SparqlParser::ObjectListContext>();
}

SparqlParser::ObjectListContext* SparqlParser::PropertyListNotEmptyContext::objectList(size_t i) {
  return getRuleContext<SparqlParser::ObjectListContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::PropertyListNotEmptyContext::SEMICOLON() {
  return getTokens(SparqlParser::SEMICOLON);
}

tree::TerminalNode* SparqlParser::PropertyListNotEmptyContext::SEMICOLON(size_t i) {
  return getToken(SparqlParser::SEMICOLON, i);
}


size_t SparqlParser::PropertyListNotEmptyContext::getRuleIndex() const {
  return SparqlParser::RulePropertyListNotEmpty;
}

void SparqlParser::PropertyListNotEmptyContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListNotEmpty(this);
}

void SparqlParser::PropertyListNotEmptyContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyListNotEmpty(this);
}


std::any SparqlParser::PropertyListNotEmptyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPropertyListNotEmpty(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::propertyListNotEmpty() {
  PropertyListNotEmptyContext *_localctx = _tracker.createInstance<PropertyListNotEmptyContext>(_ctx, getState());
  enterRule(_localctx, 64, SparqlParser::RulePropertyListNotEmpty);
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
    setState(370);
    verb();
    setState(371);
    objectList();
    setState(380);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::SEMICOLON) {
      setState(372);
      match(SparqlParser::SEMICOLON);
      setState(376);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (((((_la - 1) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 1)) & -2594073385365405695) != 0)) {
        setState(373);
        verb();
        setState(374);
        objectList();
      }
      setState(382);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListContext ------------------------------------------------------------------

SparqlParser::PropertyListContext::PropertyListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::PropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}


size_t SparqlParser::PropertyListContext::getRuleIndex() const {
  return SparqlParser::RulePropertyList;
}

void SparqlParser::PropertyListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyList(this);
}

void SparqlParser::PropertyListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyList(this);
}


std::any SparqlParser::PropertyListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListContext* SparqlParser::propertyList() {
  PropertyListContext *_localctx = _tracker.createInstance<PropertyListContext>(_ctx, getState());
  enterRule(_localctx, 66, SparqlParser::RulePropertyList);
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
    setState(384);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 1) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 1)) & -2594073385365405695) != 0)) {
      setState(383);
      propertyListNotEmpty();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectListContext ------------------------------------------------------------------

SparqlParser::ObjectListContext::ObjectListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::Object_Context *> SparqlParser::ObjectListContext::object_() {
  return getRuleContexts<SparqlParser::Object_Context>();
}

SparqlParser::Object_Context* SparqlParser::ObjectListContext::object_(size_t i) {
  return getRuleContext<SparqlParser::Object_Context>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::ObjectListContext::COMMA() {
  return getTokens(SparqlParser::COMMA);
}

tree::TerminalNode* SparqlParser::ObjectListContext::COMMA(size_t i) {
  return getToken(SparqlParser::COMMA, i);
}


size_t SparqlParser::ObjectListContext::getRuleIndex() const {
  return SparqlParser::RuleObjectList;
}

void SparqlParser::ObjectListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObjectList(this);
}

void SparqlParser::ObjectListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObjectList(this);
}


std::any SparqlParser::ObjectListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitObjectList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ObjectListContext* SparqlParser::objectList() {
  ObjectListContext *_localctx = _tracker.createInstance<ObjectListContext>(_ctx, getState());
  enterRule(_localctx, 68, SparqlParser::RuleObjectList);
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
    setState(386);
    object_();
    setState(391);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::COMMA) {
      setState(387);
      match(SparqlParser::COMMA);
      setState(388);
      object_();
      setState(393);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Object_Context ------------------------------------------------------------------

SparqlParser::Object_Context::Object_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GraphNodeContext* SparqlParser::Object_Context::graphNode() {
  return getRuleContext<SparqlParser::GraphNodeContext>(0);
}


size_t SparqlParser::Object_Context::getRuleIndex() const {
  return SparqlParser::RuleObject_;
}

void SparqlParser::Object_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObject_(this);
}

void SparqlParser::Object_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObject_(this);
}


std::any SparqlParser::Object_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitObject_(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::Object_Context* SparqlParser::object_() {
  Object_Context *_localctx = _tracker.createInstance<Object_Context>(_ctx, getState());
  enterRule(_localctx, 70, SparqlParser::RuleObject_);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(394);
    graphNode();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbContext ------------------------------------------------------------------

SparqlParser::VerbContext::VerbContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrIRIrefContext* SparqlParser::VerbContext::varOrIRIref() {
  return getRuleContext<SparqlParser::VarOrIRIrefContext>(0);
}

tree::TerminalNode* SparqlParser::VerbContext::A() {
  return getToken(SparqlParser::A, 0);
}


size_t SparqlParser::VerbContext::getRuleIndex() const {
  return SparqlParser::RuleVerb;
}

void SparqlParser::VerbContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVerb(this);
}

void SparqlParser::VerbContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVerb(this);
}


std::any SparqlParser::VerbContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitVerb(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VerbContext* SparqlParser::verb() {
  VerbContext *_localctx = _tracker.createInstance<VerbContext>(_ctx, getState());
  enterRule(_localctx, 72, SparqlParser::RuleVerb);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(398);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(396);
        varOrIRIref();
        break;
      }

      case SparqlParser::A: {
        enterOuterAlt(_localctx, 2);
        setState(397);
        match(SparqlParser::A);
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

//----------------- TriplesNodeContext ------------------------------------------------------------------

SparqlParser::TriplesNodeContext::TriplesNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::CollectionContext* SparqlParser::TriplesNodeContext::collection() {
  return getRuleContext<SparqlParser::CollectionContext>(0);
}

SparqlParser::BlankNodePropertyListContext* SparqlParser::TriplesNodeContext::blankNodePropertyList() {
  return getRuleContext<SparqlParser::BlankNodePropertyListContext>(0);
}


size_t SparqlParser::TriplesNodeContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesNode;
}

void SparqlParser::TriplesNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesNode(this);
}

void SparqlParser::TriplesNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesNode(this);
}


std::any SparqlParser::TriplesNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitTriplesNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesNodeContext* SparqlParser::triplesNode() {
  TriplesNodeContext *_localctx = _tracker.createInstance<TriplesNodeContext>(_ctx, getState());
  enterRule(_localctx, 74, SparqlParser::RuleTriplesNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(402);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::L_PAREN: {
        enterOuterAlt(_localctx, 1);
        setState(400);
        collection();
        break;
      }

      case SparqlParser::L_SQUARE: {
        enterOuterAlt(_localctx, 2);
        setState(401);
        blankNodePropertyList();
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

//----------------- BlankNodePropertyListContext ------------------------------------------------------------------

SparqlParser::BlankNodePropertyListContext::BlankNodePropertyListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BlankNodePropertyListContext::L_SQUARE() {
  return getToken(SparqlParser::L_SQUARE, 0);
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::BlankNodePropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}

tree::TerminalNode* SparqlParser::BlankNodePropertyListContext::R_SQUARE() {
  return getToken(SparqlParser::R_SQUARE, 0);
}


size_t SparqlParser::BlankNodePropertyListContext::getRuleIndex() const {
  return SparqlParser::RuleBlankNodePropertyList;
}

void SparqlParser::BlankNodePropertyListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNodePropertyList(this);
}

void SparqlParser::BlankNodePropertyListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNodePropertyList(this);
}


std::any SparqlParser::BlankNodePropertyListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBlankNodePropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BlankNodePropertyListContext* SparqlParser::blankNodePropertyList() {
  BlankNodePropertyListContext *_localctx = _tracker.createInstance<BlankNodePropertyListContext>(_ctx, getState());
  enterRule(_localctx, 76, SparqlParser::RuleBlankNodePropertyList);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(404);
    match(SparqlParser::L_SQUARE);
    setState(405);
    propertyListNotEmpty();
    setState(406);
    match(SparqlParser::R_SQUARE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CollectionContext ------------------------------------------------------------------

SparqlParser::CollectionContext::CollectionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::CollectionContext::L_PAREN() {
  return getToken(SparqlParser::L_PAREN, 0);
}

tree::TerminalNode* SparqlParser::CollectionContext::R_PAREN() {
  return getToken(SparqlParser::R_PAREN, 0);
}

std::vector<SparqlParser::GraphNodeContext *> SparqlParser::CollectionContext::graphNode() {
  return getRuleContexts<SparqlParser::GraphNodeContext>();
}

SparqlParser::GraphNodeContext* SparqlParser::CollectionContext::graphNode(size_t i) {
  return getRuleContext<SparqlParser::GraphNodeContext>(i);
}


size_t SparqlParser::CollectionContext::getRuleIndex() const {
  return SparqlParser::RuleCollection;
}

void SparqlParser::CollectionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCollection(this);
}

void SparqlParser::CollectionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCollection(this);
}


std::any SparqlParser::CollectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitCollection(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::CollectionContext* SparqlParser::collection() {
  CollectionContext *_localctx = _tracker.createInstance<CollectionContext>(_ctx, getState());
  enterRule(_localctx, 78, SparqlParser::RuleCollection);
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
    setState(408);
    match(SparqlParser::L_PAREN);
    setState(410); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(409);
      graphNode();
      setState(412); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (((((_la - 29) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 29)) & 14003310299709443) != 0));
    setState(414);
    match(SparqlParser::R_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphNodeContext ------------------------------------------------------------------

SparqlParser::GraphNodeContext::GraphNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::GraphNodeContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::TriplesNodeContext* SparqlParser::GraphNodeContext::triplesNode() {
  return getRuleContext<SparqlParser::TriplesNodeContext>(0);
}


size_t SparqlParser::GraphNodeContext::getRuleIndex() const {
  return SparqlParser::RuleGraphNode;
}

void SparqlParser::GraphNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphNode(this);
}

void SparqlParser::GraphNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphNode(this);
}


std::any SparqlParser::GraphNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGraphNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphNodeContext* SparqlParser::graphNode() {
  GraphNodeContext *_localctx = _tracker.createInstance<GraphNodeContext>(_ctx, getState());
  enterRule(_localctx, 80, SparqlParser::RuleGraphNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(418);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::TRUE:
      case SparqlParser::FALSE:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(416);
        varOrTerm();
        break;
      }

      case SparqlParser::L_PAREN:
      case SparqlParser::L_SQUARE: {
        enterOuterAlt(_localctx, 2);
        setState(417);
        triplesNode();
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

//----------------- VarOrTermContext ------------------------------------------------------------------

SparqlParser::VarOrTermContext::VarOrTermContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::Var_Context* SparqlParser::VarOrTermContext::var_() {
  return getRuleContext<SparqlParser::Var_Context>(0);
}

SparqlParser::GraphTermContext* SparqlParser::VarOrTermContext::graphTerm() {
  return getRuleContext<SparqlParser::GraphTermContext>(0);
}


size_t SparqlParser::VarOrTermContext::getRuleIndex() const {
  return SparqlParser::RuleVarOrTerm;
}

void SparqlParser::VarOrTermContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVarOrTerm(this);
}

void SparqlParser::VarOrTermContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVarOrTerm(this);
}


std::any SparqlParser::VarOrTermContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitVarOrTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VarOrTermContext* SparqlParser::varOrTerm() {
  VarOrTermContext *_localctx = _tracker.createInstance<VarOrTermContext>(_ctx, getState());
  enterRule(_localctx, 82, SparqlParser::RuleVarOrTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(422);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(420);
        var_();
        break;
      }

      case SparqlParser::TRUE:
      case SparqlParser::FALSE:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 2);
        setState(421);
        graphTerm();
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

//----------------- VarOrIRIrefContext ------------------------------------------------------------------

SparqlParser::VarOrIRIrefContext::VarOrIRIrefContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::Var_Context* SparqlParser::VarOrIRIrefContext::var_() {
  return getRuleContext<SparqlParser::Var_Context>(0);
}

SparqlParser::IriRefContext* SparqlParser::VarOrIRIrefContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}


size_t SparqlParser::VarOrIRIrefContext::getRuleIndex() const {
  return SparqlParser::RuleVarOrIRIref;
}

void SparqlParser::VarOrIRIrefContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVarOrIRIref(this);
}

void SparqlParser::VarOrIRIrefContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVarOrIRIref(this);
}


std::any SparqlParser::VarOrIRIrefContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitVarOrIRIref(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VarOrIRIrefContext* SparqlParser::varOrIRIref() {
  VarOrIRIrefContext *_localctx = _tracker.createInstance<VarOrIRIrefContext>(_ctx, getState());
  enterRule(_localctx, 84, SparqlParser::RuleVarOrIRIref);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(426);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(424);
        var_();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 2);
        setState(425);
        iriRef();
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

//----------------- Var_Context ------------------------------------------------------------------

SparqlParser::Var_Context::Var_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::Var_Context::VAR1() {
  return getToken(SparqlParser::VAR1, 0);
}

tree::TerminalNode* SparqlParser::Var_Context::VAR2() {
  return getToken(SparqlParser::VAR2, 0);
}


size_t SparqlParser::Var_Context::getRuleIndex() const {
  return SparqlParser::RuleVar_;
}

void SparqlParser::Var_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVar_(this);
}

void SparqlParser::Var_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVar_(this);
}


std::any SparqlParser::Var_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitVar_(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::Var_Context* SparqlParser::var_() {
  Var_Context *_localctx = _tracker.createInstance<Var_Context>(_ctx, getState());
  enterRule(_localctx, 86, SparqlParser::RuleVar_);
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
    setState(428);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::VAR1

    || _la == SparqlParser::VAR2)) {
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

//----------------- GraphTermContext ------------------------------------------------------------------

SparqlParser::GraphTermContext::GraphTermContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriRefContext* SparqlParser::GraphTermContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}

SparqlParser::RdfLiteralContext* SparqlParser::GraphTermContext::rdfLiteral() {
  return getRuleContext<SparqlParser::RdfLiteralContext>(0);
}

SparqlParser::NumericLiteralContext* SparqlParser::GraphTermContext::numericLiteral() {
  return getRuleContext<SparqlParser::NumericLiteralContext>(0);
}

SparqlParser::BooleanLiteralContext* SparqlParser::GraphTermContext::booleanLiteral() {
  return getRuleContext<SparqlParser::BooleanLiteralContext>(0);
}

SparqlParser::BlankNodeContext* SparqlParser::GraphTermContext::blankNode() {
  return getRuleContext<SparqlParser::BlankNodeContext>(0);
}

tree::TerminalNode* SparqlParser::GraphTermContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}


size_t SparqlParser::GraphTermContext::getRuleIndex() const {
  return SparqlParser::RuleGraphTerm;
}

void SparqlParser::GraphTermContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphTerm(this);
}

void SparqlParser::GraphTermContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphTerm(this);
}


std::any SparqlParser::GraphTermContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitGraphTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphTermContext* SparqlParser::graphTerm() {
  GraphTermContext *_localctx = _tracker.createInstance<GraphTermContext>(_ctx, getState());
  enterRule(_localctx, 88, SparqlParser::RuleGraphTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(436);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 1);
        setState(430);
        iriRef();
        break;
      }

      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2: {
        enterOuterAlt(_localctx, 2);
        setState(431);
        rdfLiteral();
        break;
      }

      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(432);
        numericLiteral();
        break;
      }

      case SparqlParser::TRUE:
      case SparqlParser::FALSE: {
        enterOuterAlt(_localctx, 4);
        setState(433);
        booleanLiteral();
        break;
      }

      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 5);
        setState(434);
        blankNode();
        break;
      }

      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 6);
        setState(435);
        match(SparqlParser::NIL);
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

//----------------- ExpressionContext ------------------------------------------------------------------

SparqlParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ConditionalOrExpressionContext* SparqlParser::ExpressionContext::conditionalOrExpression() {
  return getRuleContext<SparqlParser::ConditionalOrExpressionContext>(0);
}


size_t SparqlParser::ExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleExpression;
}

void SparqlParser::ExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpression(this);
}

void SparqlParser::ExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpression(this);
}


std::any SparqlParser::ExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ExpressionContext* SparqlParser::expression() {
  ExpressionContext *_localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 90, SparqlParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(438);
    conditionalOrExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalOrExpressionContext ------------------------------------------------------------------

SparqlParser::ConditionalOrExpressionContext::ConditionalOrExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ConditionalAndExpressionContext *> SparqlParser::ConditionalOrExpressionContext::conditionalAndExpression() {
  return getRuleContexts<SparqlParser::ConditionalAndExpressionContext>();
}

SparqlParser::ConditionalAndExpressionContext* SparqlParser::ConditionalOrExpressionContext::conditionalAndExpression(size_t i) {
  return getRuleContext<SparqlParser::ConditionalAndExpressionContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::ConditionalOrExpressionContext::DOUBLE_BAR() {
  return getTokens(SparqlParser::DOUBLE_BAR);
}

tree::TerminalNode* SparqlParser::ConditionalOrExpressionContext::DOUBLE_BAR(size_t i) {
  return getToken(SparqlParser::DOUBLE_BAR, i);
}


size_t SparqlParser::ConditionalOrExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleConditionalOrExpression;
}

void SparqlParser::ConditionalOrExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalOrExpression(this);
}

void SparqlParser::ConditionalOrExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalOrExpression(this);
}


std::any SparqlParser::ConditionalOrExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConditionalOrExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConditionalOrExpressionContext* SparqlParser::conditionalOrExpression() {
  ConditionalOrExpressionContext *_localctx = _tracker.createInstance<ConditionalOrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 92, SparqlParser::RuleConditionalOrExpression);
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
    setState(440);
    conditionalAndExpression();
    setState(445);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::DOUBLE_BAR) {
      setState(441);
      match(SparqlParser::DOUBLE_BAR);
      setState(442);
      conditionalAndExpression();
      setState(447);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalAndExpressionContext ------------------------------------------------------------------

SparqlParser::ConditionalAndExpressionContext::ConditionalAndExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ValueLogicalContext *> SparqlParser::ConditionalAndExpressionContext::valueLogical() {
  return getRuleContexts<SparqlParser::ValueLogicalContext>();
}

SparqlParser::ValueLogicalContext* SparqlParser::ConditionalAndExpressionContext::valueLogical(size_t i) {
  return getRuleContext<SparqlParser::ValueLogicalContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::ConditionalAndExpressionContext::DOUBLE_AMP() {
  return getTokens(SparqlParser::DOUBLE_AMP);
}

tree::TerminalNode* SparqlParser::ConditionalAndExpressionContext::DOUBLE_AMP(size_t i) {
  return getToken(SparqlParser::DOUBLE_AMP, i);
}


size_t SparqlParser::ConditionalAndExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleConditionalAndExpression;
}

void SparqlParser::ConditionalAndExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalAndExpression(this);
}

void SparqlParser::ConditionalAndExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalAndExpression(this);
}


std::any SparqlParser::ConditionalAndExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitConditionalAndExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConditionalAndExpressionContext* SparqlParser::conditionalAndExpression() {
  ConditionalAndExpressionContext *_localctx = _tracker.createInstance<ConditionalAndExpressionContext>(_ctx, getState());
  enterRule(_localctx, 94, SparqlParser::RuleConditionalAndExpression);
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
    setState(448);
    valueLogical();
    setState(453);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::DOUBLE_AMP) {
      setState(449);
      match(SparqlParser::DOUBLE_AMP);
      setState(450);
      valueLogical();
      setState(455);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueLogicalContext ------------------------------------------------------------------

SparqlParser::ValueLogicalContext::ValueLogicalContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::RelationalExpressionContext* SparqlParser::ValueLogicalContext::relationalExpression() {
  return getRuleContext<SparqlParser::RelationalExpressionContext>(0);
}


size_t SparqlParser::ValueLogicalContext::getRuleIndex() const {
  return SparqlParser::RuleValueLogical;
}

void SparqlParser::ValueLogicalContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterValueLogical(this);
}

void SparqlParser::ValueLogicalContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitValueLogical(this);
}


std::any SparqlParser::ValueLogicalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitValueLogical(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ValueLogicalContext* SparqlParser::valueLogical() {
  ValueLogicalContext *_localctx = _tracker.createInstance<ValueLogicalContext>(_ctx, getState());
  enterRule(_localctx, 96, SparqlParser::RuleValueLogical);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(456);
    relationalExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationalExpressionContext ------------------------------------------------------------------

SparqlParser::RelationalExpressionContext::RelationalExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::NumericExpressionContext *> SparqlParser::RelationalExpressionContext::numericExpression() {
  return getRuleContexts<SparqlParser::NumericExpressionContext>();
}

SparqlParser::NumericExpressionContext* SparqlParser::RelationalExpressionContext::numericExpression(size_t i) {
  return getRuleContext<SparqlParser::NumericExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::EQUAL() {
  return getToken(SparqlParser::EQUAL, 0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::NOT_EQUAL() {
  return getToken(SparqlParser::NOT_EQUAL, 0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::LESS() {
  return getToken(SparqlParser::LESS, 0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::GREATER() {
  return getToken(SparqlParser::GREATER, 0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::LESS_OR_EQUAL() {
  return getToken(SparqlParser::LESS_OR_EQUAL, 0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::GREATER_OR_EQUAL() {
  return getToken(SparqlParser::GREATER_OR_EQUAL, 0);
}


size_t SparqlParser::RelationalExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleRelationalExpression;
}

void SparqlParser::RelationalExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelationalExpression(this);
}

void SparqlParser::RelationalExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRelationalExpression(this);
}


std::any SparqlParser::RelationalExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitRelationalExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RelationalExpressionContext* SparqlParser::relationalExpression() {
  RelationalExpressionContext *_localctx = _tracker.createInstance<RelationalExpressionContext>(_ctx, getState());
  enterRule(_localctx, 98, SparqlParser::RuleRelationalExpression);
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
    setState(458);
    numericExpression();
    setState(461);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 2385940232273920) != 0)) {
      setState(459);
      _la = _input->LA(1);
      if (!((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & 2385940232273920) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(460);
      numericExpression();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericExpressionContext ------------------------------------------------------------------

SparqlParser::NumericExpressionContext::NumericExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::AdditiveExpressionContext* SparqlParser::NumericExpressionContext::additiveExpression() {
  return getRuleContext<SparqlParser::AdditiveExpressionContext>(0);
}


size_t SparqlParser::NumericExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleNumericExpression;
}

void SparqlParser::NumericExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericExpression(this);
}

void SparqlParser::NumericExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericExpression(this);
}


std::any SparqlParser::NumericExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNumericExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericExpressionContext* SparqlParser::numericExpression() {
  NumericExpressionContext *_localctx = _tracker.createInstance<NumericExpressionContext>(_ctx, getState());
  enterRule(_localctx, 100, SparqlParser::RuleNumericExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(463);
    additiveExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AdditiveExpressionContext ------------------------------------------------------------------

SparqlParser::AdditiveExpressionContext::AdditiveExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::MultiplicativeExpressionContext *> SparqlParser::AdditiveExpressionContext::multiplicativeExpression() {
  return getRuleContexts<SparqlParser::MultiplicativeExpressionContext>();
}

SparqlParser::MultiplicativeExpressionContext* SparqlParser::AdditiveExpressionContext::multiplicativeExpression(size_t i) {
  return getRuleContext<SparqlParser::MultiplicativeExpressionContext>(i);
}

std::vector<SparqlParser::NumericLiteralPositiveContext *> SparqlParser::AdditiveExpressionContext::numericLiteralPositive() {
  return getRuleContexts<SparqlParser::NumericLiteralPositiveContext>();
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::AdditiveExpressionContext::numericLiteralPositive(size_t i) {
  return getRuleContext<SparqlParser::NumericLiteralPositiveContext>(i);
}

std::vector<SparqlParser::NumericLiteralNegativeContext *> SparqlParser::AdditiveExpressionContext::numericLiteralNegative() {
  return getRuleContexts<SparqlParser::NumericLiteralNegativeContext>();
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::AdditiveExpressionContext::numericLiteralNegative(size_t i) {
  return getRuleContext<SparqlParser::NumericLiteralNegativeContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::AdditiveExpressionContext::PLUS() {
  return getTokens(SparqlParser::PLUS);
}

tree::TerminalNode* SparqlParser::AdditiveExpressionContext::PLUS(size_t i) {
  return getToken(SparqlParser::PLUS, i);
}

std::vector<tree::TerminalNode *> SparqlParser::AdditiveExpressionContext::MINUS() {
  return getTokens(SparqlParser::MINUS);
}

tree::TerminalNode* SparqlParser::AdditiveExpressionContext::MINUS(size_t i) {
  return getToken(SparqlParser::MINUS, i);
}


size_t SparqlParser::AdditiveExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleAdditiveExpression;
}

void SparqlParser::AdditiveExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAdditiveExpression(this);
}

void SparqlParser::AdditiveExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAdditiveExpression(this);
}


std::any SparqlParser::AdditiveExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitAdditiveExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AdditiveExpressionContext* SparqlParser::additiveExpression() {
  AdditiveExpressionContext *_localctx = _tracker.createInstance<AdditiveExpressionContext>(_ctx, getState());
  enterRule(_localctx, 102, SparqlParser::RuleAdditiveExpression);
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
    setState(465);
    multiplicativeExpression();
    setState(472);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 50) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 50)) & 33030149) != 0)) {
      setState(470);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlParser::MINUS:
        case SparqlParser::PLUS: {
          setState(466);
          _la = _input->LA(1);
          if (!(_la == SparqlParser::MINUS

          || _la == SparqlParser::PLUS)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(467);
          multiplicativeExpression();
          break;
        }

        case SparqlParser::INTEGER_POSITIVE:
        case SparqlParser::DECIMAL_POSITIVE:
        case SparqlParser::DOUBLE_POSITIVE: {
          setState(468);
          numericLiteralPositive();
          break;
        }

        case SparqlParser::INTEGER_NEGATIVE:
        case SparqlParser::DECIMAL_NEGATIVE:
        case SparqlParser::DOUBLE_NEGATIVE: {
          setState(469);
          numericLiteralNegative();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(474);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionContext ------------------------------------------------------------------

SparqlParser::MultiplicativeExpressionContext::MultiplicativeExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::UnaryExpressionContext *> SparqlParser::MultiplicativeExpressionContext::unaryExpression() {
  return getRuleContexts<SparqlParser::UnaryExpressionContext>();
}

SparqlParser::UnaryExpressionContext* SparqlParser::MultiplicativeExpressionContext::unaryExpression(size_t i) {
  return getRuleContext<SparqlParser::UnaryExpressionContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::MultiplicativeExpressionContext::STAR() {
  return getTokens(SparqlParser::STAR);
}

tree::TerminalNode* SparqlParser::MultiplicativeExpressionContext::STAR(size_t i) {
  return getToken(SparqlParser::STAR, i);
}

std::vector<tree::TerminalNode *> SparqlParser::MultiplicativeExpressionContext::SLASH() {
  return getTokens(SparqlParser::SLASH);
}

tree::TerminalNode* SparqlParser::MultiplicativeExpressionContext::SLASH(size_t i) {
  return getToken(SparqlParser::SLASH, i);
}


size_t SparqlParser::MultiplicativeExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleMultiplicativeExpression;
}

void SparqlParser::MultiplicativeExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplicativeExpression(this);
}

void SparqlParser::MultiplicativeExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplicativeExpression(this);
}


std::any SparqlParser::MultiplicativeExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitMultiplicativeExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::MultiplicativeExpressionContext* SparqlParser::multiplicativeExpression() {
  MultiplicativeExpressionContext *_localctx = _tracker.createInstance<MultiplicativeExpressionContext>(_ctx, getState());
  enterRule(_localctx, 104, SparqlParser::RuleMultiplicativeExpression);
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
    setState(475);
    unaryExpression();
    setState(480);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::SLASH

    || _la == SparqlParser::STAR) {
      setState(476);
      _la = _input->LA(1);
      if (!(_la == SparqlParser::SLASH

      || _la == SparqlParser::STAR)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(477);
      unaryExpression();
      setState(482);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnaryExpressionContext ------------------------------------------------------------------

SparqlParser::UnaryExpressionContext::UnaryExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PrimaryExpressionContext* SparqlParser::UnaryExpressionContext::primaryExpression() {
  return getRuleContext<SparqlParser::PrimaryExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::UnaryExpressionContext::EXCLAMATION() {
  return getToken(SparqlParser::EXCLAMATION, 0);
}

tree::TerminalNode* SparqlParser::UnaryExpressionContext::PLUS() {
  return getToken(SparqlParser::PLUS, 0);
}

tree::TerminalNode* SparqlParser::UnaryExpressionContext::MINUS() {
  return getToken(SparqlParser::MINUS, 0);
}


size_t SparqlParser::UnaryExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleUnaryExpression;
}

void SparqlParser::UnaryExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnaryExpression(this);
}

void SparqlParser::UnaryExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnaryExpression(this);
}


std::any SparqlParser::UnaryExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitUnaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::UnaryExpressionContext* SparqlParser::unaryExpression() {
  UnaryExpressionContext *_localctx = _tracker.createInstance<UnaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 106, SparqlParser::RuleUnaryExpression);
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
    setState(484);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & 5633897580724224) != 0)) {
      setState(483);
      _la = _input->LA(1);
      if (!((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & 5633897580724224) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(486);
    primaryExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryExpressionContext ------------------------------------------------------------------

SparqlParser::PrimaryExpressionContext::PrimaryExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::PrimaryExpressionContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

SparqlParser::BuiltInCallContext* SparqlParser::PrimaryExpressionContext::builtInCall() {
  return getRuleContext<SparqlParser::BuiltInCallContext>(0);
}

SparqlParser::IriRefOrFunctionContext* SparqlParser::PrimaryExpressionContext::iriRefOrFunction() {
  return getRuleContext<SparqlParser::IriRefOrFunctionContext>(0);
}

SparqlParser::RdfLiteralContext* SparqlParser::PrimaryExpressionContext::rdfLiteral() {
  return getRuleContext<SparqlParser::RdfLiteralContext>(0);
}

SparqlParser::NumericLiteralContext* SparqlParser::PrimaryExpressionContext::numericLiteral() {
  return getRuleContext<SparqlParser::NumericLiteralContext>(0);
}

SparqlParser::BooleanLiteralContext* SparqlParser::PrimaryExpressionContext::booleanLiteral() {
  return getRuleContext<SparqlParser::BooleanLiteralContext>(0);
}

SparqlParser::Var_Context* SparqlParser::PrimaryExpressionContext::var_() {
  return getRuleContext<SparqlParser::Var_Context>(0);
}


size_t SparqlParser::PrimaryExpressionContext::getRuleIndex() const {
  return SparqlParser::RulePrimaryExpression;
}

void SparqlParser::PrimaryExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrimaryExpression(this);
}

void SparqlParser::PrimaryExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrimaryExpression(this);
}


std::any SparqlParser::PrimaryExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPrimaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrimaryExpressionContext* SparqlParser::primaryExpression() {
  PrimaryExpressionContext *_localctx = _tracker.createInstance<PrimaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 108, SparqlParser::RulePrimaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(495);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::L_PAREN: {
        enterOuterAlt(_localctx, 1);
        setState(488);
        brackettedExpression();
        break;
      }

      case SparqlParser::BOUND:
      case SparqlParser::DATATYPE:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::REGEX:
      case SparqlParser::STR:
      case SparqlParser::IS_LITERAL:
      case SparqlParser::IS_BLANK:
      case SparqlParser::IS_URI:
      case SparqlParser::IS_IRI:
      case SparqlParser::SAME_TERM: {
        enterOuterAlt(_localctx, 2);
        setState(489);
        builtInCall();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 3);
        setState(490);
        iriRefOrFunction();
        break;
      }

      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2: {
        enterOuterAlt(_localctx, 4);
        setState(491);
        rdfLiteral();
        break;
      }

      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 5);
        setState(492);
        numericLiteral();
        break;
      }

      case SparqlParser::TRUE:
      case SparqlParser::FALSE: {
        enterOuterAlt(_localctx, 6);
        setState(493);
        booleanLiteral();
        break;
      }

      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 7);
        setState(494);
        var_();
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

//----------------- BrackettedExpressionContext ------------------------------------------------------------------

SparqlParser::BrackettedExpressionContext::BrackettedExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BrackettedExpressionContext::L_PAREN() {
  return getToken(SparqlParser::L_PAREN, 0);
}

SparqlParser::ExpressionContext* SparqlParser::BrackettedExpressionContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::BrackettedExpressionContext::R_PAREN() {
  return getToken(SparqlParser::R_PAREN, 0);
}


size_t SparqlParser::BrackettedExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleBrackettedExpression;
}

void SparqlParser::BrackettedExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBrackettedExpression(this);
}

void SparqlParser::BrackettedExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBrackettedExpression(this);
}


std::any SparqlParser::BrackettedExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBrackettedExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BrackettedExpressionContext* SparqlParser::brackettedExpression() {
  BrackettedExpressionContext *_localctx = _tracker.createInstance<BrackettedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 110, SparqlParser::RuleBrackettedExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(497);
    match(SparqlParser::L_PAREN);
    setState(498);
    expression();
    setState(499);
    match(SparqlParser::R_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BuiltInCallContext ------------------------------------------------------------------

SparqlParser::BuiltInCallContext::BuiltInCallContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STR() {
  return getToken(SparqlParser::STR, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::L_PAREN() {
  return getToken(SparqlParser::L_PAREN, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::BuiltInCallContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::BuiltInCallContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::R_PAREN() {
  return getToken(SparqlParser::R_PAREN, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::LANG() {
  return getToken(SparqlParser::LANG, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::LANGMATCHES() {
  return getToken(SparqlParser::LANGMATCHES, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::COMMA() {
  return getToken(SparqlParser::COMMA, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::DATATYPE() {
  return getToken(SparqlParser::DATATYPE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::BOUND() {
  return getToken(SparqlParser::BOUND, 0);
}

SparqlParser::Var_Context* SparqlParser::BuiltInCallContext::var_() {
  return getRuleContext<SparqlParser::Var_Context>(0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SAME_TERM() {
  return getToken(SparqlParser::SAME_TERM, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IS_IRI() {
  return getToken(SparqlParser::IS_IRI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IS_URI() {
  return getToken(SparqlParser::IS_URI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IS_BLANK() {
  return getToken(SparqlParser::IS_BLANK, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IS_LITERAL() {
  return getToken(SparqlParser::IS_LITERAL, 0);
}

SparqlParser::RegexExpressionContext* SparqlParser::BuiltInCallContext::regexExpression() {
  return getRuleContext<SparqlParser::RegexExpressionContext>(0);
}


size_t SparqlParser::BuiltInCallContext::getRuleIndex() const {
  return SparqlParser::RuleBuiltInCall;
}

void SparqlParser::BuiltInCallContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBuiltInCall(this);
}

void SparqlParser::BuiltInCallContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBuiltInCall(this);
}


std::any SparqlParser::BuiltInCallContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBuiltInCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BuiltInCallContext* SparqlParser::builtInCall() {
  BuiltInCallContext *_localctx = _tracker.createInstance<BuiltInCallContext>(_ctx, getState());
  enterRule(_localctx, 112, SparqlParser::RuleBuiltInCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(556);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::STR: {
        enterOuterAlt(_localctx, 1);
        setState(501);
        match(SparqlParser::STR);
        setState(502);
        match(SparqlParser::L_PAREN);
        setState(503);
        expression();
        setState(504);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::LANG: {
        enterOuterAlt(_localctx, 2);
        setState(506);
        match(SparqlParser::LANG);
        setState(507);
        match(SparqlParser::L_PAREN);
        setState(508);
        expression();
        setState(509);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::LANGMATCHES: {
        enterOuterAlt(_localctx, 3);
        setState(511);
        match(SparqlParser::LANGMATCHES);
        setState(512);
        match(SparqlParser::L_PAREN);
        setState(513);
        expression();
        setState(514);
        match(SparqlParser::COMMA);
        setState(515);
        expression();
        setState(516);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::DATATYPE: {
        enterOuterAlt(_localctx, 4);
        setState(518);
        match(SparqlParser::DATATYPE);
        setState(519);
        match(SparqlParser::L_PAREN);
        setState(520);
        expression();
        setState(521);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::BOUND: {
        enterOuterAlt(_localctx, 5);
        setState(523);
        match(SparqlParser::BOUND);
        setState(524);
        match(SparqlParser::L_PAREN);
        setState(525);
        var_();
        setState(526);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::SAME_TERM: {
        enterOuterAlt(_localctx, 6);
        setState(528);
        match(SparqlParser::SAME_TERM);
        setState(529);
        match(SparqlParser::L_PAREN);
        setState(530);
        expression();
        setState(531);
        match(SparqlParser::COMMA);
        setState(532);
        expression();
        setState(533);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::IS_IRI: {
        enterOuterAlt(_localctx, 7);
        setState(535);
        match(SparqlParser::IS_IRI);
        setState(536);
        match(SparqlParser::L_PAREN);
        setState(537);
        expression();
        setState(538);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::IS_URI: {
        enterOuterAlt(_localctx, 8);
        setState(540);
        match(SparqlParser::IS_URI);
        setState(541);
        match(SparqlParser::L_PAREN);
        setState(542);
        expression();
        setState(543);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::IS_BLANK: {
        enterOuterAlt(_localctx, 9);
        setState(545);
        match(SparqlParser::IS_BLANK);
        setState(546);
        match(SparqlParser::L_PAREN);
        setState(547);
        expression();
        setState(548);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::IS_LITERAL: {
        enterOuterAlt(_localctx, 10);
        setState(550);
        match(SparqlParser::IS_LITERAL);
        setState(551);
        match(SparqlParser::L_PAREN);
        setState(552);
        expression();
        setState(553);
        match(SparqlParser::R_PAREN);
        break;
      }

      case SparqlParser::REGEX: {
        enterOuterAlt(_localctx, 11);
        setState(555);
        regexExpression();
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

//----------------- RegexExpressionContext ------------------------------------------------------------------

SparqlParser::RegexExpressionContext::RegexExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::RegexExpressionContext::REGEX() {
  return getToken(SparqlParser::REGEX, 0);
}

tree::TerminalNode* SparqlParser::RegexExpressionContext::L_PAREN() {
  return getToken(SparqlParser::L_PAREN, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::RegexExpressionContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::RegexExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::RegexExpressionContext::COMMA() {
  return getTokens(SparqlParser::COMMA);
}

tree::TerminalNode* SparqlParser::RegexExpressionContext::COMMA(size_t i) {
  return getToken(SparqlParser::COMMA, i);
}

tree::TerminalNode* SparqlParser::RegexExpressionContext::R_PAREN() {
  return getToken(SparqlParser::R_PAREN, 0);
}


size_t SparqlParser::RegexExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleRegexExpression;
}

void SparqlParser::RegexExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRegexExpression(this);
}

void SparqlParser::RegexExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRegexExpression(this);
}


std::any SparqlParser::RegexExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitRegexExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RegexExpressionContext* SparqlParser::regexExpression() {
  RegexExpressionContext *_localctx = _tracker.createInstance<RegexExpressionContext>(_ctx, getState());
  enterRule(_localctx, 114, SparqlParser::RuleRegexExpression);
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
    setState(558);
    match(SparqlParser::REGEX);
    setState(559);
    match(SparqlParser::L_PAREN);
    setState(560);
    expression();
    setState(561);
    match(SparqlParser::COMMA);
    setState(562);
    expression();
    setState(565);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::COMMA) {
      setState(563);
      match(SparqlParser::COMMA);
      setState(564);
      expression();
    }
    setState(567);
    match(SparqlParser::R_PAREN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IriRefOrFunctionContext ------------------------------------------------------------------

SparqlParser::IriRefOrFunctionContext::IriRefOrFunctionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriRefContext* SparqlParser::IriRefOrFunctionContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}

SparqlParser::ArgListContext* SparqlParser::IriRefOrFunctionContext::argList() {
  return getRuleContext<SparqlParser::ArgListContext>(0);
}


size_t SparqlParser::IriRefOrFunctionContext::getRuleIndex() const {
  return SparqlParser::RuleIriRefOrFunction;
}

void SparqlParser::IriRefOrFunctionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIriRefOrFunction(this);
}

void SparqlParser::IriRefOrFunctionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIriRefOrFunction(this);
}


std::any SparqlParser::IriRefOrFunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitIriRefOrFunction(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IriRefOrFunctionContext* SparqlParser::iriRefOrFunction() {
  IriRefOrFunctionContext *_localctx = _tracker.createInstance<IriRefOrFunctionContext>(_ctx, getState());
  enterRule(_localctx, 116, SparqlParser::RuleIriRefOrFunction);
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
    setState(569);
    iriRef();
    setState(571);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::L_PAREN

    || _la == SparqlParser::NIL) {
      setState(570);
      argList();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RdfLiteralContext ------------------------------------------------------------------

SparqlParser::RdfLiteralContext::RdfLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::String_Context* SparqlParser::RdfLiteralContext::string_() {
  return getRuleContext<SparqlParser::String_Context>(0);
}

tree::TerminalNode* SparqlParser::RdfLiteralContext::LANGTAG() {
  return getToken(SparqlParser::LANGTAG, 0);
}

tree::TerminalNode* SparqlParser::RdfLiteralContext::DOUBLE_CARET() {
  return getToken(SparqlParser::DOUBLE_CARET, 0);
}

SparqlParser::IriRefContext* SparqlParser::RdfLiteralContext::iriRef() {
  return getRuleContext<SparqlParser::IriRefContext>(0);
}


size_t SparqlParser::RdfLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleRdfLiteral;
}

void SparqlParser::RdfLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRdfLiteral(this);
}

void SparqlParser::RdfLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRdfLiteral(this);
}


std::any SparqlParser::RdfLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitRdfLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RdfLiteralContext* SparqlParser::rdfLiteral() {
  RdfLiteralContext *_localctx = _tracker.createInstance<RdfLiteralContext>(_ctx, getState());
  enterRule(_localctx, 118, SparqlParser::RuleRdfLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(573);
    string_();
    setState(577);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::LANGTAG: {
        setState(574);
        match(SparqlParser::LANGTAG);
        break;
      }

      case SparqlParser::DOUBLE_CARET: {
        setState(575);
        match(SparqlParser::DOUBLE_CARET);
        setState(576);
        iriRef();
        break;
      }

      case SparqlParser::A:
      case SparqlParser::FILTER:
      case SparqlParser::GRAPH:
      case SparqlParser::OPTIONAL:
      case SparqlParser::TRUE:
      case SparqlParser::FALSE:
      case SparqlParser::COMMA:
      case SparqlParser::DOT:
      case SparqlParser::DOUBLE_AMP:
      case SparqlParser::DOUBLE_BAR:
      case SparqlParser::EQUAL:
      case SparqlParser::GREATER:
      case SparqlParser::GREATER_OR_EQUAL:
      case SparqlParser::LESS:
      case SparqlParser::LESS_OR_EQUAL:
      case SparqlParser::L_CURLY:
      case SparqlParser::L_PAREN:
      case SparqlParser::L_SQUARE:
      case SparqlParser::MINUS:
      case SparqlParser::NOT_EQUAL:
      case SparqlParser::PLUS:
      case SparqlParser::R_CURLY:
      case SparqlParser::R_PAREN:
      case SparqlParser::R_SQUARE:
      case SparqlParser::SEMICOLON:
      case SparqlParser::SLASH:
      case SparqlParser::STAR:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
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

//----------------- NumericLiteralContext ------------------------------------------------------------------

SparqlParser::NumericLiteralContext::NumericLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::NumericLiteralUnsignedContext* SparqlParser::NumericLiteralContext::numericLiteralUnsigned() {
  return getRuleContext<SparqlParser::NumericLiteralUnsignedContext>(0);
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::NumericLiteralContext::numericLiteralPositive() {
  return getRuleContext<SparqlParser::NumericLiteralPositiveContext>(0);
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::NumericLiteralContext::numericLiteralNegative() {
  return getRuleContext<SparqlParser::NumericLiteralNegativeContext>(0);
}


size_t SparqlParser::NumericLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteral;
}

void SparqlParser::NumericLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteral(this);
}

void SparqlParser::NumericLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteral(this);
}


std::any SparqlParser::NumericLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNumericLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralContext* SparqlParser::numericLiteral() {
  NumericLiteralContext *_localctx = _tracker.createInstance<NumericLiteralContext>(_ctx, getState());
  enterRule(_localctx, 120, SparqlParser::RuleNumericLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(582);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE: {
        enterOuterAlt(_localctx, 1);
        setState(579);
        numericLiteralUnsigned();
        break;
      }

      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE: {
        enterOuterAlt(_localctx, 2);
        setState(580);
        numericLiteralPositive();
        break;
      }

      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(581);
        numericLiteralNegative();
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

//----------------- NumericLiteralUnsignedContext ------------------------------------------------------------------

SparqlParser::NumericLiteralUnsignedContext::NumericLiteralUnsignedContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::DECIMAL() {
  return getToken(SparqlParser::DECIMAL, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::DOUBLE() {
  return getToken(SparqlParser::DOUBLE, 0);
}


size_t SparqlParser::NumericLiteralUnsignedContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralUnsigned;
}

void SparqlParser::NumericLiteralUnsignedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralUnsigned(this);
}

void SparqlParser::NumericLiteralUnsignedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralUnsigned(this);
}


std::any SparqlParser::NumericLiteralUnsignedContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralUnsigned(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralUnsignedContext* SparqlParser::numericLiteralUnsigned() {
  NumericLiteralUnsignedContext *_localctx = _tracker.createInstance<NumericLiteralUnsignedContext>(_ctx, getState());
  enterRule(_localctx, 122, SparqlParser::RuleNumericLiteralUnsigned);
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
    setState(584);
    _la = _input->LA(1);
    if (!(((((_la - 66) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 66)) & 7) != 0))) {
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

//----------------- NumericLiteralPositiveContext ------------------------------------------------------------------

SparqlParser::NumericLiteralPositiveContext::NumericLiteralPositiveContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::INTEGER_POSITIVE() {
  return getToken(SparqlParser::INTEGER_POSITIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::DECIMAL_POSITIVE() {
  return getToken(SparqlParser::DECIMAL_POSITIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::DOUBLE_POSITIVE() {
  return getToken(SparqlParser::DOUBLE_POSITIVE, 0);
}


size_t SparqlParser::NumericLiteralPositiveContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralPositive;
}

void SparqlParser::NumericLiteralPositiveContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralPositive(this);
}

void SparqlParser::NumericLiteralPositiveContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralPositive(this);
}


std::any SparqlParser::NumericLiteralPositiveContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralPositive(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::numericLiteralPositive() {
  NumericLiteralPositiveContext *_localctx = _tracker.createInstance<NumericLiteralPositiveContext>(_ctx, getState());
  enterRule(_localctx, 124, SparqlParser::RuleNumericLiteralPositive);
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
    setState(586);
    _la = _input->LA(1);
    if (!(((((_la - 69) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 69)) & 7) != 0))) {
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

//----------------- NumericLiteralNegativeContext ------------------------------------------------------------------

SparqlParser::NumericLiteralNegativeContext::NumericLiteralNegativeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::INTEGER_NEGATIVE() {
  return getToken(SparqlParser::INTEGER_NEGATIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::DECIMAL_NEGATIVE() {
  return getToken(SparqlParser::DECIMAL_NEGATIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::DOUBLE_NEGATIVE() {
  return getToken(SparqlParser::DOUBLE_NEGATIVE, 0);
}


size_t SparqlParser::NumericLiteralNegativeContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralNegative;
}

void SparqlParser::NumericLiteralNegativeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralNegative(this);
}

void SparqlParser::NumericLiteralNegativeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralNegative(this);
}


std::any SparqlParser::NumericLiteralNegativeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralNegative(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::numericLiteralNegative() {
  NumericLiteralNegativeContext *_localctx = _tracker.createInstance<NumericLiteralNegativeContext>(_ctx, getState());
  enterRule(_localctx, 126, SparqlParser::RuleNumericLiteralNegative);
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
    setState(588);
    _la = _input->LA(1);
    if (!(((((_la - 72) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 72)) & 7) != 0))) {
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

//----------------- BooleanLiteralContext ------------------------------------------------------------------

SparqlParser::BooleanLiteralContext::BooleanLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BooleanLiteralContext::TRUE() {
  return getToken(SparqlParser::TRUE, 0);
}

tree::TerminalNode* SparqlParser::BooleanLiteralContext::FALSE() {
  return getToken(SparqlParser::FALSE, 0);
}


size_t SparqlParser::BooleanLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleBooleanLiteral;
}

void SparqlParser::BooleanLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanLiteral(this);
}

void SparqlParser::BooleanLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanLiteral(this);
}


std::any SparqlParser::BooleanLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBooleanLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BooleanLiteralContext* SparqlParser::booleanLiteral() {
  BooleanLiteralContext *_localctx = _tracker.createInstance<BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 128, SparqlParser::RuleBooleanLiteral);
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
    setState(590);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::TRUE

    || _la == SparqlParser::FALSE)) {
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

//----------------- String_Context ------------------------------------------------------------------

SparqlParser::String_Context::String_Context(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::String_Context::STRING_LITERAL1() {
  return getToken(SparqlParser::STRING_LITERAL1, 0);
}

tree::TerminalNode* SparqlParser::String_Context::STRING_LITERAL2() {
  return getToken(SparqlParser::STRING_LITERAL2, 0);
}


size_t SparqlParser::String_Context::getRuleIndex() const {
  return SparqlParser::RuleString_;
}

void SparqlParser::String_Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterString_(this);
}

void SparqlParser::String_Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitString_(this);
}


std::any SparqlParser::String_Context::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitString_(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::String_Context* SparqlParser::string_() {
  String_Context *_localctx = _tracker.createInstance<String_Context>(_ctx, getState());
  enterRule(_localctx, 130, SparqlParser::RuleString_);
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
    setState(592);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::STRING_LITERAL1

    || _la == SparqlParser::STRING_LITERAL2)) {
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

//----------------- IriRefContext ------------------------------------------------------------------

SparqlParser::IriRefContext::IriRefContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::IriRefContext::IRI_REF() {
  return getToken(SparqlParser::IRI_REF, 0);
}

SparqlParser::PrefixedNameContext* SparqlParser::IriRefContext::prefixedName() {
  return getRuleContext<SparqlParser::PrefixedNameContext>(0);
}


size_t SparqlParser::IriRefContext::getRuleIndex() const {
  return SparqlParser::RuleIriRef;
}

void SparqlParser::IriRefContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIriRef(this);
}

void SparqlParser::IriRefContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIriRef(this);
}


std::any SparqlParser::IriRefContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitIriRef(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IriRefContext* SparqlParser::iriRef() {
  IriRefContext *_localctx = _tracker.createInstance<IriRefContext>(_ctx, getState());
  enterRule(_localctx, 132, SparqlParser::RuleIriRef);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(596);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF: {
        enterOuterAlt(_localctx, 1);
        setState(594);
        match(SparqlParser::IRI_REF);
        break;
      }

      case SparqlParser::PNAME_NS:
      case SparqlParser::PNAME_LN: {
        enterOuterAlt(_localctx, 2);
        setState(595);
        prefixedName();
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

//----------------- PrefixedNameContext ------------------------------------------------------------------

SparqlParser::PrefixedNameContext::PrefixedNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::PrefixedNameContext::PNAME_LN() {
  return getToken(SparqlParser::PNAME_LN, 0);
}

tree::TerminalNode* SparqlParser::PrefixedNameContext::PNAME_NS() {
  return getToken(SparqlParser::PNAME_NS, 0);
}


size_t SparqlParser::PrefixedNameContext::getRuleIndex() const {
  return SparqlParser::RulePrefixedName;
}

void SparqlParser::PrefixedNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrefixedName(this);
}

void SparqlParser::PrefixedNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrefixedName(this);
}


std::any SparqlParser::PrefixedNameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitPrefixedName(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrefixedNameContext* SparqlParser::prefixedName() {
  PrefixedNameContext *_localctx = _tracker.createInstance<PrefixedNameContext>(_ctx, getState());
  enterRule(_localctx, 134, SparqlParser::RulePrefixedName);
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
    setState(598);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::PNAME_NS

    || _la == SparqlParser::PNAME_LN)) {
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

//----------------- BlankNodeContext ------------------------------------------------------------------

SparqlParser::BlankNodeContext::BlankNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BlankNodeContext::BLANK_NODE_LABEL() {
  return getToken(SparqlParser::BLANK_NODE_LABEL, 0);
}

tree::TerminalNode* SparqlParser::BlankNodeContext::ANON() {
  return getToken(SparqlParser::ANON, 0);
}


size_t SparqlParser::BlankNodeContext::getRuleIndex() const {
  return SparqlParser::RuleBlankNode;
}

void SparqlParser::BlankNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNode(this);
}

void SparqlParser::BlankNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlParserListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNode(this);
}


std::any SparqlParser::BlankNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlParserVisitor*>(visitor))
    return parserVisitor->visitBlankNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BlankNodeContext* SparqlParser::blankNode() {
  BlankNodeContext *_localctx = _tracker.createInstance<BlankNodeContext>(_ctx, getState());
  enterRule(_localctx, 136, SparqlParser::RuleBlankNode);
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
    setState(600);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::BLANK_NODE_LABEL

    || _la == SparqlParser::ANON)) {
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

void SparqlParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  sparqlparserParserInitialize();
#else
  ::antlr4::internal::call_once(sparqlparserParserOnceFlag, sparqlparserParserInitialize);
#endif
}
