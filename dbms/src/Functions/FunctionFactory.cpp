#include <DB/Functions/FunctionFactory.h>
#include <DB/Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_FUNCTION;
}


/** These functions are defined in a separate translation units.
  * This is done in order to reduce the consumption of RAM during build, and to speed up the parallel build.
  */
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsArray(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsEmbeddedDictionaries(FunctionFactory &);
void registerFunctionsExternalDictionaries(FunctionFactory &);
void registerFunctionsFormatting(FunctionFactory &);
void registerFunctionsHashing(FunctionFactory &);
void registerFunctionsHigherOrder(FunctionFactory &);
void registerFunctionsLogical(FunctionFactory &);
void registerFunctionsMiscellaneous(FunctionFactory &);
void registerFunctionsRandom(FunctionFactory &);
void registerFunctionsReinterpret(FunctionFactory &);
void registerFunctionsRound(FunctionFactory &);
void registerFunctionsString(FunctionFactory &);
void registerFunctionsStringArray(FunctionFactory &);
void registerFunctionsStringSearch(FunctionFactory &);
void registerFunctionsURL(FunctionFactory &);
void registerFunctionsVisitParam(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsTransform(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
void registerFunctionsCharset(FunctionFactory &);
void registerFunctionsNull(FunctionFactory &);


FunctionFactory::FunctionFactory()
{
	registerFunctionsArithmetic(*this);
	registerFunctionsArray(*this);
	registerFunctionsCoding(*this);
	registerFunctionsComparison(*this);
	registerFunctionsConditional(*this);
	registerFunctionsConversion(*this);
	registerFunctionsDateTime(*this);
	registerFunctionsEmbeddedDictionaries(*this);
	registerFunctionsExternalDictionaries(*this);
	registerFunctionsFormatting(*this);
	registerFunctionsHashing(*this);
	registerFunctionsHigherOrder(*this);
	registerFunctionsLogical(*this);
	registerFunctionsMiscellaneous(*this);
	registerFunctionsRandom(*this);
	registerFunctionsReinterpret(*this);
	registerFunctionsRound(*this);
	registerFunctionsString(*this);
	registerFunctionsStringArray(*this);
	registerFunctionsStringSearch(*this);
	registerFunctionsURL(*this);
	registerFunctionsVisitParam(*this);
	registerFunctionsMath(*this);
	registerFunctionsTransform(*this);
	registerFunctionsGeo(*this);
	registerFunctionsCharset(*this);
	registerFunctionsNull(*this);
}


FunctionPtr FunctionFactory::get(
	const std::string & name,
	const Context & context) const
{
	auto res = tryGet(name, context);
	if (!res)
		throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
	return res;
}


FunctionPtr FunctionFactory::tryGet(
	const std::string & name,
	const Context & context) const
{
	auto it = functions.find(name);
	if (functions.end() != it)
		return it->second(context);
	else
		return {};
}

}
