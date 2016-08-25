#include <DB/Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_FUNCTION;
}


/** Эти функции определены в отдельных translation unit-ах.
  * Это сделано для того, чтобы уменьшить потребление оперативки при сборке, и ускорить параллельную сборку.
  */
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsArray(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsDictionaries(FunctionFactory &);
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


FunctionFactory::FunctionFactory()
{
	registerFunctionsArithmetic(*this);
	registerFunctionsArray(*this);
	registerFunctionsCoding(*this);
	registerFunctionsComparison(*this);
	registerFunctionsConditional(*this);
	registerFunctionsConversion(*this);
	registerFunctionsDateTime(*this);
	registerFunctionsDictionaries(*this);
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
}


FunctionPtr FunctionFactory::get(
	const String & name,
	const Context & context) const
{
	auto res = tryGet(name, context);
	if (!res)
		throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
	return res;
}


FunctionPtr FunctionFactory::tryGet(
	const String & name,
	const Context & context) const
{
	auto it = functions.find(name);
	if (functions.end() != it)
		return it->second(context);
	else
		return {};
}

}
