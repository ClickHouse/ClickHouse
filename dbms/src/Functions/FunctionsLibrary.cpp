#include <boost/assign/list_inserter.hpp>

#include <DB/Functions/FunctionsArithmetic.h>
#include <DB/Functions/FunctionsComparison.h>
#include <DB/Functions/FunctionsLogical.h>
#include <DB/Functions/FunctionsString.h>
#include <DB/Functions/FunctionsConversion.h>
#include <DB/Functions/FunctionsDateTime.h>
#include <DB/Functions/FunctionsStringSearch.h>
#include <DB/Functions/FunctionsHashing.h>
#include <DB/Functions/FunctionsRandom.h>
#include <DB/Functions/FunctionsURL.h>
#include <DB/Functions/FunctionsMiscellaneous.h>

#include <DB/Functions/FunctionsLibrary.h>


namespace DB
{

namespace FunctionsLibrary
{
	SharedPtr<Functions> get()
	{
		Functions * res = new Functions;
		
		boost::assign::insert(*res)
			("plus",						new FunctionPlus)
			("minus",						new FunctionMinus)
			("multiply",					new FunctionMultiply)
			("divide",						new FunctionDivideFloating)
			("intDiv",						new FunctionDivideIntegral)
			("modulo",						new FunctionModulo)
			("negate",						new FunctionNegate)

			("equals",						new FunctionEquals)
			("notEquals",					new FunctionNotEquals)
			("less",						new FunctionLess)
			("greater",						new FunctionGreater)
			("lessOrEquals",				new FunctionLessOrEquals)
			("greaterOrEquals",				new FunctionGreaterOrEquals)

			("and",							new FunctionAnd)
			("or",							new FunctionOr)
			("xor",							new FunctionXor)
			("not",							new FunctionNot)

			("length",						new FunctionLength)
			("lengthUTF8",					new FunctionLengthUTF8)
			("lower",						new FunctionLower)
			("upper",						new FunctionUpper)
			("lowerUTF8",					new FunctionLowerUTF8)
			("upperUTF8",					new FunctionUpperUTF8)
			("reverse",						new FunctionReverse)
			("reverseUTF8",					new FunctionReverseUTF8)
			("concat",						new FunctionConcat)
			("substring",					new FunctionSubstring)
			("substringUTF8",				new FunctionSubstringUTF8)

			("toUInt8",						new FunctionToUInt8)
			("toUInt16",					new FunctionToUInt16)
			("toUInt32",					new FunctionToUInt32)
			("toUInt64",					new FunctionToUInt64)
			("toInt8",						new FunctionToInt8)
			("toInt16",						new FunctionToInt16)
			("toInt32",						new FunctionToInt32)
			("toInt64",						new FunctionToInt64)
			("toFloat32",					new FunctionToFloat32)
			("toFloat64",					new FunctionToFloat64)
			("toVarUInt",					new FunctionToVarUInt)
			("toVarInt",					new FunctionToVarInt)
			("toDate",						new FunctionToDate)
			("toDateTime",					new FunctionToDateTime)
			("toString",					new FunctionToString)

			("toYear",						new FunctionToYear)
			("toMonth",						new FunctionToMonth)
			("toDayOfMonth",				new FunctionToDayOfMonth)
			("toDayOfWeek",					new FunctionToDayOfWeek)
			("toHour",						new FunctionToHour)
			("toMinute",					new FunctionToMinute)
			("toSecond",					new FunctionToSecond)
			("toMonday",					new FunctionToMonday)
			("toStartOfMonth",				new FunctionToStartOfMonth)
			("toTime",						new FunctionToTime)

			("position",					new FunctionPosition)
			("positionUTF8",				new FunctionPositionUTF8)
			("match",						new FunctionMatch)
			("like",						new FunctionLike)
			("notLike",						new FunctionNotLike)

			("halfMD5",						new FunctionHalfMD5)
			("cityHash64",					new FunctionCityHash64)
			("intHash32",					new FunctionIntHash32)

			("rand",						new FunctionRand)
			("rand64",						new FunctionRand64)

			("protocol",					new FunctionProtocol)
			("domain",						new FunctionDomain)
			("domainWithoutWWW",			new FunctionDomainWithoutWWW)
			("topLevelDomain",				new FunctionTopLevelDomain)
			("path",						new FunctionPath)
			("queryString",					new FunctionQueryString)
			("fragment",					new FunctionFragment)
			("queryStringAndFragment",		new FunctionQueryStringAndFragment)
			("cutWWW",						new FunctionCutWWW)
			("cutQueryString",				new FunctionCutQueryString)
			("cutFragment",					new FunctionCutFragment)
			("cutQueryStringAndFragment",	new FunctionCutQueryStringAndFragment)

			("visibleWidth",				new FunctionVisibleWidth)
			("toTypeName",					new FunctionToTypeName)
		;

		return res;
	}
};

}
