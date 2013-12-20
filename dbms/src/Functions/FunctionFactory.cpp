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
#include <DB/Functions/FunctionsArray.h>
#include <DB/Functions/FunctionsStringArray.h>
#include <DB/Functions/FunctionsConditional.h>
#include <DB/Functions/FunctionsDictionaries.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Functions/FunctionsRound.h>
#include <DB/Functions/FunctionsReinterpret.h>
#include <DB/Functions/FunctionsFormatting.h>
#include <DB/Functions/FunctionsCoding.h>
#include <DB/Functions/FunctionsHigherOrder.h>
#include <DB/Functions/FunctionsVisitParam.h>

#include <DB/Functions/FunctionFactory.h>


namespace DB
{


FunctionPtr FunctionFactory::get(
	const String & name,
	const Context & context) const
{
	/// Немного неоптимально.
	
		 if (name == "plus")						return new FunctionPlus;
	else if (name == "minus")						return new FunctionMinus;
	else if (name == "multiply")					return new FunctionMultiply;
	else if (name == "divide")						return new FunctionDivideFloating;
	else if (name == "intDiv")						return new FunctionDivideIntegral;
	else if (name == "modulo")						return new FunctionModulo;
	else if (name == "negate")						return new FunctionNegate;
	else if (name == "bitAnd")						return new FunctionBitAnd;
	else if (name == "bitOr")						return new FunctionBitOr;
	else if (name == "bitXor")						return new FunctionBitXor;
	else if (name == "bitNot")						return new FunctionBitNot;
	else if (name == "bitShiftLeft")				return new FunctionBitShiftLeft;
	else if (name == "bitShiftRight")				return new FunctionBitShiftRight;

	else if (name == "equals")						return new FunctionEquals;
	else if (name == "notEquals")					return new FunctionNotEquals;
	else if (name == "less")						return new FunctionLess;
	else if (name == "greater")						return new FunctionGreater;
	else if (name == "lessOrEquals")				return new FunctionLessOrEquals;
	else if (name == "greaterOrEquals")			return new FunctionGreaterOrEquals;

	else if (name == "and")							return new FunctionAnd;
	else if (name == "or")							return new FunctionOr;
	else if (name == "xor")							return new FunctionXor;
	else if (name == "not")							return new FunctionNot;
	
	else if (name == "roundToExp2")				return new FunctionRoundToExp2;
	else if (name == "roundDuration")				return new FunctionRoundDuration;
	else if (name == "roundAge")					return new FunctionRoundAge;

	else if (name == "empty")						return new FunctionEmpty;
	else if (name == "notEmpty")					return new FunctionNotEmpty;
	else if (name == "length")						return new FunctionLength;
	else if (name == "lengthUTF8")					return new FunctionLengthUTF8;
	else if (name == "lower")						return new FunctionLower;
	else if (name == "upper")						return new FunctionUpper;
	else if (name == "lowerUTF8")					return new FunctionLowerUTF8;
	else if (name == "upperUTF8")					return new FunctionUpperUTF8;
	else if (name == "reverse")						return new FunctionReverse;
	else if (name == "reverseUTF8")				return new FunctionReverseUTF8;
	else if (name == "concat")						return new FunctionConcat;
	else if (name == "substring")					return new FunctionSubstring;
	else if (name == "substringUTF8")				return new FunctionSubstringUTF8;
	else if (name == "bitmaskToList")				return new FunctionBitmaskToList;
	else if (name == "bitmaskToArray")				return new FunctionBitmaskToArray;

	else if (name == "toUInt8")						return new FunctionToUInt8;
	else if (name == "toUInt16")					return new FunctionToUInt16;
	else if (name == "toUInt32")					return new FunctionToUInt32;
	else if (name == "toUInt64")					return new FunctionToUInt64;
	else if (name == "toInt8")						return new FunctionToInt8;
	else if (name == "toInt16")						return new FunctionToInt16;
	else if (name == "toInt32")						return new FunctionToInt32;
	else if (name == "toInt64")						return new FunctionToInt64;
	else if (name == "toFloat32")					return new FunctionToFloat32;
	else if (name == "toFloat64")					return new FunctionToFloat64;
	else if (name == "toDate")						return new FunctionToDate;
	else if (name == "toDateTime")					return new FunctionToDateTime;
	else if (name == "toString")					return new FunctionToString;
	else if (name == "toFixedString")				return new FunctionToFixedString;

	else if (name == "reinterpretAsUInt8")			return new FunctionReinterpretAsUInt8;
	else if (name == "reinterpretAsUInt16")		return new FunctionReinterpretAsUInt16;
	else if (name == "reinterpretAsUInt32")		return new FunctionReinterpretAsUInt32;
	else if (name == "reinterpretAsUInt64")		return new FunctionReinterpretAsUInt64;
	else if (name == "reinterpretAsInt8")			return new FunctionReinterpretAsInt8;
	else if (name == "reinterpretAsInt16")			return new FunctionReinterpretAsInt16;
	else if (name == "reinterpretAsInt32")			return new FunctionReinterpretAsInt32;
	else if (name == "reinterpretAsInt64")			return new FunctionReinterpretAsInt64;
	else if (name == "reinterpretAsFloat32")		return new FunctionReinterpretAsFloat32;
	else if (name == "reinterpretAsFloat64")		return new FunctionReinterpretAsFloat64;
	else if (name == "reinterpretAsDate")			return new FunctionReinterpretAsDate;
	else if (name == "reinterpretAsDateTime")		return new FunctionReinterpretAsDateTime;
	else if (name == "reinterpretAsString")		return new FunctionReinterpretAsString;

	else if (name == "toYear")						return new FunctionToYear;
	else if (name == "toMonth")						return new FunctionToMonth;
	else if (name == "toDayOfMonth")				return new FunctionToDayOfMonth;
	else if (name == "toDayOfWeek")				return new FunctionToDayOfWeek;
	else if (name == "toHour")						return new FunctionToHour;
	else if (name == "toMinute")					return new FunctionToMinute;
	else if (name == "toSecond")					return new FunctionToSecond;
	else if (name == "toMonday")					return new FunctionToMonday;
	else if (name == "toStartOfMonth")				return new FunctionToStartOfMonth;
	else if (name == "toStartOfQuarter")			return new FunctionToStartOfQuarter;
	else if (name == "toStartOfYear")				return new FunctionToStartOfYear;
	else if (name == "toStartOfMinute")			return new FunctionToStartOfMinute;
	else if (name == "toStartOfHour")				return new FunctionToStartOfHour;
	else if (name == "toTime")						return new FunctionToTime;
	else if (name == "now")							return new FunctionNow;
	else if (name == "timeSlot")					return new FunctionTimeSlot;
	else if (name == "timeSlots")					return new FunctionTimeSlots;

	else if (name == "position")					return new FunctionPosition;
	else if (name == "positionUTF8")				return new FunctionPositionUTF8;
	else if (name == "match")						return new FunctionMatch;
	else if (name == "like")						return new FunctionLike;
	else if (name == "notLike")						return new FunctionNotLike;
	else if (name == "extract")						return new FunctionExtract;
	else if (name == "extractAll")					return new FunctionExtractAll;

	else if (name == "halfMD5")						return new FunctionHalfMD5;
	else if (name == "sipHash64")					return new FunctionSipHash64;
	else if (name == "cityHash64")					return new FunctionCityHash64;
	else if (name == "intHash32")					return new FunctionIntHash32;
	
	else if (name == "IPv4NumToString")			return new FunctionIPv4NumToString;
	else if (name == "IPv4StringToNum")			return new FunctionIPv4StringToNum;
	else if (name == "hex")							return new FunctionHex;
	else if (name == "unhex")						return new FunctionUnhex;
	else if (name == "toStringCutToZero")			return new FunctionToStringCutToZero;

	else if (name == "rand")						return new FunctionRand;
	else if (name == "rand64")						return new FunctionRand64;

	else if (name == "protocol")					return new FunctionProtocol;
	else if (name == "domain")						return new FunctionDomain;
	else if (name == "domainWithoutWWW")			return new FunctionDomainWithoutWWW;
	else if (name == "topLevelDomain")				return new FunctionTopLevelDomain;
	else if (name == "path")						return new FunctionPath;
	else if (name == "queryString")				return new FunctionQueryString;
	else if (name == "fragment")					return new FunctionFragment;
	else if (name == "queryStringAndFragment")		return new FunctionQueryStringAndFragment;
	else if (name == "cutWWW")						return new FunctionCutWWW;
	else if (name == "cutQueryString")				return new FunctionCutQueryString;
	else if (name == "cutFragment")				return new FunctionCutFragment;
	else if (name == "cutQueryStringAndFragment")	return new FunctionCutQueryStringAndFragment;
	else if (name == "extractURLParameter")		return new FunctionExtractURLParameter;
	else if (name == "extractURLParameters")		return new FunctionExtractURLParameters;
	else if (name == "extractURLParameterNames")		return new FunctionExtractURLParameterNames;
	else if (name == "cutURLParameter")			return new FunctionCutURLParameter;
	else if (name == "URLHierarchy")				return new FunctionURLHierarchy;

	else if (name == "hostName")					return new FunctionHostName;
	else if (name == "visibleWidth")				return new FunctionVisibleWidth;
	else if (name == "toTypeName")					return new FunctionToTypeName;
	else if (name == "blockSize")					return new FunctionBlockSize;
	else if (name == "sleep")						return new FunctionSleep;
	else if (name == "materialize")				return new FunctionMaterialize;
	else if (name == "ignore")						return new FunctionIgnore;
	else if (name == "arrayJoin")					return new FunctionArrayJoin;

	else if (name == "tuple")						return new FunctionTuple;
	else if (name == "tupleElement")				return new FunctionTupleElement;
	else if (name == "in")							return new FunctionIn;
	else if (name == "notIn")						return new FunctionIn(true);

	else if (name == "array")						return new FunctionArray;
	else if (name == "arrayElement")				return new FunctionArrayElement;
	else if (name == "has")							return new FunctionHas;
	else if (name == "indexOf")						return new FunctionIndexOf;
	else if (name == "countEqual")					return new FunctionCountEqual;
	else if (name == "arrayEnumerate")				return new FunctionArrayEnumerate;
	else if (name == "arrayEnumerateUniq")			return new FunctionArrayEnumerateUniq;
	
	else if (name == "arrayMap")					return new FunctionArrayMap;
	else if (name == "arrayFilter")				return new FunctionArrayFilter;
	else if (name == "arrayCount")					return new FunctionArrayCount;
	else if (name == "arrayExists")				return new FunctionArrayExists;
	else if (name == "arrayAll")					return new FunctionArrayAll;
	else if (name == "arraySum")					return new FunctionArraySum;
	
	else if (name == "alphaTokens")				return new FunctionAlphaTokens;
	else if (name == "splitByChar")				return new FunctionSplitByChar;
	else if (name == "splitByString")				return new FunctionSplitByString;

	else if (name == "if")							return new FunctionIf;

	else if (name == "regionToCity")				return new FunctionRegionToCity(context.getDictionaries().getRegionsHierarchy());
	else if (name == "regionToArea")				return new FunctionRegionToArea(context.getDictionaries().getRegionsHierarchy());
	else if (name == "regionToCountry")			return new FunctionRegionToCountry(context.getDictionaries().getRegionsHierarchy());
	else if (name == "OSToRoot")					return new FunctionOSToRoot(context.getDictionaries().getTechDataHierarchy());
	else if (name == "SEToRoot")					return new FunctionSEToRoot(context.getDictionaries().getTechDataHierarchy());
	else if (name == "categoryToRoot")				return new FunctionCategoryToRoot(context.getDictionaries().getCategoriesHierarchy());
	else if (name == "categoryToSecondLevel")		return new FunctionCategoryToSecondLevel(context.getDictionaries().getCategoriesHierarchy());
	else if (name == "regionIn")					return new FunctionRegionIn(context.getDictionaries().getRegionsHierarchy());
	else if (name == "OSIn")						return new FunctionOSIn(context.getDictionaries().getTechDataHierarchy());
	else if (name == "SEIn")						return new FunctionSEIn(context.getDictionaries().getTechDataHierarchy());
	else if (name == "categoryIn")					return new FunctionCategoryIn(context.getDictionaries().getCategoriesHierarchy());
	else if (name == "regionHierarchy")			return new FunctionRegionHierarchy(context.getDictionaries().getRegionsHierarchy());
	else if (name == "OSHierarchy")				return new FunctionOSHierarchy(context.getDictionaries().getTechDataHierarchy());
	else if (name == "SEHierarchy")				return new FunctionSEHierarchy(context.getDictionaries().getTechDataHierarchy());
	else if (name == "categoryHierarchy")			return new FunctionCategoryHierarchy(context.getDictionaries().getCategoriesHierarchy());
	else if (name == "regionToName")				return new FunctionRegionToName(context.getDictionaries().getRegionsNames());
	
	else if (name == "visitParamHas")				return new FunctionVisitParamHas;
	else if (name == "visitParamExtractUInt")		return new FunctionVisitParamExtractUInt;
	else if (name == "visitParamExtractInt")		return new FunctionVisitParamExtractInt;
	else if (name == "visitParamExtractFloat")		return new FunctionVisitParamExtractFloat;
	else if (name == "visitParamExtractBool")		return new FunctionVisitParamExtractBool;
	else if (name == "visitParamExtractRaw")		return new FunctionVisitParamExtractRaw;
	else if (name == "visitParamExtractString")	return new FunctionVisitParamExtractString;

	else
		throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
}

}
