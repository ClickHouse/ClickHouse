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
	static const std::unordered_map<
		std::string,
		std::function<IFunction* (const Context & context)>> functions =
	{
#define F [](const Context & context)
		{"plus", 			F { return new FunctionPlus; } },
		{"minus", 			F { return new FunctionMinus; } },
		{"multiply", 		F { return new FunctionMultiply; } },
		{"divide", 			F { return new FunctionDivideFloating; } },
		{"intDiv", 			F { return new FunctionDivideIntegral; } },
		{"modulo", 			F { return new FunctionModulo; } },
		{"negate", 			F { return new FunctionNegate; } },
		{"bitAnd", 			F { return new FunctionBitAnd; } },
		{"bitOr", 			F { return new FunctionBitOr; } },
		{"bitXor", 			F { return new FunctionBitXor; } },
		{"bitNot", 			F { return new FunctionBitNot; } },
		{"bitShiftLeft", 	F { return new FunctionBitShiftLeft; } },
		{"bitShiftRight", 	F { return new FunctionBitShiftRight; } },

		{"equals", 			F { return new FunctionEquals; } },
		{"notEquals", 		F { return new FunctionNotEquals; } },
		{"less", 			F { return new FunctionLess; } },
		{"greater", 		F { return new FunctionGreater; } },
		{"lessOrEquals", 	F { return new FunctionLessOrEquals; } },
		{"greaterOrEquals", F { return new FunctionGreaterOrEquals; } },

		{"and", 			F { return new FunctionAnd; } },
		{"or", 				F { return new FunctionOr; } },
		{"xor", 			F { return new FunctionXor; } },
		{"not", 			F { return new FunctionNot; } },

		{"roundToExp2", 	F { return new FunctionRoundToExp2; } },
		{"roundDuration", 	F { return new FunctionRoundDuration; } },
		{"roundAge", 		F { return new FunctionRoundAge; } },

		{"empty", 				F { return new FunctionEmpty; } },
		{"notEmpty", 			F { return new FunctionNotEmpty; } },
		{"length", 				F { return new FunctionLength; } },
		{"lengthUTF8", 			F { return new FunctionLengthUTF8; } },
		{"lower", 				F { return new FunctionLower; } },
		{"upper", 				F { return new FunctionUpper; } },
		{"lowerUTF8", 			F { return new FunctionLowerUTF8; } },
		{"upperUTF8", 			F { return new FunctionUpperUTF8; } },
		{"reverse", 			F { return new FunctionReverse; } },
		{"reverseUTF8", 		F { return new FunctionReverseUTF8; } },
		{"concat", 				F { return new FunctionConcat; } },
		{"substring", 			F { return new FunctionSubstring; } },
		{"replaceOne", 			F { return new FunctionReplaceOne; } },
		{"replaceAll", 			F { return new FunctionReplaceAll; } },
		{"replaceRegexpOne", 	F { return new FunctionReplaceRegexpOne; } },
		{"replaceRegexpAll", 	F { return new FunctionReplaceRegexpAll; } },
		{"substringUTF8", 		F { return new FunctionSubstringUTF8; } },

		{"toUInt8", 			F { return new FunctionToUInt8; } },
		{"toUInt16", 			F { return new FunctionToUInt16; } },
		{"toUInt32", 			F { return new FunctionToUInt32; } },
		{"toUInt64", 			F { return new FunctionToUInt64; } },
		{"toInt8", 				F { return new FunctionToInt8; } },
		{"toInt16", 			F { return new FunctionToInt16; } },
		{"toInt32", 			F { return new FunctionToInt32; } },
		{"toInt64", 			F { return new FunctionToInt64; } },
		{"toFloat32", 			F { return new FunctionToFloat32; } },
		{"toFloat64", 			F { return new FunctionToFloat64; } },
		{"toDate", 				F { return new FunctionToDate; } },
		{"toDateTime", 			F { return new FunctionToDateTime; } },
		{"toString", 			F { return new FunctionToString; } },
		{"toFixedString", 		F { return new FunctionToFixedString; } },
		{"toStringCutToZero", 	F { return new FunctionToStringCutToZero; } },

		{"reinterpretAsUInt8",		F { return new FunctionReinterpretAsUInt8; } },
		{"reinterpretAsUInt16", 	F { return new FunctionReinterpretAsUInt16; } },
		{"reinterpretAsUInt32", 	F { return new FunctionReinterpretAsUInt32; } },
		{"reinterpretAsUInt64", 	F { return new FunctionReinterpretAsUInt64; } },
		{"reinterpretAsInt8", 		F { return new FunctionReinterpretAsInt8; } },
		{"reinterpretAsInt16", 		F { return new FunctionReinterpretAsInt16; } },
		{"reinterpretAsInt32", 		F { return new FunctionReinterpretAsInt32; } },
		{"reinterpretAsInt64", 		F { return new FunctionReinterpretAsInt64; } },
		{"reinterpretAsFloat32", 	F { return new FunctionReinterpretAsFloat32; } },
		{"reinterpretAsFloat64", 	F { return new FunctionReinterpretAsFloat64; } },
		{"reinterpretAsDate", 		F { return new FunctionReinterpretAsDate; } },
		{"reinterpretAsDateTime", 	F { return new FunctionReinterpretAsDateTime; } },
		{"reinterpretAsString", 	F { return new FunctionReinterpretAsString; } },

		{"toYear", 				F { return new FunctionToYear; } },
		{"toMonth",				F { return new FunctionToMonth; } },
		{"toDayOfMonth", 		F { return new FunctionToDayOfMonth; } },
		{"toDayOfWeek", 		F { return new FunctionToDayOfWeek; } },
		{"toHour", 				F { return new FunctionToHour; } },
		{"toMinute", 			F { return new FunctionToMinute; } },
		{"toSecond", 			F { return new FunctionToSecond; } },
		{"toMonday", 			F { return new FunctionToMonday; } },
		{"toStartOfMonth", 		F { return new FunctionToStartOfMonth; } },
		{"toStartOfQuarter", 	F { return new FunctionToStartOfQuarter; } },
		{"toStartOfYear", 		F { return new FunctionToStartOfYear; } },
		{"toStartOfMinute", 	F { return new FunctionToStartOfMinute; } },
		{"toStartOfHour", 		F { return new FunctionToStartOfHour; } },
		{"toRelativeYearNum", 	F { return new FunctionToRelativeYearNum; } },
		{"toRelativeMonthNum", 	F { return new FunctionToRelativeMonthNum; } },
		{"toRelativeWeekNum", 	F { return new FunctionToRelativeWeekNum; } },
		{"toRelativeDayNum", 	F { return new FunctionToRelativeDayNum; } },
		{"toRelativeHourNum", 	F { return new FunctionToRelativeHourNum; } },
		{"toRelativeMinuteNum", F { return new FunctionToRelativeMinuteNum; } },
		{"toRelativeSecondNum", F { return new FunctionToRelativeSecondNum; } },
		{"toTime", 				F { return new FunctionToTime; } },
		{"now", 				F { return new FunctionNow; } },
		{"timeSlot", 			F { return new FunctionTimeSlot; } },
		{"timeSlots", 			F { return new FunctionTimeSlots; } },

		{"position", 			F { return new FunctionPosition; } },
		{"positionUTF8", 		F { return new FunctionPositionUTF8; } },
		{"match", 				F { return new FunctionMatch; } },
		{"like", 				F { return new FunctionLike; } },
		{"notLike", 			F { return new FunctionNotLike; } },
		{"extract", 			F { return new FunctionExtract; } },
		{"extractAll", 			F { return new FunctionExtractAll; } },

		{"halfMD5", 			F { return new FunctionHalfMD5; } },
		{"sipHash64", 			F { return new FunctionSipHash64; } },
		{"cityHash64", 			F { return new FunctionCityHash64; } },
		{"intHash32", 			F { return new FunctionIntHash32; } },
		{"intHash64", 			F { return new FunctionIntHash64; } },

		{"IPv4NumToString", 	F { return new FunctionIPv4NumToString; } },
		{"IPv4StringToNum", 	F { return new FunctionIPv4StringToNum; } },
		{"hex", 				F { return new FunctionHex; } },
		{"unhex", 				F { return new FunctionUnhex; } },
		{"bitmaskToList", 		F { return new FunctionBitmaskToList; } },
		{"bitmaskToArray",		F { return new FunctionBitmaskToArray; } },

		{"rand", 				F { return new FunctionRand; } },
		{"rand64", 				F { return new FunctionRand64; } },

		{"protocol", 					F { return new FunctionProtocol; } },
		{"domain", 						F { return new FunctionDomain; } },
		{"domainWithoutWWW", 			F { return new FunctionDomainWithoutWWW; } },
		{"topLevelDomain", 				F { return new FunctionTopLevelDomain; } },
		{"path", 						F { return new FunctionPath; } },
		{"queryString", 				F { return new FunctionQueryString; } },
		{"fragment", 					F { return new FunctionFragment; } },
		{"queryStringAndFragment", 		F { return new FunctionQueryStringAndFragment; } },
		{"extractURLParameter", 		F { return new FunctionExtractURLParameter; } },
		{"extractURLParameters", 		F { return new FunctionExtractURLParameters; } },
		{"extractURLParameterNames", 	F { return new FunctionExtractURLParameterNames; } },
		{"URLHierarchy", 				F { return new FunctionURLHierarchy; } },
		{"URLPathHierarchy", 			F { return new FunctionURLPathHierarchy; } },
		{"cutWWW", 						F { return new FunctionCutWWW; } },
		{"cutQueryString", 				F { return new FunctionCutQueryString; } },
		{"cutFragment", 				F { return new FunctionCutFragment; } },
		{"cutQueryStringAndFragment", 	F { return new FunctionCutQueryStringAndFragment; } },
		{"cutURLParameter", 			F { return new FunctionCutURLParameter; } },

		{"hostName", 			F { return new FunctionHostName; } },
		{"visibleWidth", 		F { return new FunctionVisibleWidth; } },
		{"bar", 				F { return new FunctionBar; } },
		{"toTypeName", 			F { return new FunctionToTypeName; } },
		{"blockSize", 			F { return new FunctionBlockSize; } },
		{"sleep", 				F { return new FunctionSleep; } },
		{"materialize", 		F { return new FunctionMaterialize; } },
		{"ignore", 				F { return new FunctionIgnore; } },
		{"arrayJoin", 			F { return new FunctionArrayJoin; } },

		{"tuple", 				F { return new FunctionTuple; } },
		{"tupleElement", 		F { return new FunctionTupleElement; } },
		{"in", 					F { return new FunctionIn(false, false); } },
		{"notIn", 				F { return new FunctionIn(true, false); } },
		{"globalIn", 			F { return new FunctionIn(false, true); } },
		{"globalNotIn", 		F { return new FunctionIn(true, true); } },

		{"array", 				F { return new FunctionArray; } },
		{"arrayElement", 		F { return new FunctionArrayElement; } },
		{"has", 				F { return new FunctionHas; } },
		{"indexOf", 			F { return new FunctionIndexOf; } },
		{"countEqual", 			F { return new FunctionCountEqual; } },
		{"arrayEnumerate", 		F { return new FunctionArrayEnumerate; } },
		{"arrayEnumerateUniq", 	F { return new FunctionArrayEnumerateUniq; } },

		{"arrayMap", 			F { return new FunctionArrayMap; } },
		{"arrayFilter", 		F { return new FunctionArrayFilter; } },
		{"arrayCount", 			F { return new FunctionArrayCount; } },
		{"arrayExists", 		F { return new FunctionArrayExists; } },
		{"arrayAll", 			F { return new FunctionArrayAll; } },
		{"arraySum", 			F { return new FunctionArraySum; } },

		{"alphaTokens", 		F { return new FunctionAlphaTokens; } },
		{"splitByChar", 		F { return new FunctionSplitByChar; } },
		{"splitByString", 		F { return new FunctionSplitByString; } },

		{"if", 					F { return new FunctionIf; } },

		{"regionToCity", 			F { return new FunctionRegionToCity(context.getDictionaries().getRegionsHierarchies()); } },
		{"regionToArea", 			F { return new FunctionRegionToArea(context.getDictionaries().getRegionsHierarchies()); } },
		{"regionToCountry", 		F { return new FunctionRegionToCountry(context.getDictionaries().getRegionsHierarchies()); } },
		{"regionToContinent", 		F { return new FunctionRegionToContinent(context.getDictionaries().getRegionsHierarchies()); } },
		{"OSToRoot", 				F { return new FunctionOSToRoot(context.getDictionaries().getTechDataHierarchy()); } },
		{"SEToRoot", 				F { return new FunctionSEToRoot(context.getDictionaries().getTechDataHierarchy()); } },
		{"categoryToRoot", 			F { return new FunctionCategoryToRoot(context.getDictionaries().getCategoriesHierarchy()); } },
		{"categoryToSecondLevel", 	F { return new FunctionCategoryToSecondLevel(context.getDictionaries().getCategoriesHierarchy()); } },
		{"regionIn", 				F { return new FunctionRegionIn(context.getDictionaries().getRegionsHierarchies()); } },
		{"OSIn", 					F { return new FunctionOSIn(context.getDictionaries().getTechDataHierarchy()); } },
		{"SEIn", 					F { return new FunctionSEIn(context.getDictionaries().getTechDataHierarchy()); } },
		{"categoryIn", 				F { return new FunctionCategoryIn(context.getDictionaries().getCategoriesHierarchy()); } },
		{"regionHierarchy", 		F { return new FunctionRegionHierarchy(context.getDictionaries().getRegionsHierarchies()); } },
		{"OSHierarchy", 			F { return new FunctionOSHierarchy(context.getDictionaries().getTechDataHierarchy()); } },
		{"SEHierarchy", 			F { return new FunctionSEHierarchy(context.getDictionaries().getTechDataHierarchy()); } },
		{"categoryHierarchy", 		F { return new FunctionCategoryHierarchy(context.getDictionaries().getCategoriesHierarchy()); } },
		{"regionToName", 			F { return new FunctionRegionToName(context.getDictionaries().getRegionsNames()); } },

		{"visitParamHas", 			F { return new FunctionVisitParamHas; } },
		{"visitParamExtractUInt", 	F { return new FunctionVisitParamExtractUInt; } },
		{"visitParamExtractInt", 	F { return new FunctionVisitParamExtractInt; } },
		{"visitParamExtractFloat", 	F { return new FunctionVisitParamExtractFloat; } },
		{"visitParamExtractBool", 	F { return new FunctionVisitParamExtractBool; } },
		{"visitParamExtractRaw", 	F { return new FunctionVisitParamExtractRaw; } },
		{"visitParamExtractString", F { return new FunctionVisitParamExtractString; } },
	};

	auto it = functions.find(name);
	if (functions.end() != it)
		return it->second(context);
	else
		throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
}

}
