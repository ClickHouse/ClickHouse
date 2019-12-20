#pragma once
#include "config_core.h"
#include "config_functions.h"

namespace DB
{
class FunctionFactory;

void registerFunctionCurrentDatabase(FunctionFactory &);
void registerFunctionCurrentUser(FunctionFactory &);
void registerFunctionCurrentQuota(FunctionFactory &);
void registerFunctionHostName(FunctionFactory &);
void registerFunctionFQDN(FunctionFactory &);
void registerFunctionVisibleWidth(FunctionFactory &);
void registerFunctionToTypeName(FunctionFactory &);
void registerFunctionGetSizeOfEnumType(FunctionFactory &);
void registerFunctionToColumnTypeName(FunctionFactory &);
void registerFunctionDumpColumnStructure(FunctionFactory &);
void registerFunctionDefaultValueOfArgumentType(FunctionFactory &);
void registerFunctionBlockSize(FunctionFactory &);
void registerFunctionBlockNumber(FunctionFactory &);
void registerFunctionRowNumberInBlock(FunctionFactory &);
void registerFunctionRowNumberInAllBlocks(FunctionFactory &);
void registerFunctionNeighbor(FunctionFactory &);
void registerFunctionSleep(FunctionFactory &);
void registerFunctionSleepEachRow(FunctionFactory &);
void registerFunctionMaterialize(FunctionFactory &);
void registerFunctionIgnore(FunctionFactory &);
void registerFunctionIgnoreExceptNull(FunctionFactory &);
void registerFunctionIndexHint(FunctionFactory &);
void registerFunctionIdentity(FunctionFactory &);
void registerFunctionReplicate(FunctionFactory &);
void registerFunctionBar(FunctionFactory &);
void registerFunctionHasColumnInTable(FunctionFactory &);
void registerFunctionIsFinite(FunctionFactory &);
void registerFunctionIsInfinite(FunctionFactory &);
void registerFunctionIsNaN(FunctionFactory &);
void registerFunctionThrowIf(FunctionFactory &);
void registerFunctionVersion(FunctionFactory &);
void registerFunctionUptime(FunctionFactory &);
void registerFunctionTimeZone(FunctionFactory &);
void registerFunctionRunningAccumulate(FunctionFactory &);
void registerFunctionRunningDifference(FunctionFactory &);
void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory &);
void registerFunctionFinalizeAggregation(FunctionFactory &);
void registerFunctionToLowCardinality(FunctionFactory &);
void registerFunctionLowCardinalityIndices(FunctionFactory &);
void registerFunctionLowCardinalityKeys(FunctionFactory &);
void registerFunctionsIn(FunctionFactory &);
void registerFunctionJoinGet(FunctionFactory &);
void registerFunctionFilesystem(FunctionFactory &);
void registerFunctionEvalMLMethod(FunctionFactory &);
void registerFunctionBasename(FunctionFactory &);
void registerFunctionTransform(FunctionFactory &);
void registerFunctionGetMacro(FunctionFactory &);
void registerFunctionGetScalar(FunctionFactory &);

#if USE_ICU
void registerFunctionConvertCharset(FunctionFactory &);
#endif

void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsTuple(FunctionFactory &);
void registerFunctionsBitmap(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsEmbeddedDictionaries(FunctionFactory &);
void registerFunctionsExternalDictionaries(FunctionFactory &);
void registerFunctionsExternalModels(FunctionFactory &);
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
void registerFunctionsStringRegex(FunctionFactory &);
void registerFunctionsStringSimilarity(FunctionFactory &);
void registerFunctionsVisitParam(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
void registerFunctionsIntrospection(FunctionFactory &);
void registerFunctionsNull(FunctionFactory &);
void registerFunctionsFindCluster(FunctionFactory &);
void registerFunctionsJSON(FunctionFactory &);
void registerFunctionsConsistentHashing(FunctionFactory & factory);

void registerFunctionPlus(FunctionFactory & factory);
void registerFunctionMinus(FunctionFactory & factory);
void registerFunctionMultiply(FunctionFactory & factory);
void registerFunctionDivide(FunctionFactory & factory);
void registerFunctionIntDiv(FunctionFactory & factory);
void registerFunctionIntDivOrZero(FunctionFactory & factory);
void registerFunctionModulo(FunctionFactory & factory);
void registerFunctionNegate(FunctionFactory & factory);
void registerFunctionAbs(FunctionFactory & factory);
void registerFunctionBitAnd(FunctionFactory & factory);
void registerFunctionBitOr(FunctionFactory & factory);
void registerFunctionBitXor(FunctionFactory & factory);
void registerFunctionBitNot(FunctionFactory & factory);
void registerFunctionBitShiftLeft(FunctionFactory & factory);
void registerFunctionBitShiftRight(FunctionFactory & factory);
void registerFunctionBitRotateLeft(FunctionFactory & factory);
void registerFunctionBitRotateRight(FunctionFactory & factory);
void registerFunctionLeast(FunctionFactory & factory);
void registerFunctionGreatest(FunctionFactory & factory);
void registerFunctionBitTest(FunctionFactory & factory);
void registerFunctionBitTestAny(FunctionFactory & factory);
void registerFunctionBitTestAll(FunctionFactory & factory);
void registerFunctionGCD(FunctionFactory & factory);
void registerFunctionLCM(FunctionFactory & factory);
void registerFunctionIntExp2(FunctionFactory & factory);
void registerFunctionIntExp10(FunctionFactory & factory);
void registerFunctionRoundToExp2(FunctionFactory & factory);
void registerFunctionRoundDuration(FunctionFactory & factory);
void registerFunctionRoundAge(FunctionFactory & factory);

void registerFunctionBitBoolMaskOr(FunctionFactory & factory);
void registerFunctionBitBoolMaskAnd(FunctionFactory & factory);
void registerFunctionBitWrapperFunc(FunctionFactory & factory);
void registerFunctionBitSwapLastTwo(FunctionFactory & factory);

void registerFunctionEquals(FunctionFactory & factory);
void registerFunctionNotEquals(FunctionFactory & factory);
void registerFunctionLess(FunctionFactory & factory);
void registerFunctionGreater(FunctionFactory & factory);
void registerFunctionLessOrEquals(FunctionFactory & factory);
void registerFunctionGreaterOrEquals(FunctionFactory & factory);

void registerFunctionIf(FunctionFactory & factory);
void registerFunctionMultiIf(FunctionFactory & factory);
void registerFunctionCaseWithExpression(FunctionFactory & factory);

void registerFunctionYandexConsistentHash(FunctionFactory & factory);
void registerFunctionJumpConsistentHash(FunctionFactory & factory);
void registerFunctionSumburConsistentHash(FunctionFactory & factory);

void registerFunctionToYear(FunctionFactory &);
void registerFunctionToQuarter(FunctionFactory &);
void registerFunctionToMonth(FunctionFactory &);
void registerFunctionToDayOfMonth(FunctionFactory &);
void registerFunctionToDayOfWeek(FunctionFactory &);
void registerFunctionToDayOfYear(FunctionFactory &);
void registerFunctionToHour(FunctionFactory &);
void registerFunctionToMinute(FunctionFactory &);
void registerFunctionToSecond(FunctionFactory &);
void registerFunctionToStartOfDay(FunctionFactory &);
void registerFunctionToMonday(FunctionFactory &);
void registerFunctionToISOWeek(FunctionFactory &);
void registerFunctionToISOYear(FunctionFactory &);
void registerFunctionToCustomWeek(FunctionFactory &);
void registerFunctionToStartOfMonth(FunctionFactory &);
void registerFunctionToStartOfQuarter(FunctionFactory &);
void registerFunctionToStartOfYear(FunctionFactory &);
void registerFunctionToStartOfMinute(FunctionFactory &);
void registerFunctionToStartOfFiveMinute(FunctionFactory &);
void registerFunctionToStartOfTenMinutes(FunctionFactory &);
void registerFunctionToStartOfFifteenMinutes(FunctionFactory &);
void registerFunctionToStartOfHour(FunctionFactory &);
void registerFunctionToStartOfInterval(FunctionFactory &);
void registerFunctionToStartOfISOYear(FunctionFactory &);
void registerFunctionToRelativeYearNum(FunctionFactory &);
void registerFunctionToRelativeQuarterNum(FunctionFactory &);
void registerFunctionToRelativeMonthNum(FunctionFactory &);
void registerFunctionToRelativeWeekNum(FunctionFactory &);
void registerFunctionToRelativeDayNum(FunctionFactory &);
void registerFunctionToRelativeHourNum(FunctionFactory &);
void registerFunctionToRelativeMinuteNum(FunctionFactory &);
void registerFunctionToRelativeSecondNum(FunctionFactory &);
void registerFunctionToTime(FunctionFactory &);
void registerFunctionNow(FunctionFactory &);
void registerFunctionNow64(FunctionFactory &);
void registerFunctionToday(FunctionFactory &);
void registerFunctionYesterday(FunctionFactory &);
void registerFunctionTimeSlot(FunctionFactory &);
void registerFunctionTimeSlots(FunctionFactory &);
void registerFunctionToYYYYMM(FunctionFactory &);
void registerFunctionToYYYYMMDD(FunctionFactory &);
void registerFunctionToYYYYMMDDhhmmss(FunctionFactory &);
void registerFunctionAddSeconds(FunctionFactory &);
void registerFunctionAddMinutes(FunctionFactory &);
void registerFunctionAddHours(FunctionFactory &);
void registerFunctionAddDays(FunctionFactory &);
void registerFunctionAddWeeks(FunctionFactory &);
void registerFunctionAddMonths(FunctionFactory &);
void registerFunctionAddQuarters(FunctionFactory &);
void registerFunctionAddYears(FunctionFactory &);
void registerFunctionSubtractSeconds(FunctionFactory &);
void registerFunctionSubtractMinutes(FunctionFactory &);
void registerFunctionSubtractHours(FunctionFactory &);
void registerFunctionSubtractDays(FunctionFactory &);
void registerFunctionSubtractWeeks(FunctionFactory &);
void registerFunctionSubtractMonths(FunctionFactory &);
void registerFunctionSubtractQuarters(FunctionFactory &);
void registerFunctionSubtractYears(FunctionFactory &);
void registerFunctionDateDiff(FunctionFactory &);
void registerFunctionToTimeZone(FunctionFactory &);
void registerFunctionFormatDateTime(FunctionFactory &);

void registerFunctionGeoDistance(FunctionFactory & factory);
void registerFunctionPointInEllipses(FunctionFactory & factory);
void registerFunctionPointInPolygon(FunctionFactory & factory);
void registerFunctionGeohashEncode(FunctionFactory & factory);
void registerFunctionGeohashDecode(FunctionFactory & factory);
void registerFunctionGeohashesInBox(FunctionFactory & factory);

#if USE_H3
void registerFunctionGeoToH3(FunctionFactory &);
void registerFunctionH3EdgeAngle(FunctionFactory &);
void registerFunctionH3EdgeLengthM(FunctionFactory &);
void registerFunctionH3GetResolution(FunctionFactory &);
void registerFunctionH3IsValid(FunctionFactory &);
void registerFunctionH3KRing(FunctionFactory &);
#endif

#if defined(OS_LINUX)
void registerFunctionAddressToSymbol(FunctionFactory & factory);
void registerFunctionAddressToLine(FunctionFactory & factory);
#endif
void registerFunctionDemangle(FunctionFactory & factory);
void registerFunctionTrap(FunctionFactory & factory);

void registerFunctionE(FunctionFactory & factory);
void registerFunctionPi(FunctionFactory & factory);
void registerFunctionExp(FunctionFactory & factory);
void registerFunctionLog(FunctionFactory & factory);
void registerFunctionExp2(FunctionFactory & factory);
void registerFunctionLog2(FunctionFactory & factory);
void registerFunctionExp10(FunctionFactory & factory);
void registerFunctionLog10(FunctionFactory & factory);
void registerFunctionSqrt(FunctionFactory & factory);
void registerFunctionCbrt(FunctionFactory & factory);
void registerFunctionErf(FunctionFactory & factory);
void registerFunctionErfc(FunctionFactory & factory);
void registerFunctionLGamma(FunctionFactory & factory);
void registerFunctionTGamma(FunctionFactory & factory);
void registerFunctionSin(FunctionFactory & factory);
void registerFunctionCos(FunctionFactory & factory);
void registerFunctionTan(FunctionFactory & factory);
void registerFunctionAsin(FunctionFactory & factory);
void registerFunctionAcos(FunctionFactory & factory);
void registerFunctionAtan(FunctionFactory & factory);
void registerFunctionSigmoid(FunctionFactory & factory);
void registerFunctionTanh(FunctionFactory & factory);
void registerFunctionPow(FunctionFactory & factory);

void registerFunctionIsNull(FunctionFactory & factory);
void registerFunctionIsNotNull(FunctionFactory & factory);
void registerFunctionCoalesce(FunctionFactory & factory);
void registerFunctionIfNull(FunctionFactory & factory);
void registerFunctionNullIf(FunctionFactory & factory);
void registerFunctionAssumeNotNull(FunctionFactory & factory);
void registerFunctionToNullable(FunctionFactory & factory);

void registerFunctionRand(FunctionFactory & factory);
void registerFunctionRand64(FunctionFactory & factory);
void registerFunctionRandConstant(FunctionFactory & factory);
void registerFunctionGenerateUUIDv4(FunctionFactory & factory);

void registerFunctionRepeat(FunctionFactory &);
void registerFunctionEmpty(FunctionFactory &);
void registerFunctionNotEmpty(FunctionFactory &);
void registerFunctionLengthUTF8(FunctionFactory &);
void registerFunctionIsValidUTF8(FunctionFactory &);
void registerFunctionToValidUTF8(FunctionFactory &);
void registerFunctionLower(FunctionFactory &);
void registerFunctionUpper(FunctionFactory &);
void registerFunctionLowerUTF8(FunctionFactory &);
void registerFunctionUpperUTF8(FunctionFactory &);
void registerFunctionReverse(FunctionFactory &);
void registerFunctionReverseUTF8(FunctionFactory &);
void registerFunctionsConcat(FunctionFactory &);
void registerFunctionFormat(FunctionFactory &);
void registerFunctionSubstring(FunctionFactory &);
void registerFunctionCRC(FunctionFactory &);
void registerFunctionAppendTrailingCharIfAbsent(FunctionFactory &);
void registerFunctionStartsWith(FunctionFactory &);
void registerFunctionEndsWith(FunctionFactory &);
void registerFunctionTrim(FunctionFactory &);
void registerFunctionRegexpQuoteMeta(FunctionFactory &);

#if USE_BASE64
void registerFunctionBase64Encode(FunctionFactory &);
void registerFunctionBase64Decode(FunctionFactory &);
void registerFunctionTryBase64Decode(FunctionFactory &);
#endif

void registerFunctionTuple(FunctionFactory &);
void registerFunctionTupleElement(FunctionFactory &);

void registerFunctionVisitParamHas(FunctionFactory & factory);
void registerFunctionVisitParamExtractUInt(FunctionFactory & factory);
void registerFunctionVisitParamExtractInt(FunctionFactory & factory);
void registerFunctionVisitParamExtractFloat(FunctionFactory & factory);
void registerFunctionVisitParamExtractBool(FunctionFactory & factory);
void registerFunctionVisitParamExtractRaw(FunctionFactory & factory);
void registerFunctionVisitParamExtractString(FunctionFactory & factory);

void registerFunctions();

}
