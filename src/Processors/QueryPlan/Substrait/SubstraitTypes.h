#pragma once

#include "config.h"

#if USE_SUBSTRAIT

#include <expected>
#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/Substrait/SubstraitCommon.h>
#include <substrait/type.pb.h>

/// Converts DataTypePtr to substrait::Type and owns the short-name vocabulary ("i64", "u!u64") of function signatures.

namespace DB
{

/// Maps a ClickHouse type to its exact (never approximate!) Substrait representation. The error is the blocking reason.
std::expected<substrait::Type, String> convertTypeToSubstrait(const DataTypePtr & type, SubstraitPlanExtensions & extensions);

/// The unsigned extension type named `name` ("u8".."u64"), registered in `extensions`.
substrait::Type substraitUnsignedType(std::string_view name, bool nullable, SubstraitPlanExtensions & extensions);

/// Short name of a type produced by convertTypeToSubstrait, for example "i64".
String substraitTypeShortName(const substrait::Type & type, const SubstraitPlanExtensions & extensions);

/// Only works for simple types (taxonomy of types is explained at the top of matching .cpp).
void setSubstraitTypeByShortName(substrait::Type & type, std::string_view short_name);

/// A simple type built from its short name.
substrait::Type makeSimpleSubstraitType(std::string_view short_name, bool nullable);

/// Rank of a signed integer short name ("i8" = 1 .. "i64" = 4), else 0.
int substraitSignedIntRank(std::string_view short_name);

bool isSubstraitFloatShort(std::string_view short_name);

/// "u64" -> "u!u64".
String substraitUserDefinedShortName(std::string_view type_name);

/// "u!u64" -> "u64", or empty.
std::string_view tryParseUserDefinedShortName(std::string_view short_name);

bool substraitTypesIdentical(const substrait::Type & lhs, const substrait::Type & rhs);

substrait::Type_Nullability substraitNullabilityOf(bool nullable);
bool isSubstraitTypeNullable(const substrait::Type & type);
void setSubstraitTypeNullability(substrait::Type & type, bool nullable);

}

#endif
