#pragma once

#include <base/types.h>
#include <Common/PODArray.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names);
UInt64 normalizedQueryHash(const String & query, bool keep_names);
void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names);

}
