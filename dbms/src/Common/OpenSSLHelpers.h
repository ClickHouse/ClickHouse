#pragma once
#include <Common/config.h>
#if USE_SSL

#include <Core/Types.h>


namespace DB
{

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
