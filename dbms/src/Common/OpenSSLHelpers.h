#pragma once

#include <Core/Types.h>


namespace DB
{

/**
 * Returns string with concatenation of the error strings for all errors that OpenSSL has recorded, emptying the error queue.
 */
String getOpenSSLErrors();

}
