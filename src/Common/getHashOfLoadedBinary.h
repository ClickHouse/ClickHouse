#pragma once

#include <string>
#include <Common/SipHash.h>

/** Calculate hash of the executable loaded segments of the first loaded object.
  * It can be used for integrity checks.
  * Does not work when ClickHouse is build as multiple shared libraries.
  * Note: we don't hash all loaded readonly segments, because some of them are modified by 'strip'
  *  and we want something that survives 'strip'.
  * Note: program behaviour can be affected not only by machine code but also by the data in these segments,
  * so the integrity check is going to be incomplete.
  */
SipHash getHashOfLoadedBinary();
std::string getHashOfLoadedBinaryHex();
