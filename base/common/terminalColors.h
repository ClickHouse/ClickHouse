#include <string>
#include <common/types.h>


/** Set color in terminal based on 64-bit hash value.
  * It can be used to choose some random color deterministically based on some other value.
  * Hash value should be uniformly distributed.
  */
std::string setColor(UInt64 hash);

/** Set color for logger priority value. */
const char * setColorForLogPriority(int priority);

/** Undo changes made by the functions above. */
const char * resetColor();
