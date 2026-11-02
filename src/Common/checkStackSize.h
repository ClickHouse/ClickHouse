#pragma once

#include <cstddef>

/** If the stack is large enough and is near its size, throw an exception.
  * You can call this function in "heavy" functions that may be called recursively
  *  to prevent possible stack overflows.
  */
void checkStackSize();

/** How many more bytes of stack can be used before `checkStackSize` throws: zero if it would throw
  * already, and the maximum value if the stack size is not known or is not checked, as in a coroutine.
  * Does not throw. Lets code that runs deep in a recursion ask whether a recursive call fits, and skip
  * it if it does not.
  */
size_t getStackSizeHeadroom();
