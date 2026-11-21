#pragma once

#include <cstddef>

/** If the stack is large enough and is near its size, throw an exception.
  * You can call this function in "heavy" functions that may be called recursively
  *  to prevent possible stack overflows.
  */
void checkStackSize();

/** How many more bytes of stack can be used before `checkStackSize` throws: zero if it would throw
  * already, and the maximum value if the stack size is not known, or the code runs in a coroutine or a
  * Silk fiber. Unlike `checkStackSize`, it does not look at the stack of a coroutine, where that
  * throws near the bottom. It does not throw because of the depth of the stack, but the first call in
  * a thread reads the bounds of the stack, and throws `ErrnoException` if the system cannot give them.
  * Lets code that runs deep in a recursion ask whether a recursive call fits, and skip it if it does not.
  */
size_t getStackSizeHeadroom();
