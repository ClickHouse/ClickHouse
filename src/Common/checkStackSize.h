#pragma once

/** If the stack is large enough and is near its size, throw an exception.
  * You can call this function in "heavy" functions that may be called recursively
  *  to prevent possible stack overflows.
  */
void checkStackSize();
