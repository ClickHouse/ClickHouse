#pragma once

/** exp10 from GNU libm fails to give precise result for integer arguments.
  * For example, exp10(3) gives 1000.0000000000001
  *  despite the fact that 1000 is exactly representable in double and float.
  * Better to always use implementation from MUSL.
  *
  * Note: the function names are different to avoid confusion with symbols from the system libm.
  */

extern "C"
{

double preciseExp10(double x);

}
