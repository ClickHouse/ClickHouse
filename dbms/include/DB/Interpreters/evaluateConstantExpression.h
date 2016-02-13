#pragma once

#include <DB/Core/Field.h>


namespace DB
{

class IAST;
class Context;

/** Выполнить константное выражение.
  * Используется в редких случаях - для элемента множества в IN, для данных для INSERT.
  * Весьма неоптимально.
  */
Field evaluateConstantExpression(SharedPtr<IAST> & node, const Context & context);

}
