#pragma once

#include <memory>
#include <DB/Core/Field.h>


namespace DB
{

class IAST;
class Context;

/** Выполнить константное выражение.
  * Используется в редких случаях - для элемента множества в IN, для данных для INSERT.
  * Весьма неоптимально.
  */
Field evaluateConstantExpression(std::shared_ptr<IAST> & node, const Context & context);

}
