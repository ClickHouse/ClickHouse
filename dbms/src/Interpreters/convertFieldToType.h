#pragma once

#include <Core/Field.h>


namespace DB
{

class IDataType;

/** Используется для интерпретации выражений в множестве в IN,
  *  а также в запросе вида INSERT ... VALUES ...
  *
  * Чтобы корректно работали выражения вида 1.0 IN (1) или чтобы 1 IN (1, 2.0, 2.5, -1) работало так же, как 1 IN (1, 2).
  * Проверяет совместимость типов, проверяет попадание значений в диапазон допустимых значений типа, делает преобразование типа.
  * Если значение не попадает в диапазон - возвращает Null.
  */
Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint = nullptr);

}
