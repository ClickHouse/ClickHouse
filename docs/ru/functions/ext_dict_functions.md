<a name="ext_dict_functions"></a>

# Функции для работы с внешними словарями

Информация о подключении и настройке внешних словарей смотрите в разделе "[Внешние словари](../dicts/external_dicts.md#dicts-external_dicts)".

## dictGetUInt8, dictGetUInt16, dictGetUInt32, dictGetUInt64

## dictGetInt8, dictGetInt16, dictGetInt32, dictGetInt64

## dictGetFloat32, dictGetFloat64

## dictGetDate, dictGetDateTime

## dictGetUUID

## dictGetString
`dictGetT('dict_name', 'attr_name', id)`
- получить из словаря dict_name значение атрибута attr_name по ключу id.
`dict_name` и `attr_name` - константные строки.
`id` должен иметь тип UInt64.
Если ключа `id` нет в словаре - вернуть значение по умолчанию, заданное в описании словаря.

## dictGetTOrDefault

`dictGetT('dict_name', 'attr_name', id, default)`

Аналогично функциям `dictGetT`, но значение по умолчанию берётся из последнего аргумента функции.

## dictIsIn
`dictIsIn('dict_name', child_id, ancestor_id)`
- для иерархического словаря dict_name - узнать, находится ли ключ child_id внутри ancestor_id (или совпадает с ancestor_id). Возвращает UInt8.

## dictGetHierarchy
`dictGetHierarchy('dict_name', id)`
- для иерархического словаря dict_name - вернуть массив ключей словаря, начиная с id и продолжая цепочкой родительских элементов. Возвращает Array(UInt64).

## dictHas
`dictHas('dict_name', id)`
- проверить наличие ключа в словаре. Возвращает значение типа UInt8, равное 0, если ключа нет и 1, если ключ есть.
