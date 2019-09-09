# 字典函数

有关连接和配置外部词典的信息，请参阅[外部词典](../dicts/external_dicts.md)。

## dictGetUInt8, dictGetUInt16, dictGetUInt32, dictGetUInt64

## dictGetInt8, dictGetInt16, dictGetInt32, dictGetInt64

## dictGetFloat32, dictGetFloat64

## dictGetDate, dictGetDateTime

## dictGetUUID

## dictGetString

`dictGetT('dict_name', 'attr_name', id)`

- 使用'id'键获取dict_name字典中attr_name属性的值。`dict_name`和`attr_name`是常量字符串。`id`必须是UInt64。
如果字典中没有`id`键，则返回字典描述中指定的默认值。

## dictGetTOrDefault {#ext_dict_functions_dictGetTOrDefault}

`dictGetTOrDefault('dict_name', 'attr_name', id, default)`

与`dictGetT`函数相同，但默认值取自函数的最后一个参数。

## dictIsIn

`dictIsIn ('dict_name', child_id, ancestor_id)`

- 对于'dict_name'分层字典，查找'child_id'键是否位于'ancestor_id'内（或匹配'ancestor_id'）。返回UInt8。

## dictGetHierarchy

`dictGetHierarchy('dict_name', id)`

- 对于'dict_name'分层字典，返回从'id'开始并沿父元素链继续的字典键数组。返回Array（UInt64）

## dictHas

`dictHas('dict_name', id)`

- 检查字典是否存在指定的`id`。如果不存在，则返回0;如果存在，则返回1。


[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
