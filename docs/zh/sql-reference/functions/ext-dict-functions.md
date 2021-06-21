# 字典函数 {#zi-dian-han-shu}

有关连接和配置外部词典的信息，请参阅[外部词典](../../sql-reference/functions/ext-dict-functions.md)。

## dictGetUInt8,dictGetUInt16,dictGetUInt32,dictGetUInt64 {#dictgetuint8-dictgetuint16-dictgetuint32-dictgetuint64}

## dictGetInt8,dictGetInt16,dictGetInt32,dictGetInt64 {#dictgetint8-dictgetint16-dictgetint32-dictgetint64}

## dictGetFloat32,dictGetFloat64 {#dictgetfloat32-dictgetfloat64}

## dictGetDate,dictGetDateTime {#dictgetdate-dictgetdatetime}

## dictgetuid {#dictgetuuid}

## dictGetString {#dictgetstring}

`dictGetT('dict_name', 'attr_name', id)`

-   使用’id’键获取dict\_name字典中attr\_name属性的值。`dict_name`和`attr_name`是常量字符串。`id`必须是UInt64。
    如果字典中没有`id`键，则返回字典描述中指定的默认值。

## dictGetTOrDefault {#ext_dict_functions-dictgettordefault}

`dictGetTOrDefault('dict_name', 'attr_name', id, default)`

与`dictGetT`函数相同，但默认值取自函数的最后一个参数。

## dictIsIn {#dictisin}

`dictIsIn ('dict_name', child_id, ancestor_id)`

-   对于’dict\_name’分层字典，查找’child\_id’键是否位于’ancestor\_id’内（或匹配’ancestor\_id’）。返回UInt8。

## 独裁主义 {#dictgethierarchy}

`dictGetHierarchy('dict_name', id)`

-   对于’dict\_name’分层字典，返回从’id’开始并沿父元素链继续的字典键数组。返回Array（UInt64）

## dictHas {#dicthas}

`dictHas('dict_name', id)`

-   检查字典是否存在指定的`id`。如果不存在，则返回0;如果存在，则返回1。

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
