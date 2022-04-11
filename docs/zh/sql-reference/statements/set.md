---
toc_priority: 50
toc_title: SET
---

# SET 语句 {#query-set}

``` sql
SET param = value
```

给当前会话的 `param` [配置项](../../operations/settings/index.md)赋值。你不能用这样的方式修改[服务器相关设置](../../operations/server-configuration-parameters/index.md)。


您还可以在单个查询中设置指定设置配置文件中的所有值。



``` sql
SET profile = 'profile-name-from-the-settings-file'
```

更多详情, 详见 [配置项](../../operations/settings/settings.md).
