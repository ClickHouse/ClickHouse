---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 62
sidebar_label: "\u5BF9\u8BBE\u7F6E\u7684\u9650\u5236"
---

# 对设置的限制 {#constraints-on-settings}

在设置的约束可以在定义 `profiles` 一节 `user.xml` 配置文件，并禁止用户更改一些设置与 `SET` 查询。
约束定义如下:

``` xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
    </constraints>
  </user_name>
</profiles>
```

如果用户试图违反约束，将引发异常，并且设置不会更改。
支持三种类型的约束: `min`, `max`, `readonly`. 该 `min` 和 `max` 约束指定数值设置的上边界和下边界，并且可以组合使用。 该 `readonly` constraint指定用户根本无法更改相应的设置。

**示例:** 让 `users.xml` 包括行:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

以下查询都会引发异常:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**注:** 该 `default` 配置文件具有特殊的处理：所有定义的约束 `default` 配置文件成为默认约束，因此它们限制所有用户，直到为这些用户显式复盖它们。

[原始文章](https://clickhouse.com/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
