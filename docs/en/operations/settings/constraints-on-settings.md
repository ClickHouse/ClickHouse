---
sidebar_position: 62
sidebar_label: Constraints on Settings
---

# Constraints on Settings

The constraints on settings can be defined in the `profiles` section of the `user.xml` configuration file and prohibit users from changing some of the settings with the `SET` query.
The constraints are defined as the following:

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

If the user tries to violate the constraints an exception is thrown and the setting isn’t changed.
There are supported three types of constraints: `min`, `max`, `readonly`. The `min` and `max` constraints specify upper and lower boundaries for a numeric setting and can be used in combination. The `readonly` constraint specifies that the user cannot change the corresponding setting at all.

**Example:** Let `users.xml` includes lines:

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

The following queries all throw exceptions:

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

**Note:** the `default` profile has special handling: all the constraints defined for the `default` profile become the default constraints, so they restrict all the users until they’re overridden explicitly for these users.

[Original article](https://clickhouse.com/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
