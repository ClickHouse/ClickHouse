---
slug: /en/operations/settings/constraints-on-settings
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
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
    </constraints>
  </user_name>
</profiles>
```

If the user tries to violate the constraints an exception is thrown and the setting isn’t changed.
There are supported few types of constraints: `min`, `max`, `readonly` (with alias `const`) and `changeable_in_readonly`. The `min` and `max` constraints specify upper and lower boundaries for a numeric setting and can be used in combination. The `readonly` or `const` constraint specifies that the user cannot change the corresponding setting at all. The `changeable_in_readonly` constraint type allows user to change the setting within `min`/`max` range even if `readonly` setting is set to 1, otherwise settings are not allow to be changed in `readonly=1` mode. Note that `changeable_in_readonly` is supported only if `settings_constraints_replace_previous` is enabled:
``` xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

If there are multiple profiles active for a user, then constraints are merged. Merge process depends on `settings_constraints_replace_previous`:
- **true** (recommended): constraints for the same setting are replaced during merge, such that the last constraint is used and all previous are ignored including fields that are not set in new constraint.
- **false** (default): constraints for the same setting are merged in a way that every not set type of constraint is taken from previous profile and every set type of constraint is replaced by value from new profile.

Read-only mode is enabled by `readonly` setting (not to confuse with `readonly` constraint type):
- `readonly=0`: No read-only restrictions.
- `readonly=1`: Only read queries are allowed and settings cannot be changed unless `changeable_in_readonly` is set.
- `readonly=2`: Only read queries are allowed, but settings can be changed, except for `readonly` setting itself.


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

## Constraints on Merge Tree Settings
It is possible to set constraints for [merge tree settings](merge-tree-settings.md). These constraints are applied when table with merge tree engine is created or its storage settings are altered. Name of merge tree setting must be prepended by `merge_tree_` prefix when referenced in `<constraints>` section.

**Example:** Forbid to create new tables with explicitly specified `storage_policy`

``` xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```
