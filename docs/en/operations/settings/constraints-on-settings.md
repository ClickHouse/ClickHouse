---
description: 'Constraints on settings can be defined in the `profiles` section of
  the `user.xml` configuration file and prohibit users from changing some of the settings
  with the `SET` query.'
sidebar_label: 'Constraints on Settings'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: 'Constraints on Settings'
---

# Constraints on Settings

## Overview {#overview}

In ClickHouse, "constraints" on settings refer to limitations and rules which
you can assign to settings. These constraints can be applied to maintain 
stability, security and predictable behavior of your database.

## Defining constraints {#defining-constraints}

Constraints on settings can be defined in the `profiles` section of the `user.xml`
configuration file. They prohibit users from changing some settings using the 
[`SET`](/sql-reference/statements/set) statement.

Constraints are defined as follows:

```xml
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

If the user tries to violate the constraints, an exception is thrown and the 
setting remains unchanged.

## Types of constraints {#types-of-constraints}

There are a few types of constraints supported in ClickHouse:
- `min`
- `max`
- `readonly` (with alias `const`)
- `changeable_in_readonly`

The `min` and `max` constraints specify upper and lower boundaries for a numeric 
setting and can be used in combination with each other. 

The `readonly` or `const` constraint specifies that the user cannot change the 
corresponding setting at all. 

The `changeable_in_readonly` constraint type allows users to change the setting 
within the `min`/`max` range even if the `readonly` setting is set to `1`, 
otherwise settings are not allowed to be changed in `readonly=1` mode. 

:::note
`changeable_in_readonly` is supported only if `settings_constraints_replace_previous`
is enabled:

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```
:::

## Multiple constraint profiles {#multiple-constraint-profiles}

If there are multiple profiles active for a user, then constraints are merged. 
The Merge process depends on `settings_constraints_replace_previous`:
- **true** (recommended): constraints for the same setting are replaced during 
  merge, such that the last constraint is used and all previous ones are ignored.
  This includes fields that are not set in new constraint.
- **false** (default): constraints for the same setting are merged in a way that
  every unset type of constraint is taken from the previous profile and every 
  set type of constraint is replaced by the value from the new profile.

## Read-only mode {#read-only}

Read-only mode is enabled by the `readonly` setting which is not to be confused
with the `readonly` constraint type:
- `readonly=0`: No read-only restrictions.
- `readonly=1`: Only read queries are allowed and settings cannot be changed 
   unless `changeable_in_readonly` is set.
- `readonly=2`: Only read queries are allowed, but settings can be changed, 
  except for `readonly` setting itself.


### Example {#example-read-only}

Let `users.xml` include the following lines:

```xml
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

The following queries will all throw exceptions:

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
The `default` profile is handled uniquely: all the constraints defined for the 
`default` profile become the default constraints, so they restrict all the users
until they're overridden explicitly for those users.
:::

## Constraints on MergeTree settings {#constraints-on-merge-tree-settings}

It is possible to set constraints for [merge tree settings](merge-tree-settings.md). 
These constraints are applied when a table with the MergeTree engine is created
or its storage settings are altered. 

The name of merge tree setting must be prepended by `merge_tree_` prefix when 
referenced in the `<constraints>` section.

### Example {#example-mergetree}

You can forbid creating new tables with explicitly specified `storage_policy`

```xml
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
