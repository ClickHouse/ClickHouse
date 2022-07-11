---
sidebar_position: 48
sidebar_label: "访问权限和账户管理"
---

# 访问权限和账户管理 {#access-rights}
ClickHouse支持基于[RBAC](https://en.wikipedia.org/wiki/Role-based_access_control)的访问控制管理。

ClickHouse权限实体包括：
- [用户账户](#user-account-management)
- [角色](#role-management)
- [行策略](#row-policy-management)
- [设置描述](#settings-profiles-management)
- [配额](#quotas-management)

你可以通过如下方式配置权限实体：

- 通过SQL驱动的工作流方式.

    你需要[开启](#enabling-access-control)这个功能.

- 服务端[配置文件](configuration-files.md) `users.xml` 和 `config.xml`.

我们建议你使用SQL工作流的方式。当然配置的方式也可以同时起作用, 所以如果你正在用服务端配置的方式来管理权限和账户，你可以平滑的切换到SQL驱动的工作流方式。

!!! note "警告"
    你无法同时使用两个配置的方式来管理同一个权限实体。


## 用法 {#access-control-usage}

默认ClickHouse提供了一个 `default` 账号，这个账号有所有的权限，但是不能使用SQL驱动方式的访问权限和账户管理。`default`主要用在用户名还未设置的情况，比如从客户端登录或者执行分布式查询。在分布式查询中如果服务端或者集群没有指定[用户名密码](../engines/table-engines/special/distributed.md)那默认的账户就会被使用。

如果你刚开始使用ClickHouse，考虑如下场景：

1. 为 `default` 用户[开启SQL驱动方式的访问权限和账户管理](#enabling-access-control) .
2. 使用 `default` 用户登录并且创建所需要的所有用户。 不要忘记创建管理员账户 (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`)。
3. [限制](settings/permissions-for-queries.md#permissions_for_queries) `default` 用户的权限并且禁用SQL驱动方式的访问权限和账户管理。

### 当前解决方案的特性 {#access-control-properties}

- 你甚至可以在数据库和表不存在的时候授予权限。
- 如果表被删除，和这张表关联的特权不会被删除。这意味着如果你创建一张同名的表，所有的特权仍旧有效。如果想删除这张表关联的特权，你可以执行 `REVOKE ALL PRIVILEGES ON db.table FROM ALL`  查询。
- 特权没有生命周期。

## 用户账户 {#user-account-management}

用户账户是权限实体，用来授权操作ClickHouse，用户账户包含：

- 标识符信息。
- [特权](../sql-reference/statements/grant.md#grant-privileges)用来定义用户可以执行的查询的范围。
- 可以连接到ClickHouse的主机。
- 指定或者默认的角色。
- 用户登录的时候默认的限制设置。
- 指定的设置描述。

特权可以通过[GRANT](../sql-reference/statements/grant.md)查询授权给用户或者通过[角色](#role-management)授予。如果想撤销特权，可以使用[REVOKE](../sql-reference/statements/revoke.md)查询。查询用户所有的特权，使用[SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement)语句。

查询管理：

- [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
- [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
- [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
- [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### 设置应用规则 {#access-control-settings-applying}

对于一个用户账户来说，设置可以通过多种方式配置：通过角色扮演和设置描述。对于一个登陆的账号来说，如果一个设置对应了多个不同的权限实体，这些设置的应用规则如下（优先权从高到底）：

1. 用户账户设置。
2. 用户账号默认的角色设置。如果这个设置配置了多个角色，那设置的应用是没有规定的顺序。
3. 从设置描述分批给用户或者角色的设置。如果这个设置配置了多个角色，那设置的应用是没有规定的顺序。
4. 对所有服务器有效的默认或者[default profile](server-configuration-parameters/settings.md#default-profile)的设置。


## 角色 {#role-management}

角色是权限实体的集合，可以被授予用户账号。

角色包括：

- [特权](../sql-reference/statements/grant.md#grant-privileges)
- 设置和限制
- 分配的角色列表

查询管理:

- [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
- [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
- [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
- [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
- [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
- [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

使用[GRANT](../sql-reference/statements/grant.md) 查询可以把特权授予给角色。用[REVOKE](../sql-reference/statements/revoke.md)来撤回特权。

## 行策略 {#row-policy-management}

行策略是一个过滤器，用来定义哪些行数据可以被账户或者角色访问。对一个特定的表来说，行策略包括过滤器和使用这个策略的账户和角色。

查询管理：

- [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
- [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
- [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)


## 设置描述 {#settings-profiles-management}

设置描述是[设置](settings/index.md)的汇总。设置汇总包括设置和限制，当然也包括这些描述的对象：角色和账户。

查询管理:

- [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
- [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
- [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
- [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)


## 配额 {#quotas-management}

配额用来限制资源的使用情况。参考[配额](quotas.md).

配额包括特定时间的限制条件和使用这个配额的账户和角色。

Management queries:

- [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
- [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
- [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
- [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)


## 开启SQL驱动方式的访问权限和账户管理 {#enabling-access-control}

- 为配置的存储设置一个目录.

    ClickHouse把访问实体的相关配置存储在[访问控制目录](server-configuration-parameters/settings.md#access_control_path)，而这个目录可以通过服务端进行配置.

- 为至少一个账户开启SQL驱动方式的访问权限和账户管理.

     默认情况，SQL驱动方式的访问权限和账户管理对所有用户都是关闭的。你需要在 `users.xml` 中配置至少一个用户，并且把[权限管理](settings/settings-users.md#access_management-user-setting)的值设置为1。


[Original article](https://clickhouse.com/docs/en/operations/access_rights/) <!--hide-->
