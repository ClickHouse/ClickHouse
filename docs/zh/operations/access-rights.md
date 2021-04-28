---
toc_priority: 48
toc_title: "访问控制和用户管理"
---

# 访问控制和用户管理 {#access-rights}
ClickHouse支持基于[RBAC](https://en.wikipedia.org/wiki/Role-based_access_control)的访问控制管理。

ClickHouse权限实体包括：
- [用户账户](#user-account-management)
- [角色](#role-management)
- [行策略](#row-policy-management)
- [设置配置文件](#settings-profiles-management)
- [配额](#quotas-management)

你可以通过如下方式配置访问实体：

- 通过SQL驱动的工作流方式.

    你需要[开启](#enabling-access-control)这个功能.

- 服务端[配置文件](configuration-files.md) `users.xml` 和 `config.xml`.

我们建议你使用SQL驱动的工作流的方式。两种配置方法同时工作，因此，如果你使用服务端配置文件的方式来管理用户和访问权限，则可以平稳地切换到SQL驱动的工作流方式。

!!! note "警告"
    你不能同时使用两个配置的方式来管理同一个访问实体。


## 用法 {#access-control-usage}

默认情况下，ClickHouse服务器提供了一个 `default` 用户，这个用户具有所有的权限，但是不能使用SQL驱动方式的访问控制和用户管理。`default`主要用在用户名还未定义的情况，比如从客户端登录或者在分布式查询中。如果服务器或者集群的配置没有指定[用户和密码](../engines/table-engines/special/distributed.md)属性，则在分布式查询处理中将使用default用户。

如果你刚开始使用ClickHouse，请考虑以下情况：

1. 为 `default` 用户[开启](#enabling-access-control)SQL驱动方式的访问控制和用户管理。
2. 使用 `default` 用户登录并且创建所有必需的用户。 不要忘记创建管理员账户 (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`)。
3. [限制](settings/permissions-for-queries.md#permissions_for_queries) `default` 用户的权限并且禁用SQL驱动方式的访问控制和用户管理。

### 当前解决方案的特性 {#access-control-properties}

- 你可以授予数据库和表的权限，即使它们不存在。 
- 如果表被删除，和这张表关联的特权不会被删除。这意味着如果以后你创建一张同名的表，所有的特权仍旧有效。如果想收回与已删除表关联的特权，你需要执行 `REVOKE ALL PRIVILEGES ON db.table FROM ALL`  查询。
- 特权没有生命周期。

## 用户账户 {#user-account-management}

用户账户是ClickHouse中用来授权操作的访问实体，，用户账户包含：

- 标识符信息。
- [特权](../sql-reference/statements/grant.md#grant-privileges)用来定义用户可以执行的查询的范围。
- 可以连接到ClickHouse服务器的主机。
- 分配的角色和默认的角色。
- 用户登录的时候默认的限制设置。
- 分配的设置配置文件。

特权可以通过[GRANT](../sql-reference/statements/grant.md)查询授权给用户或者通过[角色](#role-management)授予。如果想收回用户的特权，可以使用[REVOKE](../sql-reference/statements/revoke.md)查询。查询用户所有的特权，使用[SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement)语句。

管理语句：

- [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
- [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
- [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
- [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### 设置应用规则 {#access-control-settings-applying}

设置可以通过多种方式配置：对于用户来说，在其授予的角色和设置配置文件中。在用户登录时，如果为不同的访问实体配置了同一设置，则该设置的值和约束将按以下方式应用（优先级从高到低）：

1. 用户账户设置。
2. 用户账号的默认角色的设置。如果在多个角色配置了同一设置，则设置的应用顺序是不确定的。
3. 分配给用户或其默认角色的设置配置文件中的设置。如果在多个配置文件中配置了同一设置，则设置的应用顺序是不确定的。
4. 对所有服务器有效的默认或者[默认配置文件](server-configuration-parameters/settings.md#default-profile)的设置。


## 角色 {#role-management}

角色是访问实体的容器，可以被授予用户账号。

角色包括：

- [特权](../sql-reference/statements/grant.md#grant-privileges)
- 设置和约束
- 分配的角色列表

管理语句：

- [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
- [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
- [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
- [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
- [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
- [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

使用[GRANT](../sql-reference/statements/grant.md) 查询可以把特权授予给角色。用[REVOKE](../sql-reference/statements/revoke.md)来收回特权。

## 行策略 {#row-policy-management}

行策略是一个过滤器，用来定义用户或者角色可以访问哪些行。行策略包括一个特定表的过滤器和使用这个行策略的角色和/或用户的列表。

管理语句：

- [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
- [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
- [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)


## 设置配置文件 {#settings-profiles-management}

设置配置文件是[设置](settings/index.md)的集合。设置配置文件包括设置和约束，以及应用此配置文件的角色和/或用户的列表。

管理语句:

- [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
- [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
- [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
- [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)


## 配额 {#quotas-management}

配额用来限制资源的使用情况。参考[配额](quotas.md)。

配额包括一组特定时间的限制条件和使用这个配额的用户和/或用户的列表。

管理语句:

- [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
- [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
- [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
- [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)


## 开启SQL驱动的访问控制和用户管理 {#enabling-access-control}

- 设置用于配置存储的目录。

    ClickHouse把访问实体的相关配置存储在[access_control_path](server-configuration-parameters/settings.md#access_control_path)服务器配置参数中设置的文件夹中。

- 为至少一个用户账户开启SQL驱动的访问控制和用户管理。

     默认情况下，SQL驱动的访问控制和用户管理对所有用户都是禁用的。你需要在 `users.xml` 中配置至少一个用户，并且把[access_management](settings/settings-users.md#access_management-user-setting)的值设置为1。


[Original article](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
