---
sidebar_position: 48
sidebar_label: "Управление доступом"
---

# Управление доступом {#access-control}

ClickHouse поддерживает управление доступом на основе подхода [RBAC](https://ru.wikipedia.org/wiki/Управление_доступом_на_основе_ролей).

Объекты системы доступа в ClickHouse:

- [Аккаунт пользователя](#user-account-management)
- [Роль](#role-management)
- [Политика доступа к строкам](#row-policy-management)
- [Профиль настроек](#settings-profiles-management)
- [Квота](#quotas-management)

Вы можете настроить объекты системы доступа, используя:

- SQL-ориентированный воркфлоу.

    Функциональность необходимо [включить](#enabling-access-control).

- [Конфигурационные файлы](configuration-files.md) сервера: `users.xml` и `config.xml`.

Рекомендуется использовать SQL-воркфлоу. Оба метода конфигурации работают одновременно, поэтому, если для управления доступом вы используете конфигурационные файлы, вы можете плавно перейти на SQL-воркфлоу.

    :::note "Внимание"
    Нельзя одновременно использовать оба метода для управления одним и тем же объектом системы доступа.
    :::
Чтобы посмотреть список всех пользователей, ролей, профилей и пр., а также все привилегии, используйте запрос [SHOW ACCESS](../sql-reference/statements/show.md#show-access-statement).

## Использование {#access-control-usage}

По умолчанию сервер ClickHouse предоставляет аккаунт пользователя `default`, для которого выключена функция SQL-ориентированного управления доступом, но у него есть все права и разрешения. Аккаунт `default` используется во всех случаях, когда имя пользователя не определено. Например, при входе с клиента или в распределенных запросах. При распределенной обработке запроса `default` используется, если в конфигурации сервера или кластера не указаны свойства [user и password](../engines/table-engines/special/distributed.md).

Если вы начали пользоваться ClickHouse недавно, попробуйте следующий сценарий:

1. [Включите](#enabling-access-control) SQL-ориентированное управление доступом для пользователя `default`.
2. Войдите под пользователем `default` и создайте всех необходимых пользователей. Не забудьте создать аккаунт администратора (`GRANT ALL ON *.* TO admin_user_account WITH GRANT OPTION`).
3. [Ограничьте разрешения](settings/permissions-for-queries.md#permissions_for_queries) для пользователя `default` и отключите для него SQL-ориентированное управление доступом.

### Особенности реализации {#access-control-properties}

- Вы можете выдавать разрешения на базы данных или таблицы, даже если они не существуют.
- При удалении таблицы все связанные с ней привилегии не отзываются. Если вы затем создадите новую таблицу с таким же именем, все привилегии останутся действительными. Чтобы отозвать привилегии, связанные с удаленной таблицей, необходимо выполнить, например, запрос `REVOKE ALL PRIVILEGES ON db.table FROM ALL`.
- У привилегий нет настроек времени жизни.

## Аккаунт пользователя {#user-account-management}

Аккаунт пользователя — это объект системы доступа, позволяющий авторизовать кого-либо в ClickHouse. Аккаунт содержит:

- Идентификационную информацию.
- [Привилегии](../sql-reference/statements/grant.md#grant-privileges), определяющие область действия запросов, которые могут быть выполнены пользователем.
- Хосты, которые могут подключаться к серверу ClickHouse.
- Назначенные роли и роли по умолчанию.
- Настройки и их ограничения, которые применяются по умолчанию при входе пользователя.
- Присвоенные профили настроек.

Привилегии присваиваются аккаунту пользователя с помощью запроса [GRANT](../sql-reference/statements/grant.md) или через назначение [ролей](#role-management). Отозвать привилегию можно с помощью запроса [REVOKE](../sql-reference/statements/revoke.md). Чтобы вывести список присвоенных привилегий, используется выражение [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement).

Запросы управления:

- [CREATE USER](../sql-reference/statements/create/user.md#create-user-statement)
- [ALTER USER](../sql-reference/statements/alter/user.md)
- [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
- [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### Применение настроек {#access-control-settings-applying}

Настройки могут быть заданы разными способами: для аккаунта пользователя, для назначенных ему ролей или в профилях настроек. При входе пользователя, если настройка задана для разных объектов системы доступа, значение настройки и ее ограничения применяются в следующем порядке (от высшего приоритета к низшему):

1. Настройки аккаунта.
2. Настройки ролей по умолчанию для аккаунта. Если настройка задана для нескольких ролей, порядок применения не определен.
3. Настройки из профилей настроек, присвоенных пользователю или его ролям по умолчанию. Если настройка задана в нескольких профилях, порядок применения не определен.
4. Настройки, которые по умолчанию применяются ко всему серверу, или настройки из [профиля по умолчанию](server-configuration-parameters/settings.md#default-profile).


## Роль {#role-management}

Роль — это контейнер объектов системы доступа, которые можно присвоить аккаунту пользователя.

Роль содержит:

- [Привилегии](../sql-reference/statements/grant.md#grant-privileges)
- Настройки и ограничения
- Список назначенных ролей

Запросы управления:

- [CREATE ROLE](../sql-reference/statements/create/index.md#create-role-statement)
- [ALTER ROLE](../sql-reference/statements/alter/role.md)
- [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
- [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
- [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
- [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

Привилегии можно присвоить роли с помощью запроса [GRANT](../sql-reference/statements/grant.md). Для отзыва привилегий у роли ClickHouse предоставляет запрос [REVOKE](../sql-reference/statements/revoke.md).

## Политика доступа к строкам {#row-policy-management}

Политика доступа к строкам — это фильтр, определяющий, какие строки доступны пользователю или роли. Политика содержит фильтры для конкретной таблицы, а также список ролей и/или пользователей, которые должны использовать данную политику.

Запросы управления:

- [CREATE ROW POLICY](../sql-reference/statements/create/index.md#create-row-policy-statement)
- [ALTER ROW POLICY](../sql-reference/statements/alter/row-policy.md)
- [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)


## Профиль настроек {#settings-profiles-management}

Профиль настроек — это набор [настроек](settings/overview.md). Профиль настроек содержит настройки и ограничения, а также список ролей и/или пользователей, по отношению к которым применяется данный профиль.

Запросы управления:

- [CREATE SETTINGS PROFILE](../sql-reference/statements/create/index.md#create-settings-profile-statement)
- [ALTER SETTINGS PROFILE](../sql-reference/statements/alter/settings-profile.md)
- [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
- [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)


## Квота {#quotas-management}

Квота ограничивает использование ресурсов. См. [Квоты](quotas.md).

Квота содержит набор ограничений определенной длительности, а также список ролей и/или пользователей, на которых распространяется данная квота.

Запросы управления:

- [CREATE QUOTA](../sql-reference/statements/create/index.md#create-quota-statement)
- [ALTER QUOTA](../sql-reference/statements/alter/quota.md)
- [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
- [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)


## Включение SQL-ориентированного управления доступом {#enabling-access-control}

- Настройте каталог для хранения конфигураций.

    ClickHouse хранит конфигурации объектов системы доступа в каталоге, установленном в конфигурационном параметре сервера [access_control_path](server-configuration-parameters/settings.md#access_control_path).

- Включите SQL-ориентированное управление доступом как минимум для одного аккаунта.

    По умолчанию управление доступом на основе SQL выключено для всех пользователей. Вам необходимо настроить хотя бы одного пользователя в файле конфигурации `users.xml` и присвоить значение 1 параметру [access_management](settings/settings-users.md#access_management-user-setting).


