---
toc_priority: 51
toc_title: SET
---

# SET {#query-set}

``` sql
SET param = value
```

Устанавливает значение `value` для [настройки](../../operations/settings/index.md) `param` в текущей сессии. [Конфигурационные параметры сервера](../../operations/server-configuration-parameters/settings.md) нельзя изменить подобным образом.

Можно одним запросом установить все настройки из заданного профиля настроек.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Подробности смотрите в разделе [Настройки](../../operations/settings/settings.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/set/) <!--hide-->