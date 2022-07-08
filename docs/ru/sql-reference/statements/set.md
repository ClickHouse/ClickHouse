---
sidebar_position: 49
sidebar_label: SET
---

# SET {#query-set}

``` sql
SET param = value
```

Устанавливает значение `value` для [настройки](../../operations/settings/overview.md) `param` в текущей сессии. [Конфигурационные параметры сервера](../../operations/server-configuration-parameters/settings.md) нельзя изменить подобным образом.

Можно одним запросом установить все настройки из заданного профиля настроек.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Подробности смотрите в разделе [Настройки](../../operations/settings/settings.md).

