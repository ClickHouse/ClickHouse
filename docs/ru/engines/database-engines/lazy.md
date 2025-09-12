---
slug: /ru/engines/database-engines/lazy
sidebar_position: 31
sidebar_label: Lazy
---

# Lazy {#lazy}

Сохраняет таблицы только в оперативной памяти `expiration_time_in_seconds` через несколько секунд после последнего доступа. Может использоваться только с таблицами \*Log.

Он оптимизирован для хранения множества небольших таблиц \*Log, для которых обычно существует большой временной интервал между обращениями.

## Создание базы данных {#creating-a-database}

``` sql
CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);
```
