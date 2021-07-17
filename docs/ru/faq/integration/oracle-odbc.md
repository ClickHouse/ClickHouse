---
title: Что делать, если у меня проблема с кодировками при использовании Oracle через ODBC?
toc_hidden: true
toc_priority: 20
---

# Что делать, если у меня проблема с кодировками при использовании Oracle через ODBC? {#oracle-odbc-encodings}

Если вы используете Oracle через драйвер ODBC в качестве источника внешних словарей, необходимо задать правильное значение для переменной окружения `NLS_LANG` в `/etc/default/clickhouse`. Подробнее читайте в [Oracle NLS_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Пример**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```