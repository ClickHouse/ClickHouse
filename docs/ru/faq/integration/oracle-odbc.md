---
title: What if I have a problem with encodings when using Oracle via ODBC?
toc_hidden: true
toc_priority: 20
---

## Что делать, если у меня проблема с кодировками при использовании Oracle через ODBC? {#oracle-odbc-encodings-rus}

Если вы используете Oracle через драйвер ODBC в качестве источника внешних словарей, необходимо задать правильное значение для переменной окружения `NLS_LANG` в `/etc/default/clickhouse`. Подробнее читайте в