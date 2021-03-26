---
toc_priority: 52
toc_title: "\u0421\u0438\u0441\u0442\u0435\u043c\u043d\u044b\u0435\u0020\u0442\u0430\u0431\u043b\u0438\u0446\u044b"
---

# Системные таблицы {#system-tables}

## Введение {#system-tables-introduction}

Системные таблицы содержат информацию о:

-   Состоянии сервера, процессов и окружении.
-   Внутренних процессах сервера.

Системные таблицы:

-   Находятся в базе данных `system`.
-   Доступны только для чтения данных.
-   Не могут быть удалены или изменены, но их можно отсоединить.

Системные таблицы `metric_log`, `query_log`, `query_thread_log`, `trace_log` системные таблицы хранят данные в файловой системе. Остальные системные таблицы хранят свои данные в оперативной памяти. Сервер ClickHouse создает такие системные таблицы при запуске.

### Источники системных показателей 

Для сбора системных показателей сервер ClickHouse использует:

-   Возможности `CAP_NET_ADMIN`.
-   [procfs](https://ru.wikipedia.org/wiki/Procfs) (только Linux).

**procfs**

Если для сервера ClickHouse не включено `CAP_NET_ADMIN`, он пытается обратиться к `ProcfsMetricsProvider`. `ProcfsMetricsProvider` позволяет собирать системные показатели для каждого запроса (для CPU и I/O).

Если procfs поддерживается и включена в системе, то сервер ClickHouse собирает следующие системные показатели:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system-tables/) <!--hide-->
