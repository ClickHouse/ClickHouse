---
slug: /ru/operations/system-tables/tls_log
---
# system.tls_log {#system_tables-tls_log}

Содержит информацию о клиентских сертификатах, полученных сервером ClickHouse в ходе TLS-рукопожатия (как действительных, так и недействительных). Полезно для наблюдения за аутентификацией на основе сертификатов.

Столбцы:

- `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Результат проверки сертификата. Возможные значения:
    - `failure` — Сертификат недействителен.
    - `success` — Сертификат успешно проверен.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Дата подключения.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Время подключения.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Время подключения с точностью до микросекунд.
- `certificate_subjects` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — Список [субъектов](https://datatracker.ietf.org/doc/html/rfc5280#section-4.1.2.6) сертификата, сохраненных в полях CN или SAN сертификата.
- `certificate_not_before` ([DateTime](../../sql-reference/data-types/datetime.md)) — Дата и время, когда сертификат становится действительным ([rfc](https://datatracker.ietf.org/doc/html/rfc6487#section-4.6.1))
- `certificate_not_after` ([DateTime](../../sql-reference/data-types/datetime.md)) — Дата и время, когда сертификат перестает действовать ([rfc](https://datatracker.ietf.org/doc/html/rfc6487#section-4.6.2))
- `certificate_serial` [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md) — [Серийный номер](https://datatracker.ietf.org/doc/html/rfc6487#section-4.2) сертификата.
- `certificate_issuer` [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md) — [CA](https://datatracker.ietf.org/doc/html/rfc6487#section-4.4), который выдал сертификат.
- `user` ([String](../../sql-reference/data-types/string.md)) — Имя пользователя.
- `failure_reason` ([String](../../sql-reference/data-types/string.md)) — Сообщение об ошибке, возникшее при проверке сертификата.

**Пример**

Запрос:

``` sql
SELECT * FROM system.tls_log LIMIT 1 FORMAT Vertical;
```

Результат:
``` text
Row 1:
──────
type:                    success
event_date:              2024-10-15
event_time:              2024-10-15 10:15:26
event_time_microseconds: 2024-10-15 10:15:26.081684
certificate_subjects:    ['CN:client1']
certificate_not_before:  2024-06-06 12:48:46
certificate_not_after:   2034-06-04 12:48:46
certificate_serial:      05F10C67567FE30795D77AF2540F6AC8D4CF2468
certificate_issuer:      /C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=ca
failure_reason:
```
