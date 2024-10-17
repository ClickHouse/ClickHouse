---
slug: /en/operations/system-tables/tls_log
---
# tls_log

Contains information about client certificates received by ClickHouse server as part of TLS handshake (both valid and invalid). Useful for observability of cert-based authentication.

Columns:

- `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Certificate validation result. Possible values:
    - `failure` — Certificate was invalid.
    - `success` — Certificate was validated successfully.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Connection date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Connection time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Connection time with microseconds precision.
- `certificate_subjects` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) - The list of certificate [subjects](https://datatracker.ietf.org/doc/html/rfc5280#section-4.1.2.6) stored in CN or SAN fields of the certificate.
- `certificate_not_before` ([DateTime](../../sql-reference/data-types/datetime.md)) — date and time when certificate becomes valid ([rfc](https://datatracker.ietf.org/doc/html/rfc6487#section-4.6.1))
- `certificate_not_after` ([DateTime](../../sql-reference/data-types/datetime.md)) — date and time when certificate expires ([rfc](https://datatracker.ietf.org/doc/html/rfc6487#section-4.6.2))
- `certificate_serial` [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md) — [serial number](https://datatracker.ietf.org/doc/html/rfc6487#section-4.2) of the certificate.
- `certificate_issuer` [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md) — [Certificate Authority](https://datatracker.ietf.org/doc/html/rfc6487#section-4.4) that issued the certificate.
- `user` ([String](../../sql-reference/data-types/string.md)) — User name.
- `failure_reason` ([String](../../sql-reference/data-types/string.md)) — The exception message containing the reason for certificate validation failure.

**Example**

Query:

``` sql
SELECT * FROM system.tls_log LIMIT 1 FORMAT Vertical;
```

Result:

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
