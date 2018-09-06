# ClickHouse Features that Can be Considered Disadvantages

1. No full-fledged transactions.
2. Lack of ability to modify or delete already inserted data with high rate and low latency. There are batch deletes and updates available to clean up or modify data, for example to comply with [GDPR](https://gdpr-info.eu).
3. The sparse index makes ClickHouse not really suitable for point queries retrieving single rows by their keys.
