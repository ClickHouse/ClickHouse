# ClickHouse features that can be considered disadvantages

1. No full-fledged transactions.
2. Lack of ability to modify or delete already inserted data with high rate and low latency. There are batch deletes available to clean up data that is not needed anymore or to comply with [GDPR](https://gdpr-info.eu). Batch updates are currently in development as of July 2018.
3. Sparse index makes ClickHouse not really suitable for point queries retrieving single rows by their keys.

