# ClickHouse Features that Can be Considered Disadvantages

1. Lack of full transactions.
2. Previously recorded data can't be changed or deleted with low latency and high query frequency. Mass deletions to clear data that is no longer relevant or falls under [GDPR](https://gdpr-info.eu) regulations. Batch changes to data are in development (as of July, 2018).
3. The sparse index makes ClickHouse ill-suited for point-reading single rows on its own
keys.

