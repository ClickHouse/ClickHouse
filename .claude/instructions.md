# ClickHouse Development Instructions

## Running Stateless Tests

Stateless tests are located in `tests/queries/0_stateless/`.

### Prerequisites
1. Build ClickHouse: `cd build && ninja clickhouse`
2. Start the server: `./build/programs/clickhouse server --config-file ./programs/server/config.xml`
3. Wait for server to be ready: `./build/programs/clickhouse client -q "SELECT 1"`

### Running Tests
Run tests with the correct port environment variables (default config uses TCP=9000, HTTP=8123):

```bash
CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 ./tests/clickhouse-test <test_name>
```

### Useful Flags
- `--no-random-settings` - Disable settings randomization (useful for deterministic debugging)
- `--no-random-merge-tree-settings` - Disable MergeTree settings randomization
- `--record` - Automatically update `.reference` files when stdout differs

### Test File Extensions
- `.sql` - SQL test (most common)
- `.sql.j2` - Jinja2-templated SQL test
- `.sh` - Shell script test
- `.py` - Python test
- `.expect` - Expect script test
- `.reference` - Expected output (compared against stdout)
- `.gen.reference` - Generated reference for `.j2` tests

### Database Name Normalization
The test runner creates a temporary database with a random name (e.g., `test_abc123`) for each test.
After test execution, the random database name is replaced with `default` in stdout/stderr files before comparison with `.reference`.
This means `.reference` files should use `default` for database names, NOT `${CLICKHOUSE_DATABASE}` or the actual random name.

### Test Tags
Tests can have tags in the first line as a comment: `-- Tags: no-fasttest, no-parallel`
Common tags: `disabled`, `no-fasttest`, `no-parallel`, `no-random-settings`, `no-random-merge-tree-settings`, `long`

### Random Settings Limits
Tests can specify limits for randomized settings: `-- Random settings limits: max_threads=(1, 4); ...`

### Stopping the Server
Find and kill the server process:
```bash
pgrep -f "clickhouse server"  # Get PIDs
kill <pid1> <pid2>            # Stop processes
```
