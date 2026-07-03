# ClickHouse Keeper Utils

A comprehensive utility tool for managing and analyzing ClickHouse Keeper data, including snapshots and changelogs.

## Building

Build the tool as part of the main ClickHouse build process:

```bash
cd /path/to/ClickHouse
mkdir -p build
cd build
cmake ..
make -j$(nproc) clickhouse-keeper-utils
```

The binary will be available at `./programs/clickhouse-keeper-utils`.

## Available Commands

### 1. dump-state

Dump the current state of a Keeper cluster by loading snapshots and applying changelogs.

#### Usage
```bash
clickhouse-keeper-utils dump-state [options]
```

#### Options
- `--snapshot-path` (required): Path to the directory containing Keeper snapshots
- `--log-path` (required): Path to the directory containing Keeper changelogs
- `--output-file, -o`: Write output to file instead of stdout
- `--output-format, -f`: Output format when using --output-file (default: CSVWithNamesAndTypes)
- `--parallel-output`: Enable parallel output format processing when using --output-file
- `--with-acl`: Include ACL (Access Control List) information (only used with node tree output)
- `--debug-mode`: Enable debug output for log entries
- `--end-index`: Process changelog entries up to this index (exclusive). Useful for examining the state at a specific point in time.
- `--dump-sessions`: Dump session information instead of the node tree
- `--help, -h`: Show help message

#### Output Modes
1. **Node Tree (default)**:
   - Shows the hierarchical structure of all nodes in the Keeper cluster
   - Includes node metadata (version, mtime, data length, etc.)
   - Use `--with-acl` to include ACL information

2. **Session Information (--dump-sessions)**:
   - Shows active sessions with their timeouts
   - Lists number of ephemeral nodes per session
   - Shows authentication information for each session

#### Examples

```bash
# Basic usage with default CSV format
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs

# Save output to a file with JSON format
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file output.json --output-format JSONEachRow

# Enable parallel output processing for better performance
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file output.csv --parallel-output

# Include ACL information in the output
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file output_with_acl.csv --with-acl

# Combine options: parallel processing with ACL information
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file output_parallel_acl.csv --parallel-output --with-acl

# Dump session information instead of node tree
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file sessions.csv --dump-sessions

# Dump session information with parallel processing
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file sessions_parallel.csv --dump-sessions --parallel-output

# Dump state up to a specific changelog index
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file state_at_index_1000.csv --end-index 1001

# Debug state up to a specific index with detailed logging
clickhouse-keeper-utils dump-state --snapshot-path /var/lib/clickhouse/coordination/snapshots \
    --log-path /var/lib/clickhouse/coordination/logs \
    --output-file debug_state.csv --debug-mode --end-index 5001
```

#### Session Information Format
When using `--dump-sessions`, the output includes the following information for each session:
- `Session`: The session ID
- `Timeout`: Session timeout in milliseconds
- `Ephemeral nodes`: Number of ephemeral nodes owned by this session
- `Auth`: Authentication information (scheme and ID) for the session

### 2. snapshot-analyzer

Analyze Keeper snapshots and print basic information.

#### Usage
```bash
clickhouse-keeper-utils snapshot-analyzer [options]
```

#### Options
- `--snapshot-path <path>`: Path to the snapshots directory or a specific snapshot file (`.bin` or `.bin.zstd`). This is a required argument.
- `--full-storage`: If specified, the full storage (including node data) is loaded from the snapshot. This provides more detailed information like node count and digest but is slower. By default, only the node paths are loaded.
- `--with-node-stats`: If specified (and `--full-storage` is not), it calculates and displays statistics about the biggest subtrees, such as the top 10 nodes with the most descendants.
- `--help, -h`: Displays the help message.

#### Examples

1.  **Analyze all snapshots in a directory (basic info):**
    ```bash
    clickhouse-keeper-utils snapshot-analyzer --snapshot-path /var/lib/clickhouse/coordination/snapshots/
    ```

2.  **Analyze a specific snapshot file with subtree statistics:**
    ```bash
    clickhouse-keeper-utils snapshot-analyzer --snapshot-path /var/lib/clickhouse/coordination/snapshots/snapshot_123.bin --with-node-stats
    ```

3.  **Analyze a snapshot with full storage loaded:**
    ```bash
    clickhouse-keeper-utils snapshot-analyzer --snapshot-path /var/lib/clickhouse/coordination/snapshots/snapshot_123.bin --full-storage
    ```

### 3. changelog-analyzer

Analyze Keeper changelogs and print information about them.

#### Usage
```bash
clickhouse-keeper-utils changelog-analyzer --log-path <path> [--changelog <file>]
```

#### Options
- `--log-path` (required): Path to the directory containing Keeper changelogs
- `--changelog`: Analyze a specific changelog file
- `--help, -h`: Show help message

#### Examples
```bash
# Analyze all changelogs in directory
clickhouse-keeper-utils changelog-analyzer --log-path /var/lib/clickhouse/coordination/logs

# Analyze specific changelog file
clickhouse-keeper-utils changelog-analyzer --log-path /var/lib/clickhouse/coordination/logs \
    --changelog changelog_1.bin
```

### 4. changelog-splicer

Extract a range of entries from a changelog to a new file.

#### Usage
```bash
clickhouse-keeper-utils changelog-splicer --source <file> --destination <dir> [--start-index <n>] --end-index <n>
```

#### Options
- `--source` (required): Path to source changelog file
- `--destination` (required): Directory where to save the output changelog file
- `--start-index`: Start index (inclusive). If not specified, uses the source changelog's start index
- `--end-index` (required): End index (exclusive)
- `--help, -h`: Show help message

#### Examples
```bash
# Extract specific range of entries
clickhouse-keeper-utils changelog-splicer --source /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --destination /tmp --start-index 100 --end-index 200

# Extract from beginning of changelog to specified index
clickhouse-keeper-utils changelog-splicer --source /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --destination /tmp --end-index 200
```

### 5. changelog-deserializer

Deserialize and display the contents of a Keeper changelog file in a human-readable format, with options to filter by index range and include detailed request information.

#### Usage
```bash
clickhouse-keeper-utils changelog-deserializer --changelog-path <file> [options]
```

#### Options
- `--changelog-path` (required): Path to the changelog file to deserialize
- `--output-file, -o`: Write output to file instead of stdout
- `--output-format, -f`: Output format (default: CSVWithNamesAndTypes)
- `--parallel-output`: Enable parallel output format processing
- `--with-requests`: Include deserialized request details (session, ZXID, operation type, etc.)
- `--start-index`: Start index (inclusive) of entries to process (default: 0)
- `--end-index`: End index (exclusive) of entries to process (default: max uint64_t)
- `--help, -h`: Show help message

#### Output Columns

**Basic columns (always included):**
- `log_index`: The index of the log entry
- `term`: The Raft term of the entry
- `entry_type`: Type of the log entry (app_log, config_log, etc.)
- `entry_size`: Size of the entry data in bytes
- `entry_crc32`: CRC32 checksum of the entry

**Additional columns (with `--with-requests`):**
- `session_id`: Client session ID for the request
- `zxid`: ZooKeeper Transaction ID
- `request_timestamp`: Timestamp when the request was made
- `digest_value`: Digest value for request verification
- `digest_version`: Version of the digest algorithm
- `op_num`: Type of operation (Create, Set, Get, etc.)
- `xid`: Transaction ID for the request
- `request_idx`: Index of the request (0 for single requests, 1-N for multi requests)
- `path`: Path affected by the request
- `has_watch`: Whether the request includes a watch
- `version`: Version number for the node (for versioned operations)
- `is_ephemeral`: Whether the node is ephemeral (for create operations)
- `is_sequential`: Whether the node is sequential (for create operations)
- `data`: Node data (for create/set operations)

#### Examples

```bash
# Basic usage with console output
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin

# Save to file with JSON format
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --output-file output.json --output-format JSONEachRow

# Process a specific range of entries
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --start-index 100 --end-index 200

# Include detailed request information
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --with-requests

# Process a specific range with request details and save to CSV
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --start-index 1000 --end-index 2000 --with-requests --output-file requests.csv

# Enable parallel processing for large files
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --output-file output.csv --parallel-output

# Combine all options
clickhouse-keeper-utils changelog-deserializer --changelog-path /var/lib/clickhouse/coordination/logs/changelog_1.bin \
    --start-index 5000 --end-index 10000 --with-requests --output-file filtered_requests.csv --parallel-output
```

#### Notes
- When using `--with-requests`, the output will include detailed information about each request, including session information, operation type, and node data.
- For multi-requests (transactions), each sub-request will be shown on a separate row with the same `log_index` but different `request_idx`.
- The `entry_type` field indicates the type of log entry (e.g., `app_log` for regular operations, `config_log` for configuration changes).
- The `op_num` field shows the type of ZooKeeper operation (Create, Set, Get, etc.) using the standard ZooKeeper op codes.
- When processing large changelogs, consider using `--parallel-output` for better performance.

## Output Formats

The `dump-state` command supports various output formats, including:
- `CSV` / `CSVWithNames` / `CSVWithNamesAndTypes` (default)
- `JSON` / `JSONEachRow`
- `TSV` / `TSVRaw` / `TSVWithNames` / `TSVWithNamesAndTypes`
- And other formats supported by ClickHouse

## Troubleshooting

- **Permission Issues**: Ensure the utility has read access to Keeper data directories
- **Corrupted Files**: If you encounter errors about corrupted files, try validating them with the analyzer commands
- **Performance**: For large states, use `--parallel-output` and redirect output to a file

## License

This utility is part of ClickHouse and is distributed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
