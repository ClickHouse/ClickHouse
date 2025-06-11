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
- `--output-format, -f`: Output format (default: CSVWithNamesAndTypes)
- `--parallel-output`: Enable parallel output format processing
- `--debug-mode`: Enable debug output for log entries
- `--help, -h`: Show help message

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
```

### 2. snapshot-analyzer

Analyze Keeper snapshots and print basic information.

#### Usage
```bash
clickhouse-keeper-utils snapshot-analyzer --snapshot-path <path>
```

#### Options
- `--snapshot-path` (required): Path to the directory containing Keeper snapshots
- `--help, -h`: Show help message

#### Example
```bash
clickhouse-keeper-utils snapshot-analyzer --snapshot-path /var/lib/clickhouse/coordination/snapshots
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
