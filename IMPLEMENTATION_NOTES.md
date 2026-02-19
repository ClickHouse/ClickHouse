# Implementation of `/github subscribe list` Feature

## Summary

This PR implements functionality for the `/github subscribe list` command, which allows managing and listing GitHub repository subscriptions. This feature is useful for CI/CD workflows and automation scripts that need to track which repositories are being monitored.

## What Was Implemented

### 1. Core Subscription Module (`ci/praktika/subscriptions.py`)

A new Python module that provides:
- **GitHubSubscriptions class**: Manages repository subscriptions
- **list_subscriptions()**: Lists all subscribed repositories
- **add_subscription()**: Adds a new repository subscription
- **remove_subscription()**: Removes a repository subscription
- **format_list()**: Formats subscription list for display

### 2. Integration with GH Class (`ci/praktika/gh.py`)

Added three new methods to the existing `GH` class:
- `GH.list_subscriptions()`: List subscribed repositories
- `GH.add_subscription(repo)`: Add a repository subscription
- `GH.remove_subscription(repo)`: Remove a repository subscription

### 3. Command-Line Interface (`github-command.py`)

A CLI tool that mimics GitHub bot commands:
```bash
python github-command.py subscribe list
python github-command.py subscribe add owner/repo
python github-command.py subscribe remove owner/repo
```

### 4. Configuration File (`.github/subscriptions.json`)

A JSON configuration file that stores subscriptions:
```json
{
  "repositories": [
    "ClickHouse/ClickHouse"
  ],
  "version": "1.0"
}
```

### 5. Comprehensive Tests (`ci/praktika/test_subscriptions.py`)

12 unit tests covering:
- Empty subscription lists
- Adding/removing subscriptions
- Duplicate handling
- Persistence across instances
- JSON format validation
- Error handling for corrupted files

### 6. Documentation (`ci/praktika/SUBSCRIPTIONS.md`)

Comprehensive documentation including:
- Usage examples (CLI and Python API)
- Configuration file format
- Integration examples
- Error handling details
- Future enhancement ideas

## Testing

All tests pass successfully:
```
Ran 12 tests in 0.008s
OK
```

The feature has been tested with:
- Direct Python API calls
- GH class integration
- Command-line interface
- Edge cases (empty lists, duplicates, missing files)

## Usage Examples

### Command Line
```bash
# List subscriptions (implements /github subscribe list)
python github-command.py subscribe list
# Output: Subscribed to 1 repositories:
#   - ClickHouse/ClickHouse

# Add a subscription
python github-command.py subscribe add owner/repo

# Remove a subscription
python github-command.py subscribe remove owner/repo
```

### Python API
```python
from praktika.gh import GH

# List subscriptions
print(GH.list_subscriptions())

# Add subscription
GH.add_subscription("ClickHouse/ClickHouse")

# Remove subscription
GH.remove_subscription("owner/repo")
```

### Direct Module Usage
```python
from praktika.subscriptions import GitHubSubscriptions

manager = GitHubSubscriptions()
repos = manager.list_subscriptions()
print(f"Subscribed to {len(repos)} repositories")
```

## Files Changed

- **New files:**
  - `ci/praktika/subscriptions.py` - Core subscription management module
  - `ci/praktika/test_subscriptions.py` - Comprehensive test suite
  - `ci/praktika/SUBSCRIPTIONS.md` - Documentation
  - `github-command.py` - CLI interface
  - `.github/subscriptions.json` - Configuration file

- **Modified files:**
  - `ci/praktika/gh.py` - Added subscription methods to GH class

## Design Decisions

1. **JSON Storage**: Simple, human-readable format that's easy to edit manually
2. **Sorted Output**: Repositories are automatically sorted for consistency
3. **Error Handling**: Graceful handling of missing/corrupted files
4. **Integration**: Seamless integration with existing CI infrastructure
5. **Modularity**: Can be used standalone or through GH class
6. **Testing**: Comprehensive unit tests for reliability

## Future Enhancements

Potential improvements for future PRs:
- Subscription metadata (tags, labels, priorities)
- GitHub webhook integration
- Organization-level subscriptions
- Export/import functionality
- Web UI for managing subscriptions

## Notes

This implementation provides a minimal, focused solution for the `/github subscribe list` command requirement. The code is well-tested, documented, and follows existing patterns in the ClickHouse repository.
