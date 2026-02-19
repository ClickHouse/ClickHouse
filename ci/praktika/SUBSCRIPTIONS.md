# GitHub Repository Subscriptions

## Overview

The GitHub Repository Subscriptions feature provides functionality to manage and track which GitHub repositories are being monitored or subscribed to. This is particularly useful for CI/CD workflows and automation scripts that need to maintain a list of repositories to watch for changes or events.

## Usage

### Command Line Interface

The subscriptions module can be used directly from the command line:

```bash
# List all subscribed repositories
python ci/praktika/subscriptions.py list

# Add a repository subscription
python ci/praktika/subscriptions.py add owner/repo

# Remove a repository subscription
python ci/praktika/subscriptions.py remove owner/repo
```

### Python API

You can also use the subscriptions module in your Python code:

```python
from praktika.subscriptions import GitHubSubscriptions

# Create a manager instance
manager = GitHubSubscriptions()

# List subscriptions
repos = manager.list_subscriptions()
print(f"Subscribed to {len(repos)} repositories")

# Add a subscription
manager.add_subscription("ClickHouse/ClickHouse")

# Remove a subscription
manager.remove_subscription("owner/repo")

# Get formatted output
print(manager.format_list())
```

### Integration with GH Class

The subscription functionality is integrated into the `GH` class for convenient access:

```python
from praktika.gh import GH

# List subscriptions
print(GH.list_subscriptions())

# Add a subscription
GH.add_subscription("ClickHouse/ClickHouse")

# Remove a subscription
GH.remove_subscription("owner/repo")
```

## Configuration File

Subscriptions are stored in `.github/subscriptions.json` with the following format:

```json
{
  "repositories": [
    "ClickHouse/ClickHouse",
    "owner/repo1",
    "owner/repo2"
  ],
  "version": "1.0"
}
```

### Configuration Location

By default, the configuration file is located at `.github/subscriptions.json` in the repository root. You can specify a custom location by passing the `config_path` parameter to the constructor:

```python
manager = GitHubSubscriptions(config_path="/path/to/custom/subscriptions.json")
```

## Features

- **Persistent storage**: Subscriptions are stored in a JSON configuration file
- **Duplicate prevention**: Adding an existing subscription is safely handled
- **Sorted output**: Repositories are automatically sorted alphabetically
- **Error handling**: Gracefully handles missing or corrupted configuration files
- **Easy integration**: Works seamlessly with existing CI/CD infrastructure

## Implementation Details

### File Format

The configuration file uses a simple JSON format with:
- `repositories`: Array of repository names in `owner/repo` format
- `version`: Schema version for future compatibility

### Error Handling

The module handles several error conditions:
- Missing configuration file: Returns empty list
- Corrupted JSON: Prints warning and returns empty list
- IO errors: Prints error message and returns appropriate status

### Thread Safety

The current implementation is not thread-safe. If you need concurrent access to subscriptions, you should implement your own locking mechanism.

## Testing

Run the test suite to verify functionality:

```bash
cd ci/praktika
python test_subscriptions.py -v
```

## Future Enhancements

Possible future enhancements include:
- Support for subscription metadata (tags, labels, priorities)
- Integration with GitHub webhooks for real-time updates
- Support for organization-level subscriptions
- Export/import functionality for backup and restore
- Web UI for managing subscriptions
