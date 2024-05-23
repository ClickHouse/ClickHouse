# Contribution

We keep things simple. Execute all commands in this folder.

## Requirements

- docker - tested on version 20.10.12.
- golang >= go1.17.6

## Building

Creates a binary `clickhouse-diagnostics` in the local folder. Build will be versioned according to a timestamp. For a versioned release see [Releasing](#releasing).

```bash
make build
```

## Linting

We use [golangci-lint](https://golangci-lint.run/). We use a container to run so no need to install.

```bash
make lint-go
```

## Running Tests

```bash
make test
```

For a coverage report,

```bash
make test-coverage
```

## Adding Collectors

TODO


## Adding Outputs

TODO

## Frames

## Parameter Types
