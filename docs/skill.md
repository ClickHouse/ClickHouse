---
name: clickhouse
description: Get started with ClickHouse. What it is, and how to install, run, and query ClickHouse locally and in ClickHouse Cloud using the clickhousectl CLI. Use this when a user wants to understand ClickHouse or start building with it.
license: Apache-2.0
metadata:
  author: ClickHouse Inc
  version: "0.1.0"
---

# ClickHouse

ClickHouse is an open-source, column-oriented database for real-time analytics at scale, built to run aggregations over billions of rows in milliseconds. It powers observability platforms, product and web analytics, data warehousing, time-series and ML workloads, and increasingly the data layer behind AI agents.

## Products

- **[ClickHouse](https://clickhouse.com/clickhouse)**: the open-source column-oriented database for real-time analytics.
- **[ClickHouse Cloud](https://clickhouse.com/cloud)**: the fully managed service for both ClickHouse and Postgres. Handles scaling, backups, upgrades, security, and enterprise operations.
- **[Postgres](https://clickhouse.com/cloud/postgres)**: fully managed Postgres in ClickHouse Cloud.
- **[ClickStack](https://clickhouse.com/clickstack)**: open-source observability stack built on OpenTelemetry, ClickHouse, and HyperDX for logs, metrics, and traces. Also available as a [managed service](https://clickhouse.com/cloud/clickstack) in ClickHouse Cloud.
- **[chDB](https://clickhouse.com/chdb)**: in-process ClickHouse: an embeddable SQL engine you can run inside Python and other languages with no server.
- **[ClickPipes](https://clickhouse.com/cloud/clickpipes)**: managed, continuous data ingestion into ClickHouse Cloud from Kafka, object storage, Postgres, and more.
- **[Langfuse](https://langfuse.com/)**: open-source LLM engineering and observability platform, backed by ClickHouse.
- **[clickhousectl](https://clickhouse.com/docs/concepts/features/interfaces/cli)**: the official CLI for ClickHouse, for both local and Cloud. Supports ClickHouse, Postgres and ClickPipes.
- **[Agentic data stack](https://clickhouse.com/ai)**: ClickHouse + LibreChat + the ClickHouse MCP server + Langfuse for building AI-native data applications.

## Quick start (local)

```bash
# Install the clickhousectl CLI
curl -fsSL https://clickhouse.com/cli | sh

# Install the latest ClickHouse and set it as the default
clickhousectl local use latest

# Start a local server (named "default")
clickhousectl local server start

# Run a query against it
clickhousectl local client -q "SELECT 'Hello, world!'"
```

These steps install the CLI, install ClickHouse, start a local server, and run a query. `clickhousectl` can install any version of ClickHouse and run multiple named local servers side by side. It can also provision a local **Postgres** instance, so you can develop against ClickHouse and Postgres together as a unified data stack.

## ClickHouse Cloud

ClickHouse Cloud is the managed service for ClickHouse and Postgres. It handles scaling, backups, upgrades, security, and enterprise operations for both, so you don't run any infrastructure yourself.

```bash
# Create an account (opens your browser)
clickhousectl cloud auth signup

# Authenticate the CLI. The default OAuth login is read-only;
# creating and managing resources requires an API key.
clickhousectl cloud auth login --api-key <key> --api-secret <secret>

# Create a managed ClickHouse service
clickhousectl cloud service create --name <name>

# Create a managed Postgres service (beta)
clickhousectl cloud postgres create --name <name>
```

Run SQL against a Cloud service over HTTP with `clickhousectl cloud service query --name <name> -q "<sql>"`.

## Documentation

Full documentation: https://clickhouse.com/docs

## APIs

Public APIs (including the ClickHouse Cloud API and its OpenAPI spec) are listed in the API catalog ([RFC 9727](https://www.rfc-editor.org/info/rfc9727)): https://clickhouse.com/.well-known/api-catalog

## Agent skills

Install the official ClickHouse agent skills into your coding agent (Claude Code, Cursor, Codex, and more). These teach your agent how to use ClickHouse and the CLI:

```bash
clickhousectl skills
```

Browse the full skill library at https://github.com/ClickHouse/agent-skills

## ClickHouse MCP server

Connect ClickHouse to AI assistants via the Model Context Protocol: run queries, list databases, and explore tables from your agent.

Docs: https://clickhouse.com/docs/guides/use-cases/ai-ml/MCP
