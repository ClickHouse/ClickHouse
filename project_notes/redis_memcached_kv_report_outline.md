# Redis and Memcached-Compatible Key-Value Access for ClickHouse: Graduation Report Outline

## Problem Statement

ClickHouse is widely used to prepare analytical datasets, but many online systems need to read small pieces of prepared data by key with minimal protocol overhead. Existing clients in areas such as personalization, antifraud, and real-time decision making often already support Redis or Memcached protocols. The problem is to analyze and design a read-only compatibility layer that exposes selected ClickHouse-backed key-value data marts through familiar cache protocols without turning ClickHouse into a full cache server.

## Relevance of the Project

The project is relevant because it connects analytical preparation and operational serving. If prepared ClickHouse datasets can be queried by key through Redis-compatible commands, applications can reuse existing client libraries and reduce the need for separate data-export pipelines. This may simplify architectures where ClickHouse computes or stores derived features, while online services need fast key-based access to those features.

## Object and Subject of the Work

The object of the work is ClickHouse as a database system that supports multiple network protocols and several key-value-capable entities.

The subject of the work is a read-only Redis-compatible, and optionally Memcached-compatible, access layer for ClickHouse entities that implement `IKeyValueEntity`.

## Goal of the Work

The goal is to design and implement a minimal read-only Redis-compatible endpoint for ClickHouse key-value data marts, evaluate its feasibility, and define how the approach can be extended to a Memcached text protocol endpoint.

## Research and Engineering Tasks

- Analyze ClickHouse protocol server registration and existing compatibility handlers.
- Analyze `IKeyValueEntity` and current implementations such as dictionaries and `EmbeddedRocksDB`.
- Define the mapping between Redis commands and ClickHouse key-value lookup semantics.
- Design a minimal command set: `PING`, `QUIT`, `SELECT`, `GET`, and `MGET`.
- Define value encoding rules for ClickHouse typed results returned as Redis bulk strings.
- Design endpoint configuration and target selection for key-value entities.
- Implement the Redis-compatible MVP in a later engineering phase.
- Prepare tests for command parsing, missing keys, batched lookup, and connection behavior.
- Evaluate latency, throughput, and limitations against SQL-based lookup and relevant baselines.
- Describe optional extensions, including `HGET`, `HMGET`, `AUTH`, and Memcached text protocol support.

## Expected Practical Result

The expected practical result is a working prototype or MVP that allows Redis clients to read selected ClickHouse-backed key-value data through a small read-only Redis-compatible command subset. The report should also document design constraints, performance characteristics, and a path for future Memcached text protocol support.

## Scope Limitations

- The first version is read-only.
- The project is not a full Redis replacement.
- The project is not a full Memcached replacement.
- The Redis-compatible endpoint is the main MVP.
- Memcached text protocol support is an optional extension.
- Write commands, eviction policies, pub/sub, transactions, Lua scripting, clustering, and replication protocol compatibility are out of scope.

## Proposed Chapter Structure

### Chapter 1: Domain and Architecture Analysis

This chapter describes key-value data marts, Redis and Memcached access patterns, ClickHouse protocol server architecture, existing compatibility handlers, and the current `IKeyValueEntity` abstraction.

### Chapter 2: Design of the Extension

This chapter defines the endpoint model, command subset, request parsing, response encoding, target selection, authentication assumptions, error handling, and how Redis-compatible operations map to `IKeyValueEntity::getByKeys`.

### Chapter 3: Implementation

This chapter describes the implemented Redis-compatible handler, protocol parser and writer, integration with `clickhouse-server`, configuration, and test coverage. If Memcached support is implemented, it is described as an extension rather than the core MVP.

### Chapter 4: Experimental Evaluation

This chapter evaluates correctness and practical performance. It should compare single-key and multi-key lookup behavior, discuss latency and throughput, and identify bottlenecks caused by parsing, lookup, type conversion, or serialization.

## Why Use `IKeyValueEntity`

Using `IKeyValueEntity` is preferable to adding an unrelated storage interface because it reuses an abstraction already present in ClickHouse for direct key-value lookup. It is implemented by dictionaries, `EmbeddedRocksDB`, `KeeperMap`, `StorageRedis`, and the direct-join `MergeTree` adapter. `StorageRedis` is an implementation and useful example of `IKeyValueEntity` for reading from an external Redis server, but the first demonstration targets for this project should be `EmbeddedRocksDB` and dictionaries. Reusing `IKeyValueEntity` keeps the protocol endpoint independent from one storage engine and aligns the project with existing planner and direct-join infrastructure.

## Continuation of ClickHouse Issue `#33581`

ClickHouse issue [#33581](https://github.com/ClickHouse/ClickHouse/issues/33581) proposed key-value data marts and Redis or Memcached-compatible access. The current project continues that idea but adapts it to the current ClickHouse architecture: instead of introducing a new generic `get` method on `IStorage`, it can build on the existing `IKeyValueEntity` interface and the existing protocol-server registration framework in `clickhouse-server`. This keeps the project focused on a small read-only compatibility endpoint while preserving a path for broader support later.
