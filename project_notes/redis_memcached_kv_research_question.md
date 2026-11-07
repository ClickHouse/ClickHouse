# Redis and Memcached Key-Value Project Research Framing

This note reframes the graduation project as an engineering research project rather than only a feature implementation.

## Weak Framing

Implement Redis/Memcached protocol support in ClickHouse.

This framing is easy to understand, but it makes the work look like a checklist of protocol commands. It also creates pressure to maximize compatibility breadth instead of answering a concrete engineering question.

## Strong Framing

Investigate whether a specialized read-only key-value protocol path can reduce point-lookup overhead in ClickHouse without changing the storage layer.

This framing makes the project measurable. The implementation becomes an experimental instrument for comparing a normal SQL lookup path with a specialized key-value serving path over existing ClickHouse abstractions.

## Main Research Question

What part of point-lookup overhead in ClickHouse comes from the SQL/query-processing path, and how much can be reduced by a Redis-compatible endpoint over `IKeyValueEntity`?

## Secondary Research Questions

- How does ClickHouse SQL point lookup behave compared with a specialized key-value system such as Redis?
- How much do batching and `MGET`-style lookup affect throughput and tail latency?
- Which limitations appear when mapping ClickHouse typed columns to Redis/Memcached byte-string responses?
- Which safety limits are needed for production-like use, such as max command size and max number of keys in `MGET`?

## Hypotheses

- SQL point lookups have measurable overhead from SQL parsing, query analysis, planning, pipeline setup, and result serialization.
- A Redis-compatible read-only endpoint can reduce part of this overhead for prepared key-value datasets.
- `MGET`/batching can improve throughput compared with many independent single-key lookups.
- Tail latency is important for real-time serving scenarios and should be measured with p95/p99.

## Why `GET`/`MGET` Depth Matters More Than Broad Redis Coverage

The core research question is about point-lookup overhead, not full Redis compatibility. Deep work on `GET` and `MGET` is more valuable because these commands directly exercise single-key and batched key-value reads. They also expose the main technical issues: lookup target resolution, use of `IKeyValueEntity`, typed value conversion, missing-key semantics, batching limits, response serialization, throughput, and tail latency.

Implementing `SET`, `DEL`, `TTL`, Pub/Sub, Lua, Redis Cluster, streams, sorted sets, or broad command compatibility would expand the protocol surface, but it would not directly answer whether a specialized read-only path reduces ClickHouse point-lookup overhead. Those features would also introduce write semantics, lifecycle semantics, distributed cache behavior, and compatibility problems that are outside the intended research scope.

The project should therefore prefer a narrow command surface with careful measurement over a broad but shallow compatibility layer.

## How This Framing Should Appear in the Report

### Introduction

The introduction should present the project as a study of serving prepared ClickHouse datasets through a specialized read-only key-value access path. Redis compatibility should be described as a practical client-facing protocol choice, not as the main scientific novelty.

### Problem Statement

The problem statement should focus on overhead and integration: ClickHouse can store or prepare data suitable for key-value serving, but the SQL/query-processing path may add unnecessary cost for simple point lookups. The problem is to determine whether an endpoint built over `IKeyValueEntity` can reduce that cost without introducing a new storage-layer abstraction.

### Experimental Chapter

The experimental chapter should evaluate the hypotheses directly. It should compare SQL point lookups with the Redis-compatible endpoint, include single-key and `MGET` scenarios, report throughput and p95/p99 latency, and discuss where time is spent: parsing, lookup, conversion, serialization, and connection handling. A Redis baseline can be used as a specialized-system reference, but it should not be treated as a requirement for ClickHouse to match Redis in all scenarios.

### Defense Presentation

The defense presentation should lead with the research question and measured findings, not the number of implemented Redis commands. The implementation should be presented as a prototype used to test the idea that bypassing the SQL/query-processing path for prepared key-value datasets can reduce point-lookup overhead.

## One-Paragraph Version for the Supervisor

The project is not simply about adding Redis or Memcached commands to ClickHouse. The research question is whether a specialized read-only key-value protocol path can reduce the overhead of point lookups for prepared ClickHouse datasets without changing the storage layer. The implementation should focus on `GET` and `MGET` over `IKeyValueEntity`, then experimentally compare this path with normal SQL point lookups and with a specialized key-value system such as Redis, measuring throughput and p95/p99 latency. Broad Redis compatibility is deliberately out of scope because it would add many commands without answering the central performance and architecture question.
