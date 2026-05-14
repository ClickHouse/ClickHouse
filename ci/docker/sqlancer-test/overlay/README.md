# SQLancer overlay sources

This directory contains Java sources that are **overlaid on top of the upstream
`ClickHouse/sqlancer` fork** during the Docker image build, before `mvn package`.

The Dockerfile clones the fork at a pinned commit, then `COPY`s everything under
`overlay/src/` into `/sqlancer/sqlancer-main/src/`. Any file present here
replaces the corresponding file in the fork; new files are simply added. This
way we can develop ClickHouse-specific oracles in the ClickHouse repository
without forking-the-fork or maintaining patches.

## Layout

```
overlay/
└── src/sqlancer/clickhouse/
    ├── ClickHouseOracleFactory.java       # replaces upstream — registers PQS / CERT / CODDTest
    └── oracle/
        ├── pqs/                            # Pivoted Query Synthesis (OSDI '20)
        ├── cert/                           # Cardinality Estimation Restriction Testing
        └── coddtest/                       # Cross-Optimization Decision Differential Testing
```

When the upstream fork pin in the Dockerfile is bumped, this overlay must be
re-checked against the new upstream sources — in particular,
`ClickHouseOracleFactory.java` is a full replacement and will silently drop any
new upstream entries unless you re-merge by hand.
