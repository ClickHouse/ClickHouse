---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v26.8.14.3-lts (f1d4a0d3645) FIXME as compared to v26.8.13.2-lts (3eac80eef9b)

#### Improvement
* Backported in [#122540](https://github.com/ClickHouse/ClickHouse/issues/122540): TLS CA certificates configured with `openSSL.server.caConfig` / `openSSL.client.caConfig` (and `caConfig` of composable `protocols`) are now reloaded without a restart when the file changes or on `SYSTEM RELOAD CONFIG`, in the same way as `certificateFile` and `privateKeyFile`. This makes it possible to rotate the CA that is used to verify clients, other servers and Keeper nodes (including Raft connections between Keepers) without restarting anything. [#117387](https://github.com/ClickHouse/ClickHouse/pull/117387) ([James](https://github.com/sanjams2)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#122582](https://github.com/ClickHouse/ClickHouse/issues/122582): Fix a segmentation fault in Keeper when preallocating space for a changelog file fails because the disk is full. [#122337](https://github.com/ClickHouse/ClickHouse/pull/122337) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

