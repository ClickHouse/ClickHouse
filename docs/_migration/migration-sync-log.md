# Migration sync log

Record of upstream `clickhouse/clickhouse-docs` PRs that were reviewed as part
of the ongoing Docusaurus → Mintlify migration sync but resulted in **no code
change** in this repo (typically because the upstream change touched
Docusaurus-only build plumbing that has no Mintlify equivalent).

This file exists so that "no-op" ports still leave a paper trail alongside
`slug-map.csv`.

| Upstream PR | Title | Reason for no-op |
| --- | --- | --- |
| [#6507](https://github.com/ClickHouse/clickhouse-docs/pull/6507) | Remove `oom-canary` from floating-pages exceptions | Only touches `plugins/floating-pages-exceptions.txt`, a Docusaurus build-plugin config with no equivalent in Mintlify (Mintlify enforces navigation via `docs.json`, not a floating-pages plugin). The `operations/settings/oom-canary` page already exists in this repo at `docs/en/operations/settings/oom-canary.md`. |
