---
slug: /en/development/contrib
sidebar_position: 73
sidebar_label: Third-Party Libraries
description: A list of third-party libraries used
---

# Third-Party Libraries Used

ClickHouse utilizes third-party libraries for different purposes, e.g., to connect to other databases, to decode/encode data during load/save from/to disk, or to implement certain specialized SQL functions.
To be independent of the available libraries in the target system, each third-party library is imported as a Git submodule into ClickHouse's source tree and compiled and linked with ClickHouse.
A list of third-party libraries and their licenses can be obtained by the following query:

``` sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

Note that the listed libraries are the ones located in the `contrib/` directory of the ClickHouse repository.
Depending on the build options, some of the libraries may have not been compiled, and, as a result, their functionality may not be available at runtime.

[Example](https://play.clickhouse.com/play?user=play#U0VMRUNUIGxpYnJhcnlfbmFtZSwgbGljZW5zZV90eXBlLCBsaWNlbnNlX3BhdGggRlJPTSBzeXN0ZW0ubGljZW5zZXMgT1JERVIgQlkgbGlicmFyeV9uYW1lIENPTExBVEUgJ2VuJw==)

## Adding and maintaining third-party libraries

Each third-party library must reside in a dedicated directory under the `contrib/` directory of the ClickHouse repository.
Avoid dumping copies of external code into the library directory.
Instead create a Git submodule to pull third-party code from an external upstream repository.

All submodules used by ClickHouse are listed in the `.gitmodule` file.
If the library can be used as-is (the default case), you can reference the upstream repository directly.
If the library needs patching, create a fork of the upstream repository in the [ClickHouse organization on GitHub](https://github.com/ClickHouse).

In the latter case, we aim to isolate custom patches as much as possible from upstream commits.
To that end, create a branch with prefix `clickhouse/` from the branch or tag you want to integrate, e.g. `clickhouse/master` (for branch `master`) or `clickhouse/release/vX.Y.Z` (for tag `release/vX.Y.Z`).
This ensures that pulls from the upstream repository into the fork will leave custom `clickhouse/` branches unaffected.
Submodules in `contrib/` must only track `clickhouse/` branches of forked third-party repositories.

Patches are only applied against `clickhouse/` branches of external libraries.
For that, push the patch as a branch with `clickhouse/`, e.g. `clickhouse/fix-some-desaster`.
Then create a PR from the new branch against the custom tracking branch with `clickhouse/` prefix, (e.g. `clickhouse/master` or `clickhouse/release/vX.Y.Z`) and merge the patch.

Create patches of third-party libraries with the official repository in mind and consider contributing the patch back to the upstream repository.
This makes sure that others will also benefit from the patch and it will not be a maintenance burden for the ClickHouse team.

To pull upstream changes into the submodule, you can use two methods:
- (less work but less clean): merge upstream `master` into the corresponding `clickhouse/` tracking branch in the forked repository. You will need to resolve merge conflicts with previous custom patches. This method can be used when the `clickhouse/` branch tracks an upstream development branch like `master`, `main`, `dev`, etc.
- (more work but cleaner): create a new branch with `clickhouse/` prefix from the upstream commit or tag you like to integrate. Then re-apply all existing patches using new PRs (or squash them into a single PR). This method can be used when the `clickhouse/` branch tracks a specific upstream version branch or tag. It is cleaner in the sense that custom patches and upstream changes are better isolated from each other.

Once the submodule has been updated, bump the submodule in ClickHouse to point to the new hash in the fork.
