---
sidebar_position: 71
sidebar_label: Third-Party Libraries
description: A list of third-party libraries used
---

# Third-Party Libraries Used

ClickHouse utilizes third-party libraries for different purposes, e.g., to connect to other databases, to decode (encode) data during load (save) from (to) disk or to implement certain specialized SQL functions. To be independent of the available libraries in the target system, each third-party library is imported as a Git submodule into ClickHouse's source tree and compiled and linked with ClickHouse. A list of third-party libraries and their licenses can be obtained by the following query:

``` sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

(Note that the listed libraries are the ones located in the `contrib/` directory of the ClickHouse repository. Depending on the build options, some of of the libraries may have not been compiled, and as a result, their functionality may not be available at runtime.

[Example](https://play.clickhouse.com/play?user=play#U0VMRUNUIGxpYnJhcnlfbmFtZSwgbGljZW5zZV90eXBlLCBsaWNlbnNlX3BhdGggRlJPTSBzeXN0ZW0ubGljZW5zZXMgT1JERVIgQlkgbGlicmFyeV9uYW1lIENPTExBVEUgJ2VuJw==)

## Adding new third-party libraries and maintaining patches in third-party libraries {#adding-third-party-libraries}

1. Each third-party library must reside in a dedicated directory under the `contrib/` directory of the ClickHouse repository. Avoid dumps/copies of external code, instead use Git submodule feature to pull third-party code from an external upstream repository.
2. Submodules are listed in `.gitmodule`. If the external library can be used as-is, you may reference the upstream repository directly. Otherwise, i.e. the external library requires patching/customization, create a fork of the official repository in the [ClickHouse organization in GitHub](https://github.com/ClickHouse).
3. In the latter case, create a branch with `clickhouse/` prefix from the branch you want to integrate, e.g. `clickhouse/master` (for `master`) or `clickhouse/release/vX.Y.Z` (for a `release/vX.Y.Z` tag). The purpose of this branch is to isolate customization of the library from upstream work. For example, pulls from the upstream repository into the fork will leave all `clickhouse/` branches unaffected. Submodules in `contrib/` must only track `clickhouse/` branches of forked third-party repositories.
4. To patch a fork of a third-party library, create a dedicated branch with `clickhouse/` prefix in the fork, e.g. `clickhouse/fix-some-desaster`. Finally, merge the patch branch into the custom tracking branch (e.g. `clickhouse/master` or `clickhouse/release/vX.Y.Z`) using a PR.
5. Always create patches of third-party libraries with the official repository in mind. Once a PR of a patch branch to the `clickhouse/` branch in the fork repository is done and the submodule version in ClickHouse official repository is bumped, consider opening another PR from the patch branch to the upstream library repository. This ensures, that 1) the contribution has more than a single use case and importance, 2) others will also benefit from it, 3) the change will not remain a maintenance burden solely on ClickHouse developers.
9. To update a submodule with changes in the upstream repository, first merge upstream `master` (or a new `versionX.Y.Z` tag) into the `clickhouse`-tracking branch in the fork repository. Conflicts with patches/customization will need to be resolved in this merge (see Step 4.). Once the merge is done, bump the submodule in ClickHouse to point to the new hash in the fork.
