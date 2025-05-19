---
description: 'Documentation for Update'
sidebar_title: 'Self-managed Upgrade'
slug: /operations/update
title: 'Self-managed Upgrade'
---

## ClickHouse upgrade overview {#clickhouse-upgrade-overview}

This document contains:
- general guidelines
- a recommended plan
- specifics for upgrading the binaries on your systems

## General guidelines {#general-guidelines}

These notes should help you with planning, and to understand why we make the recommendations that we do later in the document.

### Upgrade ClickHouse server separately from ClickHouse Keeper or ZooKeeper {#upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper}
Unless there is a security fix needed for ClickHouse Keeper or Apache ZooKeeper it is not necessary to upgrade Keeper when you upgrade ClickHouse server.  Keeper stability is required during the upgrade process, so complete the ClickHouse server upgrades before considering an upgrade of Keeper.

### Minor version upgrades should be adopted often {#minor-version-upgrades-should-be-adopted-often}
It is highly recommended to always upgrade to the newest minor version as soon as it is released. Minor releases do not have breaking changes but do have important bug fixes (and may have security fixes).


### Test experimental features on a separate ClickHouse server running the target version {#test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version}

The compatibility of experimental features can be broken at any moment in any way.  If you are using experimental features, then check the changelogs and consider setting up a separate ClickHouse server with the target version installed and test your use of the experimental features there.

### Downgrades {#downgrades}
If you upgrade and then realize that the new version is not compatible with some feature that you depend on you may be able to downgrade to a recent (less than one year old) version if you have not started to use any of the new features.  Once the new features are used the downgrade will not work.

### Multiple ClickHouse server versions in a cluster {#multiple-clickhouse-server-versions-in-a-cluster}

We make an effort to maintain a one-year compatibility window (which includes 2 LTS versions). This means that any two versions should be able to work together in a cluster if the difference between them is less than one year (or if there are less than two LTS versions between them). However, it is recommended to upgrade all members of a cluster to the same version as quickly as possible, as some minor issues are possible (like slowdown of distributed queries, retriable errors in some background operations in ReplicatedMergeTree, etc).

We never recommend running different versions in the same cluster when the release dates are more than one year. While we do not expect that you will have data loss, the cluster may become unusable. The issues that you should expect if you have more than one year difference in versions include:

- the cluster may not work
- some (or even all) queries may fail with arbitrary errors
- arbitrary errors/warnings may appear in the logs
- it may be impossible to downgrade

### Incremental upgrades {#incremental-upgrades}

If the difference between the current version and the target version is more than one year, then it is recommended to either:
- Upgrade with downtime (stop all servers, upgrade all servers, run all servers).
- Or to upgrade through an intermediate version (a version less than one year more recent than the current version).



## Recommended plan {#recommended-plan}

These are the recommended steps for a zero-downtime ClickHouse upgrade:

1. Make sure that your configuration changes are not in the default `/etc/clickhouse-server/config.xml` file and that they are instead in `/etc/clickhouse-server/config.d/`, as `/etc/clickhouse-server/config.xml` could be overwritten during an upgrade.
2. Read through the [changelogs](/whats-new/changelog/index.md) for breaking changes (going back from the target release to the release you are currently on).
3. Make any updates identified in the breaking changes that can be made before upgrading, and a list of the changes that will need to be made after the upgrade.
4. Identify one or more replicas for each shard to keep up while the rest of the replicas for each shard are upgraded.
5. On the replicas that will be upgraded, one at a time:
   - shutdown ClickHouse server
   - upgrade the server to the target version
   - bring ClickHouse server up
   - wait for the Keeper messages to indicate that the system is stable
   - continue to the next replica
6. Check for errors in the Keeper log and the ClickHouse log
7. Upgrade the replicas identified in step 4 to the new version
8. Refer to the list of changes made in steps 1 through 3 and make the changes that need to be made after the upgrade.

:::note
This error message is expected when there are multiple versions of ClickHouse running in a replicated environment.  You will stop seeing these when all replicas are upgraded to the same version.
```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```
:::


## ClickHouse server binary upgrade process {#clickhouse-server-binary-upgrade-process}

If ClickHouse was installed from `deb` packages, execute the following commands on the server:

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

If you installed ClickHouse using something other than the recommended `deb` packages, use the appropriate update method.

:::note
You can update multiple servers at once as soon as there is no moment when all replicas of one shard are offline.
:::

The upgrade of older version of ClickHouse to specific version:

As an example:

`xx.yy.a.b` is a current stable version. The latest stable version could be found [here](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```
