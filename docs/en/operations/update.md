---
slug: /en/operations/update
sidebar_title: Self-managed Upgrade
title: Self-managed Upgrade
---

## ClickHouse upgrade overview

These are the recommended steps for a zero-downtime ClickHouse upgrade:

1. Read through the [changelogs](/docs/en/whats-new/changelog/index.md) for breaking changes (going back from the target release to the release you are currently on).
2. Make any updates identified in the breaking changes that can be made before upgrading, and a list of the changes that will need to be made after the upgrade.
3. Identify one or more replicas for each shard to keep up while the rest of the replicas for each shard are upgraded.
4. On the replicas that will be upgraded, one at a time:
   - shutdown ClickHouse server
   - upgrade the server to the target version
   - bring ClickHouse server up
   - wait for the Keeper messages to indicate that the system is stable
   - continue to the next replica
5. Check for errors in the Keeper log and the ClickHouse log
6. Upgrade the replicas identified in step 2 to the new version
7. Refer to the list of changes made in steps 1 and 2 and make the changes that need to be made after the upgrade.

:::note
This error message is expected when there are multiple versions of ClickHouse running in a replicated environment.  You will stop seeing these when all replicas are upgraded to the same version.
```
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```
:::

:::note
Unless there is a security fix needed for ClickHouse Keeper or Apache ZooKeeper it is not necessary to upgrade Keeper when you upgrade ClickHouse server.
:::

## ClickHouse server upgrade process

If ClickHouse was installed from `deb` packages, execute the following commands on the server:

``` bash
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
