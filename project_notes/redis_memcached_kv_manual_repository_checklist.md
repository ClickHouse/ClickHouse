# Manual Repository Checklist for Redis and Memcached Key-Value Analysis

This checklist contains shell commands for manually verifying the repository facts used in the analysis phase.

Run commands from the repository root.

## 1. Verify Current Git Branch and Working Tree

```bash
git branch --show-current
git status
git diff --stat
git diff --name-only
```

Expected branch for this work:

```text
redis-handler
```

## 2. Verify Protocol Server Registration in `programs/server/Server.cpp`

Find the main server creation code:

```bash
rg "void Server::createServers|Server::buildProtocolStackFromConfig|Server::createServer" programs/server/Server.cpp
```

Inspect the relevant sections:

```bash
sed -n '3280,3740p' programs/server/Server.cpp
```

Search for existing protocol ports and factories:

```bash
rg "http_port|https_port|tcp_port|tcp_with_proxy_port|tcp_port_secure|mysql_port|postgresql_port|grpc_port|prometheus.port|arrowflight_port" programs/server/Server.cpp
rg "TCPHandlerFactory|MySQLHandlerFactory|PostgreSQLHandlerFactory|GRPCServer|ArrowFlightServer|HTTPHandlerFactory" programs/server/Server.cpp
```

## 3. Search for Existing Protocol Handler Factories

List handler factories under `src/Server`:

```bash
rg --files src/Server | rg "HandlerFactory|TCPProtocolStackFactory|GRPCServer|ArrowFlightServer"
```

Search for factory classes:

```bash
rg "class .*HandlerFactory|class TCPProtocolStackFactory|createConnectionImpl" src/Server -n
```

Inspect the most relevant examples:

```bash
sed -n '1,220p' src/Server/TCPHandlerFactory.h
sed -n '1,220p' src/Server/MySQLHandlerFactory.h
sed -n '1,220p' src/Server/PostgreSQLHandlerFactory.h
sed -n '1,120p' src/Server/KeeperTCPHandlerFactory.h
```

## 4. Find `IKeyValueEntity` Definition

Locate the interface:

```bash
rg --files src | rg "IKeyValueEntity"
```

Inspect it:

```bash
sed -n '1,180p' src/Interpreters/IKeyValueEntity.h
```

Search usage:

```bash
rg "IKeyValueEntity|getByKeys|getSampleBlock|getPrimaryKey" src/Interpreters src/Planner src/Processors src/Storages src/Dictionaries -n
```

## 5. Find All Current `IKeyValueEntity` Implementations

Search direct inheritance:

```bash
rg "public IKeyValueEntity|class .*:.*IKeyValueEntity" src/Dictionaries src/Storages src/Interpreters -n
```

Expected current direct implementers or adapters:

```text
src/Dictionaries/IDictionary.h
src/Storages/RocksDB/StorageEmbeddedRocksDB.h
src/Storages/StorageKeeperMap.h
src/Storages/StorageRedis.h
src/Interpreters/DirectJoinMergeTreeEntity.h
```

Verify that related storages are not listed as `IKeyValueEntity` implementers:

```bash
rg "class StorageJoin|class StorageSet|IKeyValueEntity" src/Storages/StorageJoin.h src/Storages/StorageSet.h
```

## 6. Inspect `EmbeddedRocksDB` `getByKeys` Implementation

Find declaration and implementation:

```bash
rg "getByKeys|getBySerializedKeys|getPrimaryKey" src/Storages/RocksDB/StorageEmbeddedRocksDB.h src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp -n
```

Inspect implementation:

```bash
sed -n '860,960p' src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp
```

Also inspect class inheritance:

```bash
sed -n '1,120p' src/Storages/RocksDB/StorageEmbeddedRocksDB.h
```

## 7. Inspect `IDictionary` `getByKeys` Implementation

Find relevant methods:

```bash
rg "class IDictionary|getByKeys|getSampleBlock|getPrimaryKey|hasKeys|getColumns" src/Dictionaries/IDictionary.h -n
```

Inspect the `IKeyValueEntity` implementation block:

```bash
sed -n '330,455p' src/Dictionaries/IDictionary.h
```

## 8. Verify That `RedisHandler` and `MemcachedHandler` Do Not Exist Yet

Search server files:

```bash
rg "RedisHandler|RedisHandlerFactory|RedisProtocol|MemcachedHandler|MemcachedHandlerFactory|MemcachedProtocol" src programs tests docs project_notes -n
```

Search filenames:

```bash
rg --files | rg "RedisHandler|RedisProtocol|MemcachedHandler|MemcachedProtocol"
```

Expected result: no production handler or protocol files. Mentions may exist only in project notes or this checklist.

## 9. Verify That Only `project_notes` Files Were Changed During the Analysis Phase

Review changed files:

```bash
git diff --name-only
git status --short
```

Check project notes:

```bash
find project_notes -maxdepth 1 -type f -print
git diff --stat -- project_notes
```

Check for accidental production changes:

```bash
git diff --name-only | rg -v "^project_notes/"
```

Expected result for the last command: no output, unless there are unrelated pre-existing user changes that must not be touched.

## 10. Suggested Commands Before Committing the Analysis Phase

Review branch and diff:

```bash
git branch --show-current
git status
git diff --stat
git diff --name-only
git diff -- project_notes
```

List notes that will be committed:

```bash
find project_notes -maxdepth 1 -type f -print
```

Confirm no C++ or build files are included in the analysis change:

```bash
git diff --name-only | rg "\\.(h|hpp|cpp|c|cc|cmake)$|(^|/)CMakeLists\\.txt$|(^|/)cmake/"
```

Expected result: no output for this analysis-only phase.
