---
title: Installing chDB for Bun
sidebar_label: Bun
slug: /ja/chdb/install/bun
description: How to install chDB for Bun
keywords: [chdb, embedded, clickhouse-lite, bun, install]
---

# Bun用のchDBをインストールする

## 要件

[libchdb](https://github.com/chdb-io/chdb) をインストールします:

```bash
curl -sL https://lib.chdb.io | bash
```

## インストール

参考: [chdb-bun](https://github.com/chdb-io/chdb-bun)

## GitHubリポジトリ

プロジェクトのGitHubリポジトリは [chdb-io/chdb-bun](https://github.com/chdb-io/chdb-bun) にあります。

## 使用方法

### Query(query, *format) (一時的)

```javascript
import { query } from 'chdb-bun';

// クエリ（一時的）
var result = query("SELECT version()", "CSV");
console.log(result); // 23.10.1.1
```

### Session.Query(query, *format)

```javascript
import { Session } from 'chdb-bun';
const sess = new Session('./chdb-bun-tmp');

// セッションでクエリ（永続化）
sess.query("CREATE FUNCTION IF NOT EXISTS hello AS () -> 'Hello chDB'", "CSV");
var result = sess.query("SELECT hello()", "CSV");
console.log(result);

// クリーンアップ前に、データベースファイルを`./chdb-bun-tmp`に見つけることができます

sess.cleanup(); // セッションをクリーンアップ、これによりデータベースが削除されます
```


