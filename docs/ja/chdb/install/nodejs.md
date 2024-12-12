---
title: NodeJS 用 chDB のインストール
sidebar_label: NodeJS
slug: /ja/chdb/install/nodejs
description: NodeJS 用 chDB のインストール方法
keywords: [chdb, embedded, clickhouse-lite, nodejs, install]
---

# NodeJS 用 chDB のインストール

## 要件

[libchdb](https://github.com/chdb-io/chdb) をインストールします：

```bash
curl -sL https://lib.chdb.io | bash
```

## インストール

```bash
npm i chdb
```

## GitHub リポジトリ

プロジェクトの GitHub リポジトリは [chdb-io/chdb-node](https://github.com/chdb-io/chdb-node) で確認できます。

## 使用法

NodeJS アプリケーションで chdb-node モジュールをインポートし、その強力な機能を活用できます：

```javascript
const { query, Session } = require("chdb");

var ret;

// スタンドアローンのクエリをテスト
ret = query("SELECT version(), 'Hello chDB', chdb()", "CSV");
console.log("Standalone Query Result:", ret);

// セッションのクエリをテスト
// 新しいセッションインスタンスを作成
const session = new Session("./chdb-node-tmp");
ret = session.query("SELECT 123", "CSV")
console.log("Session Query Result:", ret);
ret = session.query("CREATE DATABASE IF NOT EXISTS testdb;" +
    "CREATE TABLE IF NOT EXISTS testdb.testtable (id UInt32) ENGINE = MergeTree() ORDER BY id;");

session.query("USE testdb; INSERT INTO testtable VALUES (1), (2), (3);")

ret = session.query("SELECT * FROM testtable;")
console.log("Session Query Result:", ret);

// セッションのクリーンアップ
session.cleanup();
```

## ソースからビルド

```bash
npm run libchdb
npm install
npm run test
```
