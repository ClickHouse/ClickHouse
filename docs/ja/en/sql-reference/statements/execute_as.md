---
description: 'EXECUTE AS ステートメントに関するドキュメント'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'EXECUTE AS ステートメント'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # EXECUTE AS ステートメント
</div>

別のユーザーとしてクエリを実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

最初の形式 (`subquery` なし) は、現在のセッションにおける以降のすべてのクエリが、指定された `target_user` として実行されるように設定します。

2 番目の形式 (`subquery` あり) は、指定された `subquery` のみを、指定された `target_user` として実行します。

両方の形式を機能させるには、config 設定 `access_control_improvements.allow_impersonate_user`
を `1` に設定し、`IMPERSONATE` 権限が付与されている必要があります。例として、次のコマンドは

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

ユーザー `user2` に `EXECUTE AS user1 ...` コマンドの実行を許可し、さらにユーザー `user3` には任意のユーザーとしてコマンドを実行することを許可します。

別のユーザーになり代わっている間、関数 [currentUser()](/ja/sql-reference/functions/other-functions#currentUser) はその別のユーザーの名前を返し、
関数 [authenticatedUser()](/ja/sql-reference/functions/other-functions#authenticatedUser) は実際に認証されたユーザーの名前を返します。

<div id="examples">
  ## 例
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```