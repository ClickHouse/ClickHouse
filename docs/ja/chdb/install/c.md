---
title: CとC++向けのchDBのインストール
sidebar_label: CとC++
slug: /ja/chdb/install/c
description: CとC++向けのchDBをインストールする方法
keywords: [chdb, 組み込み, clickhouse-lite, インストール]
---

# CとC++向けのchDBのインストール

## 要件

[libchdb](https://github.com/chdb-io/chdb)をインストールします:

```bash
curl -sL https://lib.chdb.io | bash
```


## 使用方法

[libchdb](https://github.com/chdb-io/chdb/blob/main/bindings.md)の手順に従って開始してください。

`chdb.h`

```c
#pragma once
#include <cstdint>
#include <stddef.h>

extern "C" {
struct local_result
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *の解放用
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
};

local_result * query_stable(int argc, char ** argv);
void free_result(local_result * result);
}
```
