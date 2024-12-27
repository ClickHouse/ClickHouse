---
sidebar_label: R2DBC Driver
sidebar_position: 5
keywords: [clickhouse, java, driver, integrate, r2dbc]
description: ClickHouse R2DBC Driver
slug: /ja/integrations/java/r2dbc
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

## R2DBC ドライバー

[R2DBC](https://r2dbc.io/) は、ClickHouse 向けの非同期 Java クライアントのラッパーです。

### 環境要件

- [OpenJDK](https://openjdk.java.net) バージョン >= 8

### セットアップ

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- SPI 0.9.1.RELEASE の場合、clickhouse-r2dbc_0.9.1 に変更 -->
    <artifactId>clickhouse-r2dbc</artifactId>
    <version>0.6.5</version>
    <!-- すべての依存関係を含むuber jarを使用し、より小さいjarにするためにclassifierをhttpまたはgrpcに変更 -->
    <classifier>all</classifier>
    <exclusions>
        <exclusion>
            <groupId>*</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### ClickHouse に接続

```java showLineNumbers
ConnectionFactory connectionFactory = ConnectionFactories
    .get("r2dbc:clickhouse:http://{username}:{password}@{host}:{port}/{database}");

    Mono.from(connectionFactory.create())
        .flatMapMany(connection -> connection
```

### クエリ

```java showLineNumbers
connection
    .createStatement("select domain, path,  toDate(cdate) as d, count(1) as count from clickdb.clicks where domain = :domain group by domain, path, d")
    .bind("domain", domain)
    .execute()
    .flatMap(result -> result
    .map((row, rowMetadata) -> String.format("%s%s[%s]:%d", row.get("domain", String.class),
        row.get("path", String.class),
        row.get("d", LocalDate.class),
        row.get("count", Long.class)))
    )
    .doOnNext(System.out::println)
    .subscribe();
```

### インサート

```java showLineNumbers
connection
    .createStatement("insert into clickdb.clicks values (:domain, :path, :cdate, :count)")
    .bind("domain", click.getDomain())
    .bind("path", click.getPath())
    .bind("cdate", LocalDateTime.now())
    .bind("count", 1)
    .execute();
```
