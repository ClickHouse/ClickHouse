---
description: '该引擎提供对亚马逊 S3 中现有 Apache Hudi 表的只读集成。'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Hudi 表引擎'
doc_type: '参考'
---

该引擎提供对亚马逊 S3 中现有 Apache [Hudi](https://hudi.apache.org/) 表的只读集成。

<div id="create-table">
  ## 创建表
</div>

请注意，Hudi 表必须已存在于 S3 中；此命令不支持传入用于创建新表的 DDL 参数。

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**引擎参数**

* `url` — 指向现有 Hudi 表路径的存储桶 URL。
* `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) 账户用户的长期凭证。您可以使用它们对请求进行身份验证。该参数为可选项。如果未指定凭证，则使用配置文件中的凭证。
* `extra_credentials` - 可选。用于在 ClickHouse Cloud 中传递 `role_arn` 以实现基于角色的访问。有关配置步骤，请参阅 [Secure S3](/zh/cloud/data-sources/secure-s3)。

引擎参数可以使用 [Named Collections](/zh/operations/named-collections.md) 指定。

**示例**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

使用命名集合：

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## 另请参阅
</div>

* [hudi 表函数](/zh/sql-reference/table-functions/hudi.md)