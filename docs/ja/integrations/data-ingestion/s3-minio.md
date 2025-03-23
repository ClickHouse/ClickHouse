---
sidebar_label: MinIO の使用
sidebar_position: 6
slug: /ja/integrations/minio
description: MinIO の使用
---

# MinIO の使用

import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

すべての S3 機能とテーブルは [MinIO](https://min.io/) と互換性があります。特にネットワークのローカリティが最適な場合、セルフマネージドの MinIO ストアでは優れたスループットが得られることがあります。

また、バックエンドの MergeTree 構成も一部の構成を微調整することで互換性があります:

```xml
<clickhouse>
    <storage_configuration>
        ...
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://min.io/tables//</endpoint>
                <access_key_id>your_access_key_id</access_key_id>
                <secret_access_key>your_secret_access_key</secret_access_key>
                <region></region>
                <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            </s3>
            <s3_cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/var/lib/clickhouse/disks/s3_cache/</path>
                <max_size>10Gi</max_size>
            </s3_cache>
        </disks>
        ...
    </storage_configuration>
</clickhouse>
```

:::tip
エンドポイントタグの二重スラッシュに注目してください。これはバケットルートを指定するために必要です。
:::
