---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 45
toc_title: "Hiyerar\u015Fik s\xF6zl\xFCkler"
---

# Hiyerarşik Sözlükler {#hierarchical-dictionaries}

ClickHouse bir hiyerarşik sözlükler destekler [sayısal tuş](external_dicts_dict_structure.md#ext_dict-numeric-key).

Aşağıdaki hiyerarşik yapıya bakın:

``` text
0 (Common parent)
│
├── 1 (Russia)
│   │
│   └── 2 (Moscow)
│       │
│       └── 3 (Center)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

Bu hiyerarşi aşağıdaki sözlük tablosu olarak ifade edilebilir.

| region\_id | parent\_region | region\_name |
|------------|----------------|--------------|
| 1          | 0              | Rusya        |
| 2          | 1              | Moskova      |
| 3          | 2              | Merkezli     |
| 4          | 0              | İngiltere    |
| 5          | 4              | Londra       |

Bu tablo bir sütun içerir `parent_region` bu öğe için en yakın ebeveynin anahtarını içerir.

ClickHouse destekler [hiyerarşik](external_dicts_dict_structure.md#hierarchical-dict-attr) için mülkiyet [dış sözlük](index.md) öznitelik. Bu özellik, yukarıda açıklanana benzer hiyerarşik sözlüğü yapılandırmanıza izin verir.

Bu [dictGetHierarchy](../../../sql_reference/functions/ext_dict_functions.md#dictgethierarchy) fonksiyonu bir elemanın üst zincir almak için izin verir.

Örneğimiz için, sözlüğün yapısı aşağıdaki gibi olabilir:

``` xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
