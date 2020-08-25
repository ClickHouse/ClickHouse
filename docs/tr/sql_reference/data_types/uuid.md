---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

Evrensel olarak benzersiz bir tanımlayıcı (UUID), kayıtları tanımlamak için kullanılan 16 baytlık bir sayıdır. UUID hakkında ayrıntılı bilgi için bkz [Vikipedi](https://en.wikipedia.org/wiki/Universally_unique_identifier).

UUID türü değeri örneği aşağıda temsil edilmektedir:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

Yeni bir kayıt eklerken UUID sütun değerini belirtmezseniz, UUID değeri sıfır ile doldurulur:

``` text
00000000-0000-0000-0000-000000000000
```

## Nasıl Oluşturulur {#how-to-generate}

UUID değerini oluşturmak için ClickHouse, [generateuuıdv4](../../sql_reference/functions/uuid_functions.md) işlev.

## Kullanım Örneği {#usage-example}

**Örnek 1**

Bu örnek, UUID türü sütunuyla bir tablo oluşturma ve tabloya bir değer ekleme gösterir.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Örnek 2**

Bu örnekte, yeni bir kayıt eklerken UUID sütun değeri belirtilmedi.

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Kısıtlama {#restrictions}

UUID veri türü sadece hangi fonksiyonları destekler [Dize](string.md) veri türü de destekler (örneğin, [dakika](../../sql_reference/aggregate_functions/reference.md#agg_function-min), [maksimum](../../sql_reference/aggregate_functions/reference.md#agg_function-max), ve [sayma](../../sql_reference/aggregate_functions/reference.md#agg_function-count)).

UUID veri türü aritmetik işlemler tarafından desteklenmez (örneğin, [abs](../../sql_reference/functions/arithmetic_functions.md#arithm_func-abs)) veya toplama fonksiyonları gibi [toplam](../../sql_reference/aggregate_functions/reference.md#agg_function-sum) ve [avg](../../sql_reference/aggregate_functions/reference.md#agg_function-avg).

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
