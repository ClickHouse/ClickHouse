---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: "\u0644\u063A\u062A\u0646\u0627\u0645\u0647\u0647\u0627 \u0633\u0644\u0633\
  \u0644\u0647 \u0645\u0631\u0627\u062A\u0628\u06CC"
---

# لغتنامهها سلسله مراتبی {#hierarchical-dictionaries}

کلیک هاوس از لغت نامه های سلسله مراتبی با یک [کلید عددی](external_dicts_dict_structure.md#ext_dict-numeric-key).

در ساختار سلسله مراتبی زیر نگاه کنید:

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

این سلسله مراتب را می توان به عنوان جدول فرهنگ لغت زیر بیان شده است.

| \_ورود | \_ نواحی | نام \_خانوادگی |
|--------|----------|----------------|
| 1      | 0        | روسیه          |
| 2      | 1        | مسکو           |
| 3      | 2        | مرکز           |
| 4      | 0        | بریتانیا       |
| 5      | 4        | لندن           |

این جدول شامل یک ستون است `parent_region` که شامل کلید نزدیکترین پدر و مادر برای عنصر.

تاتر از [سلسله مراتبی](external_dicts_dict_structure.md#hierarchical-dict-attr) املاک برای [فرهنگ لغت خارجی](index.md) صفات. این ویژگی اجازه می دهد تا شما را به پیکربندی فرهنگ لغت سلسله مراتبی شبیه به بالا توضیح داده شد.

این [حکومت دیکتاتوری](../../../sql_reference/functions/ext_dict_functions.md#dictgethierarchy) تابع اجازه می دهد تا شما را به زنجیره پدر و مادر از یک عنصر.

برای مثال ما ساختار فرهنگ لغت می تواند به شرح زیر است:

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

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
