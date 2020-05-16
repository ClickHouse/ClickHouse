---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "\u0648\u0627\u0698\u0647\u0646\u0627\u0645\u0647"
---

# واژهنامه {#dictionary}

این `Dictionary` موتور نمایش [واژهنامه](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) داده ها به عنوان یک جدول کلیک.

به عنوان مثال, در نظر گرفتن یک فرهنگ لغت از `products` با پیکربندی زیر:

``` xml
<dictionaries>
<dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
</dictionary>
</dictionaries>
```

پرس و جو داده فرهنگ لغت:

``` sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

``` text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

شما می توانید از [دیکته کردن\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) تابع برای دریافت داده های فرهنگ لغت در این فرمت.

این دیدگاه مفید نیست که شما نیاز به دریافت داده های خام, و یا در هنگام انجام یک `JOIN` عمل برای این موارد می توانید از `Dictionary` موتور, که نمایش داده فرهنگ لغت در یک جدول.

نحو:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

مثال طریقه استفاده:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

نگاهی به در چه چیزی در جدول.

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/dictionary/) <!--hide-->
