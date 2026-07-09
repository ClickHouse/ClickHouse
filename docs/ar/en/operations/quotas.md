---
description: 'دليل لتكوين حصص استخدام الموارد وإدارتها في ClickHouse'
sidebar_label: 'الحصص'
sidebar_position: 51
slug: /operations/quotas
title: 'الحصص'
doc_type: 'guide'
---

:::note الحصص في ClickHouse Cloud
الحصص مدعومة في ClickHouse Cloud، ولكن يجب إنشاؤها باستخدام [صيغة DDL](/ar/sql-reference/statements/create/quota). تكوين XML الموضّح أدناه **غير مدعوم**.
:::

تتيح لك الحصص تقييد استخدام الموارد خلال فترة زمنية أو تتبّع استهلاك الموارد.
تُضبط الحصص في إعدادات المستخدم، والتي تكون عادةً &#39;users.xml&#39;.

يتضمن النظام أيضًا ميزة لتقييد تعقيد استعلام واحد. راجع قسم [قيود تعقيد الاستعلام](../operations/settings/query-complexity.md).

وعلى عكس قيود تعقيد الاستعلام، فإن الحصص:

* تفرض قيودًا على مجموعة من الاستعلامات التي يمكن تشغيلها خلال فترة زمنية، بدلًا من تقييد استعلام واحد.
* تحتسب الموارد المستهلكة على جميع الخوادم البعيدة في معالجة الاستعلامات الموزعة.

لنلقِ نظرة على القسم من ملف &#39;users.xml&#39; الذي يعرّف الحصص.

```xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <query_selects>0</query_selects>
            <query_inserts>0</query_inserts>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

افتراضيًا، تتتبّع الحصة استهلاك الموارد كل ساعة، من دون فرض أي قيود على الاستخدام.
ويُسجَّل استهلاك الموارد المُحتسَب لكل فترة في سجل الخادم بعد كل طلب.

```xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <written_bytes>5000000</written_bytes>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
        <failed_sequential_authentications>5</failed_sequential_authentications>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <result_bytes>160000000000</result_bytes>
        <read_rows>500000000000</read_rows>
        <result_bytes>16000000000000</result_bytes>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

بالنسبة إلى حصة &#39;statbox&#39;، تُحدَّد القيود لكل ساعة ولكل 24 ساعة (86,400 ثانية). ويُحتسَب الفاصل الزمني بدءًا من نقطة زمنية ثابتة يحددها التنفيذ. وبعبارة أخرى، لا تبدأ فترة الـ24 ساعة بالضرورة عند منتصف الليل.

عند انتهاء الفاصل الزمني، تُصفَّر جميع القيم المجمَّعة. وفي الساعة التالية، يبدأ احتساب الحصة من جديد.

فيما يلي المقادير التي يمكن تقييدها:

`queries` – العدد الإجمالي للطلبات.

`query_selects` – العدد الإجمالي لطلبات SELECT.

`query_inserts` – العدد الإجمالي لطلبات INSERT.

`errors` – عدد الاستعلامات التي أثارت استثناءً.

`result_rows` – العدد الإجمالي للصفوف المُعادة كنتيجة.

`result_bytes` - الحجم الإجمالي للصفوف المُعادة كنتيجة.

`read_rows` – العدد الإجمالي لصفوف المصدر المقروءة من الجداول لتشغيل الاستعلام على جميع الخوادم البعيدة.

`read_bytes` - الحجم الإجمالي المقروء من الجداول لتشغيل الاستعلام على جميع الخوادم البعيدة.

`written_bytes` - الحجم الإجمالي لعمليات الكتابة.

`execution_time` – إجمالي وقت تنفيذ الاستعلام، بالثواني (الوقت الفعلي).

`failed_sequential_authentications` - العدد الإجمالي لأخطاء المصادقة المتتابعة.

`queries_per_normalized_hash` – الحد الأقصى لعدد مرات تنفيذ أي استعلام مُطبَّع واحد. والاستعلامات المُطبَّعة هي الاستعلامات التي تُستبدل فيها القيم الحرفية بعناصر نائبة، لذا يُعدّ `SELECT 1` و`SELECT 2` الاستعلام المُطبَّع نفسه. ويُتتبَّع هذا الحد بشكل مستقل لكل نمط استعلام مُطبَّع مميّز.

إذا جرى تجاوز الحد خلال فترة زمنية واحدة على الأقل، فسيتم طرح استثناء يتضمن نصًا يوضّح القيد الذي تم تجاوزه، ولأي فترة، ومتى تبدأ الفترة الجديدة (أي متى يمكن إرسال الاستعلامات مرة أخرى).

يمكن للحصص استخدام ميزة &quot;quota key&quot; للإبلاغ عن الموارد لعدة مفاتيح بشكل مستقل. وفيما يلي مثال على ذلك:

```xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)

        Instead of <keyed_by_ip /> you can use <keyed_by_forwarded_ip />, so the address
        from the X-Forwarded-For header is used as the quota key.

        For both <keyed_by_ip /> and <keyed_by_forwarded_ip /> you can additionally specify
        <ipv4_prefix_bits> and <ipv6_prefix_bits> to group clients by subnet instead of by a
        single address: the IP address is masked to the given prefix length before being used
        as the quota key. For example, <ipv4_prefix_bits>24</ipv4_prefix_bits> shares one bucket
        across a /24 IPv4 subnet, and <ipv6_prefix_bits>64</ipv6_prefix_bits> across a /64 IPv6
        subnet. These elements can only be used together with <keyed_by_ip /> or
        <keyed_by_forwarded_ip />.
    -->
    <keyed />
```

يمكنك أيضًا ربط quotas بتجزئة الاستعلام المُطبَّع، بحيث يحصل كل نمط استعلام مميّز على bucket quota مستقل. في تكوين XML، يُكتب ذلك على النحو التالي: `<keyed_by_normalized_query_hash />`:

```xml
<my_quota>
    <keyed_by_normalized_query_hash />
    <interval>
        <duration>3600</duration>
        <queries>100</queries>
    </interval>
</my_quota>
```

يمكن التعبير عن الأمر نفسه بصيغة DDL:

```sql
CREATE QUOTA my_quota KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO my_user;
```

في هذا المثال، يمكن للمستخدم تنفيذ ما يصل إلى 100 مرة من كل استعلام مُطبَّع مختلف كل ساعة. يشترك `SELECT number FROM numbers(1)` و `SELECT number FROM numbers(2)` في نفس الحاوية (لأن لهما الصيغة المُطبَّعة نفسها)، لكن `SELECT number, number FROM numbers(1)` يستخدم حاوية منفصلة.

تُسنَد الحصة إلى المستخدمين في القسم &#39;users&#39; من ملف الإعدادات. راجع قسم &quot;حقوق الوصول&quot;.

في معالجة الاستعلامات الموزعة، تُخزَّن القيم المتراكمة على الخادم المُرسِل للطلب. لذلك، إذا انتقل المستخدم إلى خادم آخر، فستبدأ الحصة هناك &quot;من الصفر&quot;.

عند إعادة تشغيل الخادم، تُصفَّر الحصص.

<div id="related-content">
  ## محتوى مرتبط
</div>

* مدونة: [بناء تطبيقات أحادية الصفحة باستخدام ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)