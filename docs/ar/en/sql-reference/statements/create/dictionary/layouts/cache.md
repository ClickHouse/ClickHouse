---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'تخطيط القاموس cache'
sidebar_label: 'cache'
sidebar_position: 6
description: 'خزّن قاموسًا في cache داخل الذاكرة بحجم ثابت.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

نوع تخطيط القاموس `cached` يخزّن القاموس في cache ذات عدد ثابت من الخلايا.
وتحتوي هذه الخلايا على العناصر كثيرة الاستخدام.

يكون مفتاح القاموس من النوع [UInt64](/ar/sql-reference/data-types/int-uint.md).

عند البحث في القاموس، يُبحث أولًا في cache. ولكل block من البيانات، تُطلَب من source جميع المفاتيح التي لم يُعثر عليها في cache أو التي أصبحت قديمة باستخدام `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. ثم تُكتَب البيانات المستلمة في cache.

إذا لم يُعثر على المفاتيح في القاموس، فسيُنشأ task لتحديث cache ويُضاف إلى update queue. ويمكن التحكم في properties الخاصة بـ update queue باستخدام settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

بالنسبة إلى قواميس cache، يمكن تعيين [lifetime](../lifetime.md) لانتهاء صلاحية البيانات في cache. وإذا انقضى وقت أطول من `lifetime` منذ loading البيانات في خلية ما، فلن تُستخدم قيمة الخلية ويصبح المفتاح منتهي الصلاحية. ويُعاد طلب المفتاح في المرة التالية التي يلزم استخدامه فيها. ويمكن ضبط هذا السلوك باستخدام setting `allow_read_expired_keys`.

هذه أقل الطرق فعاليةً بين جميع طرق تخزين القواميس. وتعتمد سرعة cache بدرجة كبيرة على الضبط الصحيح للإعدادات وعلى سيناريو الاستخدام. ولا يحقق القاموس من النوع cache أداءً جيدًا إلا عندما تكون معدلات الإصابة مرتفعة بما يكفي (الموصى به 99% فأكثر). ويمكنك عرض متوسط معدل الإصابة في table ‏[system.dictionaries](/ar/operations/system-tables/dictionaries.md).

إذا كانت setting ‏`allow_read_expired_keys` مضبوطة على 1، بينما تكون القيمة الافتراضية 0، فيمكن للقاموس عندئذٍ دعم التحديثات غير المتزامنة. وإذا طلب client مفاتيح وكانت كلها موجودة في cache، لكن بعضها منتهي الصلاحية، فسيُرجع القاموس المفاتيح منتهية الصلاحية إلى client ويطلبها بشكل غير متزامن من source.

لتحسين أداء cache، استخدم subquery مع `LIMIT`، واستدعِ function مع القاموس خارجيًا.

جميع أنواع المصادر مدعومة.

مثال على الإعدادات:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="ملف الإعدادات">
    ```xml
    <layout>
        <cache>
            <!-- حجم cache، بعدد الخلايا. يُقرَّب إلى أعلى إلى أقرب قوة للعدد اثنين. -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- يسمح بقراءة المفاتيح منتهية الصلاحية. -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- الحد الأقصى لحجم update queue. -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- الحد الأقصى للمهلة بالميلي ثانية لدفع task التحديث إلى queue. -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- الحد الأقصى لمهلة الانتظار بالميلي ثانية لاكتمال task التحديث. -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- الحد الأقصى لـ threads الخاصة بتحديث قاموس cache. -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

عيّن حجم cache كبيرًا بما يكفي. وستحتاج إلى التجربة لاختيار عدد الخلايا:

1. عيّن قيمة ما.
2. شغّل queries حتى تمتلئ cache بالكامل.
3. قيّم استهلاك الذاكرة باستخدام table ‏`system.dictionaries`.
4. زِد عدد الخلايا أو قلّله حتى تصل إلى استهلاك الذاكرة المطلوب.

:::note
لا يُوصى باستخدام ClickHouse كمصدر لهذا التخطيط. تتطلب عمليات lookup في القاموس قراءات نقطية عشوائية، وهذا ليس access pattern الذي جرى تحسين ClickHouse من أجله.
:::