---
description: 'توثيق ODBC Bridge'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'مرجع'
---

خادم HTTP بسيط يعمل كوسيط لبرنامج تشغيل ODBC. كان الدافع الرئيسي
هو احتمال حدوث أخطاء segfault أو أعطال أخرى في تطبيقات ODBC، مما قد
يؤدي إلى تعطل عملية clickhouse-server بالكامل.

تعمل هذه الأداة عبر HTTP، وليس عبر الأنابيب أو الذاكرة المشتركة أو TCP، لأن:

* تنفيذها أسهل
* تصحيحها أسهل
* يمكن تنفيذ jdbc-bridge بالطريقة نفسها

<div id="usage">
  ## الاستخدام
</div>

يستخدم `clickhouse-server` هذه الأداة داخل table function‏ odbc وStorageODBC.
ومع ذلك، يمكن استخدامها كأداة مستقلة من سطر الأوامر باستخدام
المعلمات التالية في URL الخاص بطلب POST:

* `connection_string` -- سلسلة اتصال ODBC.
* `sample_block` -- وصف الأعمدة بتنسيق ClickHouse NamesAndTypesList، مع وضع الاسم بين backticks،
  وكتابة النوع كسلسلة نصية. يُفصل بين الاسم والنوع بمسافة، وتُفصل الصفوف
  بأسطر جديدة.
* `max_block_size` -- معلمة اختيارية، تحدد الحجم الأقصى للكتلة الواحدة.
  يُرسَل الاستعلام في جسم طلب POST. وتُعاد الاستجابة بتنسيق RowBinary format.

<div id="example">
  ## مثال:
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```