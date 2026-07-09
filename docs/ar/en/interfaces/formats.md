---
description: 'نظرة عامة على تنسيقات البيانات المدعومة لبيانات الإدخال والإخراج في ClickHouse'
sidebar_label: 'عرض جميع التنسيقات...'
sidebar_position: 21
slug: /interfaces/formats
title: 'تنسيقات بيانات الإدخال والإخراج'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="formats-for-input-and-output-data">
  # تنسيقات بيانات الإدخال والإخراج
</div>

يدعم ClickHouse معظم تنسيقات النصوص والبيانات الثنائية المعروفة. ويتيح ذلك تكاملًا سهلًا مع أي خط أنابيب بيانات مستخدم تقريبًا
للاستفادة من مزايا ClickHouse.

<div id="input-formats">
  ## تنسيقات الإدخال
</div>

تُستخدم تنسيقات الإدخال في:

* تحليل البيانات المُمرَّرة إلى عبارات `INSERT`
* تنفيذ استعلامات `SELECT` من الجداول المعتمدة على الملفات مثل `File` أو `URL` أو `HDFS`
* قراءة القواميس

يُعد اختيار تنسيق الإدخال المناسب أمرًا بالغ الأهمية لكفاءة إدخال البيانات في ClickHouse. ومع توفر أكثر من 70 تنسيقًا مدعومًا،
فإن اختيار الخيار الأعلى أداءً يمكن أن يؤثر بشكل كبير في سرعة الإدراج، واستخدام CPU والذاكرة، والكفاءة العامة
للنظام. ولمساعدتك على فهم هذه الخيارات، أجرينا اختبارًا معياريًا لأداء الإدخال عبر التنسيقات، وقد كشف ذلك عن أبرز النتائج التالية:

* **يُعد تنسيق [Native](formats/Native.md) أكثر تنسيقات الإدخال كفاءة**، إذ يوفّر أفضل ضغط، وأقل
  استهلاكًا للموارد، وأدنى حمل معالجة على جانب الخادم.
* **الضغط ضروري** - إذ يقلل LZ4 حجم البيانات مع تكلفة محدودة على CPU، بينما يوفّر ZSTD مستوى ضغط أعلى على
  حساب زيادة استخدام CPU.
* **للفرز المسبق تأثير متوسط**، لأن ClickHouse يفرز البيانات بكفاءة بالفعل.
* **يُحسّن التجميع على دفعات الكفاءة بشكل كبير** - إذ تقلل الدفعات الأكبر حمل الإدراج وتُحسّن معدل النقل.

للاطلاع المتعمق على النتائج وأفضل الممارسات،
اقرأ [تحليل الاختبار المعياري](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient) الكامل.
وللاطلاع على نتائج الاختبار كاملةً، استكشف لوحة المعلومات [FastFormats](https://fastformats.clickhouse.com/) عبر الإنترنت.

<div id="output-formats">
  ## تنسيقات الإخراج
</div>

تُستخدم تنسيقات الإخراج المدعومة في:

* ترتيب نتائج استعلام `SELECT`
* تنفيذ عمليات `INSERT` في الجداول المعتمدة على الملفات

<div id="formats-overview">
  ## لمحة عامة عن التنسيقات
</div>

التنسيقات المدعومة هي:

| التنسيق                                                                                                    | الإدخال | الإخراج |
| ---------------------------------------------------------------------------------------------------------- | ------- | ------- |
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔       | ✔       |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔       | ✔       |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔       | ✔       |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔       | ✔       |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔       | ✔       |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔       | ✔       |
| [Template](./formats/Template/Template.md)                                                                 | ✔       | ✔       |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔       | ✗       |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔       | ✔       |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔       | ✔       |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔       | ✔       |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔       | ✔       |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔       | ✔       |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔       | ✔       |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗       | ✔       |
| [Values](./formats/Values.md)                                                                              | ✔       | ✔       |
| [Vertical](./formats/Vertical.md)                                                                          | ✗       | ✔       |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔       | ✔       |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔       | ✗       |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔       | ✗       |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✗       | ✔       |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔       | ✔       |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔       | ✔       |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔       | ✔       |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗       | ✔       |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔       | ✔       |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔       | ✔       |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗       | ✔       |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗       | ✔       |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔       | ✔       |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗       | ✔       |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔       | ✔       |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔       | ✔       |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔       | ✔       |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗       | ✔       |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔       | ✔       |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔       | ✔       |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔       | ✔       |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗       | ✔       |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔       | ✔       |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔       | ✔       |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔       | ✔       |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗       | ✔       |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗       | ✔       |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗       | ✔       |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗       | ✔       |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗       | ✔       |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗       | ✔       |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗       | ✔       |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗       | ✔       |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗       | ✔       |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗       | ✔       |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗       | ✔       |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗       | ✔       |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗       | ✔       |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔       | ✔       |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔       | ✔       |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔       | ✔       |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔       | ✔       |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔       | ✔       |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔       | ✔       |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔       | ✗       |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔       | ✔       |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔       | ✔       |
| [ORC](./formats/ORC.md)                                                                                    | ✔       | ✔       |
| [One](./formats/One.md)                                                                                    | ✔       | ✗       |
| [Npy](./formats/Npy.md)                                                                                    | ✔       | ✔       |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔       | ✔       |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔       | ✔       |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔       | ✔       |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔       | ✗       |
| [RowBinaryWithNamesAndTypesAndDefaults](./formats/RowBinary/RowBinaryWithNamesAndTypesAndDefaults.md)      | ✔       | ✗       |
| [Native](./formats/Native.md)                                                                              | ✔       | ✔       |
| [Buffers](./formats/Buffers.md)                                                                            | ✔       | ✔       |
| [Null](./formats/Null.md)                                                                                  | ✗       | ✔       |
| [Hash](./formats/Hash.md)                                                                                  | ✗       | ✔       |
| [XML](./formats/XML.md)                                                                                    | ✗       | ✔       |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔       | ✔       |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔       | ✔       |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✗       | ✔       |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✗       | ✔       |
| [Regexp](./formats/Regexp.md)                                                                              | ✔       | ✗       |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔       | ✔       |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔       | ✔       |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔       | ✗       |
| [GeoJSON](./formats/GeoJSON.md)                                                                            | ✔       | ✔       |
| [DWARF](./formats/DWARF.md)                                                                                | ✔       | ✗       |
| [Markdown](./formats/Markdown.md)                                                                          | ✗       | ✔       |
| [Form](./formats/Form.md)                                                                                  | ✔       | ✗       |

يمكنك التحكّم في بعض مَعلمات معالجة التنسيقات باستخدام إعدادات ClickHouse. لمزيد من المعلومات، راجع قسم [الإعدادات](/ar/operations/settings/settings-formats.md).

<div id="formatschema">
  ## مخطط التنسيق
</div>

يُحدَّد اسم الملف الذي يحتوي على مخطط التنسيق بواسطة الإعداد `format_schema`.
ويجب تعيين هذا الإعداد عند استخدام أحد التنسيقين `Cap'n Proto` و`Protobuf`.
يتكوّن مخطط التنسيق من اسم ملف واسم نوع رسالة داخل هذا الملف، يفصل بينهما نقطتان رأسيتان،
على سبيل المثال: `schemafile.proto:MessageType`.
إذا كان الملف يحمل الامتداد القياسي للتنسيق (مثل `.proto` لـ `Protobuf`)،
فيمكن حذفه، وعندئذٍ يكون مخطط التنسيق بالشكل `schemafile:MessageType`.

إذا كنت تُدخل البيانات أو تُخرجها عبر [العميل](/ar/interfaces/client.md) في الوضع التفاعلي، فإن اسم الملف المحدد في مخطط التنسيق
يمكن أن يتضمن مسارًا مطلقًا أو مسارًا نسبيًا إلى الدليل الحالي على العميل.
أما إذا كنت تستخدم العميل في [وضع الدُفعات](/ar/interfaces/client.md/#batch-mode)، فيجب أن يكون مسار المخطط نسبيًا لأسباب أمنية.

إذا كنت تُدخل البيانات أو تُخرجها عبر [واجهة HTTP](/ar/interfaces/http)، فإن اسم الملف المحدد في مخطط التنسيق
يجب أن يكون موجودًا في الدليل المحدد في [format&#95;schema&#95;path](/ar/operations/server-configuration-parameters/settings.md/#format_schema_path)
ضمن إعدادات الخادم.

<div id="skippingerrors">
  ## تخطي الأخطاء
</div>

يمكن لبعض التنسيقات مثل `CSV` و`TabSeparated` و`TSKV` و`JSONEachRow` و`Template` و`CustomSeparated` و`Protobuf` تخطي الصف التالف إذا حدث خطأ في التحليل، ومتابعة التحليل من بداية الصف التالي. راجع إعدادَي [input&#95;format&#95;allow&#95;errors&#95;num](/ar/operations/settings/settings-formats.md/#input_format_allow_errors_num) و
[input&#95;format&#95;allow&#95;errors&#95;ratio](/ar/operations/settings/settings-formats.md/#input_format_allow_errors_ratio).
القيود:

* عند حدوث خطأ في التحليل، يتخطى `JSONEachRow` جميع البيانات حتى السطر الجديد (أو EOF)، لذا يجب فصل الصفوف باستخدام `\n` لاحتساب الأخطاء بشكل صحيح.
* يستخدم `Template` و`CustomSeparated` المحدِّد بعد العمود الأخير والمحدِّد بين الصفوف لتحديد بداية الصف التالي، لذلك لا يعمل تخطي الأخطاء إلا إذا كان واحد منهما على الأقل غير فارغ.